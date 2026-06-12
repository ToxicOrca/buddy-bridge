package com.claudebuddy.bridge.hub

import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.json.JSONArray
import org.json.JSONObject
import java.util.Calendar

/**
 * Session state aggregator and heartbeat builder.
 * Direct port of buddyhub.py Hub class.
 */
class Hub(private val onHeartbeat: (JSONObject) -> Unit) {

    companion object {
        const val KEEPALIVE_ACTIVE_SEC = 10L
        const val KEEPALIVE_IDLE_SEC = 20L
        const val KEEPALIVE_FLOOR_SEC = 20L  // always retransmit after this, even if unchanged
        const val DECISION_TIMEOUT_SEC = 30L
        const val STALE_SESSION_SEC = 300L
        const val STALE_RUNNING_SEC = 1800L  // running sessions get a longer leash (30 min)
        const val PROMPT_TTL_SEC = 90L
    }

    private val lock = Mutex()

    // Daily token tracking: accumulates tokens across all sessions,
    // resets at midnight. Each session's last-seen token count is tracked
    // so we only add the delta when a session reports a new total.
    private var tokensToday: Long = 0
    private var tokenDay: Int = Calendar.getInstance().get(Calendar.DAY_OF_YEAR)
    private val sessionTokenSnapshot = mutableMapOf<Pair<String, String>, Int>()

    // sessions: key = Pair(machine, session) -> mutable state map
    private val sessions = mutableMapOf<Pair<String, String>, MutableMap<String, Any>>()
    private val entries = ArrayDeque<String>()  // recent activity, newest first, max 8
    private val pending = ArrayDeque<Pending>() // FIFO prompt queue
    private val byId = mutableMapOf<String, Pending>()
    private var current: Pending? = null

    // Channel with CONFLATED buffer: tryEmit never blocks, receiver gets
    // "something changed" without queuing every individual event.
    private val dirty = Channel<Unit>(Channel.CONFLATED)

    private fun nowMs(): Long = System.nanoTime() / 1_000_000

    private fun newSession(): MutableMap<String, Any> =
        mutableMapOf("status" to "idle", "msg" to "", "ts" to 0L, "tokens" to 0)

    // ---- session events -------------------------------------------------- //

    suspend fun event(machine: String, session: String, kind: String, msg: String?, tokens: Int?) {
        val key = machine to session
        lock.withLock {
            when (kind) {
                "session_start" -> {
                    sessions.getOrPut(key) { newSession() }
                }
                "session_end" -> {
                    sessions.remove(key)
                    sessionTokenSnapshot.remove(key)
                    // Purge entries from this machine
                    val prefix = "$machine >"
                    val filtered = entries.filter { !it.startsWith(prefix) }
                    entries.clear()
                    filtered.forEach { entries.addLast(it) }
                }
                else -> {
                    val s = sessions.getOrPut(key) { newSession() }
                    when (kind) {
                        "running" -> s["status"] = "running"
                        "tool_done" -> s["status"] = "running"
                        "idle" -> s["status"] = "idle"
                    }
                    if (!msg.isNullOrEmpty()) {
                        s["msg"] = msg
                        entries.addFirst("$machine > $msg")
                        while (entries.size > 8) entries.removeLast()
                    }
                    if (tokens != null) {
                        s["tokens"] = tokens
                        // Accumulate delta into daily total
                        val prev = sessionTokenSnapshot[key] ?: 0
                        if (tokens > prev) {
                            tokensToday += (tokens - prev)
                        }
                        sessionTokenSnapshot[key] = tokens
                    }
                }
            }
            val s = sessions[key]
            if (s != null) s["ts"] = nowMs()
        }
        dirty.trySend(Unit)
    }

    // ---- permission control ------------------------------------------------ //

    suspend fun registerPermission(machine: String, session: String, tool: String, hint: String): String {
        val p = Pending(machine, session, tool, hint)
        lock.withLock {
            byId[p.id] = p
            pending.addLast(p)
            val s = sessions.getOrPut(machine to session) {
                mutableMapOf("status" to "idle", "msg" to "", "ts" to nowMs(), "tokens" to 0)
            }
            s["status"] = "waiting"
        }
        dirty.trySend(Unit)
        return p.id
    }

    suspend fun awaitDecision(pid: String, timeoutSec: Long = DECISION_TIMEOUT_SEC): String {
        val p = lock.withLock { byId[pid] } ?: return "timeout"
        val decided = withTimeoutOrNull(timeoutSec * 1000) {
            p.event.await()
            true
        } ?: false
        lock.withLock {
            byId.remove(pid)
            pending.remove(p)
            if (current === p) current = null
            val s = sessions[p.machine to p.session]
            if (s != null && s["status"] == "waiting") s["status"] = "idle"
        }
        dirty.trySend(Unit)
        return if (decided && p.decision != null) p.decision!! else "timeout"
    }

    suspend fun resolve(pid: String, decision: String): Boolean {
        lock.withLock {
            val p = byId[pid] ?: return false
            p.decision = decision
            pending.remove(p)
            if (current === p) current = null
            val s = sessions[p.machine to p.session]
            if (s != null && s["status"] == "waiting") s["status"] = "idle"
            p.event.complete(Unit)
        }
        dirty.trySend(Unit)
        return true
    }

    suspend fun resolveCurrent(decision: String): Boolean {
        val p = lock.withLock {
            current ?: pending.firstOrNull()
        } ?: return false
        return resolve(p.id, decision)
    }

    // ---- heartbeat --------------------------------------------------------- //

    suspend fun buildHeartbeat(): JSONObject = lock.withLock {
        val now = nowMs()

        // Reap stale sessions (running sessions get a longer timeout)
        val reaped = mutableSetOf<String>()
        val staleKeys = sessions.filter { (_, s) ->
            val limit = if (s["status"] == "running") STALE_RUNNING_SEC else STALE_SESSION_SEC
            now - (s["ts"] as Long) > limit * 1000
        }.keys.toList()
        for (k in staleKeys) {
            reaped.add(k.first)
            sessions.remove(k)
            sessionTokenSnapshot.remove(k)
        }
        if (reaped.isNotEmpty()) {
            val filtered = entries.filter { e ->
                reaped.none { m -> e.startsWith("$m >") }
            }
            entries.clear()
            filtered.forEach { entries.addLast(it) }
        }

        // Reap orphaned prompts
        val stalePrompts = byId.filter { (_, p) ->
            now - p.createdMs > PROMPT_TTL_SEC * 1000
        }.keys.toList()
        for (pid in stalePrompts) {
            val p = byId.remove(pid)
            if (p != null) {
                pending.remove(p)
                p.event.complete(Unit)
            }
        }

        // Pick current prompt (FIFO)
        current = pending.firstOrNull()

        val total = sessions.size
        val running = sessions.values.count { it["status"] == "running" }
        val waiting = pending.size
        val tokens = sessions.values.sumOf { (it["tokens"] as? Int) ?: 0 }

        // Reset daily counter at midnight
        val today = Calendar.getInstance().get(Calendar.DAY_OF_YEAR)
        if (today != tokenDay) {
            tokensToday = 0
            sessionTokenSnapshot.clear()
            tokenDay = today
        }

        val hb = JSONObject().apply {
            put("total", total)
            put("running", running)
            put("waiting", waiting)
            put("tokens", tokens)
            put("tokens_today", tokensToday)
            put("entries", JSONArray(entries.toList()))
        }

        val cur = current
        if (cur != null) {
            hb.put("msg", "approve: ${cur.tool}")
            hb.put("prompt", JSONObject().apply {
                put("id", cur.id)
                put("tool", cur.tool)
                put("hint", cur.hint)
            })
        } else if (running > 0) {
            val msg = sessions.values
                .firstOrNull { it["status"] == "running" && (it["msg"] as String).isNotEmpty() }
                ?.get("msg") as? String ?: "working"
            hb.put("msg", msg)
        } else if (total > 0) {
            hb.put("msg", "idle")
        } else {
            hb.put("msg", "")
        }

        hb
    }

    // ---- driver loop ------------------------------------------------------- //

    fun startHeartbeatLoop(scope: CoroutineScope): Job = scope.launch {
        var lastSendMs = 0L
        while (isActive) {
            try {
                // Wait for dirty signal or 1s timeout (mirrors Python's _dirty.wait(timeout=1.0))
                val fired = withTimeoutOrNull(1000) {
                    dirty.receive()
                    true
                } ?: false

                val now = nowMs()
                val active = lock.withLock { sessions.isNotEmpty() }
                val intervalMs = (if (active) KEEPALIVE_ACTIVE_SEC else KEEPALIVE_IDLE_SEC) * 1000
                if (fired || (now - lastSendMs) >= intervalMs) {
                    val hb = buildHeartbeat()
                    onHeartbeat(hb)
                    lastSendMs = now
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Log.w("Hub", "heartbeat loop error (continuing): ${e.message}")
            }
        }
    }

    suspend fun sessionCount(): Int = lock.withLock { sessions.size }
}
