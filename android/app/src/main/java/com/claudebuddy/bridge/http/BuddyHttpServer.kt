package com.claudebuddy.bridge.http

import com.claudebuddy.bridge.hub.Hub
import fi.iki.elonen.NanoHTTPD
import kotlinx.coroutines.runBlocking
import org.json.JSONObject
import java.security.MessageDigest

/**
 * Embedded HTTP server for Claude Code hooks.
 * Port of buddyhub.py HTTP handler.
 */
class BuddyHttpServer(
    private val hub: Hub,
    port: Int = 8787
) : NanoHTTPD("0.0.0.0", port) {

    /** Shared token for authenticating hook requests. Empty = auth disabled. */
    var token: String = ""

    private fun authed(session: IHTTPSession): Boolean {
        if (token.isEmpty()) return true  // no token configured = open (local-only use)
        val provided = session.headers["x-buddy-token"] ?: return false
        return MessageDigest.isEqual(provided.toByteArray(), token.toByteArray())
    }

    override fun serve(session: IHTTPSession): Response {
        return try {
            when (session.method) {
                Method.POST -> handlePost(session)
                Method.GET -> handleGet(session)
                else -> jsonResponse(Response.Status.METHOD_NOT_ALLOWED, """{"ok":false,"error":"method not allowed"}""")
            }
        } catch (e: Exception) {
            val safeMsg = (e.message ?: "internal error").replace("\"", "\\\"").replace("\n", " ")
            jsonResponse(Response.Status.INTERNAL_ERROR, """{"ok":false,"error":"$safeMsg"}""")
        }
    }

    private fun handlePost(session: IHTTPSession): Response {
        if (!authed(session)) {
            return jsonResponse(Response.Status.UNAUTHORIZED, """{"ok":false,"error":"unauthorized"}""")
        }
        val body = readBody(session)
        return when (session.uri) {
            "/event" -> {
                val data = JSONObject(body)
                runBlocking {
                    hub.event(
                        machine = data.optString("machine", "?"),
                        session = data.optString("session", "?"),
                        kind = data.optString("kind", "idle"),
                        msg = data.optString("msg", null),
                        tokens = if (data.has("tokens")) data.optInt("tokens") else null
                    )
                }
                jsonResponse(Response.Status.OK, """{"ok":true}""")
            }
            "/permission" -> {
                val data = JSONObject(body)
                val pid = runBlocking {
                    hub.registerPermission(
                        machine = data.optString("machine", "?"),
                        session = data.optString("session", "?"),
                        tool = data.optString("tool", "?"),
                        hint = data.optString("hint", "")
                    )
                }
                jsonResponse(Response.Status.OK, """{"id":"$pid"}""")
            }
            "/button" -> {
                val data = JSONObject(body)
                val ok = runBlocking {
                    hub.resolveCurrent(data.optString("decision", "once"))
                }
                jsonResponse(Response.Status.OK, """{"ok":$ok}""")
            }
            else -> jsonResponse(Response.Status.NOT_FOUND, """{"ok":false,"error":"no route"}""")
        }
    }

    private fun handleGet(session: IHTTPSession): Response {
        return when (session.uri) {
            "/decision" -> {
                val params = session.parms ?: emptyMap()
                val pid = params["id"] ?: ""
                // Cap wait time to prevent threads from blocking indefinitely.
                // Claude Code polls back if it gets a timeout, so short waits are fine.
                val wait = (params["wait"]?.toLongOrNull() ?: Hub.DECISION_TIMEOUT_SEC)
                    .coerceAtMost(Hub.DECISION_TIMEOUT_SEC)
                val decision = runBlocking {
                    hub.awaitDecision(pid, wait)
                }
                jsonResponse(Response.Status.OK, """{"decision":"$decision"}""")
            }
            "/state" -> {
                val hb = runBlocking { hub.buildHeartbeat() }
                jsonResponse(Response.Status.OK, hb.toString())
            }
            else -> jsonResponse(Response.Status.NOT_FOUND, """{"ok":false}""")
        }
    }

    private fun readBody(session: IHTTPSession): String {
        val contentLength = session.headers["content-length"]?.toIntOrNull() ?: 0
        if (contentLength == 0) return "{}"
        val buf = ByteArray(contentLength)
        session.inputStream.read(buf, 0, contentLength)
        return String(buf, Charsets.UTF_8)
    }

    private fun jsonResponse(status: Response.IStatus, json: String): Response {
        return newFixedLengthResponse(status, "application/json", json)
    }
}
