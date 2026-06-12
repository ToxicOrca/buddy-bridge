package com.claudebuddy.bridge.service

import android.Manifest
import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.util.Log
import com.claudebuddy.bridge.BuddyApp
import com.claudebuddy.bridge.MainActivity
import com.claudebuddy.bridge.ble.*
import com.claudebuddy.bridge.http.BuddyHttpServer
import com.claudebuddy.bridge.hub.Hub
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.json.JSONObject

class BuddyService : Service() {
    companion object {
        private const val TAG = "BuddyService"
        private const val NOTIF_ID = 1
        private const val LOW_BATTERY_PCT = 10
        private const val BATTERY_NOTIF_ID = 2
    }

    inner class LocalBinder : Binder() {
        val service: BuddyService get() = this@BuddyService
    }

    private val binder = LocalBinder()
    private val exceptionHandler = CoroutineExceptionHandler { _, throwable ->
        Log.e(TAG, "coroutine crash (service stays alive): ${throwable.message}")
    }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + exceptionHandler)

    private var hub: Hub? = null
    private var bleManager: BleManager? = null
    private var httpServer: BuddyHttpServer? = null
    private val dedup = HeartbeatDedup()

    // Owner name — set from UI settings
    var ownerName: String = ""

    // Shared token for authenticating hook requests
    var buddyToken: String = ""
        set(value) {
            field = value
            httpServer?.token = value
        }

    // "serve_hub" (embedded hub + BLE) or "relay" (BLE + outbound RelayClient)
    var mode: String = "serve_hub"
    var remoteHubUrl: String = ""
    private var relayClient: com.claudebuddy.bridge.relay.RelayClient? = null

    private val _httpRunning = MutableStateFlow(false)
    val httpRunning: StateFlow<Boolean> = _httpRunning

    private val _deviceBattery = MutableStateFlow(-1)  // -1 = unknown
    val deviceBattery: StateFlow<Int> = _deviceBattery
    private val _deviceCharging = MutableStateFlow(false)
    val deviceCharging: StateFlow<Boolean> = _deviceCharging
    private var lowBatteryNotified = false

    val bleState get() = bleManager?.state
    val bleDeviceName get() = bleManager?.deviceName

    override fun onBind(intent: Intent?): IBinder = binder

    @SuppressLint("ForegroundServiceType")
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        // On API 34+ startForeground requires BLUETOOTH_CONNECT at call time.
        // If the service was restarted by START_STICKY without an activity to
        // prompt, the permission may not be granted — bail quietly instead of
        // crashing in a loop.
        if (Build.VERSION.SDK_INT >= 31 &&
            checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED
        ) {
            Log.w(TAG, "BLUETOOTH_CONNECT not granted, deferring service start")
            stopSelf()
            return START_NOT_STICKY
        }

        // Build foreground notification
        val pendingIntent = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE
        )

        val notification = Notification.Builder(this, BuddyApp.CHANNEL_ID)
            .setContentTitle("Buddy Bridge")
            .setContentText("Relaying to Claude Buddy")
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .build()

        try {
            if (Build.VERSION.SDK_INT >= 34) {
                startForeground(NOTIF_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_CONNECTED_DEVICE)
            } else {
                startForeground(NOTIF_ID, notification)
            }
        } catch (e: Exception) {
            Log.e(TAG, "startForeground failed: ${e.message}")
            stopSelf()
            return START_NOT_STICKY
        }

        startBridge()
        return START_STICKY
    }

    private fun startBridge() {
        if (bleManager != null) return  // already running

        // Create BLE manager — incoming lines resolve prompts
        val ble = BleManager(
            context = this,
            namePrefix = "Claude",
            onLine = { line -> handleDeviceLine(line) }
        )
        ble.onConnected = { onBleConnected() }
        bleManager = ble

        if (mode == "relay") {
            val rc = com.claudebuddy.bridge.relay.RelayClient(
                hubUrl = remoteHubUrl, token = buddyToken,
                onLine = { line -> bleManager?.sendJson(line) })
            relayClient = rc
            ble.start(scope)
            rc.start(scope)
            Log.i(TAG, "bridge started (relay -> $remoteHubUrl)")
            return
        }

        // Create Hub — heartbeats go to BLE
        val h = Hub { hb -> sendHeartbeat(hb) }
        hub = h

        // Create HTTP server
        val http = BuddyHttpServer(h, 8787)
        http.token = buddyToken  // apply token configured before bridge started
        httpServer = http

        // Start everything
        try {
            http.start()
            _httpRunning.value = true
            Log.i(TAG, "HTTP server listening on 0.0.0.0:8787")
        } catch (e: Exception) {
            Log.e(TAG, "HTTP server failed: ${e.message}")
        }

        ble.start(scope)
        h.startHeartbeatLoop(scope)

        Log.i(TAG, "bridge started (serve_hub)")
    }

    private fun sendHeartbeat(hb: JSONObject) {
        try {
            if (!dedup.isDifferent(hb)) {
                Log.d(TAG, "heartbeat deduped, skipping")
                return
            }
            val ble = bleManager
            if (ble == null) {
                dedup.reset()  // don't let dedup cache a heartbeat that was never sent
                return
            }
            Log.i(TAG, "sending heartbeat: ${hb.toString().take(100)}")
            ble.sendJson(hb.toString())
        } catch (e: Exception) {
            Log.e(TAG, "sendHeartbeat error (heartbeat loop stays alive): ${e.message}")
            dedup.reset()  // allow retry on next cycle
        }
    }

    private fun onBleConnected() {
        // Connection handshake: time sync → owner → initial heartbeat
        val ble = bleManager ?: return
        scope.launch {
            ble.sendJson(buildTimeSync())
            delay(50)
            if (ownerName.isNotEmpty()) {
                ble.sendJson(buildOwnerCmd(ownerName))
                delay(50)
            }
            val hb = hub?.buildHeartbeat()
            if (hb != null) ble.sendJson(hb.toString())
        }
    }

    private fun handleDeviceLine(line: String) {
        try {
            val json = JSONObject(line)
            if (json.has("battery")) {
                val pct = json.optInt("battery", -1)
                val charging = json.optBoolean("charging", false)
                _deviceBattery.value = pct
                _deviceCharging.value = charging
                if (pct in 0..LOW_BATTERY_PCT && !charging && !lowBatteryNotified) {
                    lowBatteryNotified = true
                    sendLowBatteryNotification(pct)
                } else if (pct > LOW_BATTERY_PCT || charging) {
                    lowBatteryNotified = false
                }
                return
            }
            if (json.optString("cmd") == "permission") {
                val pid = json.optString("id", "")
                val decision = json.optString("decision", "deny")
                val rc = relayClient
                if (rc != null) {
                    scope.launch(Dispatchers.IO) { rc.postButton(pid, decision) }
                } else {
                    scope.launch {
                        val ok = hub?.resolve(pid, decision) ?: false
                        Log.i(TAG, "device decision $decision for $pid -> ${if (ok) "ok" else "stale"}")
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "bad device line: $line")
        }
    }

    private fun sendLowBatteryNotification(pct: Int) {
        val intent = PendingIntent.getActivity(
            this, 0, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE
        )
        val notification = Notification.Builder(this, BuddyApp.CHANNEL_ID)
            .setContentTitle("Buddy Battery Low")
            .setContentText("Claude Buddy is at $pct% battery")
            .setSmallIcon(android.R.drawable.stat_notify_error)
            .setContentIntent(intent)
            .setAutoCancel(true)
            .build()
        val nm = getSystemService(NotificationManager::class.java)
        nm.notify(BATTERY_NOTIF_ID, notification)
        Log.i(TAG, "low battery notification: $pct%")
    }

    override fun onDestroy() {
        relayClient?.stop(); relayClient = null
        bleManager?.stop()
        httpServer?.stop()
        _httpRunning.value = false
        scope.cancel()
        hub = null
        bleManager = null
        httpServer = null
        Log.i(TAG, "bridge stopped")
        super.onDestroy()
    }
}
