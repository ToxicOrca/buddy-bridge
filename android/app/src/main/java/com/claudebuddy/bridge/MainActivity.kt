package com.claudebuddy.bridge

import android.Manifest
import android.annotation.SuppressLint
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.IBinder
import android.os.PowerManager
import android.provider.Settings
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.runtime.*
import androidx.core.content.ContextCompat
import com.claudebuddy.bridge.ble.BleState
import com.claudebuddy.bridge.data.SettingsRepository
import com.claudebuddy.bridge.service.BuddyService
import com.claudebuddy.bridge.ui.BridgeScreen
import com.claudebuddy.bridge.ui.theme.BuddyBridgeTheme
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val serviceRef = mutableStateOf<BuddyService?>(null)
    private val isRunning = mutableStateOf(false)
    private var bindRequested = false  // true after bindService returns true

    private lateinit var settings: SettingsRepository

    private val connection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName, binder: IBinder) {
            val svc = (binder as BuddyService.LocalBinder).service
            serviceRef.value = svc
            isRunning.value = true
            // Push persisted settings onto the service
            CoroutineScope(Dispatchers.Main).launch {
                svc.ownerName = settings.ownerName.first()
                svc.buddyToken = settings.buddyToken.first()
                svc.mode = settings.mode.first()
                svc.remoteHubUrl = settings.remoteHubUrl.first()
            }
            Log.i("MainActivity", "service bound")
        }
        override fun onServiceDisconnected(name: ComponentName) {
            // Only called when service process dies unexpectedly
            serviceRef.value = null
            isRunning.value = false
        }
    }

    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { results ->
        Log.i("MainActivity", "permissions: $results")
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        settings = SettingsRepository(applicationContext)
        requestPermissions()
        requestBatteryOptExemption()

        setContent {
            BuddyBridgeTheme {
                val svc by serviceRef
                val running by isRunning

                // Stable state holders — avoid conditional collectAsState which
                // can destabilize Compose's call tree when svc flips null/non-null
                var bleState by remember { mutableStateOf(BleState.DISCONNECTED) }
                var bleDeviceName by remember { mutableStateOf<String?>(null) }
                var httpRunning by remember { mutableStateOf(false) }
                var deviceBattery by remember { mutableStateOf(-1) }
                var deviceCharging by remember { mutableStateOf(false) }

                LaunchedEffect(svc) {
                    val s = svc
                    if (s != null) {
                        launch { s.bleState?.collect { bleState = it } }
                        launch { s.bleDeviceName?.collect { bleDeviceName = it } }
                        launch { s.httpRunning.collect { httpRunning = it } }
                        launch { s.deviceBattery.collect { deviceBattery = it } }
                        launch { s.deviceCharging.collect { deviceCharging = it } }
                    } else {
                        bleState = BleState.DISCONNECTED
                        bleDeviceName = null
                        httpRunning = false
                        deviceBattery = -1
                        deviceCharging = false
                    }
                }

                // Local state for text fields (instant updates); DataStore
                // provides the initial value and persists in the background.
                val savedOwner by settings.ownerName.collectAsState(initial = "")
                val savedToken by settings.buddyToken.collectAsState(initial = "")
                val savedMode by settings.mode.collectAsState(initial = "serve_hub")
                val savedRemoteHubUrl by settings.remoteHubUrl.collectAsState(initial = "")
                var ownerName by remember { mutableStateOf("") }
                var buddyToken by remember { mutableStateOf("") }
                var mode by remember { mutableStateOf("serve_hub") }
                var remoteHubUrl by remember { mutableStateOf("") }
                val scope = rememberCoroutineScope()

                // Seed local state from DataStore once loaded
                LaunchedEffect(savedOwner) {
                    if (ownerName.isEmpty() && savedOwner.isNotEmpty()) ownerName = savedOwner
                }
                LaunchedEffect(savedToken) {
                    if (buddyToken.isEmpty() && savedToken.isNotEmpty()) buddyToken = savedToken
                }
                LaunchedEffect(savedMode) {
                    mode = savedMode
                }
                LaunchedEffect(savedRemoteHubUrl) {
                    if (remoteHubUrl.isEmpty() && savedRemoteHubUrl.isNotEmpty()) remoteHubUrl = savedRemoteHubUrl
                }

                BridgeScreen(
                    isRunning = running,
                    bleState = bleState,
                    bleDeviceName = bleDeviceName,
                    httpRunning = httpRunning,
                    deviceBattery = deviceBattery,
                    deviceCharging = deviceCharging,
                    ownerName = ownerName,
                    onOwnerNameChange = { name ->
                        ownerName = name
                        svc?.ownerName = name
                        scope.launch { settings.setOwnerName(name) }
                    },
                    buddyToken = buddyToken,
                    onBuddyTokenChange = { token ->
                        buddyToken = token
                        svc?.buddyToken = token
                        scope.launch { settings.setBuddyToken(token) }
                    },
                    mode = mode,
                    onModeChange = { m ->
                        mode = m
                        svc?.mode = m
                        scope.launch { settings.setMode(m) }
                    },
                    remoteHubUrl = remoteHubUrl,
                    onRemoteHubUrlChange = { url ->
                        remoteHubUrl = url
                        svc?.remoteHubUrl = url
                        scope.launch { settings.setRemoteHubUrl(url) }
                    },
                    onToggle = {
                        if (running) {
                            if (bindRequested) {
                                try { unbindService(connection) } catch (_: Exception) {}
                                bindRequested = false
                            }
                            stopService(Intent(this, BuddyService::class.java))
                            serviceRef.value = null
                            isRunning.value = false
                        } else {
                            try {
                                val intent = Intent(this, BuddyService::class.java)
                                ContextCompat.startForegroundService(this, intent)
                                if (bindService(intent, connection, Context.BIND_AUTO_CREATE)) {
                                    bindRequested = true
                                }
                            } catch (e: Exception) {
                                Log.e("MainActivity", "failed to start service", e)
                            }
                        }
                    }
                )
            }
        }
    }

    override fun onStart() {
        super.onStart()
        // Rebind to the service if it's still running (activity was recreated)
        if (!bindRequested) {
            val intent = Intent(this, BuddyService::class.java)
            val ok = try {
                bindService(intent, connection, 0)  // don't auto-create, just attach if running
            } catch (e: Exception) {
                Log.d("MainActivity", "rebind failed: ${e.message}")
                false
            }
            if (ok) {
                bindRequested = true
            } else {
                // Service is not running — clear any stale state
                serviceRef.value = null
                isRunning.value = false
            }
        }
    }

    override fun onStop() {
        if (bindRequested) {
            try { unbindService(connection) } catch (_: Exception) {}
            bindRequested = false
        }
        super.onStop()
    }

    private fun requestPermissions() {
        val needed = mutableListOf<String>()
        if (Build.VERSION.SDK_INT >= 31) {
            if (checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) != PackageManager.PERMISSION_GRANTED)
                needed.add(Manifest.permission.BLUETOOTH_SCAN)
            if (checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) != PackageManager.PERMISSION_GRANTED)
                needed.add(Manifest.permission.BLUETOOTH_CONNECT)
        } else {
            if (checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED)
                needed.add(Manifest.permission.ACCESS_FINE_LOCATION)
        }
        if (Build.VERSION.SDK_INT >= 33) {
            if (checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED)
                needed.add(Manifest.permission.POST_NOTIFICATIONS)
        }
        if (needed.isNotEmpty()) {
            permissionLauncher.launch(needed.toTypedArray())
        }
    }

    @SuppressLint("BatteryLife")
    private fun requestBatteryOptExemption() {
        val pm = getSystemService(Context.POWER_SERVICE) as PowerManager
        if (!pm.isIgnoringBatteryOptimizations(packageName)) {
            val intent = Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS).apply {
                data = Uri.parse("package:$packageName")
            }
            try {
                startActivity(intent)
            } catch (e: Exception) {
                Log.w("MainActivity", "battery opt exemption request failed: ${e.message}")
            }
        }
    }
}
