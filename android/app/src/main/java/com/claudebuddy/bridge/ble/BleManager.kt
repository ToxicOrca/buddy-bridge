package com.claudebuddy.bridge.ble

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.os.Build
import android.os.ParcelUuid
import android.util.Log
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.json.JSONObject
import java.util.UUID

enum class BleState {
    DISCONNECTED, SCANNING, CONNECTING, BONDING, SUBSCRIBING, CONNECTED
}

/**
 * BLE connection manager for the M5 Claude Buddy.
 * Handles scan, connect, bond, NUS subscribe, chunked write, reconnect.
 */
@SuppressLint("MissingPermission")  // permissions checked before starting
class BleManager(
    private val context: Context,
    private val namePrefix: String = "Claude",
    private val onLine: (String) -> Unit  // incoming JSON lines from device
) {
    companion object {
        private const val TAG = "BleManager"
        private const val RETRY_DELAY_MS = 5000L
        private const val PAIR_TIMEOUT_MS = 60000L
        private const val SCAN_TIMEOUT_MS = 15000L
        private const val INTER_CHUNK_DELAY_MS = 5L
        private val CCCD_UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")
    }

    private val _state = MutableStateFlow(BleState.DISCONNECTED)
    val state: StateFlow<BleState> = _state

    private val _deviceName = MutableStateFlow<String?>(null)
    val deviceName: StateFlow<String?> = _deviceName

    private var gatt: BluetoothGatt? = null
    private var rxChar: BluetoothGattCharacteristic? = null
    private var negotiatedMtu = 23
    private var scope: CoroutineScope? = null
    private var connectJob: Job? = null
    private val lineBuffer = LineBuffer()
    private val writeQueue = kotlinx.coroutines.channels.Channel<ByteArray>(64)
    private var writeJob: Job? = null

    // Called when we have a connected, subscribed link — service wires this
    var onConnected: (() -> Unit)? = null

    private val adapter: BluetoothAdapter?
        get() = (context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter

    private fun hasBlePermission(): Boolean {
        if (Build.VERSION.SDK_INT < 31) return true
        return context.checkSelfPermission(Manifest.permission.BLUETOOTH_SCAN) == PackageManager.PERMISSION_GRANTED &&
               context.checkSelfPermission(Manifest.permission.BLUETOOTH_CONNECT) == PackageManager.PERMISSION_GRANTED
    }

    fun start(scope: CoroutineScope) {
        this.scope = scope
        connectJob = scope.launch { supervisionLoop() }
    }

    fun stop() {
        connectJob?.cancel()
        writeJob?.cancel()
        disconnect()
    }

    private fun disconnect() {
        gatt?.close()
        gatt = null
        rxChar = null
        negotiatedMtu = 23
        _state.value = BleState.DISCONNECTED
        _deviceName.value = null
    }

    // ---- write to device --------------------------------------------------- //

    fun sendJson(json: String) {
        if (_state.value != BleState.CONNECTED) return
        Log.d(TAG, "sendJson: ${json.take(80)}...")
        val chunks = chunkMessage(json, negotiatedMtu)
        for (chunk in chunks) {
            writeQueue.trySend(chunk)
        }
    }

    @SuppressLint("NewApi")
    private fun startWriteProcessor(scope: CoroutineScope) {
        writeJob?.cancel()
        writeJob = scope.launch {
            for (chunk in writeQueue) {
                try {
                    val char = rxChar ?: continue
                    val g = gatt ?: continue
                    if (Build.VERSION.SDK_INT >= 33) {
                        g.writeCharacteristic(
                            char, chunk, BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                        )
                    } else {
                        @Suppress("DEPRECATION")
                        char.value = chunk
                        @Suppress("DEPRECATION")
                        char.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                        @Suppress("DEPRECATION")
                        g.writeCharacteristic(char)
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "BLE write failed (continuing): ${e.message}")
                }
                delay(INTER_CHUNK_DELAY_MS)
            }
        }
    }

    // ---- supervision loop -------------------------------------------------- //

    private suspend fun supervisionLoop() = coroutineScope {
        while (isActive) {
            try {
                connectOnce()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Log.i(TAG, "relay error: ${e.message}")
            }
            disconnect()
            delay(RETRY_DELAY_MS)
        }
    }

    private suspend fun connectOnce() {
        if (!hasBlePermission()) {
            Log.w(TAG, "BLE permissions not granted, skipping connect cycle")
            return
        }
        // Scan
        _state.value = BleState.SCANNING
        val device = scan() ?: run {
            Log.i(TAG, "no device found advertising '${namePrefix}*'")
            return
        }
        _deviceName.value = device.name
        Log.i(TAG, "found ${device.name} [${device.address}]")

        // Connect
        _state.value = BleState.CONNECTING
        val connected = CompletableDeferred<Boolean>()
        val servicesDiscovered = CompletableDeferred<Boolean>()
        val mtuNegotiated = CompletableDeferred<Int>()
        val subscribed = CompletableDeferred<Boolean>()
        val disconnected = CompletableDeferred<Unit>()

        val callback = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    connected.complete(true)
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                    connected.complete(false)
                    disconnected.complete(Unit)
                }
            }

            override fun onServicesDiscovered(g: BluetoothGatt, status: Int) {
                servicesDiscovered.complete(status == BluetoothGatt.GATT_SUCCESS)
            }

            override fun onMtuChanged(g: BluetoothGatt, mtu: Int, status: Int) {
                if (status == BluetoothGatt.GATT_SUCCESS) {
                    negotiatedMtu = mtu
                    Log.i(TAG, "mtu=$mtu")
                }
                mtuNegotiated.complete(mtu)
            }

            override fun onDescriptorWrite(g: BluetoothGatt, desc: BluetoothGattDescriptor, status: Int) {
                subscribed.complete(status == BluetoothGatt.GATT_SUCCESS)
            }

            @Deprecated("Deprecated in API 33")
            override fun onCharacteristicChanged(g: BluetoothGatt, char: BluetoothGattCharacteristic) {
                val lines = lineBuffer.feed(char.value)
                for (line in lines) onLine(line)
            }
        }

        // autoConnect=true for bonded devices: Android's BLE stack handles
        // transient disconnects (e.g. M5 light sleep) internally instead of
        // reporting them, preventing the frequent scan/reconnect cycling.
        val auto = device.bondState == BluetoothDevice.BOND_BONDED
        gatt = device.connectGatt(context, auto, callback, BluetoothDevice.TRANSPORT_LE)
        val g = gatt ?: return

        // Wait for connection
        if (!withTimeoutOrNull(10000) { connected.await() }.let { it == true }) {
            Log.i(TAG, "connection failed")
            return
        }

        // Request MTU
        g.requestMtu(517)
        withTimeoutOrNull(5000) { mtuNegotiated.await() }

        // Discover services
        g.discoverServices()
        if (!withTimeoutOrNull(10000) { servicesDiscovered.await() }.let { it == true }) {
            Log.i(TAG, "service discovery failed")
            return
        }

        // Find NUS characteristics
        val nusService = g.getService(UUID.fromString(NusUuids.NUS_SERVICE))
        if (nusService == null) {
            Log.i(TAG, "NUS service not found")
            return
        }
        rxChar = nusService.getCharacteristic(UUID.fromString(NusUuids.NUS_RX))
        val txChar = nusService.getCharacteristic(UUID.fromString(NusUuids.NUS_TX))
        if (rxChar == null || txChar == null) {
            Log.i(TAG, "NUS characteristics not found")
            return
        }

        // Bond if needed (device requires encrypted link)
        if (device.bondState != BluetoothDevice.BOND_BONDED) {
            _state.value = BleState.BONDING
            device.createBond()
        }

        // Subscribe to TX notifications — retry during bonding
        _state.value = BleState.SUBSCRIBING
        val deadline = System.currentTimeMillis() + PAIR_TIMEOUT_MS
        var notifyOk = false
        while (System.currentTimeMillis() < deadline) {
            try {
                g.setCharacteristicNotification(txChar, true)
                val cccd = txChar.getDescriptor(CCCD_UUID)
                if (cccd != null) {
                    cccd.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    g.writeDescriptor(cccd)
                    notifyOk = withTimeoutOrNull(5000) { subscribed.await() } == true
                    if (notifyOk) break
                }
            } catch (e: Exception) {
                Log.i(TAG, "waiting for pairing: ${e.message}")
            }
            delay(2000)
        }

        if (!notifyOk) {
            Log.i(TAG, "failed to subscribe to NUS TX")
            return
        }

        _state.value = BleState.CONNECTED
        Log.i(TAG, "connected and subscribed")

        // Start write processor
        startWriteProcessor(scope!!)

        // Notify service that we're connected (triggers handshake)
        onConnected?.invoke()

        // Wait for disconnect
        disconnected.await()
        Log.i(TAG, "device disconnected")
    }

    // ---- scanning ---------------------------------------------------------- //

    private suspend fun scan(): BluetoothDevice? {
        val scanner = adapter?.bluetoothLeScanner ?: return null
        val result = CompletableDeferred<BluetoothDevice?>()

        val scanCallback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, scanResult: ScanResult) {
                val name = scanResult.device.name ?: return
                if (name.startsWith(namePrefix)) {
                    scanner.stopScan(this)
                    result.complete(scanResult.device)
                }
            }
            override fun onScanFailed(errorCode: Int) {
                Log.e(TAG, "scan failed: $errorCode")
                result.complete(null)
            }
        }

        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        scanner.startScan(null, settings, scanCallback)

        return withTimeoutOrNull(SCAN_TIMEOUT_MS) {
            result.await()
        }.also {
            if (it == null) {
                try { scanner.stopScan(scanCallback) } catch (_: Exception) {}
            }
        }
    }
}
