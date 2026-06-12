package com.claudebuddy.bridge.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.claudebuddy.bridge.ble.BleState

@Composable
fun BridgeScreen(
    isRunning: Boolean,
    bleState: BleState,
    bleDeviceName: String?,
    httpRunning: Boolean,
    deviceBattery: Int = -1,
    deviceCharging: Boolean = false,
    ownerName: String,
    onOwnerNameChange: (String) -> Unit,
    buddyToken: String = "",
    onBuddyTokenChange: (String) -> Unit = {},
    mode: String = "serve_hub",
    onModeChange: (String) -> Unit = {},
    remoteHubUrl: String = "",
    onRemoteHubUrlChange: (String) -> Unit = {},
    onToggle: () -> Unit
) {
    val focusManager = LocalFocusManager.current
    Column(
        modifier = Modifier
            .fillMaxSize()
            .clickable(
                indication = null,
                interactionSource = remember { MutableInteractionSource() }
            ) { focusManager.clearFocus() }
            .padding(24.dp),
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        Spacer(modifier = Modifier.height(48.dp))

        Text(
            text = "Buddy Bridge",
            fontSize = 28.sp,
            fontWeight = FontWeight.Bold,
            color = MaterialTheme.colorScheme.onBackground
        )

        Spacer(modifier = Modifier.height(8.dp))

        Text(
            text = "BLE relay for Claude Buddy",
            fontSize = 14.sp,
            color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.6f)
        )

        Spacer(modifier = Modifier.height(48.dp))

        // Status cards
        StatusRow(
            label = "BLE",
            value = when (bleState) {
                BleState.DISCONNECTED -> "Disconnected"
                BleState.SCANNING -> "Scanning..."
                BleState.CONNECTING -> "Connecting..."
                BleState.BONDING -> "Bonding..."
                BleState.SUBSCRIBING -> "Subscribing..."
                BleState.CONNECTED -> bleDeviceName ?: "Connected"
            },
            color = when (bleState) {
                BleState.CONNECTED -> Color(0xFF4CAF50)
                BleState.DISCONNECTED -> Color(0xFFFF5722)
                else -> Color(0xFFFFC107)
            }
        )

        Spacer(modifier = Modifier.height(12.dp))

        StatusRow(
            label = "HTTP",
            value = if (httpRunning) "Listening on :8787" else "Stopped",
            color = if (httpRunning) Color(0xFF4CAF50) else Color(0xFFFF5722)
        )

        if (deviceBattery >= 0) {
            Spacer(modifier = Modifier.height(12.dp))
            StatusRow(
                label = "Battery",
                value = if (deviceCharging) "$deviceBattery% (charging)" else "$deviceBattery%",
                color = when {
                    deviceCharging -> Color(0xFF4CAF50)
                    deviceBattery <= 10 -> Color(0xFFFF5722)
                    deviceBattery <= 25 -> Color(0xFFFFC107)
                    else -> Color(0xFF4CAF50)
                }
            )
        }

        Spacer(modifier = Modifier.height(48.dp))

        // Start/Stop button
        Button(
            onClick = onToggle,
            modifier = Modifier
                .fillMaxWidth()
                .height(56.dp),
            colors = ButtonDefaults.buttonColors(
                containerColor = if (isRunning)
                    MaterialTheme.colorScheme.error
                else
                    MaterialTheme.colorScheme.primary
            )
        ) {
            Text(
                text = if (isRunning) "Stop Bridge" else "Start Bridge",
                fontSize = 18.sp
            )
        }

        Spacer(modifier = Modifier.height(32.dp))

        // Settings
        OutlinedTextField(
            value = ownerName,
            onValueChange = onOwnerNameChange,
            label = { Text("Owner Name") },
            placeholder = { Text("Shown on buddy device") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth()
        )

        Spacer(modifier = Modifier.height(12.dp))

        OutlinedTextField(
            value = buddyToken,
            onValueChange = onBuddyTokenChange,
            label = { Text("Auth Token") },
            placeholder = { Text("Shared secret for hook requests") },
            singleLine = true,
            modifier = Modifier.fillMaxWidth()
        )

        Spacer(modifier = Modifier.height(16.dp))
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Relay to remote hub", color = MaterialTheme.colorScheme.onBackground)
            Spacer(modifier = Modifier.width(12.dp))
            Switch(
                checked = mode == "relay",
                enabled = !isRunning,
                onCheckedChange = { onModeChange(if (it) "relay" else "serve_hub") }
            )
        }
        if (mode == "relay") {
            Spacer(modifier = Modifier.height(8.dp))
            OutlinedTextField(
                value = remoteHubUrl,
                onValueChange = onRemoteHubUrlChange,
                enabled = !isRunning,
                singleLine = true,
                label = { Text("Hub URL (https://buddy.example.com)") },
                modifier = Modifier.fillMaxWidth()
            )
        }
    }
}

@Composable
private fun StatusRow(label: String, value: String, color: Color) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(12.dp)
                .clip(CircleShape)
                .background(color)
        )
        Spacer(modifier = Modifier.width(12.dp))
        Text(
            text = label,
            fontWeight = FontWeight.Medium,
            modifier = Modifier.width(48.dp),
            color = MaterialTheme.colorScheme.onBackground
        )
        Text(
            text = value,
            color = MaterialTheme.colorScheme.onBackground.copy(alpha = 0.7f)
        )
    }
}
