#!/usr/bin/env python3
"""
relay.py — M5Stick BLE relay (runs on the machine with the Bluetooth radio).
Bridges a buddyhub's HTTP relay stream <-> the stick's BLE Nordic UART.

Outbound only: opens GET {hub}/relay/stream (chunked newline-JSON heartbeats),
writes each line to the stick verbatim, and POSTs the stick's button presses to
{hub}/button. No inbound port — so it works behind NAT, on a laptop, anywhere.

  buddy-relay                   # background run (logs to relay.log)
  buddy-relay --console         # foreground + console logging (debugging)
  buddy-relay --hub https://buddy.example.com   # remote hub over TLS
"""
import argparse
import asyncio
import json
import logging
import logging.handlers
import socket
import sys
import threading
import urllib.request
from pathlib import Path

from buddybridge import config as _config

NUS_RX = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"   # write   host -> device
NUS_TX = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"   # notify  device -> host

LOCK_PORT = 8791
# In the config dir (writable on every OS) — NOT next to the module, which is
# read-only / non-existent inside a frozen PyInstaller bundle.
LOGFILE = _config.config_dir() / "relay.log"
_lock = None

HEARTBEAT_TIMEOUT = 45.0   # no hub line for this long -> reconnect the STREAM
BLE_WRITE_TIMEOUT = 5.0    # a single BLE write blocking this long -> drop the link
BLE_CONNECT_TIMEOUT = 20.0     # bound BleakClient.connect so a hang can't wedge us
BLE_DISCONNECT_TIMEOUT = 10.0  # bound disconnect so cleanup always completes
STREAM_SOCKET_TIMEOUT = 60.0   # backstop: a wedged HTTP read can't block forever
STREAM_RETRY_BASE = 2.0        # stream-reconnect backoff (BLE stays up across these)
STREAM_RETRY_MAX = 30.0
READER_JOIN_TIMEOUT = 3.0      # bound the reader-thread join during stream teardown


def single_instance():
    global _lock
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", LOCK_PORT))
        s.listen(1)
    except OSError:
        return False
    _lock = s
    return True


def setup_logging(console):
    LOGFILE.parent.mkdir(parents=True, exist_ok=True)
    handlers = [logging.handlers.RotatingFileHandler(
        LOGFILE, maxBytes=512 * 1024, backupCount=1, encoding="utf-8")]
    if console:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S", handlers=handlers)


def resolve_hub(arg):
    return (arg or _config.load_config().get("hub") or "http://127.0.0.1:8787").rstrip("/")


def resolve_token():
    import os
    return os.environ.get("BUDDY_TOKEN") or _config.load_config().get("token") or ""


def post_button(hub, token, payload):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Buddy-Token"] = token
    req = urllib.request.Request(hub + "/button", data=json.dumps(payload).encode(),
                                 headers=headers, method="POST")
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception as e:
        logging.info("button POST failed: %s", e)


def open_stream(hub, token):
    url = hub + "/relay/stream"
    headers = {}
    if token:
        headers["X-Buddy-Token"] = token
    req = urllib.request.Request(url, headers=headers, method="GET")
    # A finite socket timeout is a backstop: if the TCP connection wedges (no
    # data, no FIN), the blocking read raises instead of hanging the reader
    # thread forever — which is what previously leaked threads and stalled the
    # relay. The HEARTBEAT_TIMEOUT watchdog handles normal idle first.
    return urllib.request.urlopen(req, timeout=STREAM_SOCKET_TIMEOUT)


def _stream_reader(resp, loop, queue, stop):
    """Blocking HTTP stream reader -> asyncio queue. Exits on stop, EOF, or
    error, and always enqueues a None sentinel so the consumer unblocks. Runs
    on a plain daemon thread (not the asyncio executor) so a slow read can
    never exhaust the executor pool and wedge the event loop."""
    try:
        for raw in resp:
            if stop.is_set():
                break
            line = raw.decode(errors="ignore").strip()
            if line:
                loop.call_soon_threadsafe(queue.put_nowait, line)
    except Exception as e:
        if not stop.is_set():
            logging.info("stream read ended: %s", e)
    finally:
        loop.call_soon_threadsafe(queue.put_nowait, None)   # EOF / stop sentinel


async def _pump_stream(client, hub, token, loop, mtu, delivered_box):
    """One hub-stream session: open the stream, relay its lines to the stick
    until the stream dies or goes quiet, then tear down cleanly. Returns True
    if the BLE link is still healthy (caller reopens the stream), False if the
    link failed (caller drops BLE and fully reconnects). Never raises for
    stream-side problems — only BLE failure ends the session unhealthily."""
    lines = asyncio.Queue()
    stop = threading.Event()
    try:
        resp = await loop.run_in_executor(None, open_stream, hub, token)
    except Exception as e:
        logging.info("hub stream open failed: %s", e)
        return client.is_connected            # BLE fine; caller backs off + retries
    reader = threading.Thread(target=_stream_reader,
                              args=(resp, loop, lines, stop), daemon=True)
    reader.start()
    logging.info("subscribed; relaying")
    ble_ok = True
    try:
        while client.is_connected:
            try:
                line = await asyncio.wait_for(lines.get(), timeout=HEARTBEAT_TIMEOUT)
            except asyncio.TimeoutError:
                logging.info("no hub data in %ss — reconnecting stream (BLE stays up)",
                             HEARTBEAT_TIMEOUT)
                break
            if line is None:
                logging.info("hub stream closed — reconnecting stream")
                break
            payload = (line + "\n").encode()
            chunks = [payload[i:i + mtu] for i in range(0, len(payload), mtu)]
            # Acknowledged writes (response=True): WinRT write-without-response
            # silently flow-control-hangs after the first packet, which left the
            # firmware with only the clock set and an otherwise-asleep pet.
            try:
                for idx, chunk in enumerate(chunks):
                    await asyncio.wait_for(
                        client.write_gatt_char(NUS_RX, chunk, response=True),
                        timeout=BLE_WRITE_TIMEOUT)
                    if idx < len(chunks) - 1:
                        await asyncio.sleep(0.005)
            except Exception as e:
                logging.info("BLE write failed (%s) — dropping link to reconnect", e)
                ble_ok = False
                break
            delivered_box[0] += 1
            if delivered_box[0] == 1:
                logging.info("first heartbeat delivered to the stick")
    finally:
        stop.set()
        try:
            resp.close()
        except Exception:
            pass
        # Bounded join: the socket timeout / close unblocks the reader; never
        # wait forever on it (that was the source of the stall).
        await loop.run_in_executor(None, reader.join, READER_JOIN_TIMEOUT)
    return ble_ok and client.is_connected


async def relay_once(hub, token, name_prefix, scan_timeout, do_pair, pair_timeout):
    from bleak import BleakScanner, BleakClient
    logging.info("scanning for the stick")
    dev = await BleakScanner.find_device_by_filter(
        lambda d, ad: (d.name or "").startswith(name_prefix), timeout=scan_timeout)
    if not dev:
        logging.info("no BLE device advertising '%s*' found", name_prefix)
        return
    logging.info("found %s [%s]; connecting BLE", dev.name, dev.address)
    loop = asyncio.get_running_loop()

    client = BleakClient(dev)
    # Bound the connect — a hung connect must not wedge the supervise loop.
    await asyncio.wait_for(client.connect(), timeout=BLE_CONNECT_TIMEOUT)
    try:
        if do_pair:
            try:
                await client.pair()
            except Exception as e:
                logging.info("pair() note: %s", e)
        logging.info("BLE connected")

        def on_notify(_s, data: bytearray):
            # Device -> host: button-press permission lines. Parse and POST.
            for piece in bytes(data).decode(errors="ignore").splitlines():
                piece = piece.strip()
                if not piece:
                    continue
                try:
                    msg = json.loads(piece)
                except json.JSONDecodeError:
                    continue
                if msg.get("cmd") == "permission":
                    payload = {"id": msg.get("id", ""),
                               "decision": msg.get("decision", "deny")}
                    # on_notify may fire off the loop thread — schedule safely.
                    loop.call_soon_threadsafe(
                        loop.run_in_executor, None, post_button, hub, token, payload)

        deadline = loop.time() + pair_timeout
        while True:
            try:
                await client.start_notify(NUS_TX, on_notify)
                break
            except Exception as e:
                if loop.time() >= deadline:
                    raise
                logging.info("waiting for pairing — enter the passkey on the desktop (%s)", e)
                await asyncio.sleep(2.0)

        logging.info("connecting hub stream %s", hub)
        mtu = (client.mtu_size - 3) if getattr(client, "mtu_size", 0) else 20
        delivered_box = [0]
        backoff = STREAM_RETRY_BASE
        # Keep the BLE link up across hub-stream reconnects. Idle hub silence or
        # a dropped stream now only re-opens the HTTP stream — the stick stays
        # connected, so it isn't churned awake (battery) and prompts that land
        # on a live stream are delivered without a reconnect gap.
        while client.is_connected:
            t0 = loop.time()
            healthy = await _pump_stream(client, hub, token, loop, mtu, delivered_box)
            if not healthy:
                break
            # A session that ran a while was a normal idle/EOF reconnect — reopen
            # promptly so prompts aren't stalled. Only a fast-failing stream
            # (e.g. open keeps erroring) earns exponential backoff.
            if loop.time() - t0 >= 10.0:
                backoff = STREAM_RETRY_BASE
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, STREAM_RETRY_MAX)
    finally:
        try:
            await asyncio.wait_for(client.disconnect(), timeout=BLE_DISCONNECT_TIMEOUT)
        except Exception as e:
            logging.info("disconnect note: %s", e)


async def supervise(args):
    hub = resolve_hub(args.hub)
    token = resolve_token()
    while True:
        try:
            await relay_once(hub, token, args.name, args.scan_timeout,
                             not args.no_pair, args.pair_timeout)
        except Exception as e:
            logging.info("relay error: %s", e)
        await asyncio.sleep(args.retry)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub", default=None,
                    help="hub base URL (default: config hub or http://127.0.0.1:8787)")
    ap.add_argument("--name", default="Claude")
    ap.add_argument("--scan-timeout", type=float, default=15.0)
    ap.add_argument("--no-pair", action="store_true")
    ap.add_argument("--pair-timeout", type=float, default=60.0,
                    help="seconds to keep one passkey on screen while you enter it")
    ap.add_argument("--retry", type=float, default=5.0)
    ap.add_argument("--console", action="store_true")
    args = ap.parse_args(argv)

    try:
        import bleak  # noqa: F401
    except ModuleNotFoundError:
        print("buddy-relay needs Bluetooth support. Install it with:\n"
              "    pipx install 'buddy-bridge[relay]'   (or: pip install bleak)",
              file=sys.stderr)
        sys.exit(1)

    setup_logging(args.console)
    if not single_instance():
        logging.info("another relay instance already running (lock %d held); exiting", LOCK_PORT)
        return
    logging.info("relay starting (hub %s)", resolve_hub(args.hub))
    try:
        asyncio.run(supervise(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
