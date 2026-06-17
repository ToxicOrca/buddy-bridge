"""Unit tests for the relay's stream reader — the piece whose old version
could block forever and leak threads, stalling the relay. These cover the
contract that keeps reconnect handling robust: it always enqueues a final
None sentinel (so the async consumer unblocks) and stops promptly on request."""
import threading

from buddybridge import relay


class FakeLoop:
    """Runs call_soon_threadsafe callbacks inline (single-threaded test)."""
    def call_soon_threadsafe(self, func, *args):
        func(*args)


class FakeQueue:
    def __init__(self):
        self.items = []

    def put_nowait(self, item):
        self.items.append(item)


def test_reader_enqueues_stripped_lines_then_sentinel():
    resp = [b"alpha\n", b"   \n", b"  beta \n", b"\n"]
    q = FakeQueue()
    relay._stream_reader(resp, FakeLoop(), q, threading.Event())
    # blank lines dropped, content stripped, and a single None sentinel at EOF
    assert q.items == ["alpha", "beta", None]


def test_reader_always_emits_sentinel_on_error():
    class Boom:
        def __iter__(self):
            raise OSError("socket exploded")

    q = FakeQueue()
    relay._stream_reader(Boom(), FakeLoop(), q, threading.Event())
    assert q.items == [None]          # consumer must still unblock


def test_reader_stops_on_stop_event():
    def forever():
        i = 0
        while True:
            i += 1
            yield b"x\n"
            assert i < 1000, "reader did not honor stop event"

    stop = threading.Event()
    stop.set()                         # already stopped before first check
    q = FakeQueue()
    relay._stream_reader(forever(), FakeLoop(), q, stop)
    # reads at most one item, then breaks and emits the sentinel
    assert q.items[-1] is None
    assert q.items.count(None) == 1
