import pytest

from app.broadcasting.manager import ConnectionManager


class FakeWebSocket:
    def __init__(self, fail_on_send: bool = False):
        self.sent = []
        self.accepted = False
        self.fail_on_send = fail_on_send

    async def accept(self):
        self.accepted = True

    async def send_text(self, data):
        if self.fail_on_send:
            raise ConnectionError("client gone")
        self.sent.append(data)


@pytest.mark.asyncio
async def test_connect_marks_accepted_and_tracks_client():
    manager = ConnectionManager()
    ws = FakeWebSocket()
    await manager.connect(ws)
    assert ws.accepted
    assert manager.active_count == 1


@pytest.mark.asyncio
async def test_broadcast_reaches_all_connected_clients():
    manager = ConnectionManager()
    ws1, ws2 = FakeWebSocket(), FakeWebSocket()
    await manager.connect(ws1)
    await manager.connect(ws2)

    await manager.broadcast({"hello": "world"})

    assert len(ws1.sent) == 1
    assert len(ws2.sent) == 1
    assert '"hello"' in ws1.sent[0]


@pytest.mark.asyncio
async def test_disconnect_removes_client():
    manager = ConnectionManager()
    ws = FakeWebSocket()
    await manager.connect(ws)
    await manager.disconnect(ws)
    assert manager.active_count == 0


@pytest.mark.asyncio
async def test_broadcast_prunes_dead_connections():
    manager = ConnectionManager()
    good = FakeWebSocket()
    dead = FakeWebSocket(fail_on_send=True)
    await manager.connect(good)
    await manager.connect(dead)

    await manager.broadcast({"x": 1})

    assert manager.active_count == 1
    assert len(good.sent) == 1
