from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import adapter

LOCAL = 0xAABBCCDD
ALLOWED = 0x11223344
OTHER = 0x55667788


@pytest.fixture(autouse=True)
def canonical_meshtastic_environment(monkeypatch):
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "!11223344")
    monkeypatch.delenv("MESHTASTIC_USB_SERIAL", raising=False)
    monkeypatch.delenv("MESHTASTIC_PORT", raising=False)
    monkeypatch.delenv("MESHTASTIC_HOME_NODE", raising=False)


class FakePub:
    def __init__(self):
        self.subscriptions = []

    def subscribe(self, callback, topic):
        self.subscriptions.append((callback, topic))

    def unsubscribe(self, callback, topic):
        self.subscriptions.remove((callback, topic))


class FakeInterface:
    def __init__(self, devPath=None):
        self.devPath = devPath
        self.localNode = SimpleNamespace(nodeNum=LOCAL)
        self.sent = []
        self.closed = False

    def sendText(self, text, destinationId=None, wantAck=False):
        self.sent.append((text, destinationId, wantAck))
        return {"id": len(self.sent)}

    def close(self):
        self.closed = True


@dataclass
class Port:
    device: str
    serial_number: str | None


def config(**extra):
    return SimpleNamespace(extra=extra)


def packet(
    *,
    sender=ALLOWED,
    destination=LOCAL,
    packet_id=7,
    text="hello",
    portnum="TEXT_MESSAGE_APP",
):
    return {
        "from": sender,
        "to": destination,
        "id": packet_id,
        "decoded": {"portnum": portnum, "payload": text.encode("utf-8")},
    }


@pytest.fixture
def fake_runtime():
    pub = FakePub()
    made = []

    def factory(devPath=None):
        iface = FakeInterface(devPath)
        made.append(iface)
        return iface

    return pub, made, factory


def make_adapter(fake_runtime, **extra):
    pub, _, factory = fake_runtime
    return adapter.MeshtasticAdapter(
        config(**extra),
        interface_factory=factory,
        pub=pub,
        port_lister=lambda: [Port("/dev/cu.usbmodem1", "SERIAL-1")],
    )


def test_requirements_probe_is_passive(monkeypatch):
    calls = []
    monkeypatch.setattr(
        adapter.importlib.util, "find_spec", lambda name: calls.append(name) or object()
    )
    assert adapter.check_requirements() is True
    assert calls == ["meshtastic.serial_interface", "pubsub", "serial.tools.list_ports"]


def test_config_is_default_deny_and_requires_stable_or_explicit_port(monkeypatch):
    monkeypatch.delenv("MESHTASTIC_ALLOWED_USERS", raising=False)
    monkeypatch.delenv("MESHTASTIC_USB_SERIAL", raising=False)
    monkeypatch.delenv("MESHTASTIC_PORT", raising=False)
    assert adapter.validate_config(config()) is False
    assert adapter._env_enablement() is None


def test_allowlist_requires_canonical_lowercase_node_ids(monkeypatch):
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "0x11223344")
    assert adapter.validate_config(config(usb_serial="SERIAL-1")) is False
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "!11223344")
    assert adapter.validate_config(config(usb_serial="SERIAL-1")) is True


def test_env_enablement_seeds_home_target_without_exposing_secrets(monkeypatch):
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "!11223344")
    monkeypatch.setenv("MESHTASTIC_USB_SERIAL", "SERIAL-1")
    monkeypatch.setenv("MESHTASTIC_HOME_NODE", "!11223344")
    seed = adapter._env_enablement()
    assert seed == {
        "usb_serial": "SERIAL-1",
        "allowed_users": ["!11223344"],
        "home_channel": {"chat_id": "!11223344", "name": "Meshtastic !11223344"},
    }


@pytest.mark.asyncio
async def test_port_resolution_prefers_explicit_port(fake_runtime):
    instance = make_adapter(
        fake_runtime,
        port="/dev/cu.explicit",
        usb_serial="SERIAL-1",
        allowed_users=["!11223344"],
    )
    assert await instance._resolve_port() == "/dev/cu.explicit"


@pytest.mark.asyncio
async def test_port_resolution_matches_exact_usb_serial(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    assert await instance._resolve_port() == "/dev/cu.usbmodem1"


@pytest.mark.asyncio
async def test_port_resolution_rejects_missing_or_ambiguous_serial(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="MISSING", allowed_users=["!11223344"]
    )
    with pytest.raises(adapter.MeshtasticConfigurationError, match="MISSING"):
        await instance._resolve_port()
    instance._port_lister = lambda: [
        Port("/dev/a", "SERIAL-1"),
        Port("/dev/b", "SERIAL-1"),
    ]
    instance.usb_serial = "SERIAL-1"
    with pytest.raises(adapter.MeshtasticConfigurationError, match="multiple"):
        await instance._resolve_port()


def test_outbound_pacing_is_bounded_and_malformed_values_are_safe(fake_runtime):
    low = make_adapter(
        fake_runtime,
        usb_serial="SERIAL-1",
        allowed_users=["!11223344"],
        outbound_pacing_seconds=-10,
    )
    high = make_adapter(
        fake_runtime,
        usb_serial="SERIAL-1",
        allowed_users=["!11223344"],
        outbound_pacing_seconds=999,
    )
    malformed = make_adapter(
        fake_runtime,
        usb_serial="SERIAL-1",
        allowed_users=["!11223344"],
        outbound_pacing_seconds="invalid",
        dedupe_cache_size="invalid",
    )
    assert low.outbound_pacing_seconds == 0.1
    assert high.outbound_pacing_seconds == 5.0
    assert malformed.outbound_pacing_seconds == 0.35
    assert malformed._dedupe_limit == 2048


@pytest.mark.asyncio
async def test_serial_timeout_quarantines_adapter_until_gateway_restart(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    instance.serial_operation_timeout_seconds = 0.01
    with pytest.raises(TimeoutError):
        await instance._run_serial_operation(time.sleep, 0.1)
    assert instance._serial_quarantined is True
    assert await instance.connect() is False
    assert instance.fatal_error_code == "serial_operation_timeout"


@pytest.mark.asyncio
async def test_port_discovery_is_bounded_by_serial_timeout(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    instance.serial_operation_timeout_seconds = 0.01

    def slow_port_lister():
        time.sleep(0.1)
        return []

    instance._port_lister = slow_port_lister
    assert await asyncio.wait_for(instance.connect(), timeout=0.05) is False
    assert instance._serial_quarantined is True


@pytest.mark.asyncio
async def test_send_timeout_notifies_fatal_handler_without_lock_deadlock(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    await instance.connect()
    instance.serial_operation_timeout_seconds = 0.01
    handler_finished = asyncio.Event()

    async def fatal_handler(failed_adapter):
        await failed_adapter.disconnect()
        handler_finished.set()

    def stalled_send(text, destinationId=None, wantAck=False):
        time.sleep(0.1)
        return {"id": 1}

    instance.set_fatal_error_handler(fatal_handler)
    instance._interface.sendText = stalled_send
    result = await asyncio.wait_for(
        instance.send("!11223344", "timeout test"), timeout=0.05
    )
    assert result.success is False
    assert instance.fatal_error_code == "serial_operation_timeout"
    assert instance._serial_quarantined is True
    await asyncio.wait_for(handler_finished.wait(), timeout=0.05)


@pytest.mark.asyncio
async def test_connect_subscribes_once_and_reconnect_cleans_old_interface(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    assert await instance.connect() is True
    pub, made, _ = fake_runtime
    assert {topic for _, topic in pub.subscriptions} == {
        "meshtastic.receive",
        "meshtastic.connection.lost",
    }
    assert len(pub.subscriptions) == 2
    assert await instance.connect(is_reconnect=True) is True
    assert made[0].closed is True
    assert len(pub.subscriptions) == 2
    await instance.disconnect()
    assert made[-1].closed is True
    assert pub.subscriptions == []


@pytest.mark.asyncio
async def test_callback_thread_only_schedules_work_on_gateway_loop(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    await instance.connect()
    loop_thread = threading.get_ident()
    received = []
    done = asyncio.Event()

    async def capture(event):
        received.append((event, threading.get_ident()))
        done.set()

    instance.handle_message = capture
    thread = threading.Thread(
        target=lambda: instance._on_receive(packet(), instance._interface)
    )
    thread.start()
    thread.join()
    await asyncio.wait_for(done.wait(), 1)

    event, handler_thread = received[0]
    assert handler_thread == loop_thread
    assert event.text == "hello"
    assert event.source.chat_id == "!11223344"
    assert event.source.user_id == "!11223344"
    assert event.source.chat_type == "dm"
    await instance.disconnect()


def test_environment_allowlist_is_visible_to_hermes_core_authorization(
    monkeypatch, fake_runtime
):
    import logging
    import sys

    from gateway.authz_mixin import GatewayAuthorizationMixin
    from gateway.platform_registry import platform_registry

    monkeypatch.setattr(
        platform_registry,
        "get",
        lambda name: SimpleNamespace(
            allowed_users_env="MESHTASTIC_ALLOWED_USERS", allow_all_env=""
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "gateway.run",
        SimpleNamespace(logger=logging.getLogger("test.gateway.authz")),
    )
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "!11223344")
    monkeypatch.setenv("GATEWAY_ALLOWED_USERS", "unrelated-user")
    monkeypatch.delenv("GATEWAY_ALLOW_ALL_USERS", raising=False)
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    source = instance.build_source(
        chat_id="!11223344",
        chat_name="!11223344",
        chat_type="dm",
        user_id="!11223344",
        user_name="!11223344",
    )

    runner = object.__new__(GatewayAuthorizationMixin)
    runner.adapters = {instance.platform: instance}
    runner._profile_adapters = {}
    runner.pairing_stores = {}
    runner.pairing_store = None

    assert runner._is_user_authorized(source) is True
    source.user_id = "!55667788"
    assert runner._is_user_authorized(source) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blocked",
    [
        packet(sender=OTHER),
        packet(destination=0xFFFFFFFF),
        packet(sender=LOCAL),
        packet(portnum="POSITION_APP"),
        {
            "from": ALLOWED,
            "to": LOCAL,
            "id": 8,
            "decoded": {"portnum": "TEXT_MESSAGE_APP", "payload": b"\xff"},
        },
        {
            "from": ALLOWED,
            "to": LOCAL,
            "decoded": {
                "portnum": "TEXT_MESSAGE_APP",
                "payload": b"missing packet id",
            },
        },
    ],
)
async def test_inbound_filtering_default_denies_non_dm_non_text_self_and_malformed(
    fake_runtime, blocked
):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    await instance.connect()
    instance.handle_message = AsyncMock()
    await instance._process_packet(blocked, instance._interface)
    instance.handle_message.assert_not_awaited()
    await instance.disconnect()


@pytest.mark.asyncio
async def test_packet_id_dedupe_is_per_remote_node(monkeypatch, fake_runtime):
    monkeypatch.setenv("MESHTASTIC_ALLOWED_USERS", "!11223344,!55667788")
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344", "!55667788"]
    )
    await instance.connect()
    instance.handle_message = AsyncMock()
    await instance._process_packet(packet(packet_id=9), instance._interface)
    await instance._process_packet(packet(packet_id=9), instance._interface)
    await instance._process_packet(
        packet(sender=OTHER, packet_id=9), instance._interface
    )
    assert instance.handle_message.await_count == 2
    await instance.disconnect()


def test_utf8_chunking_is_conservative_reconstructable_and_marked():
    text = ("mesh 🚀 telemetry and unicode café " * 20).strip()
    chunks = adapter.split_text_utf8(text, max_bytes=180)
    assert len(chunks) > 1
    assert all(len(chunk.encode("utf-8")) <= 180 for chunk in chunks)
    bodies = [chunk.split(" ", 1)[1] for chunk in chunks]
    assert "".join(bodies) == text
    assert chunks[0].startswith(f"(1/{len(chunks)}) ")
    assert chunks[-1].startswith(f"({len(chunks)}/{len(chunks)}) ")


def test_text_port_compatibility_and_native_chunking_contract():
    assert adapter._is_text_port("PortNum.TEXT_MESSAGE_APP") is True
    assert adapter.MeshtasticAdapter.MAX_MESSAGE_LENGTH == 180
    assert adapter.MeshtasticAdapter.splits_long_messages is True


@pytest.mark.asyncio
async def test_send_direct_chunks_with_pacing_and_same_destination(fake_runtime):
    instance = make_adapter(
        fake_runtime,
        usb_serial="SERIAL-1",
        allowed_users=["!11223344"],
        outbound_pacing_seconds=0.25,
    )
    await instance.connect()
    instance._sleep = AsyncMock()
    result = await instance.send("!11223344", "🚀" * 100)
    sent = instance._interface.sent
    assert result.success is True
    assert len(sent) > 1
    assert all(
        destination == ALLOWED and want_ack is False
        for _, destination, want_ack in sent
    )
    assert all(len(text.encode("utf-8")) <= 180 for text, _, _ in sent)
    assert instance._sleep.await_count == len(sent) - 1
    await instance.disconnect()


@pytest.mark.asyncio
async def test_disconnect_waits_for_inflight_send_before_closing_interface(
    fake_runtime,
):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    await instance.connect()
    interface = instance._interface
    started = threading.Event()
    release = threading.Event()

    def blocking_send(text, destinationId=None, wantAck=False):
        started.set()
        release.wait(timeout=1)
        interface.sent.append((text, destinationId, wantAck))
        return {"id": 1}

    interface.sendText = blocking_send
    send_task = asyncio.create_task(instance.send("!11223344", "hello"))
    assert await asyncio.to_thread(started.wait, 1)
    disconnect_task = asyncio.create_task(instance.disconnect())
    await asyncio.sleep(0.01)
    assert interface.closed is False
    release.set()
    assert (await send_task).success is True
    await disconnect_task
    assert interface.closed is True


@pytest.mark.asyncio
async def test_send_rejects_broadcast_invalid_target_and_disconnected(fake_runtime):
    instance = make_adapter(
        fake_runtime, usb_serial="SERIAL-1", allowed_users=["!11223344"]
    )
    assert (await instance.send("!11223344", "hello")).success is False
    await instance.connect()
    assert (await instance.send("!ffffffff", "hello")).success is False
    assert (await instance.send("!55667788", "hello")).success is False
    assert (await instance.send("not-a-node", "hello")).success is False
    await instance.disconnect()


@pytest.mark.asyncio
async def test_standalone_sender_connects_sends_and_always_closes(
    monkeypatch, fake_runtime
):
    pub, made, factory = fake_runtime
    monkeypatch.setattr(
        adapter,
        "_load_meshtastic_runtime",
        lambda: (factory, pub, lambda: [Port("/dev/cu.mesh", "SERIAL-1")]),
    )
    result = await adapter._standalone_send(
        config(usb_serial="SERIAL-1", allowed_users=["!11223344"]),
        "!11223344",
        "hello from cron",
    )
    assert result["success"] is True
    assert made[0].sent == [("hello from cron", ALLOWED, False)]
    assert made[0].closed is True
    assert pub.subscriptions == []


def test_registration_declares_platform_contract():
    captured = {}
    ctx = SimpleNamespace(register_platform=lambda **kwargs: captured.update(kwargs))
    adapter.register(ctx)
    assert captured["name"] == "meshtastic"
    assert captured["allowed_users_env"] == "MESHTASTIC_ALLOWED_USERS"
    assert captured["max_message_length"] == 180
    assert captured["cron_deliver_env_var"] == "MESHTASTIC_HOME_NODE"
    assert captured["standalone_sender_fn"] is adapter._standalone_send
    assert "plain text" in captured["platform_hint"].lower()
    assert "concise" in captured["platform_hint"].lower()
    assert "allow_all_env" not in captured
