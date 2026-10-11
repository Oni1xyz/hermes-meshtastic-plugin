"""Hermes gateway adapter for direct Meshtastic text messages.

The Meshtastic SDK delivers pubsub callbacks from worker threads.  Those
callbacks only enqueue work onto the gateway's asyncio loop; all filtering and
Hermes calls happen on that loop.
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import math
import os
import re
from collections import OrderedDict
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "meshtastic"
MAX_TEXT_BYTES = 180
BROADCAST_NODE = 0xFFFFFFFF
_TEXT_PORT_NAMES = {"TEXT_MESSAGE_APP", "PORTNUM.TEXT_MESSAGE_APP"}
_CANONICAL_NODE_ID_RE = re.compile(r"^![0-9a-f]{8}$")
_RECEIVE_TOPIC = "meshtastic.receive"
_LOST_TOPIC = "meshtastic.connection.lost"


class MeshtasticConfigurationError(ValueError):
    """Raised when the configured radio cannot be selected safely."""


def _load_meshtastic_runtime():
    """Import the optional SDK only when an adapter is actually used."""
    from meshtastic.serial_interface import SerialInterface
    from pubsub import pub
    from serial.tools.list_ports import comports

    return SerialInterface, pub, comports


def check_requirements() -> bool:
    """Passively report whether runtime dependencies are importable."""
    names = ("meshtastic.serial_interface", "pubsub", "serial.tools.list_ports")
    try:
        return all(importlib.util.find_spec(name) is not None for name in names)
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def _split_csv(value: Any) -> list[str]:
    if isinstance(value, str):
        values: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        values = ()
    return [str(item).strip() for item in values if str(item).strip()]


def _parse_node_id(value: Any) -> int:
    if isinstance(value, bool):
        raise TypeError("boolean is not a node ID")
    if isinstance(value, int):
        node = value
    else:
        raw = str(value or "").strip().lower()
        if raw.startswith("!"):
            raw = raw[1:]
            if len(raw) != 8:
                raise ValueError("Meshtastic node IDs must use ! plus eight hex digits")
            node = int(raw, 16)
        elif raw.startswith("0x"):
            node = int(raw, 16)
        elif raw.isdecimal():
            node = int(raw, 10)
        else:
            raise ValueError("invalid Meshtastic node ID")
    if not 0 <= node <= BROADCAST_NODE:
        raise ValueError("Meshtastic node ID is out of range")
    return node


def _canonical_node_id(value: Any) -> str:
    return f"!{_parse_node_id(value):08x}"


def _normalize_allowed_users(value: Any) -> set[int]:
    allowed: set[int] = set()
    for item in _split_csv(value):
        if not _CANONICAL_NODE_ID_RE.fullmatch(item):
            logger.warning("Meshtastic: ignoring non-canonical allowed node ID")
            continue
        try:
            node = _parse_node_id(item)
        except (TypeError, ValueError):
            logger.warning("Meshtastic: ignoring malformed allowed node ID")
            continue
        if node != BROADCAST_NODE:
            allowed.add(node)
    return allowed


def _bounded_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    if not math.isfinite(parsed):
        parsed = default
    return min(maximum, max(minimum, parsed))


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(maximum, max(minimum, parsed))


def _config_values(config: Any) -> tuple[str, str, list[str]]:
    extra = getattr(config, "extra", {}) or {}
    port = (os.getenv("MESHTASTIC_PORT") or extra.get("port") or "").strip()
    usb_serial = (
        os.getenv("MESHTASTIC_USB_SERIAL") or extra.get("usb_serial") or ""
    ).strip()
    # Keep one canonical source of truth for both this intake filter and
    # Hermes core's second authorization gate. Config-only allowlists cannot be
    # mirrored safely across multiplexed profiles, so the manifest-required env
    # value is intentional.
    raw_allowed = os.getenv("MESHTASTIC_ALLOWED_USERS", "")
    allowed = _split_csv(raw_allowed)
    return port, usb_serial, allowed


def validate_config(config: Any) -> bool:
    port, usb_serial, allowed = _config_values(config)
    return bool((port or usb_serial) and _normalize_allowed_users(allowed))


def is_connected(config: Any) -> bool:
    return validate_config(config) and check_requirements()


def _env_enablement() -> dict[str, Any] | None:
    port = os.getenv("MESHTASTIC_PORT", "").strip()
    usb_serial = os.getenv("MESHTASTIC_USB_SERIAL", "").strip()
    allowed = _split_csv(os.getenv("MESHTASTIC_ALLOWED_USERS", ""))
    if not (port or usb_serial) or not _normalize_allowed_users(allowed):
        return None
    seed: dict[str, Any] = {"allowed_users": allowed}
    if port:
        seed["port"] = port
    if usb_serial:
        seed["usb_serial"] = usb_serial
    home = os.getenv("MESHTASTIC_HOME_NODE", "").strip()
    if home:
        try:
            canonical = _canonical_node_id(home)
        except ValueError:
            logger.warning("Meshtastic: ignoring malformed MESHTASTIC_HOME_NODE")
        else:
            seed["home_channel"] = {
                "chat_id": canonical,
                "name": f"Meshtastic {canonical}",
            }
    return seed


def _prefix_by_utf8_bytes(text: str, budget: int) -> int:
    low, high = 0, len(text)
    while low < high:
        mid = (low + high + 1) // 2
        if len(text[:mid].encode("utf-8")) <= budget:
            low = mid
        else:
            high = mid - 1
    return low


def _split_assuming_total(text: str, max_bytes: int, total: int) -> list[str]:
    chunks: list[str] = []
    rest = text
    index = 1
    while rest:
        marker = f"({index}/{total}) "
        budget = max_bytes - len(marker.encode("utf-8"))
        if budget < 1:
            raise ValueError("max_bytes is too small for continuation markers")
        end = _prefix_by_utf8_bytes(rest, budget)
        if end < 1:
            raise ValueError("max_bytes cannot hold one UTF-8 character")
        chunks.append(marker + rest[:end])
        rest = rest[end:]
        index += 1
    return chunks


def split_text_utf8(text: str, max_bytes: int = MAX_TEXT_BYTES) -> list[str]:
    """Split text without breaking UTF-8 and include markers in the byte cap."""
    text = str(text)
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]
    assumed_total = 2
    for _ in range(20):
        chunks = _split_assuming_total(text, max_bytes, assumed_total)
        if len(chunks) == assumed_total:
            return chunks
        assumed_total = len(chunks)
    raise RuntimeError("unable to stabilize Meshtastic continuation markers")


def _packet_node(packet: dict[str, Any], numeric_key: str, id_key: str) -> int | None:
    value = packet.get(numeric_key)
    if value is None:
        value = packet.get(id_key)
    try:
        return _parse_node_id(value)
    except (TypeError, ValueError):
        return None


def _local_node_number(interface: Any) -> int | None:
    candidates = (
        getattr(getattr(interface, "localNode", None), "nodeNum", None),
        getattr(getattr(interface, "myInfo", None), "my_node_num", None),
        getattr(interface, "nodeNum", None),
    )
    for candidate in candidates:
        if candidate is not None:
            try:
                return _parse_node_id(candidate)
            except (TypeError, ValueError):
                pass
    return None


def _is_text_port(portnum: Any) -> bool:
    if isinstance(portnum, int):
        return portnum == 1
    name = getattr(portnum, "name", None)
    if name:
        return str(name).upper() == "TEXT_MESSAGE_APP"
    return str(portnum).upper() in _TEXT_PORT_NAMES


def _decode_text(decoded: dict[str, Any]) -> str | None:
    payload = decoded.get("payload")
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return None
    if isinstance(payload, str):
        return payload
    text = decoded.get("text")
    return text if isinstance(text, str) else None


def _message_id(result: Any) -> str | None:
    if isinstance(result, dict):
        value = result.get("id") or result.get("packetId")
    else:
        value = getattr(result, "id", None) or getattr(result, "packetId", None)
    return str(value) if value is not None else None


def _platform_member() -> Platform:
    """Return the plugin platform member, including direct standalone use.

    Hermes normally registers the plugin before constructing an adapter, so
    ``Platform._missing_`` can resolve it via the registry.  The standalone
    sender is also intentionally usable when imported directly (and tests do
    that), so create the same identity-stable pseudo-member when necessary.
    """
    try:
        return Platform(PLATFORM_NAME)
    except ValueError:
        pseudo = object.__new__(Platform)
        pseudo._value_ = PLATFORM_NAME
        pseudo._name_ = PLATFORM_NAME.upper()
        Platform._value2member_map_[PLATFORM_NAME] = pseudo
        Platform._member_map_[pseudo._name_] = pseudo
        return pseudo


class MeshtasticAdapter(BasePlatformAdapter):
    """Serial Meshtastic adapter accepting allowlisted direct text packets."""

    MAX_MESSAGE_LENGTH = MAX_TEXT_BYTES
    splits_long_messages = True

    def __init__(
        self,
        config: Any,
        *,
        interface_factory: Callable[..., Any] | None = None,
        pub: Any = None,
        port_lister: Callable[[], Iterable[Any]] | None = None,
    ) -> None:
        super().__init__(config=config, platform=_platform_member())
        extra = getattr(config, "extra", {}) or {}
        port, usb_serial, allowed = _config_values(config)
        self.port = port
        self.usb_serial = usb_serial
        self.allowed_users = _normalize_allowed_users(allowed)
        self._dm_policy = "allowlist"
        self.outbound_pacing_seconds = _bounded_float(
            extra.get("outbound_pacing_seconds", 0.35), 0.35, 0.1, 5.0
        )
        self.serial_operation_timeout_seconds = _bounded_float(
            extra.get("serial_operation_timeout_seconds", 30.0), 30.0, 1.0, 120.0
        )
        self._interface_factory = interface_factory
        self._pub = pub
        self._port_lister = port_lister
        self._interface: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscriptions: list[tuple[Callable[..., Any], str]] = []
        self._dispatch_tasks: set[asyncio.Task] = set()
        self._fatal_notification_tasks: set[asyncio.Task] = set()
        self._seen_packets: OrderedDict[tuple[int, int], None] = OrderedDict()
        self._dedupe_limit = _bounded_int(
            extra.get("dedupe_cache_size", 2048), 2048, 32, 65536
        )
        self._send_lock = asyncio.Lock()
        self._sleep = asyncio.sleep
        self._serial_quarantined = False

    @property
    def name(self) -> str:
        return "Meshtastic"

    @property
    def enforces_own_access_policy(self) -> bool:
        """Tell Hermes core that intake is protected by this local allowlist."""
        return True

    def _is_dm_allowed(self, user_id: str) -> bool:
        """Re-check an inbound canonical node ID for Hermes core authorization."""
        if not _CANONICAL_NODE_ID_RE.fullmatch(str(user_id or "")):
            return False
        try:
            return _parse_node_id(user_id) in self.allowed_users
        except (TypeError, ValueError):
            return False

    async def _run_serial_operation(
        self, function: Callable[..., Any], *args, **kwargs
    ):
        """Bound SDK calls so a stalled USB operation cannot block the event loop."""
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(function, *args, **kwargs),
                timeout=self.serial_operation_timeout_seconds,
            )
        except TimeoutError:
            # Python cannot terminate a thread blocked in an OS/USB driver. Keep
            # this adapter quarantined so reconnect never opens a competing
            # handle; a gateway restart creates a fresh adapter/process boundary.
            was_connected = self.is_connected
            self._serial_quarantined = True
            self._set_fatal_error(
                "serial_operation_timeout",
                "a Meshtastic serial operation timed out; restart the gateway",
                retryable=False,
            )
            if was_connected:
                self._mark_disconnected()
                # A gateway fatal handler normally calls disconnect(), which
                # needs _send_lock. This helper can run while send() owns that
                # lock, so notify in a retained detached task and let send()
                # unwind/release the lock first.
                task = asyncio.create_task(self._notify_fatal_error())
                self._fatal_notification_tasks.add(task)
                task.add_done_callback(self._fatal_notification_done)
            raise

    def _fatal_notification_done(self, task: asyncio.Task) -> None:
        self._fatal_notification_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is not None:
            logger.error(
                "Meshtastic: fatal notification failed",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    def _ensure_runtime(self) -> None:
        if (
            self._interface_factory is not None
            and self._pub is not None
            and self._port_lister is not None
        ):
            return
        factory, pub, port_lister = _load_meshtastic_runtime()
        self._interface_factory = self._interface_factory or factory
        self._pub = self._pub or pub
        self._port_lister = self._port_lister or port_lister

    async def _resolve_port(self) -> str:
        if self.port:
            return self.port
        if not self.usb_serial:
            raise MeshtasticConfigurationError(
                "set MESHTASTIC_USB_SERIAL (recommended) or MESHTASTIC_PORT"
            )
        if self._port_lister is None:
            self._ensure_runtime()
        ports = await self._run_serial_operation(lambda: list(self._port_lister()))
        matches = [
            str(port.device)
            for port in ports
            if str(getattr(port, "serial_number", "") or "") == self.usb_serial
        ]
        if not matches:
            raise MeshtasticConfigurationError(
                f"no serial device matched MESHTASTIC_USB_SERIAL={self.usb_serial!r}"
            )
        if len(matches) > 1:
            raise MeshtasticConfigurationError(
                f"multiple serial devices matched MESHTASTIC_USB_SERIAL={self.usb_serial!r}"
            )
        return matches[0]

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if self._interface is not None or self._subscriptions:
            await self.disconnect()
        if self._serial_quarantined:
            self._set_fatal_error(
                "serial_operation_timeout",
                "a Meshtastic serial operation timed out; restart the gateway",
                retryable=False,
            )
            return False
        if not self.allowed_users:
            self._set_fatal_error(
                "config_missing",
                "MESHTASTIC_ALLOWED_USERS must contain at least one valid node ID",
                retryable=False,
            )
            return False
        try:
            self._ensure_runtime()
            port = await self._resolve_port()
            self._loop = asyncio.get_running_loop()
            interface = await self._run_serial_operation(
                self._interface_factory, devPath=port
            )
            if _local_node_number(interface) is None:
                await self._run_serial_operation(interface.close)
                raise RuntimeError(
                    "Meshtastic interface did not expose a local node ID"
                )
            self._interface = interface
            self._subscribe(self._on_receive, _RECEIVE_TOPIC)
            self._subscribe(self._on_connection_lost, _LOST_TOPIC)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - SDK/serial errors are not stable types
            logger.error("Meshtastic: connection failed: %s", exc)
            await self._cleanup_transport()
            if not self._serial_quarantined:
                self._set_fatal_error("connect_failed", str(exc), retryable=True)
            return False
        self._mark_connected()
        logger.info("Meshtastic: connected via configured serial device")
        return True

    def _subscribe(self, callback: Callable[..., Any], topic: str) -> None:
        self._pub.subscribe(callback, topic)
        self._subscriptions.append((callback, topic))

    async def _cleanup_transport(self) -> None:
        for callback, topic in reversed(self._subscriptions):
            try:
                self._pub.unsubscribe(callback, topic)
            except Exception:
                logger.debug("Meshtastic: pubsub cleanup failed", exc_info=True)
        self._subscriptions.clear()
        tasks = list(self._dispatch_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._dispatch_tasks.clear()
        async with self._send_lock:
            interface, self._interface = self._interface, None
            if interface is not None:
                try:
                    await self._run_serial_operation(interface.close)
                except Exception:
                    logger.debug("Meshtastic: interface close failed", exc_info=True)
        self._loop = None

    async def disconnect(self) -> None:
        self._mark_disconnected()
        await self._cleanup_transport()
        await self.cancel_background_tasks()

    def _on_receive(self, packet: Any = None, interface: Any = None, **_: Any) -> None:
        """SDK-thread callback: enqueue only; never call Hermes here."""
        loop = self._loop
        if loop is None or loop.is_closed() or interface is not self._interface:
            return
        try:
            loop.call_soon_threadsafe(self._schedule_packet, packet, interface)
        except RuntimeError:
            return

    def _schedule_packet(self, packet: Any, interface: Any) -> None:
        if interface is not self._interface or not isinstance(packet, dict):
            return
        task = asyncio.create_task(self._process_packet(packet, interface))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_task_done)

    def _dispatch_task_done(self, task: asyncio.Task) -> None:
        self._dispatch_tasks.discard(task)
        if task.cancelled():
            return
        try:
            exception = task.exception()
        except asyncio.CancelledError:
            return
        if exception is not None:
            logger.error(
                "Meshtastic: inbound dispatch failed",
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    def _on_connection_lost(self, interface: Any = None, **_: Any) -> None:
        loop = self._loop
        if loop is None or loop.is_closed() or interface is not self._interface:
            return
        try:
            loop.call_soon_threadsafe(self._schedule_connection_lost, interface)
        except RuntimeError:
            return

    def _schedule_connection_lost(self, interface: Any) -> None:
        if interface is not self._interface:
            return
        task = asyncio.create_task(self._handle_connection_lost(interface))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_task_done)

    async def _handle_connection_lost(self, interface: Any) -> None:
        if interface is not self._interface:
            return
        self._mark_disconnected()
        self._set_fatal_error(
            "connection_lost", "Meshtastic serial connection was lost", retryable=True
        )
        await self._notify_fatal_error()

    def _remember_packet(self, sender: int, packet_id: Any) -> bool:
        try:
            key = (sender, int(packet_id))
        except (TypeError, ValueError):
            return False
        if key in self._seen_packets:
            self._seen_packets.move_to_end(key)
            return False
        self._seen_packets[key] = None
        while len(self._seen_packets) > self._dedupe_limit:
            self._seen_packets.popitem(last=False)
        return True

    async def _process_packet(self, packet: dict[str, Any], interface: Any) -> None:
        if interface is not self._interface:
            return
        local = _local_node_number(interface)
        sender = _packet_node(packet, "from", "fromId")
        destination = _packet_node(packet, "to", "toId")
        if local is None or sender is None or destination is None:
            return
        if sender == local or destination != local or destination == BROADCAST_NODE:
            return
        if sender not in self.allowed_users:
            return
        decoded = packet.get("decoded")
        if not isinstance(decoded, dict) or not _is_text_port(decoded.get("portnum")):
            return
        text = _decode_text(decoded)
        if text is None or not text.strip():
            return
        if not self._remember_packet(sender, packet.get("id")):
            return
        remote = _canonical_node_id(sender)
        source = self.build_source(
            chat_id=remote,
            chat_name=remote,
            chat_type="dm",
            user_id=remote,
            user_name=remote,
            message_id=str(packet.get("id")) if packet.get("id") is not None else None,
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            user_id=remote,
            user_name=remote,
            raw_message=packet,
            message_id=str(packet.get("id")) if packet.get("id") is not None else None,
            timestamp=datetime.now(),
        )
        await self.handle_message(event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SendResult:
        del reply_to, metadata
        try:
            destination = _parse_node_id(chat_id)
        except (TypeError, ValueError) as exc:
            return SendResult(success=False, error=str(exc))
        if destination == BROADCAST_NODE:
            return SendResult(
                success=False, error="Meshtastic broadcast sends are disabled"
            )
        if destination not in self.allowed_users:
            return SendResult(
                success=False, error="Meshtastic destination is not allowlisted"
            )
        if not str(content):
            return SendResult(success=False, error="Meshtastic message is empty")
        chunks = split_text_utf8(str(content), MAX_TEXT_BYTES)
        last_id: str | None = None
        async with self._send_lock:
            if self._serial_quarantined:
                return SendResult(
                    success=False,
                    error="a Meshtastic serial operation timed out; restart the gateway",
                )
            interface = self._interface
            if interface is None or not self.is_connected:
                return SendResult(success=False, error="Meshtastic is not connected")
            for index, chunk in enumerate(chunks):
                try:
                    result = await self._run_serial_operation(
                        interface.sendText,
                        chunk,
                        destinationId=destination,
                        wantAck=False,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - SDK errors are not stable types
                    logger.warning("Meshtastic: direct send failed: %s", exc)
                    return SendResult(success=False, error=str(exc))
                last_id = _message_id(result) or last_id
                if index + 1 < len(chunks):
                    await self._sleep(self.outbound_pacing_seconds)
        return SendResult(success=True, message_id=last_id)

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        del chat_id, metadata

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        canonical = _canonical_node_id(chat_id)
        return {"name": canonical, "type": "dm"}


def _parse_target_ref(raw: str) -> tuple[str, str | None] | None:
    value = str(raw or "").strip()
    if not value.startswith("!"):
        return None
    try:
        return _canonical_node_id(value), None
    except ValueError:
        return None


def _validate_target_ref(raw: str) -> bool | str:
    try:
        node = _parse_node_id(raw)
    except (TypeError, ValueError):
        return "expected a Meshtastic node ID like !11223344"
    if node == BROADCAST_NODE:
        return "Meshtastic broadcast targets are disabled"
    return True


async def _standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: str | None = None,
    media_files: list[str] | None = None,
    force_document: bool = False,
) -> dict[str, Any]:
    del thread_id, force_document
    if media_files:
        return {"error": "Meshtastic standalone send supports text only"}
    adapter = MeshtasticAdapter(pconfig)
    try:
        if not await adapter.connect():
            return {
                "error": adapter.fatal_error_message or "Meshtastic connection failed"
            }
        result = await adapter.send(chat_id, message)
        if not result.success:
            return {"error": result.error or "Meshtastic send failed"}
        return {"success": True, "message_id": result.message_id}
    finally:
        await adapter.disconnect()


def register(ctx: Any) -> None:
    """Register the Meshtastic platform with Hermes."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label="Meshtastic",
        adapter_factory=lambda cfg: MeshtasticAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MESHTASTIC_ALLOWED_USERS"],
        install_hint="pip install 'meshtastic>=2.5,<3' 'pypubsub>=4,<5' 'pyserial>=3.5,<4'",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MESHTASTIC_HOME_NODE",
        standalone_sender_fn=_standalone_send,
        parse_target_ref_fn=_parse_target_ref,
        validate_target_ref_fn=_validate_target_ref,
        allowed_users_env="MESHTASTIC_ALLOWED_USERS",
        max_message_length=MAX_TEXT_BYTES,
        emoji="📻",
        pii_safe=False,
        allow_update_command=False,
        platform_hint=(
            "You are replying over a low-bandwidth Meshtastic direct message. "
            "Use concise plain text; avoid markdown, tables, and unnecessary detail."
        ),
    )
