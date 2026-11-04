#!/usr/bin/env python3
"""Integrated IPv4 UDP sender/receiver with a Textual command interface."""

from __future__ import annotations

import argparse
import asyncio
import bisect
import collections
import ipaddress
import json
import os
import queue
import socket
import struct
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, TextIO

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Vertical, VerticalScroll
from textual.suggester import Suggester
from textual.widgets import Footer, Header, Input, Label, OptionList, Static
from textual.widgets.option_list import Option


MAX_PAYLOAD_SIZE = 32_768
MAX_UDP_PAYLOAD_SIZE = 65_507
DEFAULT_PACKET_QUEUE_BYTES = 8 * 1024 * 1024
DEFAULT_PACKET_QUEUE_PACKETS = 4_096
DEFAULT_RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024
LINUX_SO_RXQ_OVFL = 40
MAX_WAIT_MS = 1_800_000
FLUSH_INTERVAL_SECONDS = 1.0
FLUSH_LINE_COUNT = 256
UI_MAX_EVENTS = 100
COMMAND_HISTORY_LIMIT = 100
COMPLETION_OPTION_LIMIT = 200
UI_REFRESH_INTERVAL = 0.1
EVENT_LOG_QUEUE_LIMIT = 4_096
_EVENT_WAKE = object()


class ConfigError(ValueError):
    """A user-facing startup configuration error."""


class OneLineArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ConfigError(f"usage: udpcomapp.py <settings-json>: {safe_one_line(message)}")


class SendFileError(Exception):
    """An error processing a payload file, with a safe line reference."""

    def __init__(self, line_number: int, message: str) -> None:
        super().__init__(message)
        self.line_number = line_number


def timestamp(now: datetime | None = None) -> str:
    return (now or datetime.now()).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def safe_one_line(message: object) -> str:
    return str(message).replace("\r", " ").replace("\n", " ")


def error_line(message: object) -> str:
    return f"[{timestamp()}] [error] Error: {safe_one_line(message)}"


@dataclass
class Settings:
    listen_address: str
    listen_port: int
    destination_address: str
    destination_port: int
    packet_log_path: Path
    event_log_path: Path
    packet_log_queue_limit_bytes: int = DEFAULT_PACKET_QUEUE_BYTES
    packet_log_queue_limit_packets: int = DEFAULT_PACKET_QUEUE_PACKETS
    receive_socket_buffer_bytes: int = DEFAULT_RECEIVE_BUFFER_BYTES


def _positive_integer(document: dict[str, object], key: str, default: int) -> int:
    value = document.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{key} must be a positive integer")
    return value


def _port(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65_535:
        raise ConfigError(f"{key} must be an integer from 1 to 65535")
    return value


def _ipv4(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be an IPv4 address string")
    try:
        return str(ipaddress.IPv4Address(value))
    except ipaddress.AddressValueError as exc:
        raise ConfigError(f"{key} must be a valid IPv4 address") from exc


def _resolve_log_path(value: object, key: str, settings_dir: Path) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    path = Path(value)
    if not path.is_absolute():
        path = settings_dir / path
    return path.resolve(strict=False)


def _new_log_path(directory: Path, prefix: str, now: datetime | None = None) -> Path:
    stem = f"{prefix}_{(now or datetime.now()).astimezone().strftime('%Y%m%d-%H%M%S-%f')[:-3]}"
    candidate = directory / f"{stem}.log"
    index = 0
    while candidate.exists():
        index += 1
        candidate = directory / f"{stem}-{index}.log"
    return candidate.resolve(strict=False)


def load_settings(settings_path: str | Path) -> Settings:
    supplied_path = Path(settings_path).expanduser()
    try:
        absolute_path = supplied_path.resolve(strict=True)
        raw = absolute_path.read_text(encoding="utf-8")
        document = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read settings file: {safe_one_line(exc)}") from exc
    if not isinstance(document, dict):
        raise ConfigError("settings root must be a JSON object")

    settings_dir = absolute_path.parent
    if "packet_log_path" in document:
        packet_path = _resolve_log_path(document["packet_log_path"], "packet_log_path", settings_dir)
    else:
        packet_path = _new_log_path(settings_dir, "udp_packets")
    if "event_log_path" in document:
        event_path = _resolve_log_path(document["event_log_path"], "event_log_path", settings_dir)
    else:
        event_path = _new_log_path(settings_dir, "udp_events")

    same_path = packet_path == event_path
    if not same_path and packet_path.exists() and event_path.exists():
        try:
            same_path = os.path.samefile(packet_path, event_path)
        except OSError:
            pass
    if same_path:
        raise ConfigError("packet_log_path and event_log_path must refer to different files")

    for key, path in (("packet_log_path", packet_path), ("event_log_path", event_path)):
        if not path.parent.is_dir():
            raise ConfigError(f"parent directory for {key} does not exist: {path.parent}")

    return Settings(
        listen_address=_ipv4(document, "listen_address"),
        listen_port=_port(document, "listen_port"),
        destination_address=_ipv4(document, "destination_address"),
        destination_port=_port(document, "destination_port"),
        packet_log_path=packet_path,
        event_log_path=event_path,
        packet_log_queue_limit_bytes=_positive_integer(
            document, "packet_log_queue_limit_bytes", DEFAULT_PACKET_QUEUE_BYTES
        ),
        packet_log_queue_limit_packets=_positive_integer(
            document, "packet_log_queue_limit_packets", DEFAULT_PACKET_QUEUE_PACKETS
        ),
        receive_socket_buffer_bytes=_positive_integer(
            document, "receive_socket_buffer_bytes", DEFAULT_RECEIVE_BUFFER_BYTES
        ),
    )


def _open_one_log(path: Path, explicit: bool) -> TextIO:
    if explicit:
        return path.open("a", encoding="utf-8")
    # Exclusive creation protects the auto-generated name from a concurrent start.
    return path.open("x", encoding="utf-8")


def open_log_files(settings: Settings, document: dict[str, object]) -> tuple[TextIO, TextIO]:
    packet_explicit = "packet_log_path" in document
    event_explicit = "event_log_path" in document
    packet_file: TextIO | None = None

    def open_path(path: Path, explicit: bool, prefix: str) -> tuple[TextIO, Path]:
        current = path
        while True:
            try:
                return _open_one_log(current, explicit), current
            except FileExistsError:
                if explicit:
                    raise
                current = _new_log_path(path.parent, prefix)

    try:
        packet_file, packet_path = open_path(settings.packet_log_path, packet_explicit, "udp_packets")
        settings.packet_log_path = packet_path
        event_file, event_path = open_path(settings.event_log_path, event_explicit, "udp_events")
        settings.event_log_path = event_path
        return packet_file, event_file
    except OSError:
        if packet_file is not None:
            packet_file.close()
        raise


def load_settings_document(settings_path: str | Path) -> tuple[Settings, dict[str, object]]:
    supplied_path = Path(settings_path).expanduser()
    try:
        absolute_path = supplied_path.resolve(strict=True)
        document = json.loads(absolute_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read settings file: {safe_one_line(exc)}") from exc
    if not isinstance(document, dict):
        raise ConfigError("settings root must be a JSON object")
    # Re-read through the validated loader to keep one source of validation rules.
    return load_settings(absolute_path), document


@dataclass(frozen=True)
class PacketRecord:
    received_at: datetime
    sequence: int
    payload: bytes


class FailureSignal:
    """Shares fatal writer failures with the UI and all network workers."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self._lock = threading.Lock()
        self.message: str | None = None
        self._notify: Callable[[str], None] | None = None
        self._record_failure: Callable[[str], None] | None = None

    def bind_notify(self, notify: Callable[[str], None]) -> None:
        with self._lock:
            self._notify = notify

    def bind_event_recorder(self, record_failure: Callable[[str], None]) -> None:
        with self._lock:
            self._record_failure = record_failure

    def fail(self, message: str, *, source: str = "application") -> None:
        clean = safe_one_line(message)
        with self._lock:
            if self.message is not None:
                return
            self.message = clean
            notify = self._notify
            record_failure = self._record_failure
            self.event.set()
        if notify is not None:
            notify(clean)
        sys.stderr.write(error_line(clean) + "\n")
        sys.stderr.flush()
        if source != "event-log" and record_failure is not None:
            record_failure(clean)


class UIBridge:
    """Bounded worker-to-Textual mailbox with coalesced receive summaries."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: collections.deque[str] = collections.deque(maxlen=UI_MAX_EVENTS)
        self._display_dropped = 0
        self._receive: tuple[datetime, int, int, int] | None = None
        self._receive_count = 0
        self._pending = False
        self._callback_scheduled = False
        self._loop = None
        self._app: UDPApp | None = None
        self._last_delivery = 0.0

    def bind(self, loop: object, app: UDPApp) -> None:
        with self._lock:
            self._loop = loop
            self._app = app
            self._pending = True
        self.request_refresh()

    def unbind(self, app: UDPApp) -> None:
        with self._lock:
            if self._app is app:
                self._app = None
                self._loop = None
                self._callback_scheduled = False

    def event(self, when: datetime, level: str, message: str) -> None:
        row = f"[{timestamp(when)}] [{level}] {safe_one_line(message)}"
        with self._lock:
            if len(self._events) == self._events.maxlen:
                self._display_dropped += 1
            self._events.append(row)
            self._pending = True
        self.request_refresh()

    def receive(self, when: datetime, size: int, sequence: int, total: int) -> None:
        with self._lock:
            if self._receive is None:
                self._receive_count = 0
            self._receive = (when, size, sequence, total)
            self._receive_count += 1
            self._pending = True
        self.request_refresh()

    def request_refresh(self) -> None:
        with self._lock:
            loop = self._loop
            if (
                loop is None
                or self._callback_scheduled
                or time.monotonic() - self._last_delivery < UI_REFRESH_INTERVAL
            ):
                return
            self._callback_scheduled = True
        try:
            loop.call_soon_threadsafe(self._deliver)  # type: ignore[attr-defined]
        except RuntimeError:
            with self._lock:
                self._callback_scheduled = False

    def pump_main(self, force: bool = False) -> None:
        self._deliver(force=force)

    def _deliver(self, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            self._callback_scheduled = False
            if not self._pending or (
                not force and now - self._last_delivery < UI_REFRESH_INTERVAL
            ):
                return
            self._pending = False
            self._last_delivery = now
            app = self._app
            events = list(self._events)
            dropped = self._display_dropped
            self._display_dropped = 0
            receive = self._receive if self._receive_count else None
            receive_count = self._receive_count
            self._receive_count = 0
        if app is not None:
            app.refresh_from_workers(events, dropped, receive, receive_count)


class EventLogWriter:
    def __init__(self, output: TextIO, failures: FailureSignal) -> None:
        self._output = output
        self._failures = failures
        self._queue: queue.Queue[object] = queue.Queue(maxsize=EVENT_LOG_QUEUE_LIMIT)
        self._fatal_queue: queue.Queue[tuple[datetime, str, str]] = queue.Queue(maxsize=1)
        self._thread = threading.Thread(target=self._run, name="event-log-writer", daemon=True)
        self._started = False
        self._stopped = False
        self._lock = threading.Lock()
        self._dropped_pending = 0
        self._dropped_total = 0
        self.written = 0

    @property
    def dropped_total(self) -> int:
        with self._lock:
            return self._dropped_total

    def start(self) -> None:
        self._started = True
        self._thread.start()

    def emit(
        self,
        level: str,
        message: str,
        when: datetime | None = None,
        *,
        block: bool = True,
    ) -> None:
        if self._stopped or self._failures.event.is_set():
            return
        item = (when or datetime.now().astimezone(), level, safe_one_line(message))
        if not block:
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                with self._lock:
                    self._dropped_pending += 1
                    self._dropped_total += 1
            return
        while not self._failures.event.is_set():
            try:
                self._queue.put(item, timeout=0.1)
                return
            except queue.Full:
                continue

    def emit_failure(self, message: str) -> None:
        """Record another writer's fatal error through this healthy writer."""
        if not self._started or self._stopped or not self._thread.is_alive():
            return
        record = (datetime.now().astimezone(), "error", f"Error: {safe_one_line(message)}")
        try:
            self._fatal_queue.put_nowait(record)
        except queue.Full:
            return
        try:
            self._queue.put_nowait(_EVENT_WAKE)
        except queue.Full:
            # The writer is already draining regular records and checks the
            # priority slot before taking each next item.
            pass

    def stop(self) -> None:
        if self._stopped:
            return
        if self._started:
            with self._lock:
                dropped = self._dropped_pending
                self._dropped_pending = 0
            if dropped and self._thread.is_alive() and not self._failures.event.is_set():
                self.emit("warn", f"Warning: event log queue overflow; dropped {dropped} events")
            self._stopped = True
            while self._thread.is_alive():
                try:
                    self._queue.put(None, timeout=0.1)
                    break
                except queue.Full:
                    continue
            self._thread.join()
        else:
            self._stopped = True
            self._output.flush()
            self._output.close()

    def _run(self) -> None:
        pending_lines = 0
        last_flush = time.monotonic()
        try:
            while True:
                try:
                    item = self._fatal_queue.get_nowait()
                except queue.Empty:
                    timeout = max(0.01, FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush))
                    try:
                        item = self._queue.get(timeout=timeout)
                    except queue.Empty:
                        item = False
                if item is _EVENT_WAKE:
                    continue
                if item is None:
                    break
                if item is not False:
                    when, level, message = item
                    self._output.write(f"[{timestamp(when)}] [{level}] {message}\n")
                    pending_lines += 1
                    self.written += 1
                if pending_lines and (
                    pending_lines >= FLUSH_LINE_COUNT
                    or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS
                ):
                    self._output.flush()
                    pending_lines = 0
                    last_flush = time.monotonic()
            # Sentinel follows all accepted events, so the queue is drained here.
            self._output.flush()
        except Exception as exc:
            self._failures.fail(
                f"event log writer failed: {safe_one_line(exc)}",
                source="event-log",
            )
        finally:
            try:
                self._output.close()
            except Exception as exc:
                self._failures.fail(
                    f"event log close failed: {safe_one_line(exc)}",
                    source="event-log",
                )


class PacketLogWriter:
    def __init__(
        self,
        output: TextIO,
        failures: FailureSignal,
        limit_bytes: int,
        limit_packets: int,
    ) -> None:
        self._output = output
        self._failures = failures
        self._limit_bytes = limit_bytes
        self._limit_packets = limit_packets
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._records: collections.deque[PacketRecord] = collections.deque()
        self._queued_bytes = 0
        self._max_depth = 0
        self._max_bytes = 0
        self._enqueued = 0
        self._written = 0
        self._dropped = 0
        self._stopping = False
        self._started = False
        self._thread = threading.Thread(target=self._run, name="packet-log-writer", daemon=True)

    def start(self) -> None:
        self._started = True
        self._thread.start()

    def enqueue(self, record: PacketRecord) -> bool:
        size = len(record.payload)
        with self._condition:
            if self._stopping or self._failures.event.is_set():
                return False
            if (
                len(self._records) >= self._limit_packets
                or size > self._limit_bytes - self._queued_bytes
            ):
                self._dropped += 1
                return False
            self._records.append(record)
            self._queued_bytes += size
            self._enqueued += 1
            self._max_depth = max(self._max_depth, len(self._records))
            self._max_bytes = max(self._max_bytes, self._queued_bytes)
            self._condition.notify()
            return True

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "enqueued": self._enqueued,
                "written": self._written,
                "dropped": self._dropped,
                "depth": len(self._records),
                "bytes": self._queued_bytes,
                "max_depth": self._max_depth,
                "max_bytes": self._max_bytes,
            }

    def stop(self) -> None:
        if not self._started:
            self._output.flush()
            self._output.close()
            return
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        self._thread.join()

    def _run(self) -> None:
        pending_lines = 0
        pending_written = 0
        last_flush = time.monotonic()
        try:
            while True:
                with self._condition:
                    if not self._records and not self._stopping:
                        remaining = FLUSH_INTERVAL_SECONDS - (time.monotonic() - last_flush)
                        self._condition.wait(timeout=max(0.01, remaining))
                    if self._records:
                        record = self._records.popleft()
                        self._queued_bytes -= len(record.payload)
                    elif self._stopping:
                        break
                    else:
                        record = None
                if record is not None:
                    hex_payload = record.payload.hex().upper()
                    summary = f"Received({len(record.payload)}): {record.sequence}: {hex_payload}"
                    self._output.write(f"[{timestamp(record.received_at)}] [info] {summary}\n")
                    pending_lines += 1
                    pending_written += 1
                if pending_lines and (
                    pending_lines >= FLUSH_LINE_COUNT
                    or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS
                ):
                    self._output.flush()
                    with self._lock:
                        self._written += pending_written
                    pending_lines = 0
                    pending_written = 0
                    last_flush = time.monotonic()
            self._output.flush()
            with self._lock:
                self._written += pending_written
        except Exception as exc:
            self._failures.fail(
                f"packet log writer failed: {safe_one_line(exc)}",
                source="packet-log",
            )
        finally:
            try:
                self._output.close()
            except Exception as exc:
                self._failures.fail(
                    f"packet log close failed: {safe_one_line(exc)}",
                    source="packet-log",
                )


def parse_hex_payload(line: str, line_number: int) -> bytes:
    digits: list[str] = []
    for character in line:
        if character.isspace():
            continue
        if character not in "0123456789abcdefABCDEF":
            raise SendFileError(line_number, "invalid hexadecimal character")
        digits.append(character)
        if len(digits) > MAX_PAYLOAD_SIZE * 2:
            raise SendFileError(line_number, "decoded payload exceeds 32768 bytes")
    if len(digits) % 2:
        raise SendFileError(line_number, "hexadecimal payload has an odd number of digits")
    return bytes.fromhex("".join(digits))


def parse_wait_ms(argument: str, line_number: int) -> int:
    compact = "".join(character for character in argument if not character.isspace())
    if not compact:
        raise SendFileError(line_number, "wait delay is missing")
    if not compact.isascii() or not compact.isdecimal():
        raise SendFileError(line_number, "delay must be a decimal integer in milliseconds")
    delay = int(compact)
    if delay > MAX_WAIT_MS:
        raise SendFileError(line_number, "delay exceeds 1800000 milliseconds")
    return delay


def parse_payload_line(line: str, line_number: int) -> tuple[str, bytes | int | None]:
    if not line.strip() or line.startswith("#"):
        return "ignore", None
    if line.startswith("/"):
        command_end = 1
        while command_end < len(line) and not line[command_end].isspace():
            command_end += 1
        command = line[1:command_end]
        if not command:
            raise SendFileError(line_number, "special command name is missing")
        if command != "Wait":
            raise SendFileError(line_number, "unsupported special command")
        if command_end == len(line):
            raise SendFileError(line_number, "wait delay is missing")
        argument = line[command_end:].lstrip()
        if not argument:
            raise SendFileError(line_number, "wait delay is missing")
        return "wait", parse_wait_ms(argument, line_number)
    return "send", parse_hex_payload(line, line_number)


def parse_send_path(command: str) -> Path:
    remainder = command[len("send") :]
    if not remainder or not remainder[0].isspace():
        raise ValueError("usage: send <file-path>")
    path_text = remainder.strip()
    if not path_text:
        raise ValueError("usage: send <file-path>")
    if path_text[0] in "'\"":
        quote = path_text[0]
        if len(path_text) < 2 or path_text[-1] != quote:
            raise ValueError("file path has an unmatched quote")
        path_text = path_text[1:-1]
    elif path_text[-1] in "'\"":
        raise ValueError("file path has an unmatched quote")
    if not path_text:
        raise ValueError("file path is empty")
    return Path(path_text)


@dataclass(frozen=True)
class CompletionCandidate:
    value: str
    label: str
    is_directory: bool


@dataclass(frozen=True)
class CompletionResults:
    candidates: tuple[CompletionCandidate, ...]
    truncated: bool = False


@dataclass(frozen=True)
class CompletionQuery:
    command_prefix: str
    path_text: str
    quote: str | None


def parse_completion_query(value: str) -> CompletionQuery | None:
    """Return the send path being edited, if the whole input is a completable send."""
    if not value.startswith("send") or len(value) == 4 or not value[4].isspace():
        return None
    path_start = 4
    while path_start < len(value) and value[path_start].isspace():
        path_start += 1
    raw_path = value[path_start:]
    command_prefix = value[:path_start]
    quote: str | None = None
    if raw_path.startswith(("'", '"')):
        quote = raw_path[0]
        if len(raw_path) > 1 and raw_path[-1] == quote:
            # A closed quoted path is already complete; a trailing quote in an
            # unquoted path is invalid according to parse_send_path as well.
            return None
        path_text = raw_path[1:]
    else:
        if raw_path.endswith(("'", '"')):
            return None
        # Unquoted send paths are trimmed at submission time.
        path_text = raw_path.strip()
    return CompletionQuery(command_prefix, path_text, quote)


def scan_path_candidates(path_text: str, command_prefix: str, quote: str | None, base: Path) -> CompletionResults:
    """Scan a directory for matching send paths. Call from a worker thread."""
    separators = {os.sep}
    if os.altsep:
        separators.add(os.altsep)
    has_trailing_separator = bool(path_text) and path_text[-1] in separators

    if path_text in (".", ".."):
        directory_path = base if path_text == "." else base.parent
        if directory_path.is_dir():
            completed_path = "./" if path_text == "." else "../"
            completed_value = f"{command_prefix}{quote or ''}{completed_path}"
            candidate = CompletionCandidate(completed_value, completed_path, True)
            return CompletionResults((candidate,))

    if not path_text:
        directory_part = Path(".")
        typed_directory = ""
        name_prefix = ""
    elif has_trailing_separator:
        directory_part = Path(path_text)
        typed_directory = path_text
        name_prefix = ""
    else:
        path = Path(path_text)
        name_prefix = path.name
        directory_part = path.parent
        typed_directory = path_text[: len(path_text) - len(name_prefix)] if name_prefix else path_text

    directory = directory_part if directory_part.is_absolute() else base / directory_part
    include_hidden = name_prefix.startswith(".")
    case_sensitive = os.name != "nt"
    candidates: list[tuple[str, str, CompletionCandidate]] = []
    retained_limit = COMPLETION_OPTION_LIMIT + 1
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                name = entry.name
                if not include_hidden and name.startswith("."):
                    continue
                matches = name.startswith(name_prefix) if case_sensitive else name.casefold().startswith(name_prefix.casefold())
                if not matches:
                    continue
                name_key = (name.casefold(), name)
                if len(candidates) >= retained_limit and name_key >= candidates[-1][:2]:
                    continue
                try:
                    is_directory = entry.is_dir(follow_symlinks=True)
                    is_file = entry.is_file(follow_symlinks=True)
                except OSError:
                    continue
                if not is_directory and not is_file:
                    continue
                suffix = "/" if is_directory else ""
                completed_path = f"{typed_directory}{name}{suffix}"
                if quote:
                    if quote in completed_path:
                        continue
                    completed_value = f"{command_prefix}{quote}{completed_path}"
                    if not is_directory:
                        completed_value += quote
                else:
                    needs_quotes = (
                        completed_path != completed_path.strip()
                        or completed_path.startswith(("'", '"'))
                        or completed_path.endswith(("'", '"'))
                    )
                    if needs_quotes:
                        candidate_quote = next((mark for mark in ('"', "'") if mark not in completed_path), None)
                        if candidate_quote is None:
                            continue
                        completed_value = f"{command_prefix}{candidate_quote}{completed_path}"
                        if not is_directory:
                            completed_value += candidate_quote
                    else:
                        completed_value = f"{command_prefix}{completed_path}"
                candidate = CompletionCandidate(completed_value, f"{name}{suffix}", is_directory)
                bisect.insort(candidates, (name_key[0], name_key[1], candidate))
                if len(candidates) > retained_limit:
                    candidates.pop()
    except OSError:
        return CompletionResults(())

    truncated = len(candidates) > COMPLETION_OPTION_LIMIT
    return CompletionResults(tuple(item[2] for item in candidates[:COMPLETION_OPTION_LIMIT]), truncated)


class SendPathSuggester(Suggester):
    """Provides the first matching path as Textual's inline completion."""

    def __init__(self, app: UDPApp, base: Path) -> None:
        super().__init__(use_cache=False, case_sensitive=True)
        self._app = app
        self._base = base

    async def get_suggestion(self, value: str) -> str | None:
        query = parse_completion_query(value)
        if query is None or not self._app._can_inline_complete(value):
            return None
        results = await asyncio.to_thread(
            scan_path_candidates,
            query.path_text,
            query.command_prefix,
            query.quote,
            self._base,
        )
        if not self._app._can_inline_complete(value) or not results.candidates:
            return None
        suggestion = results.candidates[0].value
        return None if suggestion == value else suggestion


class CommandInput(Input):
    """Input with command history and completion-mode navigation keys."""

    BINDINGS = [
        *Input.BINDINGS,
        Binding("up", "cursor_up", "Previous command or completion", show=False, priority=True),
        Binding("down", "cursor_down", "Next command or completion", show=False, priority=True),
        Binding("tab", "completion_tab", "Complete path", show=False, priority=True),
        Binding("escape", "completion_escape", "Cancel completion", show=False, priority=True),
    ]

    def _watch_selection(self, selection: object) -> None:
        super()._watch_selection(selection)
        suggester = getattr(self, "suggester", None)
        if not self.cursor_at_end:
            self._suggestion = ""
        elif suggester is not None and self.value:
            self.run_worker(suggester._get_suggestion(self, self.value))

    async def action_submit(self) -> None:
        app = self.app
        if isinstance(app, UDPApp) and app._completion_mode:
            app._accept_completion()
            return
        await super().action_submit()

    def action_cursor_right(self, select: bool = False) -> None:
        app = self.app
        if isinstance(app, UDPApp) and app._completion_mode and not select:
            app._accept_completion()
            return
        super().action_cursor_right(select)

    def action_completion_tab(self) -> None:
        if isinstance(self.app, UDPApp):
            if self.app._completion_mode:
                self.app._accept_completion()
            else:
                self.app._request_completion_from_tab()

    def action_completion_escape(self) -> None:
        if isinstance(self.app, UDPApp):
            self.app._cancel_completion()

    def action_cursor_up(self) -> None:
        if isinstance(self.app, UDPApp):
            if self.app._completion_mode:
                self.app._move_completion(-1)
            else:
                self.app._history_up()

    def action_cursor_down(self) -> None:
        if isinstance(self.app, UDPApp):
            if self.app._completion_mode:
                self.app._move_completion(1)
            else:
                self.app._history_down()


class CompletionOptionList(OptionList):
    """Candidate display that never takes focus away from the command prompt."""

    can_focus = False


@dataclass(frozen=True)
class SendJob:
    path: Path


class SenderWorker:
    def __init__(self, engine: Runtime) -> None:
        self.engine = engine
        self._condition = threading.Condition()
        self._jobs: collections.deque[SendJob] = collections.deque()
        self._thread = threading.Thread(target=self._run, name="udp-sender", daemon=True)
        self._stopping = False
        self._paused = False
        self._current_path: Path | None = None
        self._current_line = 0
        self._last_failed_path: Path | None = None
        self._last_failed_line: int | None = None
        self._completed = 0
        self._failed = 0
        self._sent_datagrams = 0

    def start(self) -> None:
        self._thread.start()

    def enqueue(self, path: Path) -> int:
        with self._condition:
            if self._stopping:
                raise RuntimeError("application is shutting down")
            self._jobs.append(SendJob(path))
            count = len(self._jobs)
            self._condition.notify()
            return count

    def resume(self) -> bool:
        with self._condition:
            if not self._paused or self._stopping:
                return False
            self._paused = False
            self._condition.notify_all()
            return True

    def request_stop(self) -> tuple[int, Path | None, int]:
        with self._condition:
            if self._stopping:
                return 0, self._current_path, self._current_line
            dropped = len(self._jobs)
            self._jobs.clear()
            self._stopping = True
            current_path = self._current_path
            current_line = self._current_line
            self._condition.notify_all()
            return dropped, current_path, current_line

    def join(self) -> None:
        if self._thread.ident is not None:
            self._thread.join()

    def snapshot(self) -> dict[str, object]:
        with self._condition:
            if self._stopping:
                state = "stopping"
            elif self._paused:
                state = "paused"
            elif self._current_path is not None:
                state = "sending"
            else:
                state = "idle"
            return {
                "state": state,
                "current_path": str(self._current_path) if self._current_path else None,
                "current_line": self._current_line,
                "last_failed_path": str(self._last_failed_path) if self._last_failed_path else None,
                "last_failed_line": self._last_failed_line,
                "queued": len(self._jobs),
                "completed": self._completed,
                "failed": self._failed,
                "sent_datagrams": self._sent_datagrams,
            }

    def _run(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._stopping or (self._jobs and not self._paused))
                if self._stopping:
                    return
                job = self._jobs.popleft()
                self._current_path = job.path
                self._current_line = 0
            self.engine.log("info", f"send started: {job.path}")
            error: SendFileError | None = None
            try:
                self._process_file(job.path)
            except SendFileError as exc:
                error = exc
            except OSError as exc:
                error = SendFileError(max(1, self._current_line), f"file I/O failed: {safe_one_line(exc)}")
            except Exception as exc:
                error = SendFileError(max(1, self._current_line), f"send processing failed: {safe_one_line(exc)}")
            with self._condition:
                if self._stopping or self.engine.failures.event.is_set():
                    self._current_path = None
                    self._current_line = 0
                    return
                if error is None:
                    self._completed += 1
                    self._current_path = None
                    self._current_line = 0
                    completed = True
                else:
                    self._failed += 1
                    self._last_failed_path = job.path
                    self._last_failed_line = error.line_number
                    self._paused = True
                    self._current_path = None
                    self._current_line = 0
                    completed = False
            if completed:
                self.engine.log("info", f"send file completed: {job.path}")
            else:
                if str(error).startswith("UDP send failed ("):
                    event_message = f"{safe_one_line(error)}; file {job.path} line {error.line_number}"
                else:
                    event_message = f"{job.path} line {error.line_number}: {safe_one_line(error)}"
                self.engine.log("error", event_message)
            self.engine.ui.request_refresh()

    def _process_file(self, path: Path) -> None:
        line_number = 0
        try:
            source = path.open("r", encoding="utf-8", newline=None)
        except OSError as exc:
            raise SendFileError(1, f"unable to open file: {safe_one_line(exc)}") from exc
        with source:
            while not self.engine.stop_event.is_set() and not self.engine.failures.event.is_set():
                try:
                    line = source.readline()
                except (OSError, UnicodeError) as exc:
                    raise SendFileError(max(1, line_number + 1), f"file read failed: {safe_one_line(exc)}") from exc
                if line == "":
                    break
                line_number += 1
                with self._condition:
                    self._current_line = line_number
                operation, value = parse_payload_line(line.rstrip("\r\n"), line_number)
                if operation == "ignore":
                    continue
                if operation == "wait":
                    if self.engine.stop_event.wait(int(value) / 1000) or self.engine.failures.event.is_set():
                        return
                    continue
                payload = value
                assert isinstance(payload, bytes)
                destination = (self.engine.settings.destination_address, self.engine.settings.destination_port)
                try:
                    sent = self.engine.sock.sendto(payload, destination)
                    sent_at = datetime.now().astimezone()
                    if sent != len(payload):
                        raise OSError(f"short UDP send: {sent} of {len(payload)} bytes")
                except OSError as exc:
                    raise SendFileError(
                        line_number,
                        f"UDP send failed ({len(payload)} bytes to {destination[0]}:{destination[1]}): {safe_one_line(exc)}",
                    ) from exc
                with self._condition:
                    self._sent_datagrams += 1
                self.engine.log(
                    "info",
                    f"Transmitted({len(payload)}): {destination[0]}:{destination[1]}",
                    when=sent_at,
                )


class Runtime:
    def __init__(
        self,
        settings: Settings,
        packet_output: TextIO,
        event_output: TextIO,
        sock: socket.socket,
        receive_buffer_effective: int | None,
        receive_buffer_error: str | None,
        os_drop_supported: bool,
    ) -> None:
        self.settings = settings
        self.sock = sock
        self.receive_buffer_effective = receive_buffer_effective
        self.receive_buffer_error = receive_buffer_error
        self.os_drop_supported = os_drop_supported
        self.os_drops: int | None = 0 if os_drop_supported else None
        self.failures = FailureSignal()
        self.ui = UIBridge()
        self.events = EventLogWriter(event_output, self.failures)
        self.packet_logs = PacketLogWriter(
            packet_output,
            self.failures,
            settings.packet_log_queue_limit_bytes,
            settings.packet_log_queue_limit_packets,
        )
        self.sender = SenderWorker(self)
        self.stop_event = threading.Event()
        self.receive_stop = threading.Event()
        self._receive_thread = threading.Thread(target=self._receive_loop, name="udp-receiver", daemon=True)
        self._state_lock = threading.Lock()
        self.received = 0
        self._sequence = 0
        self._receive_error_count = 0
        self._oversize_count = 0
        self._last_oversize_report = time.monotonic()
        self._reported_log_drops = 0
        self._last_log_drop_report = time.monotonic()
        self._shutdown_lock = threading.Lock()
        self._started = False
        self._shutdown = False
        self.failures.bind_notify(self._on_failure)
        self.failures.bind_event_recorder(self.events.emit_failure)

    def _on_failure(self, message: str) -> None:
        self.ui.event(
            datetime.now().astimezone(),
            "error",
            f"Error: {safe_one_line(message)}",
        )

    def start(self) -> None:
        self.events.start()
        self.packet_logs.start()
        self._started = True
        self.log("info", "application started")
        self.log(
            "info",
            f"UDP bound to {self.settings.listen_address}:{self.settings.listen_port}; "
            f"destination {self.settings.destination_address}:{self.settings.destination_port}",
        )
        if self.receive_buffer_error:
            effective = str(self.receive_buffer_effective) if self.receive_buffer_effective is not None else "unavailable"
            self.log(
                "warn",
                f"SO_RCVBUF setting failed: {self.receive_buffer_error}; OS-reported value: {effective}",
            )
        else:
            effective = str(self.receive_buffer_effective) if self.receive_buffer_effective is not None else "unavailable"
            self.log(
                "info",
                f"SO_RCVBUF requested={self.settings.receive_socket_buffer_bytes}; effective={effective}",
            )
        self.log(
            "info",
            "OS UDP receive drops: " + ("available" if self.os_drop_supported else "unavailable"),
        )
        self._receive_thread.start()
        self.sender.start()

    def log(
        self,
        level: str,
        message: str,
        when: datetime | None = None,
        *,
        block: bool = True,
    ) -> None:
        actual_time = when or datetime.now().astimezone()
        clean = safe_one_line(message)
        self.events.emit(level, clean, actual_time, block=block)
        self.ui.event(actual_time, level, clean)

    def snapshot(self) -> dict[str, object]:
        with self._state_lock:
            received = self.received
            rx_errors = self._receive_error_count
        result: dict[str, object] = {
            "received": received,
            "receive_state": "stopping" if self.receive_stop.is_set() else "active",
            "rx_errors": rx_errors,
            "os_drops": self.os_drops,
            "socket_buffer_requested": self.settings.receive_socket_buffer_bytes,
            "socket_buffer_effective": self.receive_buffer_effective,
            "socket_buffer_error": self.receive_buffer_error,
        }
        result.update(self.packet_logs.snapshot())
        result.update(self.sender.snapshot())
        return result

    def _read_os_drop_count(self, ancillary: list[tuple[int, int, bytes]]) -> None:
        option = self._so_rxq_overflow_option()
        if option is None:
            return
        for level, kind, data in ancillary:
            if level == socket.SOL_SOCKET and kind == option and len(data) >= 4:
                current = struct.unpack("=I", data[:4])[0]
                previous = self.os_drops or 0
                self.os_drops = previous + ((current - previous) & 0xFFFFFFFF)

    def _receive_loop(self) -> None:
        use_recvmsg = self.os_drop_supported and hasattr(self.sock, "recvmsg")
        while not self.receive_stop.is_set() and not self.failures.event.is_set():
            try:
                if use_recvmsg:
                    payload, ancillary, _flags, _peer = self.sock.recvmsg(65_535, 256)
                    self._read_os_drop_count(ancillary)
                else:
                    payload, _peer = self.sock.recvfrom(65_535)
            except socket.timeout:
                self._report_oversize_batch()
                self._report_log_drop_batch()
                continue
            except OSError as exc:
                if self.receive_stop.is_set():
                    break
                with self._state_lock:
                    self._receive_error_count += 1
                self.log("error", f"UDP receive failed: {safe_one_line(exc)}", block=False)
                if self.failures.event.wait(0.05):
                    break
                continue

            received_at = datetime.now().astimezone()
            with self._state_lock:
                self._sequence += 1
                sequence = self._sequence
                self.received += 1
                total = self.received
            record = PacketRecord(received_at, sequence, bytes(payload))
            accepted = self.packet_logs.enqueue(record)
            self.ui.receive(received_at, len(payload), sequence, total)
            if len(payload) > MAX_PAYLOAD_SIZE:
                self._oversize_count += 1
                self._report_oversize_batch()
            if not accepted:
                self._report_log_drop_batch()
        self._report_oversize_batch(force=True)
        self._report_log_drop_batch(force=True)

    def _report_oversize_batch(self, force: bool = False) -> None:
        now = time.monotonic()
        if self._oversize_count and (force or now - self._last_oversize_report >= 1.0):
            count = self._oversize_count
            self._oversize_count = 0
            self._last_oversize_report = now
            self.log(
                "warn",
                f"received {count} UDP datagrams larger than 32768 bytes",
                block=False,
            )

    def _report_log_drop_batch(self, force: bool = False) -> None:
        now = time.monotonic()
        total = self.packet_logs.snapshot()["dropped"]
        if total > self._reported_log_drops and (
            force or now - self._last_log_drop_report >= 1.0
        ):
            dropped = total - self._reported_log_drops
            self._reported_log_drops = total
            self._last_log_drop_report = now
            self.log(
                "warn",
                f"Warning: packet log queue overflow; discarded {dropped} packet logs",
                block=False,
            )

    @staticmethod
    def _so_rxq_overflow_option() -> int | None:
        option = getattr(socket, "SO_RXQ_OVFL", None)
        if option is None and sys.platform.startswith("linux"):
            return LINUX_SO_RXQ_OVFL
        return option

    def request_stop(self) -> None:
        self.stop_event.set()
        self.receive_stop.set()
        dropped, current_path, current_line = self.sender.request_stop()
        stopped = f"{current_path} line {current_line}" if current_path else "none"
        self.log(
            "info",
            f"shutdown requested; discarded queued files={dropped}; stopped file={stopped}",
            block=False,
        )

    def shutdown(self) -> None:
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True
        if not self.stop_event.is_set():
            self.request_stop()
        self.sender.join()
        if self._started and self._receive_thread.ident is not None:
            self._receive_thread.join(timeout=0.6)
            if self._receive_thread.is_alive():
                try:
                    self.sock.close()
                except OSError:
                    pass
                # The receiver has a 250 ms socket timeout; wait for its final
                # callback and enqueue to finish before draining/closing packet logs.
                self._receive_thread.join()
        try:
            self.sock.close()
        except OSError:
            pass
        if self._started:
            self.packet_logs.stop()
            stats = self.packet_logs.snapshot()
            os_drops = str(self.os_drops) if self.os_drops is not None else "unavailable"
            self.log(
                "info",
                f"shutdown summary: received={self.received}; packet_logs_written={stats['written']}; "
                f"packet_log_queue_dropped={stats['dropped']}; os_udp_drops={os_drops}; "
                f"packet_log_queue_depth={stats['depth']}; "
                f"event_log_queue_dropped={self.events.dropped_total}",
            )
            self.events.stop()
        else:
            self.packet_logs.stop()
            self.events.stop()


def create_runtime(settings_path: str | Path) -> Runtime:
    settings, document = load_settings_document(settings_path)
    try:
        packet_output, event_output = open_log_files(settings, document)
    except OSError as exc:
        raise ConfigError(f"cannot open log file: {safe_one_line(exc)}") from exc

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except OSError as exc:
        packet_output.close()
        event_output.close()
        raise ConfigError(f"cannot create UDP socket: {safe_one_line(exc)}") from exc
    receive_buffer_error: str | None = None
    try:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, settings.receive_socket_buffer_bytes)
        except (OSError, OverflowError) as exc:
            receive_buffer_error = safe_one_line(exc)
        try:
            receive_buffer_effective: int | None = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        except OSError:
            receive_buffer_effective = None
        os_drop_supported = False
        overflow_option = Runtime._so_rxq_overflow_option()
        if overflow_option is not None and hasattr(sock, "recvmsg"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, overflow_option, 1)
                os_drop_supported = True
            except OSError:
                os_drop_supported = False
        sock.bind((settings.listen_address, settings.listen_port))
        sock.settimeout(0.25)
    except OSError as exc:
        packet_output.close()
        event_output.close()
        sock.close()
        raise ConfigError(f"UDP socket setup failed: {safe_one_line(exc)}") from exc
    return Runtime(
        settings,
        packet_output,
        event_output,
        sock,
        receive_buffer_effective,
        receive_buffer_error,
        os_drop_supported,
    )


class UDPApp(App[None]):
    TITLE = "UDP Communication"
    # A ten-row outer height leaves eight option rows inside OptionList's border.
    CSS = """
    Screen { layout: vertical; }
    #connection { height: auto; padding: 0 1; }
    #panels { height: 1fr; grid-size: 2 1; grid-columns: 1fr 2fr; grid-rows: 1fr; grid-gutter: 1; }
    #status-panel, #events-panel { border: round $primary; padding: 0 1; }
    #status-panel { width: 1fr; }
    #events-panel { width: 2fr; }
    #status-text, #event-list { width: 1fr; height: auto; }
    #command-area { dock: bottom; height: auto; width: 100%; }
    #completion-list {
        width: 100%;
        height: auto;
        max-height: 10;
        display: none;
        border: round $accent;
        background: $surface;
    }
    """

    BINDINGS = [("ctrl+c", "quit_app", "Quit")]

    def __init__(self, runtime: Runtime, *, completion_base: Path | None = None) -> None:
        super().__init__()
        self.runtime = runtime
        self._completion_base = (completion_base or Path.cwd()).resolve()
        self._suggester = SendPathSuggester(self, self._completion_base)
        self._udpcomapp_return_code = 0
        self._fatal_shutdown_started = False
        self._visible_events: collections.deque[str] = collections.deque(maxlen=UI_MAX_EVENTS)
        self._last_receive_row: str | None = None
        self._last_receive_count = 0
        self._history: list[str] = []
        self._history_index: int | None = None
        self._history_draft = ""
        self._completion_mode = False
        self._completion_request_id = 0
        self._pending_tab_value: str | None = None
        self._completion_value: str | None = None
        self._completion_candidates: tuple[CompletionCandidate, ...] = ()
        self._completion_truncated = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Static(self._connection_text(), id="connection", markup=False)
        with Grid(id="panels"):
            with VerticalScroll(id="status-panel"):
                yield Label("Status", id="status-heading")
                yield Static("Starting…", id="status-text", markup=False)
            with VerticalScroll(id="events-panel"):
                yield Label("Recent events (up to 100)", id="events-heading")
                yield Static("", id="event-list", markup=False)
        with Vertical(id="command-area"):
            yield CompletionOptionList(id="completion-list", markup=False)
            yield CommandInput(
                placeholder="send <file-path> | status | help | resume | quit",
                id="command-input",
                suggester=self._suggester,
            )
        yield Footer()

    def on_mount(self) -> None:
        self.runtime.ui.bind(asyncio.get_running_loop(), self)
        self.runtime.start()
        self.set_interval(UI_REFRESH_INTERVAL, self.runtime.ui.pump_main)
        self.set_interval(UI_REFRESH_INTERVAL, self._check_fatal)
        self.query_one("#command-input", Input).focus()
        self._update_status()
        self._on_resize_width(self.size.width)

    def on_resize(self, event: object) -> None:
        event_size = getattr(event, "size", self.size)
        self._on_resize_width(event_size.width)

    def on_unmount(self) -> None:
        self.runtime.ui.unbind(self)
        self._completion_request_id += 1
        self._pending_tab_value = None
        self._completion_mode = False
        self._completion_value = None
        self._completion_candidates = ()

    def _on_resize_width(self, width: int) -> None:
        panels = self.query_one("#panels", Grid)
        if width >= 100:
            panels.styles.grid_size = (2, 1)
            panels.styles.grid_columns = "1fr 2fr"
            panels.styles.grid_rows = "1fr"
        else:
            panels.styles.grid_size = (1, 2)
            panels.styles.grid_columns = "1fr"
            panels.styles.grid_rows = "1fr 1fr"

    def _connection_text(self) -> str:
        cfg = self.runtime.settings
        return (
            f"Listen: {cfg.listen_address}:{cfg.listen_port}    "
            f"Destination: {cfg.destination_address}:{cfg.destination_port}\n"
            f"Packet log: {safe_one_line(cfg.packet_log_path)}\n"
            f"Event log:  {safe_one_line(cfg.event_log_path)}"
        )

    def _command_input(self) -> CommandInput:
        return self.query_one("#command-input", CommandInput)

    def _completion_list(self) -> OptionList:
        return self.query_one("#completion-list", OptionList)

    def _can_inline_complete(self, value: str) -> bool:
        if self._completion_mode:
            return False
        try:
            input_widget = self._command_input()
        except Exception:
            return False
        return input_widget.value == value and input_widget.cursor_at_end

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "command-input":
            return
        input_widget = self._command_input()
        if event.value != input_widget.value:
            return
        if self._completion_mode:
            if input_widget.cursor_at_end and parse_completion_query(event.value) is not None:
                self._refresh_completion_options(event.value)
            else:
                self._close_completion_mode()
        else:
            if self._pending_tab_value == event.value:
                return
            # A pending Tab scan must never reopen a list for text that has since changed.
            self._completion_request_id += 1
            self._pending_tab_value = None

    def _request_completion_from_tab(self) -> None:
        input_widget = self._command_input()
        value = input_widget.value
        query = parse_completion_query(value)
        if query is None or not input_widget.cursor_at_end:
            return
        self._completion_request_id += 1
        request_id = self._completion_request_id
        self._pending_tab_value = value

        async def find_candidates() -> None:
            results = await asyncio.to_thread(
                scan_path_candidates,
                query.path_text,
                query.command_prefix,
                query.quote,
                self._completion_base,
            )
            if request_id != self._completion_request_id:
                return
            current = self._command_input()
            if current.value != value or not current.cursor_at_end or self._completion_mode:
                return
            self._pending_tab_value = None
            if not results.candidates:
                return
            if len(results.candidates) == 1:
                self._apply_direct_completion(results.candidates[0])
                return
            self._completion_mode = True
            self._show_completion_results(value, results)

        asyncio.create_task(find_candidates())

    def _refresh_completion_options(self, value: str) -> None:
        query = parse_completion_query(value)
        if query is None:
            self._close_completion_mode()
            return
        self._completion_request_id += 1
        request_id = self._completion_request_id
        self._completion_value = None

        async def find_candidates() -> None:
            results = await asyncio.to_thread(
                scan_path_candidates,
                query.path_text,
                query.command_prefix,
                query.quote,
                self._completion_base,
            )
            if request_id != self._completion_request_id or not self._completion_mode:
                return
            current = self._command_input()
            if current.value != value or not current.cursor_at_end:
                return
            if not results.candidates:
                self._close_completion_mode()
                return
            self._show_completion_results(value, results)

        asyncio.create_task(find_candidates())

    def _show_completion_results(self, value: str, results: CompletionResults) -> None:
        option_list = self._completion_list()
        options = [Option(candidate.label, id=f"path-{index}") for index, candidate in enumerate(results.candidates)]
        if results.truncated:
            options.append(Option("More than 200 results; type more of the path to narrow the list", disabled=True))
        option_list.set_options(options)
        option_list.highlighted = 0
        option_list.styles.display = "block"
        self._completion_candidates = results.candidates
        self._completion_truncated = results.truncated
        self._completion_value = value

    def _close_completion_mode(self) -> None:
        self._completion_request_id += 1
        self._pending_tab_value = None
        self._completion_mode = False
        self._completion_value = None
        self._completion_candidates = ()
        self._completion_truncated = False
        if self.is_mounted:
            option_list = self._completion_list()
            option_list.clear_options()
            option_list.styles.display = "none"

    def _cancel_completion(self) -> None:
        # Also invalidate a Tab lookup that is still running before the list opens.
        self._close_completion_mode()
        input_widget = self._command_input()
        if input_widget.suggester is not None and input_widget.cursor_at_end and input_widget.value:
            input_widget._suggestion = ""
            input_widget.run_worker(input_widget.suggester._get_suggestion(input_widget, input_widget.value))

    def _move_completion(self, direction: int) -> None:
        if not self._completion_mode or self._completion_value != self._command_input().value:
            return
        option_list = self._completion_list()
        if direction < 0:
            option_list.action_cursor_up()
        else:
            option_list.action_cursor_down()

    def _accept_completion(self) -> None:
        if not self._completion_mode or self._completion_value != self._command_input().value:
            return
        option_list = self._completion_list()
        index = option_list.highlighted
        if index is None or index < 0 or index >= len(self._completion_candidates):
            return
        candidate = self._completion_candidates[index]
        input_widget = self._command_input()
        self._completion_request_id += 1
        self._completion_candidates = ()
        self._completion_value = None
        option_list.clear_options()
        if candidate.is_directory:
            # Keep selection mode open while offering entries below the selected directory.
            self._completion_mode = True
            input_widget.value = candidate.value
            input_widget.cursor_position = len(input_widget.value)
        else:
            self._completion_mode = False
            self._completion_truncated = False
            option_list.styles.display = "none"
            input_widget.value = candidate.value
            input_widget.cursor_position = len(input_widget.value)

    def _apply_direct_completion(self, candidate: CompletionCandidate) -> None:
        input_widget = self._command_input()
        self._completion_request_id += 1
        self._pending_tab_value = None
        input_widget.value = candidate.value
        input_widget.cursor_position = len(input_widget.value)

    def _history_up(self) -> None:
        if not self._history:
            return
        input_widget = self._command_input()
        if self._history_index is None:
            self._history_draft = input_widget.value
            self._history_index = len(self._history) - 1
        elif self._history_index > 0:
            self._history_index -= 1
        input_widget.value = self._history[self._history_index]
        input_widget.cursor_position = len(input_widget.value)

    def _history_down(self) -> None:
        if self._history_index is None:
            return
        input_widget = self._command_input()
        if self._history_index < len(self._history) - 1:
            self._history_index += 1
            input_widget.value = self._history[self._history_index]
        else:
            input_widget.value = self._history_draft
            self._history_index = None
        input_widget.cursor_position = len(input_widget.value)

    def _record_command_history(self, command: str) -> None:
        if not command.strip():
            return
        if not self._history or self._history[-1] != command:
            self._history.append(command)
            if len(self._history) > COMMAND_HISTORY_LIMIT:
                del self._history[: len(self._history) - COMMAND_HISTORY_LIMIT]
        self._history_index = None
        self._history_draft = ""

    def refresh_from_workers(
        self,
        events: list[str],
        display_dropped: int,
        receive: tuple[datetime, int, int, int] | None,
        receive_count: int,
    ) -> None:
        if receive is not None:
            when, size, sequence, _total = receive
            aggregate = f" [{receive_count} received]" if receive_count > 1 else ""
            self._last_receive_row = f"[{timestamp(when)}] [info] RX Received({size}): {sequence}{aggregate}"
            self._last_receive_count = receive_count
        rows = list(events)
        if display_dropped:
            rows.append(f"[{timestamp()}] [warn] UI omitted {display_dropped} older event rows")
        if self._last_receive_row is not None:
            rows.append(self._last_receive_row)
        rows.sort(key=lambda row: row[1:24])
        self._visible_events = collections.deque(rows[-UI_MAX_EVENTS:], maxlen=UI_MAX_EVENTS)
        rows = list(self._visible_events)
        self.query_one("#event-list", Static).update("\n".join(rows))
        self._update_status()

    def _update_status(self) -> None:
        stats = self.runtime.snapshot()
        os_drops = str(stats["os_drops"]) if stats["os_drops"] is not None else "unavailable"
        effective = (
            str(stats["socket_buffer_effective"])
            if stats["socket_buffer_effective"] is not None
            else "unavailable"
        )
        buffer_state = f"requested {stats['socket_buffer_requested']}, effective {effective}"
        if stats["socket_buffer_error"]:
            buffer_state += f"; setting error: {stats['socket_buffer_error']}"
        current = safe_one_line(stats["current_path"] or stats["last_failed_path"] or "none")
        line = stats["current_line"] or stats["last_failed_line"] or "-"
        text = (
            f"RX: {stats['receive_state']} | Received: {stats['received']} | RX errors: {stats['rx_errors']}\n"
            f"Packet logs written: {stats['written']} | Log queue dropped: {stats['dropped']}\n"
            f"Log queue: {stats['depth']} packets / {stats['bytes']} bytes "
            f"(max {stats['max_depth']} packets / {stats['max_bytes']} bytes)\n"
            f"OS UDP drops: {os_drops} | SO_RCVBUF: {buffer_state}\n"
            f"TX: {stats['state']} | File: {current} | Line: {line}\n"
            f"Queued files: {stats['queued']} | Completed: {stats['completed']} | Failed: {stats['failed']} | "
            f"Datagrams sent: {stats['sent_datagrams']}"
        )
        self.query_one("#status-text", Static).update(text)

    def _check_fatal(self) -> None:
        if self.runtime.failures.event.is_set() and not self._fatal_shutdown_started:
            self._fatal_shutdown_started = True
            self._udpcomapp_return_code = 1
            self.runtime.request_stop()
            self.runtime.ui.pump_main(force=True)
            self.set_timer(0.15, self.exit)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        command = event.value
        self._record_command_history(command)
        event.input.value = ""
        self._handle_command(command)

    def _handle_command(self, command: str) -> None:
        command = command.lstrip()
        if not command.strip():
            return
        command_name = command.split(None, 1)[0]
        if command_name == "send":
            try:
                path = parse_send_path(command)
                if not path.is_file():
                    raise ValueError("path is not a regular file")
                with path.open("rb"):
                    pass
                queued = self.runtime.sender.enqueue(path.resolve())
                self.runtime.log(
                    "info",
                    f"file queued: {path.resolve()} (queued files={queued})",
                    block=False,
                )
            except (OSError, ValueError, RuntimeError) as exc:
                self.runtime.log("error", f"send command rejected: {safe_one_line(exc)}", block=False)
                self._append_command_message(f"Error: {safe_one_line(exc)}. Type help for commands.")
        elif command == "status":
            self._update_status()
            self._append_command_message("Status refreshed.")
        elif command == "help":
            self._append_command_message(
                "Commands: send <file-path> (spaces are part of the path; quote to preserve leading/trailing spaces), "
                "status, help, resume, quit. Up/Down recalls commands; Tab completes send paths. "
                "With multiple matches, Up/Down selects and Enter/Tab/Right accepts; press Enter again to send. "
                "Esc closes the candidate list."
            )
        elif command == "resume":
            if self.runtime.sender.resume():
                self.runtime.log("info", "send queue resumed", block=False)
                self._append_command_message("Send queue resumed.")
            else:
                self._append_command_message("There is no paused send queue to resume.")
        elif command == "quit":
            self.runtime.request_stop()
            self.exit()
        else:
            message = f"unknown command: {command_name}. Type help for commands."
            self.runtime.log("error", message, block=False)
            self._append_command_message(message)

    def _append_command_message(self, message: str) -> None:
        self.runtime.ui.event(datetime.now().astimezone(), "info", message)

    def action_quit_app(self) -> None:
        self.runtime.request_stop()
        self.exit()


def main(argv: list[str] | None = None) -> int:
    parser = OneLineArgumentParser(description="Integrated IPv4 UDP sender and receiver")
    parser.add_argument("settings_json", help="path to the settings JSON file")
    try:
        arguments = parser.parse_args(argv)
        runtime = create_runtime(arguments.settings_json)
    except (ConfigError, OSError, ValueError) as exc:
        sys.stderr.write(error_line(exc) + "\n")
        sys.stderr.flush()
        return 1

    app = UDPApp(runtime)
    try:
        app.run()
    except KeyboardInterrupt:
        runtime.request_stop()
    except Exception as exc:
        runtime.failures.fail(f"TUI failed: {safe_one_line(exc)}")
        runtime.request_stop()
        app._udpcomapp_return_code = 1
    finally:
        runtime.shutdown()
    return app._udpcomapp_return_code or (1 if runtime.failures.event.is_set() else 0)


if __name__ == "__main__":
    raise SystemExit(main())
