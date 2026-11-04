from __future__ import annotations

import asyncio
import contextlib
import io
import json
import socket
import sys
import threading
import tempfile
import time
from unittest.mock import patch
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from udpcomapp import (  # noqa: E402
    ConfigError,
    EventLogWriter,
    FailureSignal,
    PacketLogWriter,
    PacketRecord,
    SendFileError,
    UDPApp,
    create_runtime,
    main,
    parse_payload_line,
    parse_send_path,
    scan_path_candidates,
)
from textual.containers import Grid  # noqa: E402
from textual.widgets import OptionList  # noqa: E402


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def write_settings(directory: Path, destination_port: int, **options: object) -> Path:
    settings_path = directory / "settings.json"
    document: dict[str, object] = {
        "listen_address": "127.0.0.1",
        "listen_port": free_udp_port(),
        "destination_address": "127.0.0.1",
        "destination_port": destination_port,
        "packet_log_path": "received.log",
        "event_log_path": "events.log",
    }
    document.update(options)
    settings_path.write_text(json.dumps(document), encoding="utf-8")
    return settings_path


async def wait_until(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return predicate()


class PayloadParserTests(unittest.TestCase):
    def test_compatible_payload_lines(self) -> None:
        self.assertEqual(parse_payload_line("DE ad 00 ff", 1), ("send", bytes.fromhex("DEAD00FF")))
        self.assertEqual(parse_payload_line("   ", 2), ("ignore", None))
        self.assertEqual(parse_payload_line("# comment", 3), ("ignore", None))
        self.assertEqual(parse_payload_line("/Wait 1 2 3", 4), ("wait", 123))
        self.assertEqual(parse_payload_line("/Wait 0", 5), ("wait", 0))
        self.assertEqual(parse_payload_line("AB" * 32_768, 6)[1], bytes.fromhex("AB" * 32_768))

    def test_invalid_lines_report_line_without_echoing_payload(self) -> None:
        for source in ("A", "GG", "AB" * 32_769, " /Wait 5", "/wait 5", "/Wait 1.5"):
            with self.subTest(source=source[:20]), self.assertRaises(SendFileError) as caught:
                parse_payload_line(source, 17)
            self.assertEqual(caught.exception.line_number, 17)
            self.assertNotIn(source, str(caught.exception))

    def test_send_command_path_uses_remaining_text_and_quotes(self) -> None:
        self.assertEqual(parse_send_path("send ./folder/file.txt"), Path("./folder/file.txt"))
        self.assertEqual(parse_send_path('send "./folder with spaces/file.txt"'), Path("./folder with spaces/file.txt"))
        self.assertEqual(parse_send_path("send '  spaced name.txt  '"), Path("  spaced name.txt  "))
        with self.assertRaisesRegex(ValueError, "unmatched quote"):
            parse_send_path('send "unfinished')

    def test_path_completion_quotes_hidden_names_and_dot_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            (base / "alpha one.txt").touch()
            (base / ".secret").touch()
            (base / "folder space").mkdir()
            (base / " spaced file ").touch()

            plain = scan_path_candidates("al", "send ", None, base)
            self.assertEqual([item.value for item in plain.candidates], ["send alpha one.txt"])
            quoted = scan_path_candidates("fol", 'send ', '"', base)
            self.assertEqual(quoted.candidates[0].value, 'send "folder space/')
            self.assertTrue(quoted.candidates[0].is_directory)
            empty = scan_path_candidates("", "send ", None, base)
            self.assertIn("alpha one.txt", [item.label for item in empty.candidates])
            self.assertFalse(any(item.label == ".secret" for item in empty.candidates))
            spaced = next(item for item in empty.candidates if item.label == " spaced file ")
            self.assertEqual(spaced.value, 'send " spaced file "')
            self.assertEqual(parse_send_path(spaced.value), Path(" spaced file "))
            self.assertEqual(scan_path_candidates(".s", "send ", None, base).candidates[0].label, ".secret")
            self.assertEqual(scan_path_candidates(".", "send ", None, base).candidates[0].value, "send ./")
            self.assertEqual(scan_path_candidates("..", "send ", None, base).candidates[0].value, "send ../")

            many = base / "many"
            many.mkdir()
            for index in range(205):
                (many / f"item-{index:03}.txt").touch()
            capped = scan_path_candidates("many/", "send ", None, base)
            self.assertEqual(len(capped.candidates), 200)
            self.assertTrue(capped.truncated)
            self.assertEqual(capped.candidates[0].label, "item-000.txt")


class ConfigurationTests(unittest.TestCase):
    def test_missing_settings_argument_is_a_timestamped_single_line_error(self) -> None:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.assertEqual(main([]), 1)
        rows = stderr.getvalue().splitlines()
        self.assertEqual(len(rows), 1)
        self.assertIn("[error] Error: usage:", rows[0])

    def test_existing_cpp_settings_and_ipc_port_are_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            settings_path = write_settings(directory, 55_001, ipc_port="ignored even if invalid")
            runtime = create_runtime(settings_path)
            try:
                self.assertEqual(runtime.settings.packet_log_queue_limit_packets, 4_096)
                self.assertEqual(runtime.settings.receive_socket_buffer_bytes, 4 * 1024 * 1024)
                self.assertEqual(runtime.settings.listen_address, "127.0.0.1")
            finally:
                runtime.shutdown()

    def test_boolean_is_not_an_integer_and_log_paths_must_differ(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            path = write_settings(directory, 55_002, listen_port=True)
            with self.assertRaisesRegex(ConfigError, "listen_port"):
                create_runtime(path)
            path = write_settings(directory, 55_003, event_log_path="received.log")
            with self.assertRaisesRegex(ConfigError, "different files"):
                create_runtime(path)

    def test_relative_log_paths_resolve_from_settings_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "logs").mkdir()
            settings_path = write_settings(directory, 55_004, packet_log_path="logs/p.log", event_log_path="logs/e.log")
            runtime = create_runtime(settings_path)
            try:
                self.assertEqual(runtime.settings.packet_log_path, (directory / "logs/p.log").resolve())
                self.assertEqual(runtime.settings.event_log_path, (directory / "logs/e.log").resolve())
            finally:
                runtime.shutdown()


class PacketWriterTests(unittest.TestCase):
    def test_packet_line_format_and_payload_queue_drop_new(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            log_path = directory / "packets.log"
            output = log_path.open("a", encoding="utf-8")
            from udpcomapp import FailureSignal

            writer = PacketLogWriter(output, FailureSignal(), limit_bytes=2, limit_packets=4)
            writer.start()
            received_at = datetime(2026, 8, 18, 12, 34, 56, 789_000).astimezone()
            self.assertTrue(writer.enqueue(PacketRecord(received_at, 1, bytes.fromhex("dead"))))
            self.assertFalse(writer.enqueue(PacketRecord(received_at, 2, b"too long")))
            self.assertTrue(writer.enqueue(PacketRecord(received_at, 3, b"")))
            writer.stop()

            rows = log_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                rows,
                [
                    "[2026-08-18 12:34:56.789] [info] Received(2): 1: DEAD",
                    "[2026-08-18 12:34:56.789] [info] Received(0): 3: ",
                ],
            )
            stats = writer.snapshot()
            self.assertEqual(stats["enqueued"], 2)
            self.assertEqual(stats["written"], 2)
            self.assertEqual(stats["dropped"], 1)

    def test_packet_writer_keeps_full_receive_payload_above_send_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "packets.log"
            from udpcomapp import FailureSignal

            payload = bytes(range(256)) * 129  # 33,024 bytes.
            writer = PacketLogWriter(
                log_path.open("a", encoding="utf-8"),
                FailureSignal(),
                limit_bytes=len(payload),
                limit_packets=1,
            )
            writer.start()
            writer.enqueue(PacketRecord(datetime.now().astimezone(), 7, payload))
            writer.stop()
            row = log_path.read_text(encoding="utf-8").strip()
            self.assertIn("Received(33024): 7: ", row)
            self.assertTrue(row.endswith(payload.hex().upper()))

    def test_zero_length_datagrams_still_obey_packet_count_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "packets.log"
            from udpcomapp import FailureSignal

            writer = PacketLogWriter(
                log_path.open("a", encoding="utf-8"),
                FailureSignal(),
                limit_bytes=4,
                limit_packets=1,
            )
            received_at = datetime(2026, 8, 18, 12, 34, 56, 789_000).astimezone()
            self.assertTrue(writer.enqueue(PacketRecord(received_at, 1, b"")))
            self.assertFalse(writer.enqueue(PacketRecord(received_at, 2, b"")))
            self.assertEqual(writer.snapshot()["depth"], 1)
            writer.start()
            writer.stop()
            self.assertIn("Received(0): 1: ", log_path.read_text(encoding="utf-8"))
            self.assertEqual(writer.snapshot()["dropped"], 1)

    def test_packet_writer_failure_is_fatal_and_unflushed_record_is_not_counted(self) -> None:
        class FlushFailure:
            def __init__(self) -> None:
                self.wrote = threading.Event()

            def write(self, _value: str) -> int:
                self.wrote.set()
                return 1

            def flush(self) -> None:
                raise OSError("disk full")

            def close(self) -> None:
                pass

        failures = FailureSignal()
        output = FlushFailure()
        writer = PacketLogWriter(output, failures, limit_bytes=10, limit_packets=1)  # type: ignore[arg-type]
        with contextlib.redirect_stderr(io.StringIO()):
            writer.start()
            writer.enqueue(PacketRecord(datetime.now().astimezone(), 1, b"x"))
            self.assertTrue(output.wrote.wait(1))
            self.assertTrue(failures.event.wait(2))
            writer.stop()
        self.assertEqual(writer.snapshot()["written"], 0)
        self.assertIn("packet log writer failed", failures.message or "")

    def test_event_writer_failure_sets_fatal_signal(self) -> None:
        class WriteFailure:
            def write(self, _value: str) -> int:
                raise OSError("disk full")

            def flush(self) -> None:
                pass

            def close(self) -> None:
                pass

        failures = FailureSignal()
        writer = EventLogWriter(WriteFailure(), failures)  # type: ignore[arg-type]
        with contextlib.redirect_stderr(io.StringIO()):
            writer.start()
            writer.emit("info", "test event")
            self.assertTrue(failures.event.wait(1))
            writer.stop()
        self.assertIn("event log writer failed", failures.message or "")

    def test_packet_writer_fatal_is_forwarded_to_healthy_event_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log_path = Path(temporary) / "events.log"
            failures = FailureSignal()
            writer = EventLogWriter(log_path.open("a", encoding="utf-8"), failures)
            failures.bind_event_recorder(writer.emit_failure)
            writer.start()
            with contextlib.redirect_stderr(io.StringIO()):
                failures.fail("packet log writer failed: disk full", source="packet-log")
            writer.stop()
            self.assertIn(
                "[error] Error: packet log writer failed: disk full",
                log_path.read_text(encoding="utf-8"),
            )


class RuntimeLoopbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_loopback_receive_logs_exact_datagram_and_separates_events(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    runtime.start()
                    target = runtime.sock.getsockname()
                    payload = bytes.fromhex("00DEADBE")
                    sender.sendto(payload, target)
                    self.assertTrue(await wait_until(lambda: runtime.snapshot()["written"] == 1))
                finally:
                    runtime.shutdown()
                    sender.close()

                packet_rows = (directory / "received.log").read_text(encoding="utf-8").splitlines()
                self.assertEqual(len(packet_rows), 1)
                self.assertRegex(packet_rows[0], r"^\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}\] \[info\] Received\(4\): 1: ")
                self.assertTrue(packet_rows[0].endswith(payload.hex().upper()))
                event_log = (directory / "events.log").read_text(encoding="utf-8")
                self.assertIn("shutdown summary: received=1", event_log)
                self.assertNotIn(payload.hex().upper(), event_log)

    async def test_packet_log_queue_drop_new_is_counted_and_warned(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(
                    directory,
                    destination.getsockname()[1],
                    packet_log_queue_limit_bytes=1,
                    packet_log_queue_limit_packets=1,
                )
                runtime = create_runtime(settings_path)
                sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    runtime.start()
                    sender.sendto(b"too large", runtime.sock.getsockname())
                    self.assertTrue(await wait_until(lambda: runtime.snapshot()["received"] == 1))
                    self.assertTrue(await wait_until(lambda: runtime.snapshot()["dropped"] == 1))
                finally:
                    runtime.shutdown()
                    sender.close()
                self.assertEqual((directory / "received.log").read_text(encoding="utf-8"), "")
                event_log = (directory / "events.log").read_text(encoding="utf-8")
                self.assertIn("Warning: packet log queue overflow; discarded 1 packet logs", event_log)
                self.assertIn("packet_log_queue_dropped=1", event_log)

    async def test_file_error_pauses_fifo_until_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                destination.settimeout(1)
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                bad = directory / "bad.txt"
                good = directory / "good.txt"
                bad.write_text("DEADBEEF\nXYZ\n", encoding="utf-8")
                good.write_text("CAFE\n", encoding="utf-8")
                try:
                    runtime.start()
                    runtime.sender.enqueue(bad)
                    runtime.sender.enqueue(good)
                    self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["state"] == "paused"))
                    state = runtime.sender.snapshot()
                    self.assertEqual(state["failed"], 1)
                    self.assertEqual(state["queued"], 1)
                    first_payload, _ = await asyncio.to_thread(destination.recvfrom, 100)
                    self.assertEqual(first_payload, bytes.fromhex("DEADBEEF"))
                    self.assertTrue(runtime.sender.resume())
                    second_payload, _ = await asyncio.to_thread(destination.recvfrom, 100)
                    self.assertEqual(second_payload, bytes.fromhex("CAFE"))
                    self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["completed"] == 1))
                finally:
                    runtime.shutdown()
                event_log = (directory / "events.log").read_text(encoding="utf-8")
                self.assertIn("bad.txt line 2", event_log)
                self.assertNotIn("XYZ", event_log)

    async def test_quit_interrupts_wait_and_drops_queued_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                wait_file = directory / "wait.txt"
                queued_file = directory / "queued.txt"
                wait_file.write_text("/Wait 120000\nDEADBEEF\n", encoding="utf-8")
                queued_file.write_text("CAFE\n", encoding="utf-8")
                runtime.start()
                runtime.sender.enqueue(wait_file)
                runtime.sender.enqueue(queued_file)
                self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["current_line"] == 1))
                started = time.monotonic()
                runtime.request_stop()
                runtime.shutdown()
                self.assertLess(time.monotonic() - started, 1.0)
                self.assertEqual(runtime.sender.snapshot()["queued"], 0)
                event_log = (directory / "events.log").read_text(encoding="utf-8")
                self.assertIn("discarded queued files=1", event_log)


class TextualPilotTests(unittest.IsolatedAsyncioTestCase):
    async def test_command_history_completion_selection_and_quoted_directory_walk(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "alpha one.txt").write_text("CAFE\n", encoding="utf-8")
            (directory / "alpha-two.txt").write_text("BEEF\n", encoding="utf-8")
            nested = directory / "folder space"
            nested.mkdir()
            (nested / "child one.txt").write_text("ABCD\n", encoding="utf-8")
            (nested / "child-two.txt").write_text("1234\n", encoding="utf-8")
            dense = directory / "dense"
            dense.mkdir()
            for index in range(12):
                (dense / f"item-{index:02}.txt").touch()
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                app = UDPApp(runtime, completion_base=directory)
                for index in range(105):
                    app._record_command_history(f"cmd {index}")
                app._record_command_history("cmd 104")
                self.assertEqual(len(app._history), 100)
                self.assertEqual(app._history[0], "cmd 5")
                app._history.clear()
                try:
                    async with app.run_test(size=(110, 35)) as pilot:
                        command = app.query_one("#command-input")
                        option_list = app.query_one("#completion-list", OptionList)

                        command.value = "status"
                        await pilot.press("enter")
                        await pilot.pause(0.02)
                        command.value = "status"
                        await pilot.press("enter")
                        await pilot.pause(0.02)
                        command.value = "help"
                        await pilot.press("enter")
                        await pilot.pause(0.02)
                        self.assertEqual(app._history, ["status", "help"])

                        command.value = "send unsubmitted draft"
                        await pilot.press("up")
                        self.assertEqual(command.value, "help")
                        await pilot.press("up")
                        self.assertEqual(command.value, "status")
                        await pilot.press("down")
                        self.assertEqual(command.value, "help")
                        await pilot.press("down")
                        self.assertEqual(command.value, "send unsubmitted draft")

                        command.value = "send al"
                        command.cursor_position = len(command.value)
                        self.assertTrue(await wait_until(lambda: bool(command._suggestion)))
                        self.assertEqual(command._suggestion, "send alpha one.txt")
                        command.cursor_position -= 1
                        self.assertEqual(command._suggestion, "")
                        command.cursor_position = len(command.value)
                        self.assertTrue(await wait_until(lambda: bool(command._suggestion)))
                        await pilot.press("right")
                        self.assertEqual(command.value, "send alpha one.txt")

                        command.value = "send al"
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: app._completion_mode))
                        self.assertEqual(len(app._completion_candidates), 2)
                        self.assertEqual(option_list.highlighted, 0)
                        self.assertTrue(command.has_focus)
                        destination.sendto(b"RX", runtime.sock.getsockname())
                        self.assertTrue(await wait_until(lambda: runtime.snapshot()["received"] == 1))
                        await pilot.pause(0.15)
                        self.assertTrue(app._completion_mode)
                        self.assertEqual(command.value, "send al")
                        await pilot.press("down")
                        self.assertEqual(option_list.highlighted, 1)
                        self.assertEqual(command.value, "send al")
                        self.assertIsNone(app._history_index)
                        await pilot.press("enter")
                        self.assertEqual(command.value, "send alpha-two.txt")
                        self.assertFalse(app._completion_mode)
                        self.assertEqual(runtime.sender.snapshot()["queued"], 0)
                        self.assertEqual(runtime.sender.snapshot()["completed"], 0)

                        command.value = "send al"
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: app._completion_mode))
                        command.value = "send fo"
                        command.cursor_position = len(command.value)
                        self.assertTrue(await wait_until(lambda: app._completion_value == "send fo"))
                        self.assertEqual([candidate.label for candidate in app._completion_candidates], ["folder space/"])
                        await pilot.press("escape")
                        self.assertEqual(command.value, "send fo")

                        command.value = "send dense/"
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: app._completion_mode))
                        self.assertEqual(len(app._completion_candidates), 12)
                        # Two border rows leave eight candidate rows visible under the ten-row cap.
                        self.assertEqual(option_list.region.height, 10)
                        self.assertEqual(option_list.scrollable_content_region.height, 8)
                        for _ in range(9):
                            await pilot.press("down")
                        self.assertEqual(option_list.highlighted, 9)
                        await pilot.press("escape")

                        command.value = 'send "fol'
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: command.value == 'send "folder space/'))
                        self.assertFalse(app._completion_mode)
                        command.value = 'send "folder space/ch'
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: app._completion_mode))
                        self.assertEqual(len(app._completion_candidates), 2)
                        await pilot.press("down")
                        await pilot.press("enter")
                        self.assertEqual(command.value, 'send "folder space/child-two.txt"')
                        self.assertEqual(runtime.sender.snapshot()["queued"], 0)

                        command.value = "send al"
                        command.cursor_position = len(command.value)
                        await pilot.press("tab")
                        self.assertTrue(await wait_until(lambda: app._completion_mode))
                        await pilot.press("escape")
                        self.assertFalse(app._completion_mode)
                        self.assertEqual(command.value, "send al")
                        self.assertEqual(option_list.styles.display, "none")
                        await pilot.press("up")
                        self.assertEqual(command.value, "help")
                        await pilot.press("down")
                        self.assertEqual(command.value, "send al")
                        command.value = "quit"
                        await pilot.press("enter")
                finally:
                    runtime.shutdown()

    async def test_escape_invalidates_a_slow_completion_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "alpha.txt").touch()
            (directory / "alpine.txt").touch()
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                app = UDPApp(runtime, completion_base=directory)
                entered = threading.Event()
                release = threading.Event()
                original_scan = scan_path_candidates

                def slow_scan(*args: object) -> object:
                    entered.set()
                    release.wait(2)
                    return original_scan(*args)  # type: ignore[arg-type]

                try:
                    async with app.run_test(size=(100, 30)) as pilot:
                        command = app.query_one("#command-input")
                        command.suggester = None
                        command.value = "send al"
                        command.cursor_position = len(command.value)
                        with patch("udpcomapp.scan_path_candidates", side_effect=slow_scan):
                            await pilot.press("tab")
                            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                            await pilot.press("escape")
                            release.set()
                            await pilot.pause(0.1)
                        self.assertFalse(app._completion_mode)
                        self.assertEqual(command.value, "send al")
                        self.assertEqual(app.query_one("#completion-list", OptionList).styles.display, "none")
                        command.value = "quit"
                        await pilot.press("enter")
                finally:
                    release.set()
                    runtime.shutdown()

    async def test_packet_writer_fatal_is_logged_and_shown_before_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                app = UDPApp(runtime)
                try:
                    async with app.run_test(size=(110, 35)) as pilot:
                        await pilot.pause(0.15)
                        with contextlib.redirect_stderr(io.StringIO()):
                            runtime.failures.fail(
                                "packet log writer failed: simulated disk error",
                                source="packet-log",
                            )
                        await pilot.pause(0.03)
                        rendered = app.query_one("#event-list").render()
                        self.assertIn("packet log writer failed: simulated disk error", str(rendered))
                        await pilot.pause(0.2)
                finally:
                    runtime.shutdown()
                event_log = (directory / "events.log").read_text(encoding="utf-8")
                self.assertIn("packet log writer failed: simulated disk error", event_log)

    async def test_command_input_survives_updates_send_receive_and_quit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as destination:
                destination.bind(("127.0.0.1", 0))
                destination.settimeout(2)
                settings_path = write_settings(directory, destination.getsockname()[1])
                runtime = create_runtime(settings_path)
                payload_path = directory / "payload with spaces.txt"
                payload_path.write_text("DE AD BE EF\n", encoding="utf-8")
                inbound = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                app = UDPApp(runtime)
                try:
                    async with app.run_test(size=(110, 35)) as pilot:
                        command = app.query_one("#command-input")
                        command.value = f'send "{payload_path}"'
                        await pilot.press("enter")
                        sent, _ = await asyncio.to_thread(destination.recvfrom, 100)
                        self.assertEqual(sent, bytes.fromhex("DEADBEEF"))
                        self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["completed"] == 1))

                        bad_path = directory / "bad file.txt"
                        bad_path.write_text("XYZ\n", encoding="utf-8")
                        command.value = f'send "{bad_path}"'
                        await pilot.press("enter")
                        self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["state"] == "paused"))
                        command.value = f'send "{payload_path}"'
                        await pilot.press("enter")
                        self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["queued"] == 1))
                        command.value = "resume"
                        await pilot.press("enter")
                        resumed_payload, _ = await asyncio.to_thread(destination.recvfrom, 100)
                        self.assertEqual(resumed_payload, bytes.fromhex("DEADBEEF"))
                        self.assertTrue(await wait_until(lambda: runtime.sender.snapshot()["completed"] == 2))

                        command.value = "status"
                        inbound.sendto(b"\xca\xfe", runtime.sock.getsockname())
                        self.assertTrue(await wait_until(lambda: runtime.snapshot()["received"] == 1))
                        await pilot.pause(0.15)
                        self.assertEqual(command.value, "status")
                        self.assertEqual(app.query_one("#panels", Grid).styles.grid_size, (2, 1))

                        await pilot.resize_terminal(80, 30)
                        await pilot.pause(0.05)
                        self.assertEqual(app.query_one("#panels", Grid).styles.grid_size, (1, 2))

                        await pilot.press("enter")
                        self.assertIn("RX: active", str(app.query_one("#status-text").render()))
                        command.value = "quit"
                        await pilot.press("enter")
                finally:
                    runtime.shutdown()
                    inbound.close()

            packet_log = (directory / "received.log").read_text(encoding="utf-8")
            self.assertIn("Received(2): 1: CAFE", packet_log)
            event_log = (directory / "events.log").read_text(encoding="utf-8")
            self.assertIn("Transmitted(4):", event_log)
            self.assertIn("send queue resumed", event_log)
            self.assertIn("bad file.txt line 1", event_log)
            self.assertNotIn("DEADBEEF", event_log)
            self.assertNotIn("XYZ", event_log)


if __name__ == "__main__":
    unittest.main()
