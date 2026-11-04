"""Boot/reset observability contract for power-path post-mortems.

Hardware 2026-11-03: multiple P4 reboots during hardware verification reported
`lastResetReason=power_on` (not panic/watchdog). The App does not query
diagnostics on reconnect and raw P4 console lines are dropped unless
P4_SERIAL_LOG_RAW=1, so the persisted boot record must also be printed once the
console transports exist. This static check keeps that breadcrumb in place and
in the right order (after init_usb/init_uart, not during early boot).
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_boot_reset_record_is_logged_after_console_transports_are_ready():
    source = (ROOT / "main/pet_p4_diagnostics.c").read_text()
    header = (ROOT / "main/pet_p4_diagnostics.h").read_text()
    main = (ROOT / "main/pet_p4_main.c").read_text()
    protocol = (ROOT / "protocol.md").read_text()

    assert "void pet_p4_diagnostics_log_boot_record(void);" in header
    assert "void pet_p4_diagnostics_log_boot_record(void) {" in source
    log = source.split("void pet_p4_diagnostics_log_boot_record(void) {", 1)[1]
    log = log.split("bool pet_p4_diagnostics_handle_topic(", 1)[0]
    assert "ESP_LOGI" in log
    assert "boot=%lu reset=%s(%u) fault=%lu last_fault=%d" in log
    assert "reset_reason_name(" in log
    assert "g_boot_record.last_reset_reason" in log

    usb = main.index("ESP_ERROR_CHECK(init_usb());")
    uart = main.index("ESP_ERROR_CHECK(init_uart());")
    call = main.index("pet_p4_diagnostics_log_boot_record();")
    assert call > usb and call > uart, "log must run after both console transports"
    assert main.count("pet_p4_diagnostics_log_boot_record();") == 1

    assert "pet-diag: boot=" in protocol
    assert "not JSON" in protocol or "忽略非 JSON" in protocol
