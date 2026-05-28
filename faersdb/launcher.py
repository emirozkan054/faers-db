"""End-user launcher for the local FAERS web application."""

from __future__ import annotations

import os
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn

from faersdb.config import settings
from faersdb.warehouse import validate_warehouse


def _app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path.cwd().resolve()


def _candidate_warehouse_dirs() -> list[Path]:
    candidates: list[Path] = []
    env_path = os.environ.get("WAREHOUSE_DIR")
    if env_path:
        candidates.append(Path(env_path))

    base_dir = _app_base_dir()
    candidates.append(base_dir / "warehouse")
    candidates.append(Path.cwd() / "warehouse")

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        candidates.append(Path(local_app_data) / "FAERS-DB" / "warehouse")

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved not in seen:
            unique.append(resolved)
            seen.add(resolved)
    return unique


def find_warehouse_dir() -> Path:
    candidates = _candidate_warehouse_dirs()
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def find_available_port(preferred: int = 8000) -> int:
    for port in range(preferred, preferred + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(port: int, timeout_seconds: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.25)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.2)
    return False


def _show_error(title: str, message: str) -> None:
    print(f"{title}\n{message}", file=sys.stderr)
    try:
        import tkinter
        from tkinter import messagebox

        root = tkinter.Tk()
        root.withdraw()
        messagebox.showerror(title, message)
        root.destroy()
    except Exception:
        pass


def launch() -> int:
    warehouse_dir = find_warehouse_dir()
    validation = validate_warehouse(warehouse_dir)
    if not validation.ready:
        expected = "\n".join(f"  - {path}" for path in _candidate_warehouse_dirs())
        detail = "\n".join(validation.errors) or "The warehouse folder was not found."
        _show_error(
            "FAERS DB data is not ready",
            (
                f"{detail}\n\n"
                "Place the released warehouse folder in one of these locations:\n"
                f"{expected}\n\n"
                "Then launch FAERS-DB again."
            ),
        )
        return 1

    settings.warehouse_dir = str(warehouse_dir)
    port = find_available_port()

    from faersdb.api import app

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        reload=False,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    if not _wait_for_server(port):
        _show_error(
            "FAERS DB could not start",
            "The local web server did not become ready. Try closing other FAERS-DB windows and launch again.",
        )
        server.should_exit = True
        return 1

    url = f"http://127.0.0.1:{port}/app"
    webbrowser.open(url)
    print(f"FAERS-DB is running at {url}")
    print("Keep this window open while using the app. Press Ctrl+C to stop.")

    try:
        while thread.is_alive():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping FAERS-DB...")
    finally:
        server.should_exit = True
        thread.join(timeout=5)
    return 0


def main() -> None:
    raise SystemExit(launch())


if __name__ == "__main__":
    main()
