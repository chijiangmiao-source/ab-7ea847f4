"""HTTP 服务：事务锁协调 API（仅依赖 Python 标准库）。

路由：
  GET  /healthz                 健康检查
  GET  /snapshot                查看当前持锁集合、等待队列、被撤销事务、裁决序号
  GET  /events                  已裁决的稳定事件标识列表
  POST /sessions                以稳定追踪标识创建会话  {"trace_id": "..."}
  POST /sessions/<id>/events    提交事件（begin/share/exclusive/upgrade/release/commit）
  POST /admin/crash?at=...      验收用故障注入（before-persist / after-persist）

事件提交支持两种持久化中断演练：
  POST body 内 "crash": "before-persist" | "after-persist"
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import (
    CRASH_AFTER_PERSIST,
    CRASH_BEFORE_PERSIST,
    LockManager,
    Rejected,
)

DATA_DIR = os.environ.get("LOCK_DATA_DIR", "/data")
DATA_FILE = os.path.join(DATA_DIR, "state.json")


class Service:
    def __init__(self, data_file: str = DATA_FILE) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(data_file)), exist_ok=True)
        self.lm = LockManager(data_file)
        self.data_file = data_file
        self.sessions: dict[str, dict] = {}
        self._sess_lock = threading.Lock()
        self.started_at = time.time()
        self._sessions_file = os.path.join(
            os.path.dirname(os.path.abspath(data_file)), "sessions.json"
        )
        self._load_sessions()

    def _load_sessions(self) -> None:
        try:
            with open(self._sessions_file, "r", encoding="utf-8") as fh:
                self.sessions = json.load(fh)
        except FileNotFoundError:
            self.sessions = {}

    def _save_sessions(self) -> None:
        tmp = self._sessions_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self.sessions, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._sessions_file)

    def create_session(self, trace_id: str | None) -> dict:
        if not trace_id or not isinstance(trace_id, str):
            trace_id = f"trace-{uuid.uuid4().hex[:12]}"
        with self._sess_lock:
            for sid, meta in self.sessions.items():
                if meta["trace_id"] == trace_id:
                    return {"session_id": sid, "trace_id": trace_id, "replay": True}
            sid = f"sess-{uuid.uuid4().hex[:16]}"
            self.sessions[sid] = {
                "trace_id": trace_id,
                "created_at": time.time(),
                "tx": [],
                "event_count": 0,
            }
            self._save_sessions()
        return {"session_id": sid, "trace_id": trace_id, "replay": False}

    MAX_TX = 4
    MAX_EVENTS = 48

    def check_quota(self, session_id: str, event: dict) -> None:
        # 稳定标识重放不受配额影响
        if event.get("event_id") in self.lm.events:
            return
        meta = self.sessions[session_id]
        if meta["event_count"] >= self.MAX_EVENTS:
            raise Rejected(f"单会话事件数已达上限 {self.MAX_EVENTS}")
        if event.get("type") == "begin":
            if len(meta["tx"]) >= self.MAX_TX:
                raise Rejected(f"单会话事务数已达上限 {self.MAX_TX}")

    def note_accepted(self, session_id: str, event: dict) -> None:
        meta = self.sessions[session_id]
        meta["event_count"] += 1
        if event.get("type") == "begin":
            meta["tx"].append(event.get("transaction"))
        self._save_sessions()


SERVICE: Service | None = None


def get_service() -> Service:
    global SERVICE
    if SERVICE is None:
        SERVICE = Service()
    return SERVICE


class Handler(BaseHTTPRequestHandler):
    server_version = "LockCoordinator/1.0"

    def log_message(self, fmt: str, *args) -> None:  # 安静日志
        if os.environ.get("LOCK_HTTP_LOG"):
            super().log_message(fmt, *args)

    # ------------------------------------------------------------------ #
    def _send_json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise Rejected(f"请求体不是合法 JSON: {exc}")
        if not isinstance(data, dict):
            raise Rejected("请求体必须是 JSON 对象")
        return data

    def _reject(self, msg: str, code: int = 409) -> None:
        self._send_json(
            code,
            {"status": "rejected", "error": msg},
        )

    # ------------------------------------------------------------------ #
    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        svc = get_service()
        if path == "/healthz":
            snap = svc.lm.snapshot()
            self._send_json(
                200,
                {
                    "status": "ok",
                    "uptime_sec": round(time.time() - svc.started_at, 3),
                    "verdict_seq": snap["verdict_seq"],
                },
            )
            return
        if path == "/snapshot":
            self._send_json(200, {"status": "ok", **svc.lm.snapshot()})
            return
        if path == "/events":
            # 仅列标识，避免响应过大
            with svc.lm._lock:
                self._send_json(
                    200,
                    {"event_ids": sorted(svc.lm.events.keys())},
                )
            return
        self._send_json(404, {"status": "rejected", "error": f"未知路径 {path}"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        svc = get_service()

        try:
            if path == "/sessions":
                data = self._read_json()
                self._send_json(201, svc.create_session(data.get("trace_id")))
                return

            if path == "/admin/crash":
                at = (query.get("at") or [""])[0]
                if at not in (CRASH_BEFORE_PERSIST, CRASH_AFTER_PERSIST):
                    self._reject("at 必须为 before-persist 或 after-persist", 400)
                    return
                self._send_json(200, {"status": "crashing", "at": at})
                self.wfile.flush()
                os._exit(91 if at == CRASH_BEFORE_PERSIST else 92)

            if path.startswith("/sessions/") and path.endswith("/events"):
                session_id = path.split("/")[2]
                if session_id not in svc.sessions:
                    self._reject(f"会话 {session_id} 不存在，请先 POST /sessions", 404)
                    return
                event = self._read_json()
                crash = event.pop("crash", None)
                if crash not in (None, CRASH_BEFORE_PERSIST, CRASH_AFTER_PERSIST):
                    self._reject("crash 取值非法", 400)
                    return
                svc.check_quota(session_id, event)
                verdict = svc.lm.submit(event, crash=crash)
                if not verdict.get("replay"):
                    svc.note_accepted(session_id, event)
                verdict["session_id"] = session_id
                self._send_json(200, verdict)
                return

            self._send_json(404, {"status": "rejected", "error": f"未知路径 {path}"})
        except Rejected as exc:
            self._reject(str(exc))


def build_server(host: str, port: int) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    return httpd


def main() -> None:
    host = os.environ.get("LOCK_HOST", "0.0.0.0")
    port = int(os.environ.get("LOCK_PORT", "8080"))
    get_service()  # 预加载/恢复状态
    httpd = build_server(host, port)
    print(f"[lock-coordinator] listening on {host}:{port}, state={DATA_FILE}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
