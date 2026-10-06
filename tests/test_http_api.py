"""HTTP API 冒烟测试：以子进程启动真实服务，覆盖会话、死锁、拒绝、快照。"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.port = free_port()
        cls.base = f"http://127.0.0.1:{cls.port}"
        env = dict(os.environ)
        env["LOCK_DATA_DIR"] = cls.tmp.name
        env["LOCK_PORT"] = str(cls.port)
        env["LOCK_HOST"] = "127.0.0.1"
        cls.proc = subprocess.Popen(
            [sys.executable, "-m", "app.server"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                urllib.request.urlopen(cls.base + "/healthz", timeout=1).read()
                break
            except Exception:
                time.sleep(0.2)
        else:
            cls.proc.kill()
            raise RuntimeError("server did not start")

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(timeout=10)
        cls.tmp.cleanup()

    def req(self, method, path, payload=None, expect_error=False):
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        r = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(r, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if not expect_error:
                raise
            return e.code, json.loads(e.read().decode())

    def event(self, sid, eid, etype, tx, channel=None, expect_error=False, **extra):
        body = {"event_id": eid, "type": etype, "transaction": tx}
        if channel is not None:
            body["channel"] = channel
        body.update(extra)
        return self.req(
            "POST", f"/sessions/{sid}/events", body, expect_error=expect_error
        )

    def test_full_flow(self):
        code, sess = self.req(
            "POST", "/sessions", {"trace_id": "trace-api-001"}
        )
        self.assertEqual(code, 201)
        sid = sess["session_id"]

        # 稳定 trace_id 重复创建返回同一会话
        _, again = self.req("POST", "/sessions", {"trace_id": "trace-api-001"})
        self.assertTrue(again["replay"])
        self.assertEqual(again["session_id"], sid)

        self.event(sid, "h-b1", "begin", "H1")
        self.event(sid, "h-b2", "begin", "H2")
        code, r = self.event(
            sid, "h-b1-dup", "begin", "H1", expect_error=True
        )  # 重复 begin（新标识）
        self.assertEqual(code, 409)

        self.event(sid, "h-s1a", "share", "H1", "HA")
        self.event(sid, "h-s2b", "share", "H2", "HB")
        self.event(sid, "h-s1b", "share", "H1", "HB")
        self.event(sid, "h-s2a", "share", "H2", "HA")
        self.event(sid, "h-u1b", "upgrade", "H1", "HB")
        code, r = self.event(sid, "h-u2a", "upgrade", "H2", "HA")
        self.assertEqual(code, 200)
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(r["victim"]["transaction"], "H2")
        holders_b = {h["transaction"]: h["mode"] for h in r["holders"]["HB"]}
        self.assertEqual(holders_b, {"H1": "X"})

        # 稳定标识重放
        _, replay = self.event(sid, "h-u2a", "upgrade", "H2", "HA")
        self.assertTrue(replay["replay"])
        # 同标识不同内容 -> 409
        code, _ = self.event(
            sid, "h-u2a", "upgrade", "H2", "OTHER", expect_error=True
        )
        self.assertEqual(code, 409)
        # 撤销后继续操作 -> 409
        code, _ = self.event(
            sid, "h-late", "share", "H2", "HC", expect_error=True
        )
        self.assertEqual(code, 409)
        # 非法释放 -> 409
        code, _ = self.event(
            sid, "h-badrel", "release", "H1", "NOPE", expect_error=True
        )
        self.assertEqual(code, 409)

        # 快照与事件标识可查
        code, snap = self.req("GET", "/snapshot")
        self.assertEqual(code, 200)
        self.assertEqual(snap["transactions"]["H2"]["state"], "aborted")
        code, evs = self.req("GET", "/events")
        self.assertIn("h-u2a", evs["event_ids"])

        # 健康响应
        code, health = self.req("GET", "/healthz")
        self.assertEqual(code, 200)
        self.assertEqual(health["status"], "ok")

    def test_fifo_via_api(self):
        _, sess = self.req("POST", "/sessions", {"trace_id": "trace-api-fifo"})
        sid = sess["session_id"]
        self.event(sid, "f-b3", "begin", "H3")
        self.event(sid, "f-b4", "begin", "H4")
        self.event(sid, "f-b5", "begin", "H5")
        self.event(sid, "f-s3", "share", "H3", "HF")
        _, r = self.event(sid, "f-x4", "exclusive", "H4", "HF")
        self.assertEqual(r["action"], "queued")
        _, r = self.event(sid, "f-s5", "share", "H5", "HF")
        self.assertEqual(r["action"], "queued")  # 不得越过队首
        self.event(sid, "f-r3", "release", "H3", "HF")
        _, snap = self.req("GET", "/snapshot")
        holders = {h["transaction"] for h in snap["channels"]["HF"]["holders"]}
        self.assertEqual(holders, {"H4"})
        self.assertEqual(
            [w["transaction"] for w in snap["channels"]["HF"]["queue"]], ["H5"]
        )
        self.event(sid, "f-c4", "commit", "H4")
        _, snap = self.req("GET", "/snapshot")
        holders = {h["transaction"] for h in snap["channels"]["HF"]["holders"]}
        self.assertEqual(holders, {"H5"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
