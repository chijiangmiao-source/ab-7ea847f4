"""引擎级测试：锁授予、FIFO 队列、死锁撤销、稳定标识幂等、非法事件、崩溃恢复。"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.engine import (  # noqa: E402
    CRASH_AFTER_PERSIST,
    CRASH_BEFORE_PERSIST,
    EXCLUSIVE,
    SHARED,
    LockManager,
    Rejected,
)


def ev(eid, etype, tx, channel=None):
    base = {"event_id": eid, "type": etype, "transaction": tx}
    if channel is not None:
        base["channel"] = channel
    return base


class LockRulesTest(unittest.TestCase):
    def setUp(self):
        self.lm = LockManager()

    def test_shared_compatible_exclusive_blocks(self):
        self.lm.submit(ev("e1", "begin", "T1"))
        self.lm.submit(ev("e2", "begin", "T2"))
        r = self.lm.submit(ev("e3", "share", "T1", "A"))
        self.assertEqual(r["action"], "granted")
        r = self.lm.submit(ev("e4", "share", "T2", "A"))
        self.assertEqual(r["action"], "granted")  # 双 S 兼容
        self.lm.submit(ev("e5", "begin", "T3"))
        r = self.lm.submit(ev("e6", "exclusive", "T3", "A"))
        self.assertEqual(r["action"], "queued")  # X 被两个 S 阻塞
        self.assertEqual(r["queues"]["A"][0]["transaction"], "T3")

    def test_verdict_seq_monotonic_and_view(self):
        for i, t in enumerate(("T1", "T2")):
            r = self.lm.submit(ev(f"b{i}", "begin", t))
            self.assertEqual(r["verdict_seq"], i + 1)
        r = self.lm.submit(ev("s1", "share", "T1", "C1"))
        self.assertEqual(r["verdict_seq"], 3)
        self.assertEqual(r["holders"]["C1"][0]["mode"], SHARED)
        self.assertEqual(self.lm.snapshot()["verdict_seq"], 3)

    def test_fifo_compatible_later_request_cannot_jump(self):
        # T1 持 S；T2 申请 X 排队（队首）；T3 申请 S 即使与当前持锁兼容也不得越过队首
        self.lm.submit(ev("b1", "begin", "T1"))
        self.lm.submit(ev("b2", "begin", "T2"))
        self.lm.submit(ev("b3", "begin", "T3"))
        self.lm.submit(ev("s1", "share", "T1", "CH"))
        r = self.lm.submit(ev("x2", "exclusive", "T2", "CH"))
        self.assertEqual(r["action"], "queued")
        r = self.lm.submit(ev("s3", "share", "T3", "CH"))
        self.assertEqual(r["action"], "queued")
        q = self.lm.snapshot()["channels"]["CH"]["queue"]
        self.assertEqual([w["transaction"] for w in q], ["T2", "T3"])
        # T1 释放：队首 T2 得 X；T3 与 X 不兼容，仍须等待
        self.lm.submit(ev("r1", "release", "T1", "CH"))
        snap = self.lm.snapshot()
        holders = {h["transaction"]: h["mode"] for h in snap["channels"]["CH"]["holders"]}
        self.assertEqual(holders, {"T2": EXCLUSIVE})
        self.assertEqual(
            [w["transaction"] for w in snap["channels"]["CH"]["queue"]], ["T3"]
        )

    def test_upgrade_when_sole_holder(self):
        self.lm.submit(ev("b1", "begin", "T1"))
        self.lm.submit(ev("s1", "share", "T1", "A"))
        r = self.lm.submit(ev("u1", "upgrade", "T1", "A"))
        self.assertEqual(r["action"], "granted")
        self.assertEqual(
            self.lm.snapshot()["channels"]["A"]["holders"][0]["mode"], EXCLUSIVE
        )


class DeadlockTest(unittest.TestCase):
    def setUp(self):
        self.lm = LockManager()
        # 两事务先分别在不同通道取共享锁，再在对方通道也取共享锁（S 互相兼容）
        self.lm.submit(ev("b1", "begin", "T1"))
        self.lm.submit(ev("b2", "begin", "T2"))
        self.lm.submit(ev("s1a", "share", "T1", "A"))
        self.lm.submit(ev("s2b", "share", "T2", "B"))
        self.lm.submit(ev("s1b", "share", "T1", "B"))
        self.lm.submit(ev("s2a", "share", "T2", "A"))

    def test_cycle_aborts_latest_begin_and_other_gets_exclusive(self):
        # T1 升级 B -> 等待 T2
        r = self.lm.submit(ev("u1b", "upgrade", "T1", "B"))
        self.assertEqual(r["action"], "queued")
        # T2 升级 A -> 环；T2 begin 序号更大，应被撤销
        r = self.lm.submit(ev("u2a", "upgrade", "T2", "A"))
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(r["victim"]["transaction"], "T2")
        self.assertEqual(r["victim"]["begin_seq"], 2)

        snap = self.lm.snapshot()
        # T2 已撤销，其锁与等待全部清除
        self.assertEqual(snap["transactions"]["T2"]["state"], "aborted")
        self.assertEqual(snap["transactions"]["T2"]["locks"], {})
        self.assertIsNone(snap["transactions"]["T2"]["waiting"])
        self.assertEqual(snap["aborted"][0]["transaction"], "T2")
        # T1 取得 B 的独占锁；A 上仍持有 S
        holders_a = {h["transaction"]: h["mode"] for h in snap["channels"]["A"]["holders"]}
        holders_b = {h["transaction"]: h["mode"] for h in snap["channels"]["B"]["holders"]}
        self.assertEqual(holders_a, {"T1": SHARED})
        self.assertEqual(holders_b, {"T1": EXCLUSIVE})
        self.assertFalse(snap["channels"]["B"]["queue"])

    def test_same_channel_upgrade_against_queued_exclusive(self):
        # T1 持 S(A)；T2 申请 X(A) 排队等待 T1；
        # T1 随后升级 A -> 排在 T2 之后且被其 X 请求阻塞 -> 环
        self.lm.submit(ev("ub1", "begin", "U1"))
        self.lm.submit(ev("ub2", "begin", "U2"))
        self.lm.submit(ev("us1", "share", "U1", "UA"))
        self.assertEqual(
            self.lm.submit(ev("ux2", "exclusive", "U2", "UA"))["action"], "queued"
        )
        r = self.lm.submit(ev("uu1", "upgrade", "U1", "UA"))
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(r["victim"]["transaction"], "U2")  # begin 序号更大
        snap = self.lm.snapshot()
        holders = {h["transaction"]: h["mode"] for h in snap["channels"]["UA"]["holders"]}
        self.assertEqual(holders, {"U1": EXCLUSIVE})  # U1 升级成功
        self.assertEqual(snap["channels"]["UA"]["queue"], [])
        self.assertEqual(snap["transactions"]["U2"]["state"], "aborted")

    def test_aborted_txn_rejected_and_state_unchanged(self):
        self.lm.submit(ev("u1b", "upgrade", "T1", "B"))
        r = self.lm.submit(ev("u2a", "upgrade", "T2", "A"))
        seq = r["verdict_seq"]
        before = self.lm.snapshot()
        for bad in (
            ev("x1", "share", "T2", "C"),
            ev("x2", "release", "T2", "B"),
            ev("x3", "commit", "T2"),
        ):
            with self.subEvent(bad["event_id"]):
                with self.assertRaises(Rejected):
                    self.lm.submit(bad)
        after = self.lm.snapshot()
        self.assertEqual(after, before)  # 拒绝不改状态
        self.assertEqual(self.lm.snapshot()["verdict_seq"], seq)  # 不消耗裁决序号

    def subEvent(self, name):  # 可读性包装
        return self.subTest(name=name)


class IdempotencyAndValidationTest(unittest.TestCase):
    def setUp(self):
        self.lm = LockManager()
        self.lm.submit(ev("b1", "begin", "T1"))

    def test_same_event_id_same_content_returns_first_verdict(self):
        r1 = self.lm.submit(ev("e1", "share", "T1", "A"))
        r2 = self.lm.submit(ev("e1", "share", "T1", "A"))
        self.assertTrue(r2["replay"])
        self.assertEqual(r2["verdict_seq"], r1["verdict_seq"])
        self.assertEqual(
            {k: r1[k] for k in ("action", "status", "holders")},
            {k: r2[k] for k in ("action", "status", "holders")},
        )

    def test_same_event_id_different_content_rejected(self):
        self.lm.submit(ev("e1", "share", "T1", "A"))
        before = self.lm.snapshot()
        with self.assertRaises(Rejected):
            self.lm.submit(ev("e1", "share", "T1", "OTHER"))
        with self.assertRaises(Rejected):
            self.lm.submit(ev("e1", "exclusive", "T1", "A"))
        with self.assertRaises(Rejected):
            self.lm.submit(ev("e1", "begin", "T9"))
        self.assertEqual(self.lm.snapshot(), before)

    def test_duplicate_begin_illegal_release_unknown_type(self):
        with self.assertRaises(Rejected):
            self.lm.submit(ev("b1dup", "begin", "T1"))  # 重复 begin（新标识）
        with self.assertRaises(Rejected):  # 非法释放
            self.lm.submit(ev("r1", "release", "T1", "ZZ"))
        with self.assertRaises(Rejected):  # 未 begin 的事务
            self.lm.submit(ev("s2", "share", "T_NOPE", "A"))
        with self.assertRaises(Rejected):  # 未知类型
            self.lm.submit({"event_id": "z", "type": "explode", "transaction": "T1"})
        with self.assertRaises(Rejected):  # 缺标识
            self.lm.submit({"type": "begin", "transaction": "T2"})
        # 全部拒绝后序号仍为 1（只有首次 begin）
        self.assertEqual(self.lm.snapshot()["verdict_seq"], 1)

    def test_release_during_waiting_upgrade_rejected(self):
        self.lm.submit(ev("b2", "begin", "T2"))
        self.lm.submit(ev("s1", "share", "T1", "A"))
        self.lm.submit(ev("s2", "share", "T2", "A"))
        # T2 升级 A，被 T1 的 S 阻塞
        self.assertEqual(
            self.lm.submit(ev("u2", "upgrade", "T2", "A"))["action"], "queued"
        )
        with self.assertRaises(Rejected):
            self.lm.submit(ev("r2", "release", "T2", "A"))

    def test_commit_clears_locks_and_advances_queue(self):
        self.lm.submit(ev("b2", "begin", "T2"))
        self.lm.submit(ev("s1", "share", "T1", "A"))
        self.assertEqual(
            self.lm.submit(ev("x2", "exclusive", "T2", "A"))["action"], "queued"
        )
        self.lm.submit(ev("c1", "commit", "T1"))
        snap = self.lm.snapshot()
        holders = {h["transaction"] for h in snap["channels"]["A"]["holders"]}
        self.assertEqual(holders, {"T2"})
        self.assertFalse(snap["channels"]["A"]["queue"])
        with self.assertRaises(Rejected):  # 提交后继续操作
            self.lm.submit(ev("s9", "share", "T1", "A"))


class PersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "state.json")

    def tearDown(self):
        self.tmp.cleanup()

    def _build_ring_prefix(self):
        lm = LockManager(self.path)
        lm.submit(ev("b1", "begin", "T1"))
        lm.submit(ev("b2", "begin", "T2"))
        lm.submit(ev("s1a", "share", "T1", "A"))
        lm.submit(ev("s2b", "share", "T2", "B"))
        lm.submit(ev("s1b", "share", "T1", "B"))
        lm.submit(ev("s2a", "share", "T2", "A"))
        lm.submit(ev("u1b", "upgrade", "T1", "B"))
        return lm

    def test_restore_then_replay(self):
        self._build_ring_prefix()
        lm2 = LockManager(self.path)
        snap = lm2.snapshot()
        self.assertEqual(snap["verdict_seq"], 7)
        self.assertEqual(
            [w["transaction"] for w in snap["channels"]["B"]["queue"]], ["T1"]
        )
        # 重启后同标识重放，仍返回首次裁决
        replay = lm2.submit(ev("u1b", "upgrade", "T1", "B"))
        self.assertTrue(replay["replay"])
        self.assertEqual(replay["verdict_seq"], 7)

    def _crash_in_subprocess(self, crash_point):
        const = (
            "CRASH_BEFORE_PERSIST"
            if crash_point == CRASH_BEFORE_PERSIST
            else "CRASH_AFTER_PERSIST"
        )
        code = (
            "import sys; sys.path.insert(0, %r);"
            "from app.engine import LockManager, %s;"
            "lm = LockManager(%r);"
            "lm.submit({'event_id': 'u2a', 'type': 'upgrade',"
            " 'transaction': 'T2', 'channel': 'A'}, crash=%s)"
            % (
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                const,
                self.path,
                const,
            )
        )
        proc = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        expected = 91 if crash_point == CRASH_BEFORE_PERSIST else 92
        self.assertEqual(proc.returncode, expected, proc.stderr)

    def test_crash_before_persist_state_goes_back_to_before_event(self):
        self._build_ring_prefix()
        self._crash_in_subprocess(CRASH_BEFORE_PERSIST)
        lm = LockManager(self.path)
        snap = lm.snapshot()
        # 回到“该事件之前”：裁决序号未推进、环尚未闭合、事件标识未知
        self.assertEqual(snap["verdict_seq"], 7)
        self.assertNotIn("u2a", lm.events)
        self.assertEqual(snap["transactions"]["T2"]["state"], "active")
        # 可以继续处理后续合法事件：再次升级 -> 完成死锁裁决
        r = lm.submit(ev("u2a", "upgrade", "T2", "A"))
        self.assertEqual(r["status"], "deadlock")
        self.assertEqual(r["victim"]["transaction"], "T2")
        self.assertEqual(
            lm.snapshot()["channels"]["B"]["holders"][0]["mode"], EXCLUSIVE
        )

    def test_crash_after_persist_state_is_full_verdict_and_replay(self):
        self._build_ring_prefix()
        self._crash_in_subprocess(CRASH_AFTER_PERSIST)
        lm = LockManager(self.path)
        snap = lm.snapshot()
        # 完整裁决后：T2 被撤销，T1 拿到 B 的独占锁
        self.assertEqual(snap["verdict_seq"], 8)
        self.assertEqual(snap["transactions"]["T2"]["state"], "aborted")
        holders_b = {h["transaction"]: h["mode"] for h in snap["channels"]["B"]["holders"]}
        self.assertEqual(holders_b, {"T1": EXCLUSIVE})
        replay = lm.submit(ev("u2a", "upgrade", "T2", "A"))
        self.assertTrue(replay["replay"])
        self.assertEqual(replay["victim"]["transaction"], "T2")
        # 撤销后继续操作仍被拒绝
        with self.assertRaises(Rejected):
            lm.submit(ev("late", "share", "T2", "C"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
