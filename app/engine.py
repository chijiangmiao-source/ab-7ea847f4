"""事务锁协调引擎（辐射实验舱校准通道）。

纯标准库实现，特性：
  * 每通道 FIFO 等待队列：队首不兼容时，后到的兼容请求不得越过队首；
  * 共享(S)/独占(X) 申请、S->X 升级、release、commit；
  * 等待图环检测，撤销 begin 序号最大的事务，原子清除其全部锁与等待；
  * 稳定事件 ID 幂等：同 ID 同内容返回首次裁决，同 ID 异内容拒绝；
  * 非法释放、重复 begin、撤销后继续操作一律拒绝且不改状态；
  * 整状态快照 + fsync + os.replace 原子持久化，支持崩溃点注入，
    重启后状态只可能位于“该事件之前”或“完整裁决之后”。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

SHARED = "S"
EXCLUSIVE = "X"

ACTIVE = "active"
ABORTED = "aborted"

CRASH_BEFORE_PERSIST = "before-persist"
CRASH_AFTER_PERSIST = "after-persist"


class Rejected(Exception):
    """事件非法：拒绝且不改变任何状态、不消耗裁决序号。"""


@dataclass
class Waiter:
    tid: str
    mode: str
    seq: int  # 该申请事件的裁决序号

    def to_dict(self) -> Dict[str, Any]:
        return {"transaction": self.tid, "mode": self.mode, "event_seq": self.seq}


@dataclass
class Transaction:
    tid: str
    begin_seq: int
    state: str = ACTIVE
    abort_seq: Optional[int] = None
    # channel -> "S" / "X"（等待升级期间仍保留原 S 锁）
    locks: Dict[str, str] = field(default_factory=dict)
    waiting: Optional[Waiter] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.tid,
            "state": self.state,
            "begin_seq": self.begin_seq,
            "abort_seq": self.abort_seq,
            "locks": dict(sorted(self.locks.items())),
            "waiting": self.waiting.to_dict() if self.waiting else None,
        }


def _incompatible(a: str, b: str) -> bool:
    return a == EXCLUSIVE or b == EXCLUSIVE


class LockManager:
    def __init__(self, data_path: Optional[str] = None) -> None:
        self._lock = threading.RLock()
        self.data_path = data_path
        self.seq = 0
        self.begin_counter = 0
        self.transactions: Dict[str, Transaction] = {}
        # channel -> {holders: tid->mode, queue: [Waiter]}
        self.channels: Dict[str, Dict[str, Any]] = {}
        # event_id -> (请求指纹, 首次裁决完整响应)
        self.events: Dict[str, Tuple[str, Dict[str, Any]]] = {}
        # 累计被撤销事务（tid, begin_seq, abort_seq）
        self.aborted_log: List[Dict[str, int]] = []
        if data_path and os.path.exists(data_path):
            self._load()

    # ------------------------------------------------------------------ #
    # 通道辅助
    # ------------------------------------------------------------------ #
    def _channel(self, name: str) -> Dict[str, Any]:
        ch = self.channels.get(name)
        if ch is None:
            ch = {"holders": {}, "queue": []}
            self.channels[name] = ch
        return ch

    def _drop_channel_if_empty(self, name: str) -> None:
        ch = self.channels.get(name)
        if ch is not None and not ch["holders"] and not ch["queue"]:
            del self.channels[name]

    # ------------------------------------------------------------------ #
    # 快照 / 持久化
    # ------------------------------------------------------------------ #
    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return self._snapshot_locked()

    def _snapshot_locked(self) -> Dict[str, Any]:
        channels: Dict[str, Any] = {}
        for name, ch in sorted(self.channels.items()):
            channels[name] = {
                "holders": [
                    {"transaction": tid, "mode": mode}
                    for tid, mode in sorted(ch["holders"].items())
                ],
                "queue": [w.to_dict() for w in ch["queue"]],
            }
        return {
            "verdict_seq": self.seq,
            "next_begin_seq": self.begin_counter + 1,
            "transactions": {
                tid: tx.to_dict() for tid, tx in sorted(self.transactions.items())
            },
            "channels": channels,
            "aborted": [dict(x) for x in self.aborted_log],
        }

    def _to_disk(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "seq": self.seq,
            "begin_counter": self.begin_counter,
            "transactions": {
                tid: {
                    "tid": tx.tid,
                    "begin_seq": tx.begin_seq,
                    "state": tx.state,
                    "abort_seq": tx.abort_seq,
                    "locks": tx.locks,
                    "waiting": (
                        None
                        if tx.waiting is None
                        else {
                            "tid": tx.waiting.tid,
                            "mode": tx.waiting.mode,
                            "seq": tx.waiting.seq,
                        }
                    ),
                }
                for tid, tx in self.transactions.items()
            },
            "channels": {
                name: {
                    "holders": dict(ch["holders"]),
                    "queue": [
                        {"tid": w.tid, "mode": w.mode, "seq": w.seq}
                        for w in ch["queue"]
                    ],
                }
                for name, ch in self.channels.items()
            },
            "events": {
                eid: {"fingerprint": fp, "verdict": verdict}
                for eid, (fp, verdict) in self.events.items()
            },
            "aborted_log": self.aborted_log,
        }

    def _load(self) -> None:
        with open(self.data_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        self.seq = data["seq"]
        self.begin_counter = data["begin_counter"]
        self.transactions = {}
        for tid, td in data["transactions"].items():
            tx = Transaction(
                tid=td["tid"],
                begin_seq=td["begin_seq"],
                state=td["state"],
                abort_seq=td.get("abort_seq"),
                locks=dict(td["locks"]),
            )
            if td.get("waiting"):
                w = td["waiting"]
                tx.waiting = Waiter(w["tid"], w["mode"], w["seq"])
            self.transactions[tid] = tx
        self.channels = {}
        for name, cd in data["channels"].items():
            self.channels[name] = {
                "holders": dict(cd["holders"]),
                "queue": [
                    Waiter(w["tid"], w["mode"], w["seq"]) for w in cd["queue"]
                ],
            }
        self.events = {
            eid: (rec["fingerprint"], rec["verdict"])
            for eid, rec in data["events"].items()
        }
        self.aborted_log = [dict(x) for x in data.get("aborted_log", [])]

    def _persist(self, crash: Optional[str]) -> None:
        if not self.data_path:
            return
        if crash == CRASH_BEFORE_PERSIST:
            os._exit(91)
        payload = json.dumps(self._to_disk(), ensure_ascii=False, sort_keys=True)
        tmp_path = self.data_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self.data_path)
        dir_fd = os.open(os.path.dirname(os.path.abspath(self.data_path)), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        if crash == CRASH_AFTER_PERSIST:
            os._exit(92)

    # ------------------------------------------------------------------ #
    # 对外提交
    # ------------------------------------------------------------------ #
    def submit(
        self, event: Dict[str, Any], crash: Optional[str] = None
    ) -> Dict[str, Any]:
        """提交一个事件，返回裁决字典。非法事件抛 Rejected，状态不变。"""
        with self._lock:
            event_id = event.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise Rejected("event_id 缺失")
            etype = event.get("type")
            if etype not in (
                "begin",
                "share",
                "exclusive",
                "upgrade",
                "release",
                "commit",
            ):
                raise Rejected(f"未知事件类型: {etype!r}")

            fingerprint = json.dumps(
                {
                    "type": etype,
                    "transaction": event.get("transaction"),
                    "channel": event.get("channel"),
                    "mode": event.get("mode"),
                },
                sort_keys=True,
                ensure_ascii=False,
            )

            # 稳定标识：重复提交
            prior = self.events.get(event_id)
            if prior is not None:
                prior_fp, prior_verdict = prior
                if prior_fp != fingerprint:
                    raise Rejected(
                        "事件标识已存在但内容不同（同一稳定标识不得重放不同请求）"
                    )
                result = json.loads(json.dumps(prior_verdict))
                result["replay"] = True
                return result

            tid = event.get("transaction")
            if not isinstance(tid, str) or not tid:
                raise Rejected("transaction 缺失")
            channel = event.get("channel")
            if etype in ("share", "exclusive", "upgrade", "release"):
                if not isinstance(channel, str) or not channel:
                    raise Rejected("channel 缺失")

            # ---- 全部合法性校验在分配裁决序号之前完成（拒绝不消耗序号、不改状态）----
            tx: Optional[Transaction] = None
            if etype == "begin":
                if tid in self.transactions:
                    raise Rejected(f"事务 {tid} 已经开始，重复 begin 被拒绝")
            else:
                tx = self._require_active(tid)
                if etype in ("share", "exclusive", "upgrade") and tx.waiting is not None:
                    raise Rejected("事务已有等待中的申请，必须先被授予或终结")
                if etype == "upgrade":
                    held = tx.locks.get(channel)
                    if held is None:
                        raise Rejected(
                            f"非法升级：事务 {tid} 未持有通道 {channel} 的共享锁"
                        )
                    if held == EXCLUSIVE:
                        raise Rejected(
                            f"非法升级：事务 {tid} 已持有通道 {channel} 的独占锁"
                        )
                elif etype == "exclusive" and tx.locks.get(channel) == SHARED:
                    raise Rejected(
                        f"事务 {tid} 已持共享锁，申请独占必须走 upgrade 事件"
                    )
                elif etype == "release":
                    if tx.waiting is not None and self._find_waiting_channel(
                        tid
                    ) == channel:
                        raise Rejected(
                            f"非法释放：事务 {tid} 在通道 {channel} 上有等待中的升级申请"
                        )
                    if channel not in tx.locks:
                        raise Rejected(
                            f"非法释放：事务 {tid} 未持有通道 {channel} 的锁"
                        )

            # ---- 校验通过，分配裁决序号并应用 ----
            self.seq += 1
            seq = self.seq
            action = ""
            victim_info: Optional[Dict[str, Any]] = None

            if etype == "begin":
                self.begin_counter += 1
                tx = Transaction(tid=tid, begin_seq=self.begin_counter)
                self.transactions[tid] = tx
                action = "began"

            elif etype in ("share", "exclusive", "upgrade"):
                mode = SHARED if etype == "share" else EXCLUSIVE
                already = tx.locks.get(channel)
                # 已持锁的重复申请幂等授予：持 S 再申 S、持 X 再申 S/X 均为无操作
                if already is not None and (
                    etype == "share" or already == EXCLUSIVE
                ):
                    action = "granted"
                else:
                    ch = self._channel(channel)
                    conflict_holders = [
                        h for h in ch["holders"]
                        if h != tid and _incompatible(ch["holders"][h], mode)
                    ]
                    # 公平队列：只要队列非空，后到者一律排在队尾，
                    # 即使它与队首都兼容也不得越过。
                    if conflict_holders or ch["queue"]:
                        waiter = Waiter(tid=tid, mode=mode, seq=seq)
                        ch["queue"].append(waiter)
                        tx.waiting = waiter
                        action = "queued"
                        victim = self._detect_deadlock()
                        if victim is not None:
                            victim_info = self._abort(victim, seq)
                            action = "deadlock"
                    else:
                        self._grant(tx, channel, mode)
                        action = "granted"

            elif etype == "release":
                del tx.locks[channel]
                ch = self.channels[channel]
                del ch["holders"][tid]
                self._drop_channel_if_empty(channel)
                self._propagate()
                action = "released"

            else:  # commit
                self._remove_everything(tx)
                self._propagate()
                action = "committed"

            # 死锁裁决中受害者已被清除；队列推进已在 _abort -> _propagate 完成
            verdict = self._make_verdict(
                seq=seq,
                event_id=event_id,
                etype=etype,
                tid=tid,
                action=action,
                victim=victim_info,
            )
            self.events[event_id] = (fingerprint, verdict)
            self._persist(crash)
            return json.loads(json.dumps(verdict))

    # ------------------------------------------------------------------ #
    # 内部规则
    # ------------------------------------------------------------------ #
    def _require_active(self, tid: str) -> Transaction:
        tx = self.transactions.get(tid)
        if tx is None:
            raise Rejected(f"事务 {tid} 尚未 begin")
        if tx.state == ABORTED:
            raise Rejected(
                f"事务 {tid} 已在裁决 {tx.abort_seq} 中被撤销，不得继续操作"
            )
        if tx.state == "committed":
            raise Rejected(f"事务 {tid} 已经提交，不得继续操作")
        return tx

    def _find_waiting_channel(self, tid: str) -> Optional[str]:
        for name, ch in self.channels.items():
            if any(w.tid == tid for w in ch["queue"]):
                return name
        return None

    def _grant(self, tx: Transaction, channel: str, mode: str) -> None:
        ch = self._channel(channel)
        held = ch["holders"].get(tx.tid)
        ch["holders"][tx.tid] = (
            EXCLUSIVE if mode == EXCLUSIVE or held == EXCLUSIVE else SHARED
        )
        tx.locks[channel] = ch["holders"][tx.tid]
        tx.waiting = None

    def _propagate(self) -> None:
        """严格按 FIFO 推进所有通道；队首不满足则停止，后续不得跳过。"""
        progressed = True
        while progressed:
            progressed = False
            for name, ch in self.channels.items():
                while ch["queue"]:
                    head = ch["queue"][0]
                    conflict = any(
                        h != head.tid
                        and _incompatible(ch["holders"][h], head.mode)
                        for h in ch["holders"]
                    )
                    if conflict:
                        break  # 队首受阻，后面的兼容请求也不得越过
                    ch["queue"].pop(0)
                    tx = self.transactions.get(head.tid)
                    if tx is not None and tx.state == ACTIVE:
                        self._grant(tx, name, head.mode)
                    progressed = True
            for name in list(self.channels):
                self._drop_channel_if_empty(name)

    def _wait_edges(self) -> Dict[str, set]:
        """等待图：等待者 -> 阻塞它的持锁者，以及队列中排在其前面的冲突等待者。"""
        edges: Dict[str, set] = {}
        for name, ch in self.channels.items():
            queue = ch["queue"]
            for i, w in enumerate(queue):
                blocked: set = set()
                for h, hm in ch["holders"].items():
                    if h != w.tid and _incompatible(hm, w.mode):
                        blocked.add(h)
                for earlier in queue[:i]:
                    if earlier.tid != w.tid and _incompatible(
                        earlier.mode, w.mode
                    ):
                        blocked.add(earlier.tid)
                if blocked:
                    edges.setdefault(w.tid, set()).update(blocked)
        return edges

    def _detect_deadlock(self) -> Optional[str]:
        """在等待图中找出所有环（Tarjan SCC），撤销环中 begin 序号最大者。"""
        edges = self._wait_edges()
        index = 0
        indices: Dict[str, int] = {}
        low: Dict[str, int] = {}
        stack: List[str] = []
        on_stack: set = set()
        cyclic: List[str] = []

        def strongconnect(v: str) -> None:
            nonlocal index
            indices[v] = low[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in sorted(edges.get(v, ())):
                if w not in indices:
                    strongconnect(w)
                    low[v] = min(low[v], low[w])
                elif w in on_stack:
                    low[v] = min(low[v], indices[w])
            if low[v] == indices[v]:
                comp: List[str] = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.append(w)
                    if w == v:
                        break
                if len(comp) > 1 or (len(comp) == 1 and comp[0] in edges.get(comp[0], set())):
                    cyclic.extend(comp)

        for node in sorted(edges):
            if node not in indices:
                strongconnect(node)

        if not cyclic:
            return None
        return max(
            cyclic,
            key=lambda t: (self.transactions[t].begin_seq, t),
        )

    def _abort(self, victim_tid: str, verdict_seq: int) -> Dict[str, Any]:
        tx = self.transactions[victim_tid]
        tx.state = ABORTED
        tx.abort_seq = verdict_seq
        tx.waiting = None
        # 原子清除其全部锁
        for name in list(tx.locks):
            ch = self.channels.get(name)
            if ch and victim_tid in ch["holders"]:
                del ch["holders"][victim_tid]
        tx.locks.clear()
        # 原子清除其全部等待（每个事务至多一个等待者）
        for ch in self.channels.values():
            ch["queue"] = [w for w in ch["queue"] if w.tid != victim_tid]
        info = {
            "transaction": victim_tid,
            "begin_seq": tx.begin_seq,
            "verdict_seq": verdict_seq,
        }
        self.aborted_log.append(info)
        for name in list(self.channels):
            self._drop_channel_if_empty(name)
        # 让其他等待事务按 FIFO 推进（另一事务可因此取得独占锁）
        self._propagate()
        return info

    def _remove_everything(self, tx: Transaction) -> None:
        for name in list(tx.locks):
            ch = self.channels.get(name)
            if ch and tx.tid in ch["holders"]:
                del ch["holders"][tx.tid]
        tx.locks.clear()
        tx.waiting = None
        for ch in self.channels.values():
            ch["queue"] = [w for w in ch["queue"] if w.tid != tx.tid]
        tx.state = "committed"

    # ------------------------------------------------------------------ #
    # 响应组装
    # ------------------------------------------------------------------ #
    def _make_verdict(
        self,
        seq: int,
        event_id: str,
        etype: str,
        tid: str,
        action: str,
        victim: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        snap = self._snapshot_locked()
        result: Dict[str, Any] = {
            "verdict_seq": seq,
            "event_id": event_id,
            "type": etype,
            "status": "deadlock" if victim else "ok",
            "action": action,
            "replay": False,
            "transaction": snap["transactions"].get(tid),
            "victim": victim,
            "queues": {
                name: cd["queue"]
                for name, cd in snap["channels"].items()
                if cd["queue"]
            },
            "holders": {
                name: cd["holders"]
                for name, cd in snap["channels"].items()
                if cd["holders"]
            },
            "aborted": [dict(x) for x in self.aborted_log],
            "snapshot": snap,
        }
        return result
