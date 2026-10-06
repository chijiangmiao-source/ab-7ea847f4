"""端到端 HTTP 冒烟：在运行中的服务上复现
 1) 共享/独占不兼容与 FIFO 队列推进（兼容请求不得越过队首）；
 2) 相互升级成环 -> 撤销 begin 序号最大者，另一事务取得独占锁；
 3) after-persist 中断重启 -> 完整裁决可观察、重放返回首次裁决；
 4) before-persist 中断重启 -> 回到事件之前，可继续提交该合法事件。

通过环境变量 BASE_URL 指定服务地址（默认 http://service:8080）。
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.environ.get("BASE_URL", "http://service:8080").rstrip("/")
PREFIX = f"acc-{os.urandom(4).hex()}"

failures = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


def request(method, path, payload=None, allow_failure=False):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        BASE_URL + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        if allow_failure:
            return exc.code, json.loads(exc.read().decode())
        raise


def event(sid, eid, etype, tx, channel=None, allow_failure=False, **extra):
    body = {"event_id": f"{PREFIX}-{eid}", "type": etype, "transaction": tx}
    if channel is not None:
        body["channel"] = channel
    body.update(extra)
    return request(
        "POST", f"/sessions/{sid}/events", body, allow_failure=allow_failure
    )


def wait_healthy(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = request("GET", "/healthz")
            if status == 200 and body.get("status") == "ok":
                return body
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("服务未在限定时间内恢复健康")


def main():
    wait_healthy()

    # --------------------------------------------------------------- #
    # 场景一：FIFO 队列推进
    # --------------------------------------------------------------- #
    _, sess = request("POST", "/sessions", {"trace_id": f"{PREFIX}-fifo"})
    sid = sess["session_id"]
    check("会话创建", sid.startswith("sess-"), sid)

    for t in ("F1", "F2", "F3"):
        event(sid, f"begin-{t}", "begin", f"{PREFIX}-{t}")
    event(sid, "f1-s", "share", f"{PREFIX}-F1", f"{PREFIX}-QF")
    _, r = event(sid, "f2-x", "exclusive", f"{PREFIX}-F2", f"{PREFIX}-QF")
    check("F2 独占请求排队", r["action"] == "queued", r["action"])
    _, r = event(sid, "f3-s", "share", f"{PREFIX}-F3", f"{PREFIX}-QF")
    check("后到的兼容请求不得越过队首", r["action"] == "queued", r["action"])
    _, snap = request("GET", "/snapshot")
    queue = [w["transaction"].split("-")[-1] for w in snap["channels"][f"{PREFIX}-QF"]["queue"]]
    check("队列顺序 F2,F3", queue == ["F2", "F3"], str(queue))

    event(sid, "f1-rel", "release", f"{PREFIX}-F1", f"{PREFIX}-QF")
    _, snap = request("GET", "/snapshot")
    holders = {
        h["transaction"].split("-")[-1]: h["mode"]
        for h in snap["channels"][f"{PREFIX}-QF"]["holders"]
    }
    queue = [w["transaction"].split("-")[-1] for w in snap["channels"][f"{PREFIX}-QF"]["queue"]]
    check("释放后队首 F2 取独占", holders == {"F2": "X"}, str(holders))
    check("F3 因不兼容继续等待", queue == ["F3"], str(queue))
    event(sid, "f2-commit", "commit", f"{PREFIX}-F2")
    _, snap = request("GET", "/snapshot")
    holders = {h["transaction"].split("-")[-1] for h in snap["channels"][f"{PREFIX}-QF"]["holders"]}
    check("提交后队列推进 F3", holders == {"F3"}, str(holders))

    # 非法事件被拒绝且不改状态
    seq_before = snap["verdict_seq"]
    code, err = event(sid, "bad-release", "release", f"{PREFIX}-F1", "NOPE", allow_failure=True)
    check("非法释放返回 409", code == 409, err.get("error", ""))
    _, snap = request("GET", "/snapshot")
    check("拒绝不消耗裁决序号", snap["verdict_seq"] == seq_before,
          f"{snap['verdict_seq']}=={seq_before}")

    # --------------------------------------------------------------- #
    # 场景二：相互升级成环 + after-persist 中断恢复
    # --------------------------------------------------------------- #
    _, sess = request("POST", "/sessions", {"trace_id": f"{PREFIX}-deadlock"})
    sid2 = sess["session_id"]
    event(sid2, "begin-d1", "begin", f"{PREFIX}-D1")
    event(sid2, "begin-d2", "begin", f"{PREFIX}-D2")
    event(sid2, "d1-sa", "share", f"{PREFIX}-D1", f"{PREFIX}-DA")
    event(sid2, "d2-sb", "share", f"{PREFIX}-D2", f"{PREFIX}-DB")
    event(sid2, "d1-sb", "share", f"{PREFIX}-D1", f"{PREFIX}-DB")
    event(sid2, "d2-sa", "share", f"{PREFIX}-D2", f"{PREFIX}-DA")
    _, r = event(sid2, "d1-upb", "upgrade", f"{PREFIX}-D1", f"{PREFIX}-DB")
    check("D1 升级 DB 排队", r["action"] == "queued", r["action"])

    seq_before_deadlock = request("GET", "/snapshot")[1]["verdict_seq"]
    # D2 升级 DA 闭合环；在“裁决落盘后”中断
    try:
        event(sid2, "d2-upa-crash", "upgrade", f"{PREFIX}-D2", f"{PREFIX}-DA",
              **{"crash": "after-persist"})
        check("after-persist 中断时连接应断开", False, "未观察到中断")
    except Exception as exc:  # 预期：服务进程退出
        check("after-persist 中断时连接断开", True, type(exc).__name__)

    health = wait_healthy()
    check("服务重启后健康", health["verdict_seq"] >= seq_before_deadlock + 1,
          f"seq={health['verdict_seq']}")
    _, snap = request("GET", "/snapshot")
    check(
        "重启后可见完整裁决：D2 被撤销",
        snap["transactions"][f"{PREFIX}-D2"]["state"] == "aborted"
        and snap["transactions"][f"{PREFIX}-D2"]["locks"] == {}
        and snap["transactions"][f"{PREFIX}-D2"]["waiting"] is None,
    )
    db = f"{PREFIX}-DB"
    holders_b = {
        h["transaction"].split("-")[-1]: h["mode"] for h in snap["channels"][db]["holders"]
    }
    check("D1 取得 DB 独占锁", holders_b == {"D1": "X"}, str(holders_b))
    check("DB 等待队列已清空", snap["channels"][db]["queue"] == [])
    victim = snap["aborted"][-1]
    d1_begin = snap["transactions"][f"{PREFIX}-D1"]["begin_seq"]
    check("被撤销者为环中 begin 序号最大者",
          victim["transaction"] == f"{PREFIX}-D2" and victim["begin_seq"] > d1_begin,
          f"victim={victim} D1.begin_seq={d1_begin}")

    # 重放闭合事件 -> 首次裁决
    _, replay = event(sid2, "d2-upa-crash", "upgrade", f"{PREFIX}-D2", f"{PREFIX}-DA",
                      allow_failure=True)
    check("重放返回首次裁决", replay.get("replay") is True
          and replay["victim"]["transaction"] == f"{PREFIX}-D2", str(replay.get("victim")))

    # --------------------------------------------------------------- #
    # 场景三：before-persist 中断 -> 回到事件之前 -> 可继续提交
    # --------------------------------------------------------------- #
    _, sess = request("POST", "/sessions", {"trace_id": f"{PREFIX}-crashbefore"})
    sid3 = sess["session_id"]
    event(sid3, "begin-g1", "begin", f"{PREFIX}-G1")
    seq_before = request("GET", "/snapshot")[1]["verdict_seq"]
    try:
        event(sid3, "g1-sa-crash", "share", f"{PREFIX}-G1", f"{PREFIX}-GA",
              **{"crash": "before-persist"})
        check("before-persist 中断时连接应断开", False)
    except Exception as exc:
        check("before-persist 中断时连接断开", True, type(exc).__name__)

    wait_healthy()
    _, snap = request("GET", "/snapshot")
    check("状态回到事件之前：裁决序号未推进",
          snap["verdict_seq"] == seq_before,
          f"{snap['verdict_seq']}=={seq_before}")
    code, evs = request("GET", "/events")
    eid = f"{PREFIX}-g1-sa-crash"
    check("事件标识未落盘", eid not in evs["event_ids"])
    # 恢复后继续处理：同标识同内容重新提交应作为新事件处理，而非重放
    _, r = event(sid3, "g1-sa-crash", "share", f"{PREFIX}-G1", f"{PREFIX}-GA")
    check("恢复后成功授予且非重放",
          r["action"] == "granted" and r.get("replay") is False, r["action"])

    # 重复标识不同内容 / 撤销后操作
    code, _ = event(sid2, "d2-upa-crash", "upgrade", f"{PREFIX}-D2", "OTHER",
                    allow_failure=True)
    check("同标识不同内容拒绝", code == 409)
    code, _ = event(sid2, "d2-late", "share", f"{PREFIX}-D2", f"{PREFIX}-DC", allow_failure=True)
    check("撤销后继续操作拒绝", code == 409)

    print("\n健康信息:", json.dumps(wait_healthy(), ensure_ascii=False))
    if failures:
        print(f"\n冒烟失败 {len(failures)} 项: {failures}")
        sys.exit(1)
    print("\n全部冒烟检查通过：锁升级成环裁决、队列推进、重启恢复均可观察。")


if __name__ == "__main__":
    main()
