#!/bin/sh
# verify 容器入口：代码测试 -> 构建检查 -> API/HTTP 冒烟（含死锁撤销与重启恢复）
set -eu

cd "$(dirname "$0")/.."

echo "==================== 1/3 单元测试（引擎 + HTTP API） ===================="
python3 -m unittest discover -s tests -p 'test_*.py' -v

echo "==================== 2/3 构建检查（语法/字节码编译） ===================="
python3 -m compileall -q app tests
echo "compileall OK"

echo "==================== 3/3 API/HTTP 端到端冒烟 ===================="
echo "目标服务：${BASE_URL:-http://lock-coordinator:8080}"
BASE_URL="${BASE_URL:-http://lock-coordinator:8080}" python3 tests/smoke_http.py

echo
echo "验收全部通过：锁升级成环撤销、FIFO 队列推进、中断重启恢复均已观察。"
