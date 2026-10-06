# 事务锁协调服务（辐射实验舱校准通道）
# 纯 Python 标准库实现，镜像内无需 pip 安装任何依赖。
FROM python:3.11-slim

WORKDIR /app

COPY app/ ./app/
COPY tests/ ./tests/

ENV LOCK_HOST=0.0.0.0 \
    LOCK_PORT=8080 \
    LOCK_DATA_DIR=/data \
    PYTHONUNBUFFERED=1

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=4s --start-period=5s --retries=10 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3).status == 200 else 1)"

CMD ["python3", "-m", "app.server"]
