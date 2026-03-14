FROM apache/superset:latest
USER root
RUN /app/.venv/bin/python -m ensurepip --upgrade && \
    /app/.venv/bin/python -m pip install clickhouse-connect
USER superset