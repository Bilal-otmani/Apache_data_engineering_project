FROM apache/superset:latest

USER root
RUN pip --python /app/.venv/bin/python install --no-cache-dir psycopg2-binary sqlalchemy-trino
COPY superset_config.py /app/pythonpath/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
USER superset
