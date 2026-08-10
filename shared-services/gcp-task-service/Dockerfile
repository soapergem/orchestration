# Matches the sibling mock services (plain pip, no uv project) -- these images
# are built standalone, outside the root uv workspace.
#
# Cloud Run injects PORT and requires the container to listen on it, hence the
# shell-form CMD; the default keeps `docker run` usable locally.
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
      fastapi \
      uvicorn \
      httpx \
      pydantic-settings \
      psycopg2-binary \
      pyarrow \
      google-cloud-storage

COPY app.py .

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --no-server-header"]
