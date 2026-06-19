FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

WORKDIR /app

# Copy project metadata first for layer caching
COPY pyproject.toml uv.lock* /app/

# Install dependencies
RUN uv sync --no-dev --frozen || uv sync --no-dev

# Copy worker code
COPY hatchet/ /app/hatchet/

WORKDIR /app/hatchet

CMD ["uv", "run", "--project", "/app", "python", "worker.py"]
