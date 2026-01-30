FROM python:3.14-slim AS builder

WORKDIR /app

# Install uv for faster dependency installation
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml uv.lock* ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

FROM python:3.14-slim

WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY *.py pyproject.toml uv.lock* ./

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "main.py"]
