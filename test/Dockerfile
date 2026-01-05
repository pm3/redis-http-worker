FROM python:3.14-slim

WORKDIR /app

# Install uv for faster dependency installation
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies using uv
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY *.py ./

# Default command (can be overridden in docker-compose)
CMD ["python", "-c", "print('Hello, World!')"]

