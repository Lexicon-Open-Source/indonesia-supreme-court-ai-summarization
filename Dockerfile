FROM python:3.10-slim

ENV PYSETUP_PATH="/opt/pysetup" \
    UV_INSTALL_DIR="/opt/uv" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENV PATH=${UV_INSTALL_DIR}:$PATH

WORKDIR $PYSETUP_PATH

# Install system dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
    build-essential \
    clang \
    curl \
    libgl1 \
    libglib2.0-0 \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-ind \
    ca-certificates \
    python3-dev \
    default-libmysqlclient-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install UV
RUN curl -LsSf https://astral.sh/uv/0.9.10/install.sh | sh && \
    uv python install 3.10

# Copy application files
COPY pyproject.toml uv.lock ./
COPY src/ ./src/
COPY main.py cli.py contexts.py settings.py nats_consumer.py entrypoint.sh ./

# Make entrypoint executable
RUN chmod u+x entrypoint.sh

# Install dependencies
RUN uv sync --frozen

# Expose port
ARG PORT=8004
ENV PORT=${PORT}
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

ENTRYPOINT ["./entrypoint.sh"]
