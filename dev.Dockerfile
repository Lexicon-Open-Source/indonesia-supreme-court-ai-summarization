FROM python:3.10-slim

ENV PYSETUP_PATH="/opt/pysetup" \
    UV_INSTALL_DIR="/opt/uv" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENV PATH=${UV_INSTALL_DIR}/bin:$PATH

WORKDIR $PYSETUP_PATH

# Install system dependencies and UV
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
    rm -rf /var/lib/apt/lists/* && \
    curl -LsSf https://astral.sh/uv/0.4.29/install.sh | sh && \
    uv python install 3.10

# Copy application files
COPY . .

# Make entrypoint executable
RUN chmod u+x entrypoint.sh

# Install all dependencies
RUN uv sync

ARG SERVICE_PORT=8004
ENV SERVICE_PORT=${SERVICE_PORT}
EXPOSE ${SERVICE_PORT}

ENTRYPOINT ["./entrypoint.sh"]
