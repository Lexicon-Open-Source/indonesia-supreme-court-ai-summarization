FROM python:3.10-slim
ENV PYSETUP_PATH="/opt/pysetup" \
    UV_INSTALL_DIR="/opt/uv"
ENV PATH=${UV_INSTALL_DIR}/bin:$PATH

WORKDIR $PYSETUP_PATH

# Install system dependencies, UV, and create credential directory in one layer
RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential \
    clang curl libgl1 libglib2.0-0 poppler-utils tesseract-ocr ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -LsSf https://astral.sh/uv/0.4.29/install.sh | sh && \
    uv python install 3.10 && \
    mkdir -p /etc/google/auth && \
    chmod 700 /etc/google/auth

# Copy service account file
COPY lexicon-bo-crawler-service-account.json /etc/google/auth/
RUN chmod 600 /etc/google/auth/lexicon-bo-crawler-service-account.json

# Copy application files
COPY . .

# Make entrypoint executable
RUN chmod u+x entrypoint.sh

# Install all dependencies in a single layer
RUN uv sync

ARG SERVICE_PORT
ENV SERVICE_PORT=${SERVICE_PORT}
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["./entrypoint.sh"]
