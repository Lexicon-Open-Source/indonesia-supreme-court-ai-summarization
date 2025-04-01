FROM python:3.10-slim
ENV PYSETUP_PATH="/opt/pysetup" \
    UV_INSTALL_DIR="/opt/uv"
ENV PATH=${UV_INSTALL_DIR}/bin:$PATH

WORKDIR $PYSETUP_PATH

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential \
    clang curl libgl1 libglib2.0-0 poppler-utils tesseract-ocr ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -LsSf https://astral.sh/uv/0.4.29/install.sh | sh && \
    uv python install 3.10

# Create credentials directory and set permissions
RUN mkdir -p /etc/google/auth && \
    chmod 700 /etc/google/auth

# Copy service account file
COPY lexicon-bo-crawler-service-account.json /etc/google/auth/
RUN chmod 600 /etc/google/auth/lexicon-bo-crawler-service-account.json

COPY . .

# Make entrypoint executable
RUN chmod u+x entrypoint.sh

# Install PyTorch CPU version first
RUN pip install torch==2.0.1 torchvision==0.15.2 --index-url https://download.pytorch.org/whl/cpu

# Install other dependencies without using torch from pyproject.toml
RUN uv pip install --system -e . --no-deps && \
    uv pip install --system aiofiles>=24.1.0 asyncpg>=0.30.0 "fastapi[standard]>=0.115.0" \
    google-cloud-storage>=2.14.0 litellm>=1.52.9 markdown>=3.7 nats-py>=2.9.0 \
    openai>=1.39.0 pydantic-settings>=2.5.2 sqlmodel>=0.0.22 \
    tenacity>=9.0.0 typer>=0.12.5 "unstructured[pdf]>=0.16.5"

ARG SERVICE_PORT
ENV SERVICE_PORT=${SERVICE_PORT}
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["./entrypoint.sh"]
