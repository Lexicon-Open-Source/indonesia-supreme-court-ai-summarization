FROM python:3.10-slim
ENV PYSETUP_PATH="/opt/pysetup" \
    UV_INSTALL_DIR="/opt/uv"
ENV PATH=${UV_INSTALL_DIR}/bin:$PATH

WORKDIR $PYSETUP_PATH

RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential \
            clang curl libgl1 libglib2.0-0 && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl -LsSf https://astral.sh/uv/0.4.29/install.sh | sh && \
    uv python install 3.10

COPY . .
RUN chmod u+x entrypoint.sh && uv sync

ARG SERVICE_PORT
ENV SERVICE_PORT=${SERVICE_PORT}
EXPOSE ${SERVICE_PORT}
ENTRYPOINT ["./entrypoint.sh"]
