#!/bin/sh
uv run uvicorn main:app --port ${SERVICE_PORT} --host 0.0.0.0