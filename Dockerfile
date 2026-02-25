FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
RUN pip install --no-cache-dir uv && uv pip install --system -r <(python - <<'PY'\nimport tomllib,sys;print('\\n'.join(tomllib.load(open('pyproject.toml','rb'))['project']['dependencies']))\nPY)
COPY src ./src
ENV PYTHONPATH=/app/src
EXPOSE 8000
CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:create_app()"]
