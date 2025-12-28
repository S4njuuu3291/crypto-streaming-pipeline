FROM python:3.12-slim

# Install poetry lewat pip
RUN pip install --no-cache-dir poetry

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml poetry.lock* /app/

RUN python -m poetry config virtualenvs.create false \
    && python -m poetry install --no-root --no-interaction --no-ansi

COPY . /app/
