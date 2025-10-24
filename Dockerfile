# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libgobject-2.0-0 libgdk-pixbuf-2.0-0 \
    libffi-dev libpangocairo-1.0-0 libpangoft2-1.0-0 \
    libcairo2 libxml2 libxslt1.1 shared-mime-info \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# System deps (optional: useful for mysqlclient; we use PyMySQL so it's pure-python)
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./
COPY templates ./templates
COPY static ./static

ENV PYTHONUNBUFFERED=1 \
    FLASK_ENV=production

EXPOSE 8000

CMD ["gunicorn", "-w", "3", "--max-requests", "1000", "--max-requests-jitter", "100", "--timeout", "120", "--worker-class", "sync", "-b", "0.0.0.0:8000", "app:app"]

