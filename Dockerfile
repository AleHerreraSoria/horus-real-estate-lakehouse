# Imagen oficial de Playwright con Python y navegadores preinstalados (Última estable verificada)
FROM mcr.microsoft.com/playwright/python:v1.63.0-noble

# Evita que Python escriba archivos .pyc en disco y fuerza logs en tiempo real en consola
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "src.main"]