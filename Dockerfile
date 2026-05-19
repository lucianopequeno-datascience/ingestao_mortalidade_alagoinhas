FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Instalar libmagic1 (exigência)
RUN apt-get update && apt-get install -y \
    build-essential \
    libmagic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar bibliotecas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o script principal
COPY main.py .

CMD ["python", "main.py"]