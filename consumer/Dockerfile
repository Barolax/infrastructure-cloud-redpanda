FROM python:3.11-slim

# Installer Java (requis pour Spark)
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# Créer le dossier output
RUN mkdir -p /output

CMD ["python", "consumer.py"]