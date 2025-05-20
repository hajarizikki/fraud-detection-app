FROM python:3.9-slim

# Installer Java
RUN apt-get update && apt-get install -y default-jdk curl && \
    apt-get clean

# Définir JAVA_HOME pour Spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Installer pyspark et flask
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY templates/ templates/
COPY static/ ./static/

EXPOSE 5000

CMD ["python", "app.py"]
