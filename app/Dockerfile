FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install flask==3.0.3 psycopg2-binary==2.9.9 redis==5.0.4 prometheus-client==0.20.0

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]