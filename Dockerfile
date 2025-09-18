FROM python:3.12-slim

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY /src /app

WORKDIR /app

CMD ["python", "app.py"]
