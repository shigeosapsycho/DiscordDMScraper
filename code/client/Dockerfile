FROM python:3.11-slim

WORKDIR /app

COPY client/requirements.txt /app/client/requirements.txt
RUN pip install --no-cache-dir -r /app/client/requirements.txt

COPY common /app/common
COPY client /app/client

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "client.client"]
