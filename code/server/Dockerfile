FROM python:3.11-slim

WORKDIR /app

COPY server/requirements.txt /app/server/requirements.txt
RUN pip install --no-cache-dir -r /app/server/requirements.txt

COPY common /app/common
COPY server /app/server

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8765

CMD ["python", "-m", "server.server"]
