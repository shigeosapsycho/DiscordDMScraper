FROM python:3.11-slim

WORKDIR /app

COPY admin/requirements.txt /app/admin/requirements.txt
RUN pip install --no-cache-dir -r /app/admin/requirements.txt

COPY common /app/common
COPY admin /app/admin

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

EXPOSE 6767

CMD ["python", "-m", "admin.app"]
