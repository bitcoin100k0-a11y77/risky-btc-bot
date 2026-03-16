FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.31.0

COPY bot.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]
