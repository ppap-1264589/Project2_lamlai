FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN pip install requests aiohttp psycopg2-binary

COPY config.py .
COPY db.py .
COPY main.py .
COPY scrapers/ ./scrapers/
COPY coordinators/ ./coordinators/

CMD ["python", "-u", "main.py"]