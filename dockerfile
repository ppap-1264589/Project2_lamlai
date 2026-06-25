FROM python:3.11-slim

WORKDIR /app

RUN pip install requests aiohttp psycopg2-binary

COPY config.py .
COPY db.py .
COPY main.py .
COPY scrapers/ ./scrapers/
COPY coordinators/ ./coordinators/

CMD ["python", "main.py"]