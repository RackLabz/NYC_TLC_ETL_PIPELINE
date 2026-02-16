FROM python:3.11-slim

# System deps (minimal; psycopg2-binary usually doesn't need build tools)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src
COPY notebooks /app/notebooks

# default command runs ETL
CMD ["python", "-m", "src.etl"]
