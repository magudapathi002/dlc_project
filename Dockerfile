FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential libpq-dev

RUN apt-get update && \
    apt-get install -y openjdk-21-jre && \
    apt-get clean

COPY Pipfile Pipfile.lock ./
RUN pip install pipenv && pipenv install --system --deploy

RUN playwright install --with-deps chromium

COPY . .

EXPOSE 8000

CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8009"]
