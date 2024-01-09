FROM python:3.11.5 AS builder
WORKDIR /app
COPY ./downloaders ./downloaders
COPY ./requirements.txt .
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
