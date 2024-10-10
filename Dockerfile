FROM python:3.12 AS builder
WORKDIR /app
COPY ./datatools ./datatools
COPY ./requirements.txt .
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "-m", "datatools"]
