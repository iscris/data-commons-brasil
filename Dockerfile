FROM python:3.12 AS builder
WORKDIR /app
COPY ./data_tools ./data_tools
COPY ./requirements.txt .
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "-m", "data_tools"]
