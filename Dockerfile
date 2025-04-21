FROM python:3.12 AS builder
LABEL org.opencontainers.image.source=https://github.com/iscris/data-commons-brasil
WORKDIR /app
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-23.0.3.tgz | \
    tar zxvf - --strip 1 -C /usr/local/bin docker/docker
COPY ./datatools ./datatools
COPY ./pyproject.toml .
RUN pip install .
ENTRYPOINT ["python", "-m", "datatools"]
