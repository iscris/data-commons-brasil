FROM python:3.9-slim-bullseye

WORKDIR /app

WORKDIR /media

COPY /src/requirements.txt /app/requirements.txt

COPY /src/main.py /app/main.py

RUN pip3 install -r /app/requirements.txt

EXPOSE 80

CMD ["python", "/app/main.py"] 
