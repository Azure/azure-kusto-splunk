FROM python:3.9-slim

LABEL authors="tanmayapanda@microsoft.com"

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt

COPY config.yml /app/

COPY app.py /app/

EXPOSE 9997

ENTRYPOINT [ "python", "app.py" ]