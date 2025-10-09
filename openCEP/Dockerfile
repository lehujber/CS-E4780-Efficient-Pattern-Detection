FROM python:3.13-slim 

WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./openCep /app/openCep

CMD ["python", "openCep/main.py"]