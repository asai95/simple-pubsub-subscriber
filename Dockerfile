FROM python:3.11-alpine

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY main.py main.py
COPY subscriber.py subscriber.py

CMD ["python", "main.py"]