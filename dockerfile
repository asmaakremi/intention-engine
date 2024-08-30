FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

ENV FLASK_APP=intention_engine.py

CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=5003"]