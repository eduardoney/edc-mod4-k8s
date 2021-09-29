FROM python:3.8-slim

WORKDIR /app
RUN mkdir -p ./data

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY extract_load_web_enade.py extract_load_web_enade.py

CMD ["python","/app/extract_load_web_enade.py"]