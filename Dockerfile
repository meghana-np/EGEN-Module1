FROM python:3.8.0
WORKDIR /app

COPY Publisher.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", ".Publisher.py"]