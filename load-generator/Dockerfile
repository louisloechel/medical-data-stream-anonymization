FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Only the synthetic data set in output_json_files/ is used (see .dockerignore).
COPY . .

CMD ["python", "-u", "producer.py"]
