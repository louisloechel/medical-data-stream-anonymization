# Use an official Python runtime as the base image (multi-arch: amd64 and arm64)
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Install Python dependencies first to make use of the layer cache
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . /app
ENV PYTHONUNBUFFERED=1

# Run the consumer when the container launches
CMD ["python", "consumer.py"]
