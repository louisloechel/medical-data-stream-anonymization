FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Synthetic ICU data set (record schema v2) for INPUT_DIR=output_json_files_v2:
# 10 patients x 600 records, generated reproducibly (fixed seed, pinned Faker).
# Keep the arguments in sync with the `synthetic-v2` target in the Makefile.
COPY generateData.py .
RUN python generateData.py --schema v2 --num-users 10 --num-timestamps 600 \
        --interval-minutes 1 --seed 2 --output-dir output_json_files_v2 > /dev/null

# Only the synthetic data sets are used (see .dockerignore).
COPY . .

CMD ["python", "-u", "producer.py"]
