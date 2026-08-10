FROM python:3.12-slim
WORKDIR /app
# boto3: both data files are fetched from S3 on boot when not bind-mounted.
RUN pip install --no-cache-dir fastapi uvicorn pydantic-settings boto3
# Code only. NEITHER data file is baked in -- the Open Library corpus and DAG 1's
# archive both come from the compose mount locally, or from S3 via
# FIXTURE_BOOKS_URL / FIXTURE_SAMPLE_ZIP_URL when deployed. build_dataset.py and
# make-sample-data.py produce them; terraform/aws/s3.tf uploads them.
COPY app.py ./
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]
