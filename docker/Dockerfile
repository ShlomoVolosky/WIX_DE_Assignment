FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Optionally run a command
CMD ["python", "src/pipeline/main_pipeline.py"]

