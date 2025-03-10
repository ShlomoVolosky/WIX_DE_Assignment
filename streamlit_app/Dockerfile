FROM python:3.10-slim

WORKDIR /app

# Install any system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements (assuming you have a dedicated requirements file)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the streamlit app
COPY app.py ./

# Expose default Streamlit port
EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.enableCORS=false", "--server.enableXsrfProtection=false", "--server.port=8501"]
