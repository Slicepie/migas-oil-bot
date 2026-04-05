FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source files
COPY bot.py .
COPY news.py .

# Webhook port (override via env if needed)
EXPOSE 8080

CMD ["python", "-u", "bot.py"]
