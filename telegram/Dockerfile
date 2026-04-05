FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (separate layer — cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && rm -rf /root/.cache

# Copy bot source files
COPY bot.py news.py tracker.py ./

EXPOSE 8080

CMD ["python", "-u", "bot.py"]
