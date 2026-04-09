FROM python:3.11-slim

WORKDIR /app

# Install git
RUN apt-get update && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps (includes playwright)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && rm -rf /root/.cache

# Install Playwright system deps + Chromium browser
RUN playwright install-deps chromium && playwright install chromium

# Startup script — pulls latest code from git, then runs the bot
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 8080

CMD ["./entrypoint.sh"]
