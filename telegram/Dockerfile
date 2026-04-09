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
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxfixes3 libxext6 libx11-6 libxi6 libxtst6 libglib2.0-0 \
    libdbus-1-3 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 \
    libgbm1 libpango-1.0-0 libcairo2 libasound2 libxshmfence1 \
    libx11-xcb1 fonts-liberation \
 && rm -rf /var/lib/apt/lists/* \
 && playwright install chromium

# Startup script — pulls latest code from git, then runs the bot
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 8080

CMD ["./entrypoint.sh"]
