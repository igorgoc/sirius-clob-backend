FROM node:20-bookworm-slim

# Install build dependencies for better-sqlite3 native binary compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    gcc \
    bash \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifests and install
COPY package.json ./
RUN npm install --production

# Copy application source
COPY . .

RUN chmod +x /app/start.sh

ENV PORT=8088
EXPOSE 8088 5051 5052 5053

CMD ["bash", "/app/start.sh"]
