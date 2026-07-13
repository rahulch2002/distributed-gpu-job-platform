FROM node:18-alpine

WORKDIR /app

# Copy package files first (better Docker caching)
COPY scheduler/package*.json ./

RUN npm install

# Copy application code
COPY scheduler/. .
COPY utils ./utils

CMD ["node", "scheduler.js"]
