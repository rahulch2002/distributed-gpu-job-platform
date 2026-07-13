FROM node:18

WORKDIR /app

COPY worker/package*.json ./
RUN npm install

COPY worker/. .
COPY utils ./utils

EXPOSE 3000

CMD ["node", "worker.js"]
