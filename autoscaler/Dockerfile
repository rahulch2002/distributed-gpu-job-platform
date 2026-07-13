FROM node:20

WORKDIR /app

COPY autoscaler/package*.json ./

RUN npm install

COPY autoscaler/. .
COPY utils ./utils

CMD ["node","autoscaler.js"]
