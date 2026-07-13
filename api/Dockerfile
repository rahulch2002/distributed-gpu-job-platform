FROM node:18

WORKDIR /app

COPY api/package*.json ./
RUN npm install

COPY api/. .
COPY utils ./utils

EXPOSE 3000

CMD ["node", "index.js"]
