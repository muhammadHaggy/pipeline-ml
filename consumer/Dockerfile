FROM node:20-alpine
WORKDIR /app

# For healthcheck
RUN apk add --no-cache curl

COPY package.json package-lock.json* ./
RUN npm ci --only=production || npm install --omit=dev

COPY src ./src

ENV NODE_ENV=production

CMD ["node", "src/index.js"]


