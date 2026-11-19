# --------------------------------------------------------------
# Dockerfile – 2‑stage build for the Raft engine
# --------------------------------------------------------------
FROM node:20-alpine AS builder

WORKDIR /app

# Install only production dependencies
COPY cloud-backend/package*.json ./
RUN npm ci --omit=dev

# Copy source & compile
COPY cloud-backend/tsconfig.json ./
COPY cloud-backend/src ./src
RUN npx tsc

# --------------------------------------------------------------
# Runtime image – tiny Alpine with only the compiled output
# --------------------------------------------------------------
FROM node:20-alpine

WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules

EXPOSE 4000

CMD ["node", "dist/server.js"]
