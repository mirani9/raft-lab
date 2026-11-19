FROM node:20-alpine
WORKDIR /app
COPY services/raft-node/package.json ./services/raft-node/package.json
COPY services/gateway/package.json ./services/gateway/package.json
RUN cd services/raft-node && npm install --omit=dev
RUN cd services/gateway && npm install --omit=dev
COPY services/raft-node/server.js ./services/raft-node/server.js
COPY services/gateway/server.js ./services/gateway/server.js
EXPOSE 4000 5000
CMD ["node", "services/raft-node/server.js"]
