# Raft Consolidation Append‑Only Log Engine Mini‑Project

## Overview
This mini‑project demonstrates a **Raft‑based consolidation append‑only log engine** built as a set of micro‑services. The goal is to show how a distributed Raft cluster can be deployed on cloud services (Render) and integrated with a lightweight front‑end (Vercel).

### Key Features
- **Raft consensus** for strong consistency across log nodes.
- **Append‑only log** – entries are never modified, only added.
- **Micro‑service architecture**:
  - **Gateway** – HTTP API entry point, discovers the current leader via `/status` on each node, forwards client writes to the leader, and provides health/metrics endpoints. CORS is enabled so the Vercel dashboard can call it directly.
  - **Raft‑node** – Implements the full Raft protocol (election timeouts, heartbeats, `RequestVote`, `AppendEntries` with `prevLogIndex`/`prevLogTerm` checks, majority commit rule, step‑down on higher term). Persists `currentTerm`, `votedFor`, and the log to a managed cloud store (Upstash Redis or Supabase Postgres) using the `STORE_URL`/`STORE_TOKEN` env vars. On restart it reloads this state.
  - **Dashboard** – Static Vercel‑hosted UI that talks to the gateway.
- **Dockerised** services for local development and **Render** for cloud deployment.

## Architecture Diagram
```mermaid
flowchart TD
    subgraph Cloud[Render Cloud]
        G[Gateway]
        N1[Raft‑Node 1]
        N2[Raft‑Node 2]
        N3[Raft‑Node 3]
    end
    subgraph Vercel[Dashboard (Vercel)]
        D[Static UI]
    end
    D -->|XHR /fetch| G
    G -->|GET /status| N1
    G -->|GET /status| N2
    G -->|GET /status| N3
    G -->|POST /write| LeaderNode
    LeaderNode -->|AppendEntries| N1
    LeaderNode -->|AppendEntries| N2
    LeaderNode -->|AppendEntries| N3
```

## Service Table
| Service | Purpose | Important Env Vars |
|---------|---------|-------------------|
| **Gateway** | Discovers leader, forwards client writes, exposes `/health` and `/metrics`. | `GATEWAY_PORT` (default 5000), `NODE_URLS` (comma‑separated list of all raft‑node URLs), `STORE_URL`, `STORE_TOKEN` |
| **Raft‑Node** | Full Raft implementation with persistent state. | `NODE_ID`, `PORT`, `PEERS` (other node URLs), `STORE_URL`, `STORE_TOKEN` |
| **Dashboard** | Static UI served by Vercel. | – |

## Raft Persistence (Cloud Store)
Each node stores the following keys under a per‑node prefix (`raft:v26:node:<NODE_ID>`):
- `term` – current term (number).
- `votedFor` – candidate voted for in current term.
- `log` – JSON array of log entries.
- `snapshot` – compacted state (last included index/term + KV map).
- `applied` – map of client request IDs to applied entries (idempotency).

The store is accessed via the HTTP API of Upstash Redis or Supabase Postgres (`STORE_URL` and `STORE_TOKEN`). If those vars are absent the node falls back to an in‑memory map (useful for local testing).

## Raft Rules Summary
- **Election**: Randomized timeout 1500‑3000 ms. Candidate increments term, votes for self, requests votes from peers.
- **Heartbeats**: Leader sends `AppendEntries` with empty entries every 150 ms.
- **AppendEntries**: Checks `prevLogIndex`/`prevLogTerm`. If mismatch, follower replies `success: false` and the leader backs off its `nextIndex`.
- **Commit**: An entry is committed once it is stored on a majority of nodes **and** its term equals the leader's current term.
- **Step‑down**: A node that receives a message with a higher term immediately steps down to follower, updates its term, and clears `votedFor`.
- **Idempotency**: Client writes include a `requestId`. Duplicate requests are deduplicated using the `applied` map.

## Local Development
```bash
# Clone the repo and start all services locally
docker compose up --build
```
- The gateway will be reachable at `http://localhost:5000`.
- Raft nodes listen on ports `4001`, `4002`, `4003`.
- The dashboard can be served locally by opening `public/index.html`.

## Cloud Deployment (Render)
Three separate Render web services are defined in `render.yaml`:
- `raft-node-1`, `raft-node-2`, `raft-node-3` – each runs the Raft node.
- `raft-gateway` – runs the gateway.
- The dashboard is deployed on Vercel (see **Live URLs** section).

Push the repository to GitHub and create Render services pointing at the corresponding `render.yaml` entries. Ensure each service receives the same `STORE_URL`/`STORE_TOKEN` so they share the persistent store.

## Failover Test Steps
1. **Identify the leader**: `curl http://localhost:5000/cluster` (or `/status` on each node) – the `leaderId` field shows the current leader.
2. **Kill the leader**: `curl -X POST http://localhost:5000/admin/<leaderId>/kill`.
3. **Observe re‑election**: Within a few seconds a new leader will appear (`curl http://localhost:5000/cluster`).
4. **Write a new entry**: `curl -X POST http://localhost:5000/write -d '{"key":"x","value":"y"}'` – it should succeed via the new leader.
5. **Revive the old node** (optional): `curl -X POST http://localhost:5000/admin/<oldId>/revive`.

## Limitations
- **Fixed cluster size** – the node list (`PEERS`) is static; dynamic membership changes are not supported.
- **No TLS** – communication is plain HTTP; add TLS in production.
- **Single‑region store** – the cloud store must be reachable by all Render services; network latency may affect performance.

## Live URLs (placeholders)
- **Dashboard (Vercel)**: `https://<your‑project>.vercel.app/`
- **Gateway (Render)**: `https://raft-gateway.onrender.com/`
- **Raft Nodes (Render)**: `https://raft-node-1.onrender.com/`, `https://raft-node-2.onrender.com/`, `https://raft-node-3.onrender.com/`

---

*Feel free to adapt the deployment scripts to your preferred cloud provider or to add CI/CD pipelines that push Docker images to a registry and automatically update Render services.*
