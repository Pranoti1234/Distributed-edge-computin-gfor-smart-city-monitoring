# City Command: Distributed Smart-City Monitoring

A zero-dependency FA-1 demonstrator for municipal traffic operations, distributed edge inference, smart surveillance, and fault-tolerant communication.

## Run

Requires Python 3.10+.

```bash
python server.py
```

Windows: double-click `run.bat`. Linux/macOS: `./run.sh`.
Open http://localhost:8080.

## Operator screens

- `/index.html` live Leaflet city map, camera intelligence, junction status, and failure controls
- `/advisor.html` live incident feed and one-click traffic/patrol recommendations
- `/analytics.html` Chart.js traffic flow, latency comparison, system metrics, and worker health

The pages use Tailwind CSS via CDN, Leaflet, Chart.js, and the browser EventSource API for the live stream.

## Syllabus implementation

The coordinator owns a `BlockingQueue` equivalent, REST RPC ingress, SSE real-time stream, naming registry, Lamport logical timestamps, 2-second heartbeats, dynamic worker utilization, automatic queue retry/failover, junction simulation, bounding-box telemetry, and operator commands. `Edge-Node-1`, `Edge-Node-2`, and `Edge-Node-3` are independent worker objects with health and telemetry state. The nodes also run a Bully coordinator election: the online node with the highest numeric node ID becomes coordinator when the current coordinator fails, and a recovered higher-priority node announces itself and reclaims leadership. The current leader is returned as `coordinator` by `GET /api/v1/state`. `docker-compose.yml` documents the coordinator and edge-node deployment topology.

## API surface

- `POST /api/v1/surveillance/process` accepts a camera frame and routes it through the coordinator.
- `GET /api/v1/state` returns junctions, node health, metrics, incidents, and chart samples.
- `GET /api/v1/stream` publishes live detections and operator events over SSE.
- `GET /api/v1/state` includes the current Bully-elected `coordinator` node ID.
- `POST /api/v1/commands` accepts `reroute`, `dispatch`, and `monitor` actions.
- `POST /api/v1/junctions/{id}` toggles simulated junction failure and recovery.
- `POST /api/nodes/{id}` with `{"action":"fail"}` or `{"action":"recover"}` simulates node failure and coordinator election.
