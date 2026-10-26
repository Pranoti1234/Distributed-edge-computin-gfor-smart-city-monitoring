"""Distributed edge smart-city command center."""
from __future__ import annotations

import json
import math
import random
import base64
import hashlib
import struct
import threading
import time
import uuid
from collections import deque
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from queue import Empty, Queue
from urllib.parse import urlparse

import grpc

import smartcity_pb2
import smartcity_pb2_grpc

HOST = "0.0.0.0"
PORT = 8080
GRPC_PORT = 50051
ROOT = Path(__file__).parent


class EdgeNode:
    """A simulated worker process with health and utilization telemetry."""

    def __init__(self, node_id: str, address: str, zone: str):
        self.node_id = node_id
        self.address = address
        self.zone = zone
        self.status = "ONLINE"
        self.cpu = random.randint(22, 46)
        self.gpu = random.randint(18, 42)
        self.ram = random.randint(36, 62)
        self.processed = 0
        self.last_heartbeat = time.time()
        self.lamport = 0
        self.lock = threading.Lock()

    def snapshot(self) -> dict:
        with self.lock:
            return {"nodeId": self.node_id, "name": self.node_id.replace("edge-", "Edge-Node-"), "address": self.address, "zone": self.zone, "status": self.status, "cpu": self.cpu, "gpu": self.gpu, "ram": self.ram, "processed": self.processed, "lastHeartbeat": self.last_heartbeat, "lamport": self.lamport}

    def tick(self) -> None:
        with self.lock:
            if self.status == "ONLINE":
                self.cpu = max(9, min(94, self.cpu + random.randint(-8, 9)))
                self.gpu = max(8, min(92, self.gpu + random.randint(-7, 8)))
                self.ram = max(24, min(90, self.ram + random.randint(-4, 5)))
                self.last_heartbeat = time.time()


class Coordinator:
    """Central location-transparent router and real-time event publisher."""

    def __init__(self):
        self.nodes = {"edge-1": EdgeNode("edge-1", "10.0.1.11:9001", "North"), "edge-2": EdgeNode("edge-2", "10.0.1.12:9002", "Central"), "edge-3": EdgeNode("edge-3", "10.0.1.13:9003", "South")}
        self.junctions = [
            {"id": "north", "name": "North Junction", "camera": "CAM-NORTH-01", "lat": 40.748, "lng": -73.985, "status": "NORMAL", "congestion": 34, "vehicles": 42},
            {"id": "tech-west", "name": "Tech Park West", "camera": "CAM-WEST-02", "lat": 40.754, "lng": -73.998, "status": "MODERATE", "congestion": 57, "vehicles": 68},
            {"id": "downtown", "name": "Downtown Hub", "camera": "CAM-CENTRAL-03", "lat": 40.741, "lng": -73.977, "status": "HEAVY", "congestion": 88, "vehicles": 116},
            {"id": "south", "name": "South Expressway", "camera": "CAM-SOUTH-04", "lat": 40.727, "lng": -73.991, "status": "NORMAL", "congestion": 29, "vehicles": 37},
        ]
        self.queue: Queue[dict] = Queue()
        self.events = deque(maxlen=180)
        self.subscribers: list[Queue] = []
        self.lock = threading.RLock()
        self.lamport = 100
        self.processed = 0
        self.total_latency = 0.0
        self.started = time.time()
        self.flow_history = deque([42, 48, 51, 47, 56, 62, 59, 66, 61, 73], maxlen=30)
        self.coordinator_node_id = "edge-3"
        self.election_in_progress = False
        self.clock_lab = ClockSynchronizationLab()
        self.logical_clock_lab = LogicalClockLab()
        self.election_lab = ElectionLab()
        self.mutual_exclusion_lab = MutualExclusionLab()
        self.snapshot_lab = SnapshotLab()
        self._event("SYSTEM", "Command center online; all junction feeds connected")
        threading.Thread(target=self._dispatcher, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._simulator_loop, daemon=True).start()

    def _event(self, kind: str, message: str, **extra) -> dict:
        with self.lock:
            self.lamport += 1
            event = {"id": uuid.uuid4().hex[:8], "timestamp": time.time(), "time": time.strftime("%H:%M:%S"), "lamport": self.lamport, "kind": kind, "message": message, **extra}
            self.events.appendleft(event)
            for subscriber in list(self.subscribers):
                try:
                    subscriber.put_nowait(event)
                except Exception:
                    self.subscribers.remove(subscriber)
            return event

    def _healthy_nodes(self) -> list[EdgeNode]:
        return [node for node in self.nodes.values() if node.status == "ONLINE"]

    @staticmethod
    def _node_priority(node_id: str) -> int:
        return int(node_id.rsplit("-", 1)[-1])

    def _announce_coordinator(self, node_id: str, reason: str) -> str:
        previous = self.coordinator_node_id
        self.coordinator_node_id = node_id
        self.election_in_progress = False
        if previous != node_id:
            self._event("COORDINATOR", f"{node_id} became coordinator via Bully election", coordinator=node_id, previousCoordinator=previous, reason=reason)
        return node_id

    def start_election(self, initiator_id: str | None = None, reason: str = "coordinator failure") -> str | None:
        """Run a Bully election among online edge nodes and announce its winner."""
        with self.lock:
            online = self._healthy_nodes()
            if not online:
                self.coordinator_node_id = None
                self.election_in_progress = False
                self._event("ALERT", "Bully election failed; no online coordinator candidates")
                return None

            initiator = self.nodes.get(initiator_id) if initiator_id else None
            if initiator is None or initiator.status != "ONLINE":
                initiator = min(online, key=lambda node: self._node_priority(node.node_id))
            self.election_in_progress = True
            higher_nodes = [node for node in online if self._node_priority(node.node_id) > self._node_priority(initiator.node_id)]
            self._event("ELECTION", f"{initiator.node_id} started Bully election", initiator=initiator.node_id, reason=reason)
            for node in sorted(higher_nodes, key=lambda candidate: self._node_priority(candidate.node_id)):
                self._event("ELECTION", f"{node.node_id} responded to election from {initiator.node_id}", initiator=initiator.node_id, responder=node.node_id)
            winner = max(online, key=lambda node: self._node_priority(node.node_id))
            self._event("ELECTION", f"{winner.node_id} announced coordinator to peer nodes", coordinator=winner.node_id)
            return self._announce_coordinator(winner.node_id, reason)

    def set_node_status(self, node_id: str, online: bool) -> EdgeNode | None:
        node = self.nodes.get(node_id)
        if node is None:
            return None
        with node.lock:
            node.status = "ONLINE" if online else "DEAD"
            if online:
                node.last_heartbeat = time.time()
        if online:
            self._event("REROUTED", f"{node_id} recovered", node=node_id)
            self.start_election(node_id, "higher-priority node recovered")
        else:
            self._event("REROUTED", f"{node_id} failed", node=node_id)
            if self.coordinator_node_id == node_id:
                self.start_election(reason="coordinator failure")
        return node

    def _choose_node(self) -> EdgeNode | None:
        healthy = self._healthy_nodes()
        return min(healthy, key=lambda node: node.cpu + node.gpu + node.processed * 0.2) if healthy else None

    def _requeue_task(self, task: dict, reason: str, node: EdgeNode | None = None, delay: float = 0.25) -> None:
        retries = int(task.get("retries", 0)) + 1
        task["retries"] = retries
        task["retried"] = True
        task["status"] = "REQUEUED"
        task["lastError"] = reason
        task["nodeId"] = node.node_id if node else task.get("nodeId")
        task["taskId"] = task["id"]
        self.queue.put(task)
        time.sleep(min(delay * retries, 2.0))
        self._event("FAULT_TOLERANCE", f"Task {task['id']} retried after {reason}", taskId=task["id"], retries=retries, reason=reason, node=node.node_id if node else None)

    
    def process_frame(self, payload: dict) -> dict:
        self.lamport += 1
        task = {"id": f"FR-{uuid.uuid4().hex[:7].upper()}", "taskId": f"FR-{uuid.uuid4().hex[:7].upper()}", "camera": payload.get("camera", "CAM-NORTH-01"), "frame": payload.get("frame", random.randint(1000, 9999)), "priority": payload.get("priority", "normal"), "queuedAt": time.time(), "lamport": self.lamport, "status": "QUEUED", "retries": 0, "retried": False}
        task["taskId"] = task["id"]
        self.queue.put(task)
        self._event("FRAME", f"Frame {task['frame']} accepted from {task['camera']}", camera=task["camera"], taskId=task["id"], lamport=task["lamport"])
        return task

    
    def _dispatcher(self) -> None:
        while True:
            try:
                task = self.queue.get(timeout=0.35)
            except Empty:
                continue

            node = self._choose_node()
            if node is None:
                self._event("ALERT", "All edge workers unavailable; frame retained", taskId=task["id"])
                self._requeue_task(task, "No healthy edge nodes available", delay=0.75)
                continue

            with node.lock:
                node.lamport = max(node.lamport, task["lamport"]) + 1
                if node.cpu > 80:
                    self._event("REROUTED", f"{node.node_id} over capacity; peer offload selected", node=node.node_id, taskId=task["id"])
                    fallback = [candidate for candidate in self.nodes.values() if candidate is not node and candidate.status == "ONLINE"]
                    if fallback:
                        fallback_node = min(fallback, key=lambda candidate: candidate.cpu + candidate.gpu + candidate.processed * 0.2)
                        node = fallback_node
                        task["nodeId"] = node.node_id
                    else:
                        self._requeue_task(task, "Primary worker overloaded and no fallback node available", node=node, delay=0.5)
                        continue

            start = time.time()
            time.sleep(random.uniform(0.045, 0.14))
            if node.status != "ONLINE":
                self._event("REROUTED", f"{node.node_id} failed during execution; frame rerouted", node=node.node_id, taskId=task["id"])
                self._requeue_task(task, "Worker failed during execution", node=node, delay=0.6)
                continue

            latency = round((time.time() - start) * 1000, 1)
            with node.lock:
                node.processed += 1
            with self.lock:
                self.processed += 1
                self.total_latency += latency
            task["status"] = "COMPLETED"
            task["nodeId"] = node.node_id
            task["latency"] = latency
            task["latency_retry"] = latency
            task["taskId"] = task["id"]
            self._event("RESULT", f"AI result ready for {task['camera']} in {latency} ms", node=node.node_id, taskId=task["id"], latency=latency, lamport=node.lamport)


    def _heartbeat_loop(self) -> None:
        while True:
            time.sleep(2)
            for node in self.nodes.values():
                node.tick()
                if node.status == "ONLINE":
                    self._event("HEARTBEAT", f"{node.node_id} heartbeat confirmed", node=node.node_id)
            if self.coordinator_node_id and self.nodes[self.coordinator_node_id].status != "ONLINE":
                self.start_election(reason="missed coordinator heartbeat")

    def _simulator_loop(self) -> None:
        tick = 0
        while True:
            time.sleep(1.0)
            tick += 1
            for index, junction in enumerate(self.junctions):
                wave = math.sin(tick / 5 + index) * 8
                junction["congestion"] = round(max(12, min(96, junction["congestion"] + wave * 0.18 + random.uniform(-2, 2))))
                junction["vehicles"] = round(junction["congestion"] * 1.18 + random.randint(3, 11))
                junction["status"] = "HEAVY" if junction["congestion"] >= 75 else "MODERATE" if junction["congestion"] >= 48 else "NORMAL"
            self.flow_history.append(round(sum(j["vehicles"] for j in self.junctions) / 4))
            camera = self.junctions[tick % len(self.junctions)]["camera"]
            self.process_frame({"camera": camera, "frame": 1000 + tick})
            boxes = [{"label": "Vehicle", "confidence": random.randint(88, 97), "x": 18 + (tick * 3) % 45, "y": 38, "w": 24, "h": 20}, {"label": "Pedestrian", "confidence": random.randint(82, 93), "x": 60, "y": 52, "w": 14, "h": 25}]
            self._event("VISION", f"Live detections updated for {camera}", camera=camera, boxes=boxes)
            if tick % 7 == 0:
                heavy = max(self.junctions, key=lambda item: item["congestion"])
                self._event("ALERT", f"Congestion Index {heavy['congestion']}% at {heavy['name']}", junction=heavy["id"])

    def command(self, command_type: str, junction: str | None = None) -> dict:
        messages = {"reroute": "Automated signal reroute applied", "dispatch": "Patrol unit dispatched", "monitor": "Junction placed under enhanced monitoring"}
        return self._event("COMMAND", messages.get(command_type, "Operator command accepted"), command=command_type, junction=junction)

    def toggle_junction(self, junction_id: str, offline: bool) -> dict | None:
        junction = next((item for item in self.junctions if item["id"] == junction_id), None)
        if not junction:
            return None
        junction["offline"] = offline
        self._event("REROUTED" if offline else "SYSTEM", f"{junction['name']} {'offline; feed rerouted' if offline else 'back online'}", junction=junction_id)
        return junction

    def snapshot(self) -> dict:
        nodes = [node.snapshot() for node in self.nodes.values()]
        active = sum(node["status"] == "ONLINE" for node in nodes)
        avg = round(self.total_latency / self.processed, 1) if self.processed else 0
        return {
            "nodes": nodes,
            "junctions": self.junctions,
            "activeNodes": active,
            "processedFrames": self.processed,
            "averageLatency": avg,
            "queueDepth": self.queue.qsize(),
            "uptime": int(time.time() - self.started),
            "bandwidth": round(38 + active * 7 + random.random() * 9, 1),
            "flowHistory": list(self.flow_history),
            "events": list(self.events),
            "lamport": self.lamport,
            "coordinator": self.coordinator_node_id,
            "electionInProgress": self.election_in_progress,
            "physicalClockSynchronization": self.clock_lab.as_dict(),
            "logicalClocks": self.logical_clock_lab.as_dict(),
            "election": self.election_lab.as_dict(),
            "mutualExclusion": self.mutual_exclusion_lab.as_dict(),
            "snapshot": self.snapshot_lab.as_dict(),
        }

    def subscribe(self) -> Queue:
        subscriber = Queue()
        with self.lock:
            self.subscribers.append(subscriber)
        return subscriber




class ClockSynchronizationLab:
    """Compact physical-clock simulation for Cristian and Berkeley synchronization."""

    def __init__(self):
        self.server = {"id": "server", "clock": 1000.0}
        self.nodes = [
            {"id": "N1", "clock": 995.5, "offset": 4.5, "request_latency": 12, "response_latency": 18},
            {"id": "N2", "clock": 1008.2, "offset": -8.2, "request_latency": 9, "response_latency": 16},
            {"id": "N3", "clock": 1002.7, "offset": 2.7, "request_latency": 15, "response_latency": 21},
        ]
        self.cristian = {"status": "idle", "nodes": []}
        self.berkeley = {"status": "idle", "master": "N2", "nodes": []}

    def reset(self):
        self.server["clock"] = 1000.0
        for node in self.nodes:
            node["clock"] = 995.5 + (len(self.nodes) - 1) * 2.0
            node["offset"] = node["clock"] - self.server["clock"]
        self.cristian = {"status": "idle", "nodes": []}
        self.berkeley = {"status": "idle", "master": "N2", "nodes": []}
        return self.as_dict()

    def run_cristian(self):
        self.server["clock"] = sum(node["clock"] for node in self.nodes) / len(self.nodes) + 10.0
        readings = []
        for node in self.nodes:
            round_trip = node["request_latency"] + node["response_latency"]
            estimate = node["clock"] + round_trip / 2.0
            adjustment = self.server["clock"] - estimate
            corrected = node["clock"] + adjustment
            error = abs(corrected - self.server["clock"])
            readings.append({
                "nodeId": node["id"],
                "localClock": round(node["clock"], 2),
                "estimatedServerClock": round(estimate, 2),
                "adjustment": round(adjustment, 2),
                "correctedClock": round(corrected, 2),
                "synchronizationError": round(error, 2),
            })
        self.cristian = {"status": "completed", "algorithm": "Cristian", "serverClock": round(self.server["clock"], 2), "nodes": readings}
        return self.cristian

    def run_berkeley(self):
        master = max(self.nodes, key=lambda node: int(node["id"].replace("N", "")))
        readings = [node["clock"] for node in self.nodes]
        average = sum(readings) / len(readings)
        adjusted = []
        for node in self.nodes:
            delta = average - node["clock"]
            adjusted.append({
                "nodeId": node["id"],
                "localClock": round(node["clock"], 2),
                "offset": round(delta, 2),
                "adjustedClock": round(node["clock"] + delta, 2),
                "synchronizationError": round(abs((node["clock"] + delta) - average), 2),
            })
        self.berkeley = {"status": "completed", "algorithm": "Berkeley", "master": master["id"], "averageClock": round(average, 2), "nodes": adjusted}
        return self.berkeley

    def as_dict(self):
        return {"algorithm": "physical-clock-synchronization", "server": self.server, "nodes": self.nodes, "cristian": self.cristian, "berkeley": self.berkeley}


class LogicalClockLab:
    """Lamport and vector clock simulation for message ordering."""

    def __init__(self):
        self.processes = {
            "P1": {"lamport": 0, "vector": {"P1": 0, "P2": 0, "P3": 0}, "events": []},
            "P2": {"lamport": 0, "vector": {"P1": 0, "P2": 0, "P3": 0}, "events": []},
            "P3": {"lamport": 0, "vector": {"P1": 0, "P2": 0, "P3": 0}, "events": []},
        }
        self.log = []
        self.current_step = 0

    def _event(self, process_id, name, **kwargs):
        proc = self.processes[process_id]
        proc["lamport"] += 1
        proc["vector"][process_id] = proc["vector"].get(process_id, 0) + 1
        event = {"id": f"{process_id}-{self.current_step}", "processId": process_id, "eventName": name, "lamport": proc["lamport"], "vector": dict(proc["vector"]), **kwargs}
        proc["events"].append(event)
        self.log.append(event)
        self.current_step += 1
        return event

    def send_message(self, sender, receiver, name="send"):
        event = self._event(sender, name, sender=sender, receiver=receiver, kind="SEND")
        event["message"] = {"from": sender, "to": receiver}
        return event

    def receive_message(self, receiver, sender, message_name="receive"):
        proc = self.processes[receiver]
        recv_vector = self.processes[sender]["vector"].copy()
        proc["lamport"] = max(proc["lamport"], max(recv_vector.values()) + 1)
        for key, value in recv_vector.items():
            proc["vector"][key] = max(proc["vector"].get(key, 0), value)
        proc["vector"][receiver] = proc["vector"].get(receiver, 0) + 1
        event = self._event(receiver, message_name, sender=sender, receiver=receiver, kind="RECEIVE", vector=dict(proc["vector"]))
        return event

    def compare(self, left, right):
        left_vec = left["vector"]
        right_vec = right["vector"]
        left_before = all(left_vec.get(key, 0) <= right_vec.get(key, 0) for key in set(left_vec) | set(right_vec)) and any(left_vec.get(key, 0) < right_vec.get(key, 0) for key in set(left_vec) | set(right_vec))
        right_before = all(right_vec.get(key, 0) <= left_vec.get(key, 0) for key in set(left_vec) | set(right_vec)) and any(right_vec.get(key, 0) < left_vec.get(key, 0) for key in set(left_vec) | set(right_vec))
        if left_before:
            return {"relationship": "causally-before", "left": left["id"], "right": right["id"]}
        if right_before:
            return {"relationship": "causally-after", "left": left["id"], "right": right["id"]}
        return {"relationship": "concurrent", "left": left["id"], "right": right["id"]}

    def as_dict(self):
        return {"processes": self.processes, "events": self.log, "currentStep": self.current_step}


class ElectionLab:
    """Bully + ring election simulation snapshots."""

    def __init__(self):
        self.nodes = [1, 2, 3]
        self.failed_nodes = set()
        self.current_coordinator = 3
        self.last_election = {"algorithm": "Bully", "messages": [], "winner": 3, "durationMs": 50}

    def set_failed_nodes(self, failed):
        self.failed_nodes = set(failed)

    def bully(self):
        active = [node for node in self.nodes if node not in self.failed_nodes]
        winner = max(active) if active else None
        self.current_coordinator = winner
        self.last_election = {"algorithm": "Bully", "messages": [f"Election started for {active}", f"Coordinator elected {winner}"], "winner": winner, "durationMs": 42, "activeNodes": active}
        return self.last_election

    def ring(self):
        active = [node for node in self.nodes if node not in self.failed_nodes]
        if not active:
            self.current_coordinator = None
            return {"algorithm": "Ring", "messages": ["No active nodes in ring"], "winner": None, "durationMs": 0, "activeNodes": []}
        ordered = active[:]
        winner = max(ordered)
        self.current_coordinator = winner
        self.last_election = {"algorithm": "Ring", "messages": [f"Ring order {ordered}", f"Coordinator elected {winner}"], "winner": winner, "durationMs": 63, "activeNodes": ordered}
        return self.last_election

    def as_dict(self):
        return {"nodes": self.nodes, "failedNodes": sorted(self.failed_nodes), "currentCoordinator": self.current_coordinator, "lastElection": self.last_election}


class MutualExclusionLab:
    """Compact mutual exclusion simulation with Lamport fairness metrics."""

    def __init__(self):
        self.processes = {
            "P1": {"state": "IDLE", "requests": 0, "waitingTime": 0, "messages": 0, "queue": [], "completed": 0},
            "P2": {"state": "IDLE", "requests": 0, "waitingTime": 0, "messages": 0, "queue": [], "completed": 0},
            "P3": {"state": "IDLE", "requests": 0, "waitingTime": 0, "messages": 0, "queue": [], "completed": 0},
        }
        self.request_queue = []
        self.current_owner = None
        self.message_log = []

    def request(self, process_id):
        proc = self.processes[process_id]
        proc["state"] = "REQUESTING"
        proc["requests"] += 1
        timestamp = proc["requests"] * 10
        request = {"pid": process_id, "timestamp": timestamp}
        self.request_queue.append(request)
        self.request_queue.sort(key=lambda item: (item["timestamp"], item["pid"]))
        self.message_log.append({"type": "REQUEST", "pid": process_id, "timestamp": timestamp})
        proc["messages"] += 1
        return {"request": request, "queue": self.request_queue, "owner": self.current_owner}

    def grant(self, process_id):
        proc = self.processes[process_id]
        proc["state"] = "IN_CRITICAL_SECTION"
        self.current_owner = process_id
        self.message_log.append({"type": "REPLY", "pid": process_id})
        proc["messages"] += 1
        return {"owner": process_id, "state": proc["state"]}

    def release(self, process_id):
        proc = self.processes[process_id]
        proc["state"] = "IDLE"
        proc["completed"] += 1
        self.message_log.append({"type": "RELEASE", "pid": process_id})
        proc["messages"] += 1
        self.current_owner = None
        return {"owner": None, "state": proc["state"]}

    def as_dict(self):
        return {"processes": self.processes, "requestQueue": self.request_queue, "currentOwner": self.current_owner, "messages": self.message_log}


class SnapshotLab:
    """Chandy-Lamport snapshot helpers for channel state analysis."""

    def __init__(self):
        self.processes = {
            "P1": {"localState": "processing traffic frame", "markerReceived": False, "inTransit": []},
            "P2": {"localState": "waiting for task", "markerReceived": False, "inTransit": []},
            "P3": {"localState": "completed task", "markerReceived": False, "inTransit": []},
        }
        self.chans = {"P1->P2": [{"taskId": "T-11", "status": "in transit"}], "P2->P3": [], "P3->P1": []}
        self.snapshot = {"status": "idle", "markedProcesses": [], "channels": {}}

    def take_snapshot(self):
        for pid, state in self.processes.items():
            state["markerReceived"] = True
        self.snapshot = {"status": "completed", "markedProcesses": sorted(self.processes.keys()), "channels": self.chans}
        return self.snapshot

    def as_dict(self):
        return {"processes": self.processes, "channels": self.chans, "snapshot": self.snapshot}


coordinator = Coordinator()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args) -> None:
        return

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/api/state", "/api/v1/state"):
            self._json(coordinator.snapshot())
        elif path in ("/api/events", "/api/v1/stream"):
            self._sse()
        elif path == "/api/v1/ws" and self.headers.get("Upgrade", "").lower() == "websocket":
            self._websocket()
        elif path == "/api/v1/registry":
            self._json({"registry": coordinator.snapshot()["nodes"]})
        elif path == "/api/v1/clock-lab":
            self._json(coordinator.clock_lab.as_dict())
        elif path == "/api/v1/clock-lab/reset":
            self._json(coordinator.clock_lab.reset())
        elif path == "/api/v1/clock-lab/cristian":
            self._json(coordinator.clock_lab.run_cristian())
        elif path == "/api/v1/clock-lab/berkeley":
            self._json(coordinator.clock_lab.run_berkeley())
        elif path == "/api/v1/logical-clocks":
            self._json(coordinator.logical_clock_lab.as_dict())
        elif path == "/api/v1/logical-clocks/reset":
            coordinator.logical_clock_lab = LogicalClockLab()
            self._json(coordinator.logical_clock_lab.as_dict())
        elif path == "/api/v1/elections":
            self._json(coordinator.election_lab.as_dict())
        elif path == "/api/v1/elections/reset":
            coordinator.election_lab = ElectionLab()
            self._json(coordinator.election_lab.as_dict())
        elif path == "/api/v1/elections/bully":
            self._json(coordinator.election_lab.bully())
        elif path == "/api/v1/elections/ring":
            self._json(coordinator.election_lab.ring())
        elif path == "/api/v1/mutual-exclusion":
            self._json(coordinator.mutual_exclusion_lab.as_dict())
        elif path == "/api/v1/mutual-exclusion/reset":
            coordinator.mutual_exclusion_lab = MutualExclusionLab()
            self._json(coordinator.mutual_exclusion_lab.as_dict())
        elif path == "/api/v1/snapshot":
            self._json(coordinator.snapshot_lab.as_dict())
        elif path == "/api/v1/snapshot/reset":
            coordinator.snapshot_lab = SnapshotLab()
            self._json(coordinator.snapshot_lab.as_dict())
        elif path == "/" or path.endswith(".html"):
            self._serve_static("index.html" if path == "/" else path.lstrip("/"))
        else:
            self._serve_static(path.lstrip("/"))

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        data = self._body()
        if path in ("/api/inference/submit", "/api/v1/surveillance/process"):
            self._json({"accepted": True, "frame": coordinator.process_frame(data)}, 202)
        elif path == "/api/v1/commands":
            self._json({"accepted": True, "command": coordinator.command(data.get("type", "monitor"), data.get("junction"))}, 202)
        elif path == "/api/v1/clock-lab/reset":
            self._json(coordinator.clock_lab.reset(), 200)
        elif path == "/api/v1/clock-lab/cristian":
            self._json(coordinator.clock_lab.run_cristian(), 200)
        elif path == "/api/v1/clock-lab/berkeley":
            self._json(coordinator.clock_lab.run_berkeley(), 200)
        elif path == "/api/v1/logical-clocks/reset":
            coordinator.logical_clock_lab = LogicalClockLab()
            self._json(coordinator.logical_clock_lab.as_dict(), 200)
        elif path == "/api/v1/elections/reset":
            coordinator.election_lab = ElectionLab()
            self._json(coordinator.election_lab.as_dict(), 200)
        elif path == "/api/v1/elections/bully":
            self._json(coordinator.election_lab.bully(), 200)
        elif path == "/api/v1/elections/ring":
            self._json(coordinator.election_lab.ring(), 200)
        elif path == "/api/v1/mutual-exclusion/reset":
            coordinator.mutual_exclusion_lab = MutualExclusionLab()
            self._json(coordinator.mutual_exclusion_lab.as_dict(), 200)
        elif path == "/api/v1/snapshot":
            self._json(coordinator.snapshot_lab.take_snapshot(), 200)
        elif path == "/api/v1/snapshot/reset":
            coordinator.snapshot_lab = SnapshotLab()
            self._json(coordinator.snapshot_lab.as_dict(), 200)
        elif path.startswith("/api/v1/junctions/"):
            junction = coordinator.toggle_junction(path.split("/")[4], data.get("offline", False))
            self._json({"junction": junction} if junction else {"error": "Unknown junction"}, 200 if junction else 404)
        elif path.startswith("/api/nodes/"):
            node_id = path.split("/")[3]
            node = coordinator.nodes.get(node_id)
            if node:
                coordinator.set_node_status(node_id, data.get("action") == "recover")
                self._json({"node": node.snapshot()})
            else:
                self._json({"error": "Unknown node"}, 404)
        else:
            self._json({"error": "Not found"}, 404)

    def _serve_static(self, filename: str) -> None:
        safe = (ROOT / "web" / filename).resolve()
        web_root = (ROOT / "web").resolve()
        if web_root not in safe.parents or not safe.is_file():
            self._json({"error": "Not found"}, 404)
            return
        content_type = "text/html" if safe.suffix == ".html" else "text/css" if safe.suffix == ".css" else "application/javascript"
        body = safe.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse(self) -> None:
        subscriber = coordinator.subscribe()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        try:
            for event in iter(subscriber.get, None):
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _websocket(self) -> None:
        key = self.headers.get("Sec-WebSocket-Key")
        if not key:
            self._json({"error": "Missing WebSocket key"}, 400)
            return
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        subscriber = coordinator.subscribe()
        try:
            while True:
                payload = json.dumps(subscriber.get())
                encoded = payload.encode()
                length = len(encoded)
                if length < 126:
                    frame = b"\x81" + bytes([length]) + encoded
                elif length < 65536:
                    frame = b"\x81\x7e" + struct.pack("!H", length) + encoded
                else:
                    frame = b"\x81\x7f" + struct.pack("!Q", length) + encoded
                self.wfile.write(frame)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass


class GrpcCoordinatorServicer(smartcity_pb2_grpc.SurveillanceServiceServicer):
    def SubmitFrame(self, request, context):
        task = coordinator.process_frame({
            "camera": request.camera,
            "frame": request.frame,
            "priority": request.priority,
        })
        node_id = task.get("nodeId", "queued")
        return smartcity_pb2.FrameReply(
            accepted=True,
            task_id=task["id"],
            node_id=node_id,
            status=task.get("status", "QUEUED"),
        )

    def GetState(self, request, context):
        state = coordinator.snapshot()
        return smartcity_pb2.StateReply(
            nodes=[node["nodeId"] for node in state["nodes"]],
            active_nodes=state["activeNodes"],
            processed_frames=state["processedFrames"],
            average_latency=float(state["averageLatency"]),
            queue_depth=state["queueDepth"],
        )


if __name__ == "__main__":
    print(f"Smart-city command center running at http://localhost:{PORT}")
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    smartcity_pb2_grpc.add_SurveillanceServiceServicer_to_server(GrpcCoordinatorServicer(), grpc_server)
    grpc_server.add_insecure_port(f"{HOST}:{GRPC_PORT}")
    grpc_server.start()
    print(f"gRPC service listening on {HOST}:{GRPC_PORT}")
    http_server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        http_server.serve_forever()
    finally:
        grpc_server.stop(0)
