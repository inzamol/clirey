# ⚡ Clirey

**Clirey** is a modern, rich CLI tool and interactive live terminal dashboard (TUI) for monitoring and controlling **Celery** clusters using **only the Celery Broker URL**—with zero dependencies on your worker application code, task definitions, or backend database.

---

## ✨ Features

- **No Task Code Required**: Connects to the Celery cluster solely via the Broker URL (`redis://`, `rediss://`, `amqp://`, `pyamqp://`).
- **🖥️ Live Interactive TUI Dashboard (`clirey top` / `clirey monitor`)**:
  - **Cluster Overview**: Online/Offline workers count, active running tasks, throughput (events/sec), total processed, failure counts.
  - **Worker Matrix**: Worker node names, status, pool concurrency, OS load averages (1m, 5m, 15m), and uptime.
  - **Active Tasks**: Real-time running tasks, executing worker node, runtime duration, and arguments.
  - **Queues & Backlog**: Worker consumers count and direct broker pending message depth (e.g. Redis `LLEN` / RabbitMQ queue declare).
  - **Live Event Stream**: Real-time event ticker (succeeded, failed, retried, started, worker heartbeats) with durations and exception details.
- **⚡ Scriptable One-Shot Commands**:
  - `clirey workers`: List active worker nodes, pool configs, uptime, and load averages.
  - `clirey tasks`: View currently active, scheduled (ETA), or reserved tasks.
  - `clirey queues`: Broker queue depths and worker subscription mapping.
  - `clirey events`: Live streaming event tail on your console.
  - `clirey ping`: Ping all active workers and calculate cluster round-trip latency.
  - `clirey task <task_id>`: Inspect task status and worker assignment.
  - `clirey revoke <task_id>`: Remotely revoke or terminate running tasks.
- **🎮 Built-in Simulator (`clirey demo`)**: Run and test the live terminal dashboard instantly without needing a live Celery cluster.
- **🔐 Automatic Credential Masking**: Passwords and secrets in broker connection strings are automatically masked in terminal outputs.

---

## 🚀 Installation

```bash
# Clone the repository
git clone https://github.com/your-org/clirey.git
cd clirey

# Create virtual environment and install
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

---

## ⚙️ Broker URL Configuration

You can provide the broker URL in any of the following ways (in priority order):

1. **CLI Option**: `--broker` or `-b`
   ```bash
   clirey top -b redis://:password@10.0.1.50:6379/0
   ```
2. **Environment Variable**: `CELERY_BROKER_URL` or `CLIREY_BROKER_URL` or `REDIS_URL`
   ```bash
   export CELERY_BROKER_URL="amqp://guest:guest@localhost:5672//"
   clirey top
   ```
3. **Config File (`.clirey.json` or `~/.clirey.json`)**:
   ```json
   {
     "broker_url": "redis://localhost:6379/0"
   }
   ```

---

## 📖 Usage Guide

### 1. Launch Live Interactive Terminal Dashboard
```bash
clirey top
# or with explicit broker
clirey top --broker redis://localhost:6379/0 --refresh 1.5
```

### 2. Try the Demo Simulator
```bash
clirey demo
```

### 3. List Connected Workers
```bash
clirey workers
```
Output:
```
                                        Active Celery Workers                                         
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┓
┃ Worker Node                ┃  Status  ┃ Active ┃ Processed ┃ Concurrency ┃ Pool    ┃ Load Avg (1,5,15m) ┃   Uptime ┃ Queues  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━┩
│ celery@worker-1.prod       │ ● ONLINE │      2 │    14,320 │           8 │ prefork │ 0.45, 0.38, 0.30   │  23h 40m │ default │
│ celery@worker-2.prod       │ ● ONLINE │      1 │     9,812 │           4 │ gevent  │ 0.12, 0.15, 0.18   │  10h 15m │ reports │
└────────────────────────────┴──────────┴────────┴━━━━━━━━━━━┴━━━━━━━━━━━━━┴━━━━━━━━━┴━━━━━━━━━━━━━━━━━━━━┴━━━━━━━━━━┴─────────┘
```

### 4. Inspect Queues & Backlog
```bash
clirey queues
```

### 5. List Active & Scheduled Tasks
```bash
clirey tasks --type active
clirey tasks --type scheduled
```

### 6. Stream Live Events
```bash
clirey events
```

### 7. Ping Active Workers
```bash
clirey ping
```

### 8. Revoke / Terminate a Task
```bash
clirey revoke <task-uuid> --terminate --signal SIGTERM
```

---

## 🧪 Running Tests

```bash
pytest
```

---

## 📜 License

MIT License
