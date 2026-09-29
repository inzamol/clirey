# Clirey

**Clirey** is a CLI tool and interactive live terminal dashboard (TUI) for monitoring and controlling **Celery** clusters using **only the Celery Broker URL** with zero dependencies on your worker application code, task definitions, or backend database.

---

## Features

- **No Task Code Required**: Connects to the Celery cluster solely via the Broker URL (`redis://`, `rediss://`, `amqp://`, `pyamqp://`).
- **Live Interactive TUI Dashboard (`clirey top` / `clirey monitor`)**:
  - **Cluster Overview**: Online/Offline workers count, active running tasks, throughput (events/sec), total processed, failure counts.
  - **Worker Matrix**: Worker node names, status, pool concurrency, OS load averages (1m, 5m, 15m), and uptime.
  - **Active Tasks**: Real-time running tasks, executing worker node, runtime duration, and arguments.
  - **Queues & Backlog**: Worker consumers count and direct broker pending message depth (e.g. Redis `LLEN` / RabbitMQ queue declare).
  - **Live Event Stream**: Real-time event ticker (succeeded, failed, retried, started, worker heartbeats) with durations and exception details.
- **Scriptable One-Shot Commands**:
  - `clirey validate` / `check`: Run preflight diagnostics on URL syntax, broker latency, worker discovery, and queues.
  - `clirey workers`: List active worker nodes, pool configs, uptime, and load averages.
  - `clirey tasks`: View currently active, scheduled (ETA), or reserved tasks.
  - `clirey queues`: Broker queue depths and worker subscription mapping.
  - `clirey events`: Live streaming event tail on your console.
  - `clirey ping`: Ping all active workers and calculate cluster round-trip latency.
  - `clirey task <task_id>`: Inspect task status and worker assignment.
  - `clirey revoke <task_id>`: Remotely revoke or terminate running tasks.
- **Preflight & URL Validation**: Automatic scheme validation (`redis://`, `amqp://`, `sqs://`, etc.) with corrective suggestions on invalid URLs or unreachable hosts.
- **Built-in Simulator (`clirey demo`)**: Run and test the live terminal dashboard instantly without needing a live Celery cluster.
- **Automatic Credential Masking**: Passwords and secrets in broker connection strings are automatically masked in terminal outputs.

---

## Installation

```bash
pip install clirey
```

### Or install with specific extra broker drivers (optional)
```bash
# For Redis support (included by default)
pip install "clirey[redis]"

# For development / from source
git clone https://github.com/inzam/clirey.git
cd clirey
pip install -e .
```

---

## Broker URL Configuration

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

## Usage Guide & Example Outputs

### 1. Launch Live Interactive Terminal Dashboard
```bash
clirey top --broker redis://localhost:6379/0
```

**Example Dashboard Output:**
```
+-----------------------------------------------------------------------------------------------------------------------------+
|  CLIREY   Broker: redis://:****@10.0.1.45:6379/0  |  Workers: 3/3 Online  |  Active: 4  |  Throughput: 8.5 ev/s  | 14:32:05 |
+-----------------------------------------------------------------------------------------------------------------------------+
| Workers (3)                                                  | Queues (3)                                                   |
| +----------------------------------------------------------+ | +----------------------------------------------------------+ |
| | Worker Node             | Status | Active | Pool | Load  | | | Queue Name    | Consumers | Pending Backlog | Workers    | |
| |-------------------------+--------+--------+------+-------| | |---------------+-----------+-----------------+------------| |
| | celery@worker-alpha     | ONLINE |      2 | pre8 | 0.42  | | | default       |         3 |              14 | a, b, c    | |
| | celery@worker-beta      | ONLINE |      2 | pre8 | 0.88  | | | high_priority |         1 |               0 | a          | |
| | celery@worker-gamma     | ONLINE |      0 | gev4 | 0.15  | | | reports       |         1 |               6 | b          | |
| +----------------------------------------------------------+ | +----------------------------------------------------------+ |
| Currently Running Tasks (4)                                                                                                 |
| +-------------------------------------------------------------------------------------------------------------------------+ |
| | Task ID          | Task Name                         | State   | Worker               | Duration | Args / Details           | |
| |------------------+-----------------------------------+---------+----------------------+----------+--------------------------| |
| | d4b2e8a1-1c2d... | tasks.generate_pdf_report         | STARTED | celery@worker-beta   | 3.2s     | args=('report_942',)     | |
| | a19f3c82-7e14... | tasks.send_transactional_email    | STARTED | celery@worker-alpha  | 0.8s     | args=('user_581',)       | |
| | c7291a04-98bc... | tasks.process_payment_webhook     | STARTED | celery@worker-alpha  | 1.1s     | args=('tx_99812',)       | |
| | f8301b55-33aa... | tasks.run_llm_embedding_pipeline  | STARTED | celery@worker-beta   | 4.5s     | args=('chunk_4',)        | |
| +-------------------------------------------------------------------------------------------------------------------------+ |
| Live Event Feed (Recent)                                                                                                    |
| +-------------------------------------------------------------------------------------------------------------------------+ |
| | Time     | Event   | Task Name                      | Task ID          | Worker               | Duration / Info           | |
| |----------+---------+--------------------------------+------------------+----------------------+---------------------------| |
| | 14:32:04 | SUCCEED | tasks.send_transactional_email | e9201a41-11b2... | celery@worker-alpha  | 0.42s                     | |
| | 14:32:03 | SUCCEED | tasks.process_payment_webhook  | b7311c88-44d1... | celery@worker-beta   | 0.89s                     | |
| | 14:32:01 | FAILED  | tasks.sync_salesforce_leads    | f1049a32-90ea... | celery@worker-gamma  | ConnectionResetError(...) | |
| +-------------------------------------------------------------------------------------------------------------------------+ |
| Press Ctrl+C to exit  |  Auto-refresh: 1.5s  |  Direct broker event streaming active                                        |
+-----------------------------------------------------------------------------------------------------------------------------+
```

---

### 2. Preflight Diagnostics & Verification
```bash
clirey validate --broker redis://localhost:6379/0
```

**Example Output:**
```
Preflight Diagnostics for Broker: redis://localhost:6379/0

  [OK] URL Syntax: Valid Redis broker URL format.
  [OK] Broker Reachability: Connected successfully (12.4ms)
  [OK] Worker Cluster: Found 3 active worker(s) (16.2ms)
     - celery@worker-alpha.prod
     - celery@worker-beta.prod
     - celery@worker-gamma.prod
  [OK] Broker Queues: 3 queue(s) detected: default, high_priority, reports

All preflight checks passed. Ready to monitor with clirey top.
```

---

### 3. List Connected Workers
```bash
clirey workers
```

**Example Output:**
```
                                        Active Celery Workers                                         
+---------------------------+---------+--------+-----------+-------------+---------+--------------------+---------+------------------------+
| Worker Node               | Status  | Active | Processed | Concurrency | Pool    | Load Avg (1,5,15m) |  Uptime | Queues                 |
+---------------------------+---------+--------+-----------+-------------+---------+--------------------+---------+------------------------+
| celery@worker-alpha.prod  | ONLINE  |      2 |    18,492 |           8 | prefork | 0.42, 0.38, 0.31   | 23h 40m | default, high_priority |
| celery@worker-beta.prod   | ONLINE  |      2 |    14,320 |           8 | prefork | 0.88, 0.65, 0.52   | 23h 39m | default, reports       |
| celery@worker-gamma.prod  | ONLINE  |      0 |     9,810 |           4 | gevent  | 0.15, 0.20, 0.18   | 10h 15m | default, webhooks      |
+---------------------------+---------+--------+-----------+-------------+---------+--------------------+---------+------------------------+
```

---

### 4. Inspect Queues & Backlog
```bash
clirey queues
```

**Example Output:**
```
                                      Broker Queues & Backlog                                       
+---------------+------------------+-----------------------------+---------------------------------+
| Queue Name    | Active Consumers | Pending Messages (Backlog)  | Workers Subscribed              |
+---------------+------------------+-----------------------------+---------------------------------+
| default       |                3 |                          14 | worker-alpha, beta, gamma       |
| high_priority |                1 |                           0 | worker-alpha                    |
| reports       |                1 |                           6 | worker-beta                     |
| ml_inference  |                1 |                           3 | worker-beta                     |
+---------------+------------------+-----------------------------+---------------------------------+
```

---

### 5. List Active & Scheduled Tasks
```bash
clirey tasks --type active
```

**Example Output:**
```
                                   Running & Tracked Tasks                                    
+--------------------+--------------------------------+---------+---------------------+----------+------------------------+
| Task ID            | Task Name                      | State   | Worker              | Duration | Args / Details         |
+--------------------+--------------------------------+---------+---------------------+----------+------------------------+
| d4b2e8a1-1c2d...   | tasks.generate_pdf_report      | STARTED | celery@worker-beta  | 3.2s     | args=('report_942',)   |
| a19f3c82-7e14...   | tasks.send_transactional_email | STARTED | celery@worker-alpha | 0.8s     | args=('user_581',)     |
+--------------------+--------------------------------+---------+---------------------+----------+------------------------+
```

---

### 6. Stream Live Events
```bash
clirey events
```

**Example Output:**
```
Streaming live Celery events from: redis://localhost:6379/0
Press Ctrl+C to stop streaming.

14:32:01  RECEIVE  tasks.send_transactional_email [a19f3c82] @celery@worker-alpha 
14:32:01  STARTED  tasks.send_transactional_email [a19f3c82] @celery@worker-alpha 
14:32:02  SUCCEED  tasks.send_transactional_email [a19f3c82] @celery@worker-alpha (0.42s)
14:32:04  STARTED  tasks.sync_salesforce_leads    [f1049a32] @celery@worker-gamma 
14:32:06  FAILED   tasks.sync_salesforce_leads    [f1049a32] @celery@worker-gamma (2.10s)
       Error: ConnectionResetError('Remote host closed connection')
```

---

### 7. Ping Active Workers
```bash
clirey ping
```

**Example Output:**
```
[OK] Received responses in 14.8ms:
  - celery@worker-alpha.prod: {'ok': 'pong'}
  - celery@worker-beta.prod: {'ok': 'pong'}
  - celery@worker-gamma.prod: {'ok': 'pong'}
```

---

### 8. Try the Demo Simulator
```bash
clirey demo
```

---

## Running Tests

```bash
pytest
```

---

## License

MIT License
