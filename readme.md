# DAG Scheduler

A lightweight Go-based DAG execution engine with:
- dependency-aware task scheduling,
- work-stealing queues,
- resource-aware dispatch (CPU + memory),
- worker pool execution,
- graceful shutdown,
- WAL backup
- basic runtime metrics.

## Run

```bash
go run ./cmd/engine
```

Server starts on `:6969`.

## API

### `POST /submit`
Submit a DAG as JSON (array of tasks).

Task format:

```json
{
  "id": "Task_A",
  "command": "echo 'Running A'",
  "dependencies": [],
  "cpu_cost": 20,
  "mem_cost": 512
}
```

Quick test:

```bash
curl -X POST http://localhost:6969/submit \
  -H "Content-Type: application/json" \
  --data-binary @tests/test1.json
```

### `GET /metrics`
Returns current aggregate metrics:
- `total_dags_completed`
- `total_tasks_processed`
- `average_queue_wait_ms`
- `average_makespan_ms`
- `cpu_utilization_pct`
- `mem_utilization_pct`

```bash
curl http://localhost:6969/metrics
```

## Notes

- Scheduler currently uses `CRITICAL_PATH` policy in the API server.
- Worker behavior is simulated:
  - `command: "fail"` -> task fails
  - `command: "panic"` -> worker panic is captured as failure
  - any other command -> simulated success after a short delay
- Example DAGs are in `examples/` and `tests/test1.json`.
