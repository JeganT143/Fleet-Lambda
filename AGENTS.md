# Agent Architecture

This project uses multiple Claude Code agents to implement the data-engineering
pipeline.

`TASKS.md` is the source of truth for **what** must be implemented.

`AGENTS.md` is the source of truth for **how** work is divided between agents.

The Lead Agent owns final integration and architectural decisions.

---

# 1. Agent Roles

## 1.1 Lead Agent

### Responsibility

The Lead Agent coordinates the entire project.

### Owns

- Overall architecture
- Project structure
- `TASKS.md`
- Cross-component interfaces
- Integration
- Architectural decisions
- Final validation
- Documentation consistency
- Resolving conflicts between agents

### Rules

The Lead Agent must:

1. Read `TASKS.md` and `AGENTS.md` before starting.
2. Inspect the existing repository before modifying it.
3. Decide task dependencies.
4. Delegate independent work to specialized agents.
5. Review all delegated work before accepting it.
6. Run integration tests after combining work.
7. Keep `TASKS.md` updated.
8. Ensure the final system works end-to-end.

The Lead Agent has final authority over architecture.

---

# 2. Infrastructure Agent

### Responsibility

Build and maintain the local infrastructure.

### Owns

- Docker Compose
- Kafka infrastructure configuration
- PostgreSQL infrastructure
- Airflow infrastructure configuration
- Spark infrastructure configuration
- Environment configuration
- Database initialization
- SQL schema
- Database indexes and constraints
- Infrastructure health checks

### Primary areas

```text
docker/
docker-compose.yml
sql/
config/
```

The exact structure may be adapted to the repository.

### Must provide

- Working Kafka
- Working PostgreSQL
- Working Airflow
- Working Spark environment
- Reproducible database initialization
- `.env.example`

### Must not

- Implement business logic.
- Implement FastAPI endpoints.
- Implement streaming transformations.
- Implement reconciliation logic.

---

# 3. Streaming Agent

### Responsibility

Implement the real-time ingestion and processing path.

### Owns

- Streaming event generator
- Kafka producer
- Kafka topic configuration required by the application
- Event serialization
- Event validation
- Spark Structured Streaming
- Streaming transformations
- Geographic zone enrichment
- Event-time processing
- Window aggregation
- Real-time metrics
- Streaming-related tests

### Pipeline

```text
Python Producer
      ↓
    Kafka
      ↓
Spark Structured Streaming
      ↓
 PostgreSQL
```

### Must provide

Real-time metrics including:

- active vehicles
- idle vehicles
- idle ratio
- trips/events per hour
- earnings
- average fare
- earnings by zone

### Must not

- Modify Airflow DAGs unless explicitly required for integration.
- Implement FastAPI endpoints.
- Redesign PostgreSQL schema without coordinating with the Lead Agent.
- Change the Lambda architecture.

---

# 4. Batch Agent

### Responsibility

Implement the daily batch and reconciliation path.

### Owns

- Daily expense generator
- Batch landing files
- Batch validation
- Airflow DAG
- Spark batch processing
- Expense loading
- Historical aggregation required for reconciliation
- Vehicle profitability
- Utilization calculations required for the daily report
- Idempotent batch processing
- Batch-related tests

### Pipeline

```text
Python Batch Generator
        ↓
Landing Files
        ↓
Airflow
        ↓
Spark Batch
        ↓
PostgreSQL
        ↓
Daily Reconciliation
```

### Must provide

Per vehicle/day:

- trips
- distance
- earnings
- fuel cost
- maintenance cost
- total cost
- estimated profit
- utilization
- profitability status
- service flag

### Profitability

Use:

```text
estimated_profit =
    earnings
    - fuel_cost
    - maintenance_cost
```

This is an operational estimate, not full accounting profit.

### Must not

- Implement FastAPI endpoints.
- Modify streaming processing unnecessarily.
- Introduce additional orchestration technologies.

---

# 5. API & Testing Agent

### Responsibility

Implement the serving layer and cross-component testing support.

### Owns

- FastAPI
- API schemas
- Database access required by API
- API error handling
- API tests
- Unit-test infrastructure
- Integration-test infrastructure
- Test fixtures
- Test configuration

### Required endpoints

```text
GET /health

GET /api/v1/fleet/summary

GET /api/v1/fleet/zones

GET /api/v1/vehicles/{vehicle_id}

GET /api/v1/alerts

GET /api/v1/reports/daily/{business_date}
```

### Must not

- Move business logic into API endpoints.
- Run Spark jobs from FastAPI.
- Run Airflow jobs from FastAPI.
- Modify Kafka/Spark architecture.

---

# 6. Reviewer Agent

### Responsibility

Review the implementation rather than independently redesigning it.

The Reviewer Agent is primarily read-only.

### Review areas

- Assignment requirements
- Architecture correctness
- Kafka usage
- Spark usage
- Airflow usage
- PostgreSQL design
- API design
- Data quality
- Idempotency
- Error handling
- Observability
- Tests
- Documentation
- Security/configuration
- Reproducibility

### Reviewer output

The Reviewer should classify relevant requirements as:

```text
PASS
FAIL
WARNING
MISSING
```

For every problem provide:

1. Problem
2. Evidence
3. Why it matters
4. Recommended fix
5. Severity

The Reviewer should not rewrite large parts of the project.

---

# 7. Ownership Rules

Each agent should primarily modify files within its responsibility.

If an agent needs to modify another agent's area:

1. Explain why.
2. Keep the change minimal.
3. Preserve existing interfaces.
4. Inform the Lead Agent.

Do not silently overwrite another agent's implementation.

---

# 8. Shared Contracts

Agents must agree on these interfaces.

## 8.1 Streaming Event Contract

Required fields:

```text
event_id
trip_id
driver_id
vehicle_id
latitude
longitude
speed
status
fare
timestamp
```

Valid statuses:

```text
idle
enroute
on_trip
```

---

## 8.2 Batch Expense Contract

Required fields:

```text
vehicle_id
fuel_cost
maintenance_cost
distance_covered
service_flag
business_date
```

---

## 8.3 Simulated Time

```text
1 simulated business day = 5 real minutes
```

The implementation must consistently distinguish:

```text
event timestamp
business date
ingestion timestamp
processing timestamp
```

---

# 9. Database Contract

The database must support at least:

```text
stream_events
realtime_vehicle_metrics
vehicle_expenses
daily_vehicle_profitability
alerts
pipeline_runs
```

Agents must not arbitrarily rename or remove shared tables.

If a schema change is necessary, coordinate with the Lead Agent.

---

# 10. Configuration Contract

Configuration must use environment variables.

Important configuration includes:

```text
Kafka bootstrap servers
Kafka topic
PostgreSQL connection
simulation speed
stream interval
alert thresholds
profitability thresholds
```

Never commit secrets.

---

# 11. Agent Coordination

The Lead Agent should identify dependencies before delegating.

## Safe parallel work

These can generally be developed in parallel after the architecture and
shared contracts are established:

```text
Infrastructure
       +
Streaming
       +
Batch
       +
API
```

However, integration must happen afterward.

## Sequential dependencies

These should normally happen in order:

```text
Architecture
    ↓
Infrastructure contracts
    ↓
Streaming / Batch implementation
    ↓
Integration
    ↓
API integration
    ↓
End-to-end testing
    ↓
Review
```

Do not parallelize work that modifies the same core files or depends directly
on unfinished interfaces.

---

# 12. Definition of Done

An agent must not mark a task complete simply because code was written.

A task is complete only when:

1. Implementation exists.
2. Relevant tests/checks pass.
3. Configuration is reproducible.
4. Logging/error handling is present where required.
5. Documentation is updated where necessary.
6. The implementation does not break existing functionality.

---

# 13. Integration Responsibility

The Lead Agent must perform final integration.

After specialized agents finish:

1. Inspect their changes.
2. Resolve conflicts.
3. Verify interfaces.
4. Run unit tests.
5. Run integration tests.
6. Start the Docker environment.
7. Run the streaming path.
8. Run the batch path.
9. Verify PostgreSQL results.
10. Verify FastAPI.
11. Verify alerts.
12. Verify Airflow.
13. Run the complete acceptance checklist.

No individual agent can declare the whole project complete.

---

# 14. Engineering Principles

All agents must follow these principles:

- Prefer simple implementations.
- Do not over-engineer.
- Do not introduce unnecessary technologies.
- Do not duplicate business logic.
- Do not hide errors.
- Do not weaken tests.
- Do not fabricate results.
- Do not hardcode secrets.
- Do not silently change shared contracts.
- Preserve reproducibility.
- Prefer deterministic tests.
- Document important architectural decisions.
- Fix root causes rather than symptoms.

The goal is a small, credible data-engineering system rather than a
collection of disconnected demonstrations.