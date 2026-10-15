# `kessel-box`: All-in-One Local Runner for Project Kessel

`kessel-box` (Kessel-in-a-Box) is a lightweight, zero-dependency, single-binary distribution of the entire Project Kessel platform.

It is designed for:
- **Local Service Development**: Run full Kessel inventory and authorization APIs without Docker or Kafka.
- **Microsecond CI/CD Testing**: Spin up an isolated, in-memory Kessel environment in GitHub Actions or GitLab CI in < 50ms.
- **Visual Authorization Debugging**: Inspect resources, browse schemas, and visually trace Zanzibar permission graphs in an embedded web dashboard.

---

## Key Features

- ⚡ **Instant Boot**: Starts in **< 50 milliseconds**; consumes **< 35 MB RAM**.
- 📦 **Single Static Binary**: Built with pure Go (`CGO_ENABLED=0`). Zero external dependencies (no Docker, no PostgreSQL, no Kafka, no SpiceDB containers).
- 🔄 **Direct Synchronous Write-Through**: When you call `ReportResource`, relationship tuples are linked into the Zanzibar engine in the same cycle. **Zero CDC lag, zero read-after-write errors, zero `time.Sleep()` required in tests**.
- 📜 **100% Protocol Compatible**:
  - Inventory API: `kessel.inventory.v1beta2.KesselInventoryService` (gRPC on port `9000`)
  - Relations API: `kessel.relations.v1beta1.KesselTupleService` (gRPC on port `9000`)
  - HTTP Gateway & Web UI: Port `8088`
- 🖥️ **Embedded Developer Console**: Open `http://localhost:8088` to visually inspect resources, validate Draft-07 schemas, and test permissions live in your browser.

---

## Quick Start

```bash
# 1. Build the binary
make build

# 2. Run all unit tests, official SDK e2e tests, and 10-scenario verification suite
make test-all

# 3. Seed demo resources into a local SQLite database
./bin/kessel-box seed --db-path=kessel.db --profile=hbi-demo --tenant=org-12345

# 4. Export database snapshot to JSON
./bin/kessel-box export --db-path=kessel.db --out=snapshot.json

# 5. Start kessel-box (gRPC on :9000, Web UI on :8088)
./bin/kessel-box start --grpc-port=9000 --http-port=8088 --db-path=kessel.db

# 6. Check permissions via CLI
./bin/kessel-box check \
  --subject=rbac/principal:redhat/test-admin \
  --relation=view \
  --object=rbac/workspace:default_12345

# 7. Build multi-arch binaries (Linux amd64/arm64, Darwin amd64/arm64)
make cross-build
```

---

## SDK Integration & Causal Consistency

Upstream services using [`kessel-sdk-go`](file:///Users/akoserwal/kessel/kessel-sdk-go) can achieve 100% causal consistency without manual token plumbing:

```go
import (
    "github.com/project-kessel/kessel-sdk-go/kessel/inventory/v1beta2"
)

// Configure client with automatic causal consistency tracking
tracker := v1beta2.NewCausalTokenTracker()
builder := v1beta2.NewClientBuilder("127.0.0.1:9000").Insecure()
client, conn, err := v1beta2.WithCausalInterceptor(builder, tracker).Build()
defer conn.Close()

// Write: automatically captures x-kessel-consistency-token from server response
client.ReportResource(ctx, reportReq)

// Read: automatically injects captured token into CheckRequest as AtLeastAsFresh!
checkResp, err := client.Check(ctx, checkReq)
```

---

## Documentation & Verification Reports

- 📘 [**`PLAN.md` (Master Blueprint & Architecture Plan)**](./PLAN.md) — Architectural design, DDL schema, component topology, and phased roadmap.
- 🧪 [**`VERIFICATION_AND_RUN_REPORT.md` (Verification & Execution Report)**](./VERIFICATION_AND_RUN_REPORT.md) — Complete 10/10 test scenario execution log, benchmarks (11,580 QPS, 86µs), and live run evidence.

