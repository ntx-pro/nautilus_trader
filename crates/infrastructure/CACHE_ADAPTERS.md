# Cache Database Adapters

Rust implementations of the `CacheDatabaseAdapter` trait for Redis and PostgreSQL,
enabling pure-Rust deployments via `LiveNode::set_cache_database()` without
Python or Cython.

**Status:** Redis — complete (40+ trait methods). PostgreSQL — partial (models and queries only).

## Architecture

The Redis adapter (`RedisCacheDatabaseAdapter`) uses a split-connection design:

- **Read connection** (`self.con`): A `redis::aio::ConnectionManager` owned by the
  main struct. Synchronous callers use `std::sync::mpsc` channels with `block_in_place`
  to bridge into async Redis operations.

- **Write connection**: Owned by a background `tokio` task spawned on `get_runtime()`.
  Receives `DatabaseCommand` messages via a `tokio::sync::mpsc::unbounded_channel`.

All write operations (`add_*`, `update_*`, `delete_*`, `index_*`) are fire-and-forget:
they serialize the payload and send a `DatabaseCommand` through the channel. The
background task batches incoming commands into a Redis pipeline (`pipe.atomic()`)
and flushes either immediately (when `buffer_interval_ms = 0`) or on a configurable
timer.

```
                 ┌────────────────────┐
  Strategy ──►   │  CacheDatabaseAdapter  │
                 │  (main thread)     │
                 └──┬──────────┬──────┘
                    │ reads    │ writes
                    ▼          ▼
              ConnectionManager  mpsc::UnboundedSender
              (sync bridge)       │
                                  ▼
                           ┌──────────────┐
                           │ Background   │
                           │ Task         │
                           │ (pipeline    │
                           │  batching)   │
                           └──────┬───────┘
                                  │
                                  ▼
                           ConnectionManager
                           (WRITE connection)
```

### Pipeline Routing

Each `DatabaseCommand` carries a key whose first segment (before `:`) identifies
the collection. The `drain_buffer` function dispatches to the correct Redis
command based on collection type and operation:

| Collection | INSERT | UPDATE | DELETE |
|-----------|--------|--------|--------|
| STRING collections (`general`, `currencies`, `instruments`, `synthetics`, `actors`, `strategies`, `health`) | `SET` | — | `DEL` |
| LIST collections (`accounts`, `orders`, `positions`, `quotes`, `trades`, `bars`, `signals`, `custom_data`, `funding_rates`, `snapshots`) | `RPUSH` | `RPUSH_EXISTS` | `DEL` |
| SET indexes (`index:orders`, `index:positions_open`, etc.) | `SADD` | — | `SREM` |
| HASH indexes (`index:order_ids`, `index:order_position`, `index:order_client`) | `HSET` | — | `HDEL` |

`RPUSH_EXISTS` (via `rpush_exists`) appends only if the key already exists, preventing
writes to non-existent orders during race conditions.

## Redis Key Schema

All keys are prefixed with `{trader_key}:` where `trader_key = "trader-{trader_id}"`.
When `use_instance_id` is enabled, the prefix becomes `"trader-{trader_id}:{instance_id}"`.

| Key Pattern | Redis Type | Operations | Description |
|------------|-----------|------------|-------------|
| `general:{key}` | STRING | SET / GET | Generic key-value state |
| `currencies:{code}` | STRING | SET / GET | Currency definitions |
| `instruments:{id}` | STRING | SET / GET | Instrument definitions |
| `synthetics:{id}` | STRING | SET / GET | Synthetic instruments |
| `accounts:{id}` | LIST | RPUSH / LRANGE | Account events (event-sourced) |
| `orders:{id}` | LIST | RPUSH / LRANGE | Order events (event-sourced) |
| `positions:{id}` | LIST | DEL+RPUSH / LRANGE | Position events (DEL-first for NETTING) |
| `quotes:{instrument_id}` | LIST | RPUSH / LRANGE | Quote tick time series |
| `trades:{instrument_id}` | LIST | RPUSH / LRANGE | Trade tick time series |
| `bars:{bar_type}` | LIST | RPUSH / SCAN+LRANGE | Bar time series |
| `funding_rates:{id}` | LIST | RPUSH / LRANGE | Funding rate updates |
| `signals:{name}` | LIST | RPUSH / LRANGE | Signal time series |
| `custom_data:{type}` | LIST | RPUSH / LRANGE | Custom data entries |
| `snapshots:orders:{id}` | LIST | RPUSH / LRANGE | Order state snapshots |
| `snapshots:positions:{id}` | LIST | RPUSH / LRANGE | Position state snapshots |
| `actors:{id}:state` | STRING | SET / GET | Actor state map |
| `strategies:{id}:state` | STRING | SET / GET | Strategy state map |
| `health:heartbeat` | STRING | SET | Liveness timestamp |
| `index:orders` | SET | SADD / SREM | All order client IDs |
| `index:orders_open` | SET | SADD / SREM | Open order IDs |
| `index:orders_closed` | SET | SADD / SREM | Closed order IDs |
| `index:orders_inflight` | SET | SADD / SREM | Inflight order IDs |
| `index:orders_emulated` | SET | SADD / SREM | Emulated order IDs |
| `index:positions` | SET | SADD / SREM | All position IDs |
| `index:positions_open` | SET | SADD / SREM | Open position IDs |
| `index:positions_closed` | SET | SADD / SREM | Closed position IDs |
| `index:order_ids` | HASH | HSET / HDEL | ClientOrderId -> VenueOrderId |
| `index:order_position` | HASH | HSET / HDEL | ClientOrderId -> PositionId |
| `index:order_client` | HASH | HSET / HDEL | ClientOrderId -> ClientId |

## Index Management

### add_order (4 index operations)

1. `SADD index:orders` — register in global order set
2. `SADD index:orders_emulated` — conditional on `emulation_trigger != NoTrigger`
3. `HSET index:order_position` — conditional on `position_id` being present
4. `HSET index:order_client` — conditional on `client_id` being provided

### update_order (5+ state-dependent operations)

1. `RPUSH_EXISTS orders:{id}` — append latest event
2. `HSET index:order_ids` — index venue order ID if assigned
3. `SADD/SREM index:orders_inflight` — based on `order.is_inflight()`
4. `SADD/SREM index:orders_open` + `SREM/SADD index:orders_closed` — mutually exclusive
5. `SADD/SREM index:orders_emulated` — based on trigger and closed state

### add_position (DEL-first + 2 indexes)

1. `DEL positions:{id}` — clear stale data (NETTING mode reuses same ID on flip)
2. `RPUSH positions:{id}` — store initial fill event
3. `SADD index:positions` — register in global position set
4. `SADD index:positions_open` — new positions start as open

### update_position (open/closed exclusive transition)

1. `RPUSH_EXISTS positions:{id}` — append fill event
2. If open: `SADD index:positions_open` + `SREM index:positions_closed`
3. If closed: `SADD index:positions_closed` + `SREM index:positions_open`

### delete_order (cleans 6 SET indexes + 2 HASH indexes)

Removes the order key and cleans: `index:order_ids`, `index:orders`,
`index:orders_open`, `index:orders_closed`, `index:orders_emulated`,
`index:orders_inflight`, `index:order_position`, `index:order_client`.

### delete_position (cleans 3 indexes)

Removes the position key and cleans: `index:positions`, `index:positions_open`,
`index:positions_closed`.

## Event Sourcing

Orders, positions, and accounts are stored as append-only event lists in Redis.

- **Orders:** `[OrderInitialized, OrderSubmitted, OrderAccepted, ..., OrderFilled]`.
  On load, all events are deserialized and replayed via `OrderAny::from_events()`
  to reconstruct the full order state including partial fills.

- **Positions:** `[OrderFilled, OrderFilled, ...]`. Each fill event updates the
  position's quantity and side. Uses DEL-first on `add_position` for NETTING mode
  where the same position ID is reused when a position flips direction.

- **Accounts:** `[AccountState, AccountState, ...]`. Each `AccountState` event
  captures the full account state at that point in time.

The `load_order` implementation tries event replay first, with a fallback to direct
`OrderAny` deserialization for legacy data formats.

## Serialization

Configurable via `SerializationEncoding`: MsgPack (compact binary) or JSON (human-readable).

Serialization path:
1. `serde_json::to_value(payload)` — convert to JSON Value
2. `convert_timestamps()` — transform timestamp fields for storage
3. Encode to final format (MsgPack or JSON bytes)

Deserialization path:
1. Decode from stored format to JSON Value
2. `convert_timestamp_strings()` — restore timestamp fields
3. `serde_json::from_value()` — convert to target Rust type

**Currency special case:** The Rust `Currency` type has a custom `Serialize` impl
that writes only the code string (e.g., `"USD"`), not the full struct. Deserialization
looks up the code in `CURRENCY_MAP` to reconstruct the full `Currency` object.

## Known Limitations

| Method | Status | Notes |
|--------|--------|-------|
| `update_actor` | No-op | Rust trait has no state parameters (Cython passes Actor object) |
| `update_strategy` | No-op | Rust trait has no state parameters (Cython passes Strategy object) |
| `add_order_book` | No-op | `OrderBook` does not implement `Serialize` |
| `heartbeat` | Implemented | No caller in the Rust runtime currently invokes it |
| `load_index_order_position` | Returns empty | Trait requires `Position` objects, HASH stores only ID strings |
| `snapshot_position_state` | No unrealized PnL | Rust trait does not provide the parameter (Cython has it) |
| `delete_account_event` | No-op | Pending redesign of account event storage |
| Currency serialization | Rust-only | Rust writes code string, Cython writes full dict. Cross-language incompatible. |
| Heartbeat format | Rust-only | Rust writes `UnixNanos` integer string, Cython writes ISO8601. |
| Order transforms | Not handled | `load_order` does not handle order type transforms (duplicate `OrderInitialized` events). Rare edge case for conditional orders. |
| Position fill assertions | Panics | `Position::new()` panics on corrupted fill data (missing position_id, mismatched instrument_id). Upstream NT design choice. |

## Bug Fixes

These fixes go beyond filling in stub implementations; they correct incorrect behavior
in the upstream codebase:

1. **`index:order_ids` SET to HASH routing** (`f91c56b79`): The upstream stub routed
   `index:order_ids` through `insert_set` (SADD), silently dropping the venue order ID
   value. Fixed to use `insert_hset` (HSET) with `client_order_id` as field and
   `venue_order_id` as value.

2. **`update_position` payload bug** (`88a601954`): The Cython `database.pyx` reuses
   serialized event bytes for index SADD/SREM operations instead of position ID strings.
   The Rust implementation correctly uses `position_id.to_string()` bytes for index ops.

3. **`update_order` trait signature** (`49bb808e4`): Changed from accepting only the
   last event to accepting `&OrderAny`, enabling accurate state queries
   (`is_open()`, `is_closed()`, `is_inflight()`) for index management. This correctly
   handles partial fills (which remain open, not closed).

4. **`load_order` event replay** (`79cbb3790`): The original implementation only
   deserialized the first list element. Fixed to deserialize all events and replay via
   `OrderAny::from_events()` for correct state reconstruction.

5. **`Cache::snapshot_position_state`** (`2b2c901e0`): Removed `todo!()` panic that
   would crash the runtime when position snapshots were requested. Now delegates to
   the database adapter.

6. **`load()` general state recovery** (`f2d91e505`): Was returning an empty map.
   Now scans `general:*` keys and returns all persisted key-value state for startup
   recovery.

### Event Replay Implementations

The following load methods now implement full event replay from Redis lists,
matching the Cython behavior:

- **`load_order`**: Replays `OrderEventAny` events via `OrderAny::from_events()`.
  Falls back to direct deserialization for legacy data.
- **`load_account`**: Replays `AccountState` events via `AccountAny::from_events()`.
  Falls back to direct deserialization for legacy data.
- **`load_position`**: Replays `OrderFilled` events, loads the instrument from Redis,
  constructs `Position::new(&instrument, initial_fill)`, then applies remaining fills.
  Falls back to direct deserialization for legacy data.

All three follow the same pattern: read all list elements, deserialize as the
event type, reconstruct the full object via the appropriate factory/replay method.
The bulk load methods (`load_orders`, `load_accounts`, `load_positions`) delegate
to these singular methods and benefit from event replay automatically.

## Testing

### Unit tests (no Docker)

```bash
cargo nextest run -p nautilus-infrastructure
```

Runs all tests that do not require a live Redis or PostgreSQL instance, including
serialization, key formatting, and pipeline routing tests.

### Integration tests (Docker Redis)

```bash
# Start Redis
docker run -d --name redis-test -p 6379:6379 redis:7

# Run Redis integration tests
cargo nextest run -p nautilus-infrastructure --features redis

# Cleanup
docker stop redis-test && docker rm redis-test
```

Integration tests are gated behind `#[cfg(feature = "redis")]` and require a Redis
instance on `localhost:6379`. They exercise the full write-read cycle for each
data type.

### PostgreSQL tests

```bash
# Start PostgreSQL
docker run -d --name pg-test -p 5432:5432 \
  -e POSTGRES_PASSWORD=pass \
  -e POSTGRES_DB=nautilus \
  postgres:16

# Run PostgreSQL integration tests
cargo nextest run -p nautilus-infrastructure --features postgres
```

### Feature flags

| Flag | Purpose |
|------|---------|
| `redis` | Enables Redis cache and message bus implementations |
| `postgres` | Enables PostgreSQL cache database backend |
| `python` | Enables PyO3 bindings |

## File Layout

```
crates/infrastructure/
├── src/
│   ├── lib.rs                 # Crate root, feature-gated module declarations
│   ├── redis/
│   │   ├── mod.rs             # Connection management, URL parsing, key helpers
│   │   ├── cache.rs           # RedisCacheDatabaseAdapter (CacheDatabaseAdapter impl)
│   │   ├── msgbus.rs          # RedisMessageBusDatabase (MessageBusDatabaseAdapter impl)
│   │   └── queries.rs         # DatabaseQueries: serialization, bulk reads, load_* methods
│   ├── sql/
│   │   ├── mod.rs             # SQL module declarations
│   │   ├── cache.rs           # PostgresCacheDatabaseAdapter
│   │   ├── pg.rs              # PostgreSQL connection and pool management
│   │   ├── queries.rs         # SQL query implementations
│   │   └── models/            # SQLx model structs for all entity types
│   └── python/                # PyO3 bindings
└── tests/
    ├── test_cache_redis.rs    # Redis integration tests
    ├── test_redis_queries.rs  # Query-level tests
    ├── test_cache_postgres.rs # PostgreSQL integration tests
    └── test_cache_database_postgres.rs  # PostgreSQL adapter tests
```
