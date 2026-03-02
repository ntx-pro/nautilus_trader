// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Redis-backed cache database for the system.
//!
//! # Architecture
//!
//! Uses two Redis connections with distinct roles:
//! - **READ** (`self.con`): synchronous queries (`keys`, `read`, `load_all`),
//!   owned by the main struct.
//! - **WRITE**: owned by a background task on `get_runtime()`, receives
//!   commands via an unbounded `tokio::sync::mpsc` channel.
//!
//! All write operations (`insert`, `update`, `delete`, `flush`) are routed
//! through the command channel so they execute on the WRITE connection. This
//! avoids cross-runtime I/O issues since the WRITE connection is always
//! created on the Nautilus runtime.
//!
//! Synchronous callers (`close`, `flushdb_sync`) use `std::sync::mpsc` reply
//! channels to block until the background task confirms completion. When
//! called from the Nautilus runtime itself, `block_in_place` is used
//! automatically to avoid stalling the worker thread.

use std::{
    collections::VecDeque,
    fmt::Debug,
    ops::ControlFlow,
    pin::Pin,
    sync::mpsc::{self, SyncSender},
    time::Duration,
};

use ahash::AHashMap;
use bytes::Bytes;
use nautilus_common::{
    cache::{
        CacheConfig,
        database::{CacheDatabaseAdapter, CacheMap},
    },
    custom::CustomData,
    enums::SerializationEncoding,
    live::get_runtime,
    logging::{log_task_awaiting, log_task_started, log_task_stopped},
    signal::Signal,
};
use nautilus_core::{UUID4, UnixNanos, correctness::check_slice_not_empty};
use nautilus_cryptography::providers::install_cryptographic_provider;
use nautilus_model::{
    accounts::AccountAny,
    data::{Bar, DataType, FundingRateUpdate, QuoteTick, TradeTick},
    enums::TriggerType,
    events::{OrderSnapshot, position::snapshot::PositionSnapshot},
    identifiers::{
        AccountId, ClientId, ClientOrderId, ComponentId, InstrumentId, PositionId, StrategyId,
        TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, SyntheticInstrument},
    orderbook::OrderBook,
    orders::{Order, OrderAny},
    position::Position,
    types::Currency,
};
use redis::{Pipeline, aio::ConnectionManager};
use ustr::Ustr;

use super::{REDIS_DELIMITER, REDIS_FLUSHDB, get_index_key};
use crate::redis::{create_redis_connection, queries::DatabaseQueries};

// Task and connection names
const CACHE_READ: &str = "cache-read";
const CACHE_WRITE: &str = "cache-write";
const CACHE_PROCESS: &str = "cache-process";

// Error constants
const FAILED_TX_CHANNEL: &str = "Failed to send to channel";

// Collection keys
const INDEX: &str = "index";
const GENERAL: &str = "general";
const CURRENCIES: &str = "currencies";
const INSTRUMENTS: &str = "instruments";
const SYNTHETICS: &str = "synthetics";
const ACCOUNTS: &str = "accounts";
const ORDERS: &str = "orders";
const POSITIONS: &str = "positions";
const ACTORS: &str = "actors";
const STRATEGIES: &str = "strategies";
const SNAPSHOTS: &str = "snapshots";
const HEALTH: &str = "health";
const QUOTES: &str = "quotes";
const TRADES: &str = "trades";
const BARS: &str = "bars";
const SIGNALS: &str = "signals";
const CUSTOM_DATA: &str = "custom_data";
const FUNDING_RATES: &str = "funding_rates";

// Index keys
const INDEX_ORDER_IDS: &str = "index:order_ids";
const INDEX_ORDER_POSITION: &str = "index:order_position";
const INDEX_ORDER_CLIENT: &str = "index:order_client";
const INDEX_ORDERS: &str = "index:orders";
const INDEX_ORDERS_OPEN: &str = "index:orders_open";
const INDEX_ORDERS_CLOSED: &str = "index:orders_closed";
const INDEX_ORDERS_EMULATED: &str = "index:orders_emulated";
const INDEX_ORDERS_INFLIGHT: &str = "index:orders_inflight";
const INDEX_POSITIONS: &str = "index:positions";
const INDEX_POSITIONS_OPEN: &str = "index:positions_open";
const INDEX_POSITIONS_CLOSED: &str = "index:positions_closed";

/// A type of database operation.
#[derive(Clone, Debug)]
pub enum DatabaseOperation {
    Insert,
    Update,
    Delete,
    Flush(SyncSender<()>),
    Close,
}

/// Represents a database command to be performed which may be executed in a task.
#[derive(Clone, Debug)]
pub struct DatabaseCommand {
    /// The database operation type.
    pub op_type: DatabaseOperation,
    /// The primary key for the operation.
    pub key: Option<String>,
    /// The data payload for the operation.
    pub payload: Option<Vec<Bytes>>,
}

impl DatabaseCommand {
    /// Creates a new [`DatabaseCommand`] instance.
    #[must_use]
    pub const fn new(op_type: DatabaseOperation, key: String, payload: Option<Vec<Bytes>>) -> Self {
        Self {
            op_type,
            key: Some(key),
            payload,
        }
    }

    /// Initialize a `Close` database command, this is meant to close the database cache channel.
    #[must_use]
    pub const fn close() -> Self {
        Self {
            op_type: DatabaseOperation::Close,
            key: None,
            payload: None,
        }
    }
}

#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.infrastructure")
)]
pub struct RedisCacheDatabase {
    pub con: ConnectionManager,
    pub trader_id: TraderId,
    pub trader_key: String,
    pub encoding: SerializationEncoding,
    pub bulk_read_batch_size: Option<usize>,
    tx: tokio::sync::mpsc::UnboundedSender<DatabaseCommand>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Debug for RedisCacheDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RedisCacheDatabase))
            .field("trader_id", &self.trader_id)
            .field("encoding", &self.encoding)
            .finish()
    }
}

impl RedisCacheDatabase {
    /// Creates a new [`RedisCacheDatabase`] instance for the given `trader_id`, `instance_id`, and `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database configuration is missing in `config`.
    /// - Establishing the Redis connection fails.
    /// - The command processing task cannot be spawned.
    pub async fn new(
        trader_id: TraderId,
        instance_id: UUID4,
        config: CacheConfig,
    ) -> anyhow::Result<Self> {
        install_cryptographic_provider();

        let db_config = config
            .database
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No database config"))?;
        let con = create_redis_connection(CACHE_READ, db_config.clone()).await?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DatabaseCommand>();
        let trader_key = get_trader_key(trader_id, instance_id, &config);
        let trader_key_clone = trader_key.clone();
        let encoding = config.encoding;
        let bulk_read_batch_size = config.bulk_read_batch_size;

        let handle = get_runtime().spawn(async move {
            if let Err(e) = process_commands(rx, trader_key_clone, config.clone()).await {
                log::error!("Error in task '{CACHE_PROCESS}': {e}");
            }
        });

        Ok(Self {
            con,
            trader_id,
            trader_key,
            encoding,
            bulk_read_batch_size,
            tx,
            handle: Some(handle),
        })
    }

    #[must_use]
    pub const fn get_encoding(&self) -> SerializationEncoding {
        self.encoding
    }

    #[must_use]
    pub fn get_trader_key(&self) -> &str {
        &self.trader_key
    }

    pub fn close(&mut self) {
        log::debug!("Closing");

        let Some(handle) = self.handle.take() else {
            log::debug!("Already closed");
            return;
        };

        if let Err(e) = self.tx.send(DatabaseCommand::close()) {
            log::debug!("Error sending close command: {e:?}");
        }

        log_task_awaiting(CACHE_PROCESS);

        let (tx, rx) = mpsc::sync_channel(1);

        get_runtime().spawn(async move {
            if let Err(e) = handle.await {
                log::error!("Error awaiting task '{CACHE_PROCESS}': {e:?}");
            }
            let _ = tx.send(());
        });
        let _ = blocking_recv(&rx);

        log::debug!("Closed");
    }

    pub async fn flushdb(&mut self) {
        if let Err(e) = redis::cmd(REDIS_FLUSHDB)
            .query_async::<()>(&mut self.con)
            .await
        {
            log::error!("Failed to flush database: {e:?}");
        }
    }

    /// Sends a flush command through the background task channel and blocks
    /// until it completes. Safe to call from any runtime context.
    ///
    /// # Errors
    ///
    /// Returns an error if the command channel is closed or the reply is lost.
    pub fn flushdb_sync(&self) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let cmd = DatabaseCommand {
            op_type: DatabaseOperation::Flush(reply_tx),
            key: None,
            payload: None,
        };
        self.tx
            .send(cmd)
            .map_err(|e| anyhow::anyhow!("{FAILED_TX_CHANNEL}: {e}"))?;
        blocking_recv(&reply_rx).map_err(|e| anyhow::anyhow!("Failed to flush database: {e}"))?;
        Ok(())
    }

    /// Retrieves all keys matching the given `pattern` from Redis for this trader.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis scan operation fails.
    pub async fn keys(&mut self, pattern: &str) -> anyhow::Result<Vec<String>> {
        let pattern = format!("{}{REDIS_DELIMITER}{pattern}", self.trader_key);
        DatabaseQueries::scan_keys(&mut self.con, pattern).await
    }

    /// Reads the value(s) associated with `key` for this trader from Redis.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read(&mut self, key: &str) -> anyhow::Result<Vec<Bytes>> {
        DatabaseQueries::read(&self.con, &self.trader_key, key).await
    }

    /// Reads multiple values using bulk operations for efficiency.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Redis read operation fails.
    pub async fn read_bulk(&mut self, keys: &[String]) -> anyhow::Result<Vec<Option<Bytes>>> {
        match self.bulk_read_batch_size {
            Some(batch_size) => {
                DatabaseQueries::read_bulk_batched(&self.con, keys, batch_size).await
            }
            None => DatabaseQueries::read_bulk(&self.con, keys).await,
        }
    }

    /// Sends an insert command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn insert(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Insert, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Sends an update command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn update(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Update, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Sends a delete command for `key` with optional `payload` to Redis via the background task.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete(&mut self, key: String, payload: Option<Vec<Bytes>>) -> anyhow::Result<()> {
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, payload);
        match self.tx.send(op) {
            Ok(()) => Ok(()),
            Err(e) => anyhow::bail!("{FAILED_TX_CHANNEL}: {e}"),
        }
    }

    /// Delete the given order from the database with comprehensive index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        let order_id_bytes = Bytes::from(client_order_id.to_string());

        // Delete the order itself
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete order command: {e}"))?;

        // Delete from all order indexes
        let index_keys = [
            INDEX_ORDER_IDS,
            INDEX_ORDERS,
            INDEX_ORDERS_OPEN,
            INDEX_ORDERS_CLOSED,
            INDEX_ORDERS_EMULATED,
            INDEX_ORDERS_INFLIGHT,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send delete order index command: {e}"))?;
        }

        // Delete from hash indexes
        let hash_indexes = [INDEX_ORDER_POSITION, INDEX_ORDER_CLIENT];
        for index_key in &hash_indexes {
            let key = (*index_key).to_string();
            let payload = vec![order_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete order hash index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given position from the database with comprehensive index cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        let position_id_bytes = Bytes::from(position_id.to_string());

        // Delete the position itself
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete position command: {e}"))?;

        // Delete from all position indexes
        let index_keys = [
            INDEX_POSITIONS,
            INDEX_POSITIONS_OPEN,
            INDEX_POSITIONS_CLOSED,
        ];

        for index_key in &index_keys {
            let key = (*index_key).to_string();
            let payload = vec![position_id_bytes.clone()];
            let op = DatabaseCommand::new(DatabaseOperation::Delete, key, Some(payload));
            self.tx.send(op).map_err(|e| {
                anyhow::anyhow!("Failed to send delete position index command: {e}")
            })?;
        }

        Ok(())
    }

    /// Delete the given account event from the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot be sent to the background task channel.
    pub fn delete_account_event(
        &self,
        _account_id: &AccountId,
        _event_id: &str,
    ) -> anyhow::Result<()> {
        log::warn!("Deleting account events currently a no-op (pending redesign)");
        Ok(())
    }
}

fn blocking_recv<T>(rx: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvError> {
    let on_nautilus_runtime = tokio::runtime::Handle::try_current()
        .ok()
        .is_some_and(|h| h.id() == get_runtime().handle().id());

    if on_nautilus_runtime {
        tokio::task::block_in_place(|| rx.recv())
    } else {
        rx.recv()
    }
}

async fn process_commands(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<DatabaseCommand>,
    trader_key: String,
    config: CacheConfig,
) -> anyhow::Result<()> {
    log_task_started(CACHE_PROCESS);

    let db_config = config
        .database
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No database config"))?;
    let mut con = create_redis_connection(CACHE_WRITE, db_config.clone()).await?;

    // Buffering
    let mut buffer: VecDeque<DatabaseCommand> = VecDeque::new();
    let buffer_interval = Duration::from_millis(config.buffer_interval_ms.unwrap_or(0) as u64);

    // A sleep used to trigger periodic flushing of the buffer.
    // When `buffer_interval` is zero we skip using the timer and flush immediately
    // after every message.
    let flush_timer = tokio::time::sleep(buffer_interval);
    tokio::pin!(flush_timer);

    // Continue to receive and handle messages until channel is hung up
    loop {
        tokio::select! {
            maybe_cmd = rx.recv() => {
                let result = handle_command(
                    maybe_cmd,
                    &mut buffer,
                    buffer_interval,
                    &mut con,
                    &trader_key,
                ).await;

                if result.is_break() {
                    break;
                }
            }
            () = &mut flush_timer, if !buffer_interval.is_zero() => {
                flush_buffer(&mut buffer, &mut con, &trader_key, &mut flush_timer, buffer_interval).await;
            }
        }
    }

    // Drain any remaining messages
    if !buffer.is_empty() {
        drain_buffer(&mut con, &trader_key, &mut buffer).await;
    }

    log_task_stopped(CACHE_PROCESS);
    Ok(())
}

async fn handle_command(
    maybe_cmd: Option<DatabaseCommand>,
    buffer: &mut VecDeque<DatabaseCommand>,
    buffer_interval: Duration,
    con: &mut ConnectionManager,
    trader_key: &str,
) -> ControlFlow<()> {
    let Some(cmd) = maybe_cmd else {
        log::debug!("Command channel closed");
        return ControlFlow::Break(());
    };

    log::trace!("Received {cmd:?}");

    match cmd.op_type {
        DatabaseOperation::Close => {
            if !buffer.is_empty() {
                drain_buffer(con, trader_key, buffer).await;
            }
            return ControlFlow::Break(());
        }
        DatabaseOperation::Flush(reply_tx) => {
            if !buffer.is_empty() {
                drain_buffer(con, trader_key, buffer).await;
            }

            if let Err(e) = redis::cmd(REDIS_FLUSHDB).query_async::<()>(con).await {
                log::error!("Failed to flush database: {e:?}");
            }
            let _ = reply_tx.send(());
            return ControlFlow::Continue(());
        }
        _ => {}
    }

    buffer.push_back(cmd);

    if buffer_interval.is_zero() {
        drain_buffer(con, trader_key, buffer).await;
    }

    ControlFlow::Continue(())
}

async fn flush_buffer(
    buffer: &mut VecDeque<DatabaseCommand>,
    con: &mut ConnectionManager,
    trader_key: &str,
    flush_timer: &mut Pin<&mut tokio::time::Sleep>,
    buffer_interval: Duration,
) {
    if !buffer.is_empty() {
        drain_buffer(con, trader_key, buffer).await;
    }
    flush_timer
        .as_mut()
        .reset(tokio::time::Instant::now() + buffer_interval);
}

async fn drain_buffer(
    conn: &mut ConnectionManager,
    trader_key: &str,
    buffer: &mut VecDeque<DatabaseCommand>,
) {
    let mut pipe = redis::pipe();
    pipe.atomic();

    for msg in buffer.drain(..) {
        let key = if let Some(key) = msg.key {
            key
        } else {
            log::error!("Null key found for message: {msg:?}");
            continue;
        };
        let collection = match get_collection_key(&key) {
            Ok(collection) => collection,
            Err(e) => {
                log::error!("{e}");
                continue; // Continue to next message
            }
        };

        let key = format!("{trader_key}{REDIS_DELIMITER}{}", &key);

        match msg.op_type {
            DatabaseOperation::Insert => {
                if let Some(payload) = msg.payload {
                    log::debug!("Processing INSERT for collection: {collection}, key: {key}");
                    if let Err(e) = insert(&mut pipe, collection, &key, payload) {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `insert`");
                }
            }
            DatabaseOperation::Update => {
                if let Some(payload) = msg.payload {
                    log::debug!("Processing UPDATE for collection: {collection}, key: {key}");
                    if let Err(e) = update(&mut pipe, collection, &key, payload) {
                        log::error!("{e}");
                    }
                } else {
                    log::error!("Null `payload` for `update`");
                }
            }
            DatabaseOperation::Delete => {
                log::debug!(
                    "Processing DELETE for collection: {}, key: {}, payload: {:?}",
                    collection,
                    key,
                    msg.payload.as_ref().map(std::vec::Vec::len)
                );
                // `payload` can be `None` for a delete operation
                if let Err(e) = delete(&mut pipe, collection, &key, msg.payload) {
                    log::error!("{e}");
                }
            }
            DatabaseOperation::Close => panic!("Close command should not be drained"),
            DatabaseOperation::Flush(_) => panic!("Flush command should not be drained"),
        }
    }

    if let Err(e) = pipe.query_async::<()>(conn).await {
        log::error!("{e}");
    }
}

fn insert(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Vec<Bytes>,
) -> anyhow::Result<()> {
    check_slice_not_empty(value.as_slice(), stringify!(value))?;

    match collection {
        INDEX => insert_index(pipe, key, &value),
        GENERAL => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        CURRENCIES => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        INSTRUMENTS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        SYNTHETICS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        ACCOUNTS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ORDERS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        POSITIONS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ACTORS => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        STRATEGIES => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        SNAPSHOTS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        HEALTH => {
            insert_string(pipe, key, value[0].as_ref());
            Ok(())
        }
        QUOTES => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        TRADES => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        BARS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        SIGNALS => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        CUSTOM_DATA => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        FUNDING_RATES => {
            insert_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `insert` for collection '{collection}'"),
    }
}

fn insert_index(pipe: &mut Pipeline, key: &str, value: &[Bytes]) -> anyhow::Result<()> {
    let index_key = get_index_key(key)?;
    match index_key {
        INDEX_ORDER_IDS => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDER_POSITION => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDER_CLIENT => {
            insert_hset(pipe, key, value[0].as_ref(), value[1].as_ref());
            Ok(())
        }
        INDEX_ORDERS => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_OPEN => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_CLOSED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_EMULATED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_INFLIGHT => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_OPEN => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_CLOSED => {
            insert_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Index unknown '{index_key}' on insert"),
    }
}

fn insert_string(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.set(key, value);
}

fn insert_set(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.sadd(key, value);
}

fn insert_hset(pipe: &mut Pipeline, key: &str, name: &[u8], value: &[u8]) {
    pipe.hset(key, name, value);
}

fn insert_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush(key, value);
}

fn update(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Vec<Bytes>,
) -> anyhow::Result<()> {
    check_slice_not_empty(value.as_slice(), stringify!(value))?;

    match collection {
        ACCOUNTS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        ORDERS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        POSITIONS => {
            update_list(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `update` for collection '{collection}'"),
    }
}

fn update_list(pipe: &mut Pipeline, key: &str, value: &[u8]) {
    pipe.rpush_exists(key, value);
}

fn delete(
    pipe: &mut Pipeline,
    collection: &str,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    log::debug!(
        "delete: collection={}, key={}, has_payload={}",
        collection,
        key,
        value.is_some()
    );

    match collection {
        INDEX => delete_from_index(pipe, key, value),
        ORDERS => {
            delete_string(pipe, key);
            Ok(())
        }
        POSITIONS => {
            delete_string(pipe, key);
            Ok(())
        }
        ACCOUNTS => {
            delete_string(pipe, key);
            Ok(())
        }
        ACTORS => {
            delete_string(pipe, key);
            Ok(())
        }
        STRATEGIES => {
            delete_string(pipe, key);
            Ok(())
        }
        _ => anyhow::bail!("Unsupported operation: `delete` for collection '{collection}'"),
    }
}

fn delete_from_index(
    pipe: &mut Pipeline,
    key: &str,
    value: Option<Vec<Bytes>>,
) -> anyhow::Result<()> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Empty `payload` for `delete` '{key}'"))?;
    let index_key = get_index_key(key)?;

    match index_key {
        INDEX_ORDER_IDS => {
            remove_from_hash(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDER_POSITION => {
            remove_from_hash(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDER_CLIENT => {
            remove_from_hash(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_OPEN => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_CLOSED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_EMULATED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_ORDERS_INFLIGHT => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_OPEN => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        INDEX_POSITIONS_CLOSED => {
            remove_from_set(pipe, key, value[0].as_ref());
            Ok(())
        }
        _ => anyhow::bail!("Unsupported index operation: remove from '{index_key}'"),
    }
}

fn remove_from_set(pipe: &mut Pipeline, key: &str, member: &[u8]) {
    pipe.srem(key, member);
}

fn remove_from_hash(pipe: &mut Pipeline, key: &str, field: &[u8]) {
    pipe.hdel(key, field);
}

fn delete_string(pipe: &mut Pipeline, key: &str) {
    pipe.del(key);
}

fn get_trader_key(trader_id: TraderId, instance_id: UUID4, config: &CacheConfig) -> String {
    let mut key = String::new();

    if config.use_trader_prefix {
        key.push_str("trader-");
    }

    key.push_str(trader_id.as_str());

    if config.use_instance_id {
        key.push(REDIS_DELIMITER);
        key.push_str(&format!("{instance_id}"));
    }

    key
}

fn get_collection_key(key: &str) -> anyhow::Result<&str> {
    key.split_once(REDIS_DELIMITER)
        .map(|(collection, _)| collection)
        .ok_or_else(|| {
            anyhow::anyhow!("Invalid `key`, missing a '{REDIS_DELIMITER}' delimiter, was {key}")
        })
}

#[derive(Debug)]
pub struct RedisCacheDatabaseAdapter {
    pub encoding: SerializationEncoding,
    pub database: RedisCacheDatabase,
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for RedisCacheDatabaseAdapter {
    /// Closes the Redis cache database connection and background writer task.
    fn close(&mut self) -> anyhow::Result<()> {
        self.database.close();
        Ok(())
    }

    /// Flushes all data from the Redis database (FLUSHDB).
    fn flush(&mut self) -> anyhow::Result<()> {
        self.database.flushdb_sync()
    }

    /// Loads all cached data concurrently: currencies, instruments, synthetics,
    /// accounts, orders, positions, greeks, and yield curves.
    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        log::debug!("Loading all data");

        let (
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        ) = tokio::try_join!(
            self.load_currencies(),
            self.load_instruments(),
            self.load_synthetics(),
            self.load_accounts(),
            self.load_orders(),
            self.load_positions(),
            self.load_greeks(),
            self.load_yield_curves()
        )
        .map_err(|e| anyhow::anyhow!("Error loading cache data: {e}"))?;

        Ok(CacheMap {
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
            greeks,
            yield_curves,
        })
    }

    /// Loads all general key-value state from Redis.
    ///
    /// Scans all keys matching `general:*`, reads values via MGET,
    /// and returns a map with original keys (trader_key prefix stripped).
    /// Called during startup by `Cache::cache_general()` to restore
    /// actor/strategy custom state persisted via `add(key, value)`.
    fn load(&self) -> anyhow::Result<AHashMap<String, Bytes>> {
        let trader_key = self.database.trader_key.clone();
        let pattern = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}{REDIS_DELIMITER}*");
        let prefix = format!("{trader_key}{REDIS_DELIMITER}{GENERAL}{REDIS_DELIMITER}");
        let mut con = self.database.con.clone();

        let (tx, rx) = mpsc::sync_channel(1);

        get_runtime().spawn(async move {
            let result: anyhow::Result<AHashMap<String, Bytes>> = async {
                let keys = DatabaseQueries::scan_keys(&mut con, pattern).await?;
                if keys.is_empty() {
                    return Ok(AHashMap::new());
                }

                let values = DatabaseQueries::read_bulk(&con, &keys).await?;

                let mut map = AHashMap::with_capacity(keys.len());
                for (key, value_opt) in keys.iter().zip(values.into_iter()) {
                    if let Some(value) = value_opt {
                        let clean_key = key.strip_prefix(&prefix).unwrap_or(key);
                        map.insert(clean_key.to_string(), value);
                    }
                }
                Ok(map)
            }
            .await;
            let _ = tx.send(result);
        });

        blocking_recv(&rx)
            .map_err(|e| anyhow::anyhow!("Failed to receive load() result: {e}"))?
    }

    /// Delegates to [`DatabaseQueries::load_currencies`] to scan and deserialize all persisted currencies.
    async fn load_currencies(&self) -> anyhow::Result<AHashMap<Ustr, Currency>> {
        DatabaseQueries::load_currencies(
            &self.database.con,
            &self.database.trader_key,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_instruments`] to scan and deserialize all persisted instruments.
    async fn load_instruments(&self) -> anyhow::Result<AHashMap<InstrumentId, InstrumentAny>> {
        DatabaseQueries::load_instruments(
            &self.database.con,
            &self.database.trader_key,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_synthetics`] to scan and deserialize all persisted synthetics.
    async fn load_synthetics(&self) -> anyhow::Result<AHashMap<InstrumentId, SyntheticInstrument>> {
        DatabaseQueries::load_synthetics(
            &self.database.con,
            &self.database.trader_key,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_accounts`] to scan and deserialize all persisted accounts.
    async fn load_accounts(&self) -> anyhow::Result<AHashMap<AccountId, AccountAny>> {
        DatabaseQueries::load_accounts(&self.database.con, &self.database.trader_key, self.encoding)
            .await
    }

    /// Delegates to [`DatabaseQueries::load_orders`] to scan and deserialize all persisted orders.
    async fn load_orders(&self) -> anyhow::Result<AHashMap<ClientOrderId, OrderAny>> {
        DatabaseQueries::load_orders(&self.database.con, &self.database.trader_key, self.encoding)
            .await
    }

    /// Delegates to [`DatabaseQueries::load_positions`] to scan and deserialize all persisted positions.
    async fn load_positions(&self) -> anyhow::Result<AHashMap<PositionId, Position>> {
        DatabaseQueries::load_positions(
            &self.database.con,
            &self.database.trader_key,
            self.encoding,
        )
        .await
    }

    /// Returns an empty map because the Redis HASH `index:order_position`
    /// stores only ID-to-ID mappings (`ClientOrderId` -> `PositionId`), not
    /// full `Position` objects.  Reconstructing positions would require
    /// loading each one individually which is expensive and unnecessary:
    /// the in-memory cache rebuilds positions from event replay at startup.
    fn load_index_order_position(&self) -> anyhow::Result<AHashMap<ClientOrderId, Position>> {
        Ok(AHashMap::new())
    }

    /// Loads the order-to-client index from Redis HASH `index:order_client`.
    ///
    /// Each entry maps a `ClientOrderId` to the `ClientId` of the execution
    /// client that manages it.  Returns an empty map when the key does not
    /// exist.
    fn load_index_order_client(&self) -> anyhow::Result<AHashMap<ClientOrderId, ClientId>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{INDEX_ORDER_CLIENT}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<std::collections::HashMap<String, String>, _> =
                redis::cmd("HGETALL").arg(&key).query_async(&mut con).await;
            let _ = tx.send(result);
        });

        let raw: std::collections::HashMap<String, String> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        let mut map = AHashMap::with_capacity(raw.len());
        for (order_str, client_str) in raw {
            let order_id = ClientOrderId::from(order_str.as_str());
            let client_id = ClientId::from(client_str.as_str());
            map.insert(order_id, client_id);
        }
        Ok(map)
    }

    /// Delegates to [`DatabaseQueries::load_currency`] to load a single currency by code.
    async fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        DatabaseQueries::load_currency(
            &self.database.con,
            &self.database.trader_key,
            code,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_instrument`] to load a single instrument by ID.
    async fn load_instrument(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<InstrumentAny>> {
        DatabaseQueries::load_instrument(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_synthetic`] to load a single synthetic instrument by ID.
    async fn load_synthetic(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Option<SyntheticInstrument>> {
        DatabaseQueries::load_synthetic(
            &self.database.con,
            &self.database.trader_key,
            instrument_id,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_account`] to load a single account by ID.
    async fn load_account(&self, account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        DatabaseQueries::load_account(
            &self.database.con,
            &self.database.trader_key,
            account_id,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_order`] to load a single order by client order ID.
    async fn load_order(
        &self,
        client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderAny>> {
        DatabaseQueries::load_order(
            &self.database.con,
            &self.database.trader_key,
            client_order_id,
            self.encoding,
        )
        .await
    }

    /// Delegates to [`DatabaseQueries::load_position`] to load a single position by ID.
    async fn load_position(&self, position_id: &PositionId) -> anyhow::Result<Option<Position>> {
        DatabaseQueries::load_position(
            &self.database.con,
            &self.database.trader_key,
            position_id,
            self.encoding,
        )
        .await
    }

    /// Loads actor state from Redis.
    ///
    /// Reads a STRING value at key `actors:{component_id}:state`, deserializes
    /// it from the configured encoding into a map of state key-value pairs.
    /// Returns an empty map when no state has been persisted.
    fn load_actor(&self, component_id: &ComponentId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{ACTORS}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<u8>, _> = redis::cmd("GET").arg(&key).query_async(&mut con).await;
            let _ = tx.send(result);
        });

        let raw: Vec<u8> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        if raw.is_empty() {
            return Ok(AHashMap::new());
        }

        let state: AHashMap<String, Bytes> =
            DatabaseQueries::deserialize_payload(self.encoding, &raw)?;
        Ok(state)
    }

    /// Deletes actor state from Redis.
    ///
    /// Removes the STRING key `actors:{component_id}:state` via the
    /// background write channel.
    fn delete_actor(&self, component_id: &ComponentId) -> anyhow::Result<()> {
        let key = format!("{ACTORS}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        log::debug!("Deleting actor state: {component_id} from Redis");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete_actor command: {e}"))
    }

    /// Loads strategy state from Redis.
    ///
    /// Reads a STRING value at key `strategies:{strategy_id}:state`,
    /// deserializes it from the configured encoding into a map of state
    /// key-value pairs.  Returns an empty map when no state has been
    /// persisted.
    fn load_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<AHashMap<String, Bytes>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{STRATEGIES}{REDIS_DELIMITER}{strategy_id}{REDIS_DELIMITER}state",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<u8>, _> = redis::cmd("GET").arg(&key).query_async(&mut con).await;
            let _ = tx.send(result);
        });

        let raw: Vec<u8> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        if raw.is_empty() {
            return Ok(AHashMap::new());
        }

        let state: AHashMap<String, Bytes> =
            DatabaseQueries::deserialize_payload(self.encoding, &raw)?;
        Ok(state)
    }

    /// Deletes strategy state from Redis.
    ///
    /// Removes the STRING key `strategies:{strategy_id}:state` via the
    /// background write channel.
    fn delete_strategy(&self, component_id: &StrategyId) -> anyhow::Result<()> {
        let key = format!("{STRATEGIES}{REDIS_DELIMITER}{component_id}{REDIS_DELIMITER}state");
        log::debug!("Deleting strategy state: {component_id} from Redis");
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key, None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete_strategy command: {e}"))
    }

    /// Delegates to [`RedisCacheDatabase::delete_order`] which removes the order and
    /// cleans up all associated indexes (orders, open, closed, emulated, inflight,
    /// order_position, order_client).
    fn delete_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<()> {
        log::debug!("Deleting order: {client_order_id} from Redis");
        self.database.delete_order(client_order_id)
    }

    /// Delegates to [`RedisCacheDatabase::delete_position`] which removes the position
    /// and cleans up all associated indexes.
    fn delete_position(&self, position_id: &PositionId) -> anyhow::Result<()> {
        log::debug!("Deleting position: {position_id} from Redis");
        self.database.delete_position(position_id)
    }

    /// Deletes an account event from Redis.
    ///
    /// Delegates to the public `delete_account_event` method which is
    /// currently a no-op (pending redesign of account event storage).
    fn delete_account_event(&self, account_id: &AccountId, event_id: &str) -> anyhow::Result<()> {
        self.database.delete_account_event(account_id, event_id)
    }

    /// Persists a generic key-value pair to Redis.
    ///
    /// Stores as a STRING value under key `general:{key}`.
    /// The value is pre-serialized bytes and passed through directly.
    fn add(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        let key = format!("{GENERAL}{REDIS_DELIMITER}{key}");
        log::debug!("Adding general key: {key} to Redis");
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![value]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add command: {e}"))
    }

    /// Persists a currency definition to Redis.
    ///
    /// Stores as a STRING value under key `currencies:{code}`.
    /// Serialized using the configured encoding (MsgPack or JSON).
    ///
    /// Note: The Rust `Currency` type serializes only the code string (e.g., "USD")
    /// via a custom `Serialize` impl. The Cython implementation writes a full dict
    /// with `{precision, iso4217, name, currency_type}`. Rust-to-Rust round-trips
    /// work correctly via `CURRENCY_MAP` lookup on deserialization.
    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        let key = format!("{CURRENCIES}{REDIS_DELIMITER}{}", currency.code);
        log::debug!("Adding currency: {} to Redis", currency.code);
        let payload = DatabaseQueries::serialize_payload(self.encoding, currency)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_currency command: {e}"))
    }

    /// Persists an instrument definition to Redis.
    ///
    /// Stores as a STRING value under key `instruments:{instrument_id}`.
    /// Serialized using the configured encoding (MsgPack or JSON).
    fn add_instrument(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        let key = format!("{INSTRUMENTS}{REDIS_DELIMITER}{}", instrument.id());
        log::debug!("Adding instrument: {} to Redis", instrument.id());
        let payload = DatabaseQueries::serialize_payload(self.encoding, instrument)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_instrument command: {e}"))
    }

    /// Persists a synthetic instrument definition to Redis.
    ///
    /// Stores as a STRING value under key `synthetics:{instrument_id}`.
    /// Serialized using the configured encoding (MsgPack or JSON).
    fn add_synthetic(&self, synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        let key = format!("{SYNTHETICS}{REDIS_DELIMITER}{}", synthetic.id);
        log::debug!("Adding synthetic instrument: {} to Redis", synthetic.id);
        let payload = DatabaseQueries::serialize_payload(self.encoding, synthetic)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_synthetic command: {e}"))
    }

    /// Persists an account state event to Redis.
    ///
    /// Stores the last `AccountState` event as a LIST entry under key
    /// `accounts:{account_id}`. Uses RPUSH to append the event to the list,
    /// preserving the full event history for the account.
    fn add_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");
        log::debug!("Adding account: {account_id} to Redis");
        let last_event = account
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Account {account_id} has no events"))?;
        let payload = DatabaseQueries::serialize_payload(self.encoding, &last_event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_account command: {e}"))
    }

    /// Persists an order's last event and updates all relevant indexes.
    ///
    /// Stores the serialized order event as a LIST entry under `orders:{client_order_id}`.
    /// Updates the following indexes:
    /// - `index:orders` (SET) — global order ID registry
    /// - `index:orders_emulated` (SET) — conditional on emulation trigger
    /// - `index:order_position` (HASH) — conditional on position_id
    /// - `index:order_client` (HASH) — conditional on client_id
    fn add_order(&self, order: &OrderAny, client_id: Option<ClientId>) -> anyhow::Result<()> {
        let client_order_id = order.client_order_id();
        let client_order_id_str = client_order_id.to_string();

        log::debug!("Adding order: {client_order_id} to Redis");

        // Store order event (RPUSH to list)
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let event = order.last_event();
        let payload = DatabaseQueries::serialize_payload(self.encoding, event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_order command: {e}"))?;

        // Index: add to global order set (SADD)
        let client_order_id_bytes = Bytes::from(client_order_id_str);
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            INDEX_ORDERS.to_string(),
            Some(vec![client_order_id_bytes.clone()]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send order index command: {e}"))?;

        // Index: emulated orders (SADD, conditional)
        if let Some(trigger) = order.emulation_trigger()
            && trigger != TriggerType::NoTrigger
        {
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_ORDERS_EMULATED.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send emulated index command: {e}"))?;
        }

        // Index: order-to-position mapping (HSET, conditional)
        if let Some(position_id) = order.position_id() {
            self.index_order_position(client_order_id, position_id)?;
        }

        // Index: order-to-client mapping (HSET, conditional)
        if let Some(cid) = client_id {
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_ORDER_CLIENT.to_string(),
                Some(vec![
                    client_order_id_bytes,
                    Bytes::from(cid.to_string()),
                ]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send order-client index command: {e}"))?;
        }

        Ok(())
    }

    /// Persists a pre-built order snapshot to Redis.
    ///
    /// Serializes the `OrderSnapshot` and appends it (RPUSH) to the LIST
    /// at key `snapshots:orders:{client_order_id}`.  Multiple snapshots
    /// for the same order accumulate as a time-series of state.
    fn add_order_snapshot(&self, snapshot: &OrderSnapshot) -> anyhow::Result<()> {
        let client_order_id = &snapshot.client_order_id;
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{ORDERS}{REDIS_DELIMITER}{client_order_id}",
        );
        log::debug!("Adding order snapshot: {client_order_id} to Redis");
        let payload = DatabaseQueries::serialize_payload(self.encoding, snapshot)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_order_snapshot command: {e}"))
    }

    /// Persists a position's initial fill event and updates position indexes.
    ///
    /// Uses the DELETE-first pattern for NETTING mode compatibility: in NETTING
    /// mode, the same position ID is reused when a position flips direction
    /// (e.g., long to short). Deleting the existing key before inserting
    /// prevents appending to a stale event list.
    ///
    /// Operations:
    /// 1. DELETE existing position key (handles NETTING mode flip)
    /// 2. RPUSH the position's last event (initial fill)
    /// 3. SADD to `index:positions` (global position set)
    /// 4. SADD to `index:positions_open` (new positions start as open)
    fn add_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        let position_id_str = position_id.to_string();
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");

        log::debug!("Adding position: {position_id} to Redis");

        // Delete existing data first (NETTING mode: same ID reused on flip)
        let op = DatabaseCommand::new(DatabaseOperation::Delete, key.clone(), None);
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send delete position command: {e}"))?;

        // Store position's last event (initial fill)
        let event = position
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Position {position_id} has no events"))?;
        let payload = DatabaseQueries::serialize_payload(self.encoding, &event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_position command: {e}"))?;

        // Index: add to global position set (SADD)
        let position_id_bytes = Bytes::from(position_id_str);
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            INDEX_POSITIONS.to_string(),
            Some(vec![position_id_bytes.clone()]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send position index command: {e}"))?;

        // Index: add to open positions (SADD)
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            INDEX_POSITIONS_OPEN.to_string(),
            Some(vec![position_id_bytes]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send position open index command: {e}"))
    }

    /// Persists a pre-built position snapshot to Redis.
    ///
    /// Serializes the `PositionSnapshot` and appends it (RPUSH) to the LIST
    /// at key `snapshots:positions:{position_id}`.  Multiple snapshots
    /// for the same position accumulate as a time-series of state.
    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        let position_id = &snapshot.position_id;
        let key = format!(
            "{SNAPSHOTS}{REDIS_DELIMITER}{POSITIONS}{REDIS_DELIMITER}{position_id}",
        );
        log::debug!("Adding position snapshot: {position_id} to Redis");
        let payload = DatabaseQueries::serialize_payload(self.encoding, snapshot)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_position_snapshot command: {e}"))
    }

    /// Persists an order book snapshot to Redis.
    ///
    /// No-op: `OrderBook` does not implement `Serialize` because it contains
    /// internal ladder structures that are rebuilt from delta events. Both the
    /// upstream Redis and PostgreSQL adapters leave this unimplemented.
    fn add_order_book(&self, order_book: &OrderBook) -> anyhow::Result<()> {
        log::debug!(
            "add_order_book called for {} (no-op: OrderBook is not serializable)",
            order_book.instrument_id,
        );
        Ok(())
    }

    /// Persists a quote tick to Redis.
    ///
    /// Appends the serialized `QuoteTick` (RPUSH) to the LIST at key
    /// `quotes:{instrument_id}`, preserving the full time-series history.
    fn add_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let key = format!("{QUOTES}{REDIS_DELIMITER}{}", quote.instrument_id);
        log::debug!("Adding quote for {} to Redis", quote.instrument_id);
        let payload = DatabaseQueries::serialize_payload(self.encoding, quote)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_quote command: {e}"))
    }

    /// Loads all persisted quote ticks for an instrument from Redis.
    ///
    /// Reads the full LIST at key `quotes:{instrument_id}` using LRANGE 0 -1,
    /// deserializes each entry, and returns them in insertion order.
    fn load_quotes(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{QUOTES}{REDIS_DELIMITER}{instrument_id}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(0i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Persists a trade tick to Redis.
    ///
    /// Appends the serialized `TradeTick` (RPUSH) to the LIST at key
    /// `trades:{instrument_id}`, preserving the full time-series history.
    fn add_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        let key = format!("{TRADES}{REDIS_DELIMITER}{}", trade.instrument_id);
        log::debug!("Adding trade for {} to Redis", trade.instrument_id);
        let payload = DatabaseQueries::serialize_payload(self.encoding, trade)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_trade command: {e}"))
    }

    /// Loads all persisted trade ticks for an instrument from Redis.
    ///
    /// Reads the full LIST at key `trades:{instrument_id}` using LRANGE 0 -1,
    /// deserializes each entry, and returns them in insertion order.
    fn load_trades(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{TRADES}{REDIS_DELIMITER}{instrument_id}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(0i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Persists a funding rate update to Redis.
    ///
    /// Appends the serialized `FundingRateUpdate` (RPUSH) to the LIST at key
    /// `funding_rates:{instrument_id}`, preserving the full time-series history.
    fn add_funding_rate(&self, funding_rate: &FundingRateUpdate) -> anyhow::Result<()> {
        let key = format!(
            "{FUNDING_RATES}{REDIS_DELIMITER}{}",
            funding_rate.instrument_id,
        );
        log::debug!(
            "Adding funding rate for {} to Redis",
            funding_rate.instrument_id,
        );
        let payload = DatabaseQueries::serialize_payload(self.encoding, funding_rate)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_funding_rate command: {e}"))
    }

    /// Loads all persisted funding rate updates for an instrument from Redis.
    ///
    /// Reads the full LIST at key `funding_rates:{instrument_id}` using
    /// LRANGE 0 -1, deserializes each entry, and returns them in insertion
    /// order.
    fn load_funding_rates(
        &self,
        instrument_id: &InstrumentId,
    ) -> anyhow::Result<Vec<FundingRateUpdate>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{FUNDING_RATES}{REDIS_DELIMITER}{instrument_id}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(0i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Persists a bar to Redis.
    ///
    /// Appends the serialized `Bar` (RPUSH) to the LIST at key
    /// `bars:{bar_type}`, where `bar_type` encodes the instrument ID,
    /// bar specification (step, aggregation), and aggregation source.
    fn add_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        let key = format!("{BARS}{REDIS_DELIMITER}{}", bar.bar_type);
        log::debug!("Adding bar for {} to Redis", bar.bar_type);
        let payload = DatabaseQueries::serialize_payload(self.encoding, bar)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_bar command: {e}"))
    }

    /// Loads all persisted bars for an instrument from Redis.
    ///
    /// Scans for all LIST keys matching `bars:{instrument_id}*` (covering
    /// all bar types for the instrument), reads each list via LRANGE 0 -1,
    /// deserializes the entries, and returns them concatenated.
    fn load_bars(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        let pattern = format!(
            "{}{REDIS_DELIMITER}{BARS}{REDIS_DELIMITER}{instrument_id}*",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let keys = match DatabaseQueries::scan_keys(&mut con, pattern).await {
                Ok(k) => k,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };

            let mut all_bars = Vec::new();
            for key in &keys {
                let items: Vec<Vec<u8>> = match redis::cmd("LRANGE")
                    .arg(key)
                    .arg(0i64)
                    .arg(-1i64)
                    .query_async(&mut con)
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        let _ = tx.send(Err(anyhow::anyhow!("LRANGE failed for {key}: {e}")));
                        return;
                    }
                };
                all_bars.extend(items);
            }
            let _ = tx.send(Ok(all_bars));
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Persists a signal to Redis.
    ///
    /// Appends the serialized `Signal` (RPUSH) to the LIST at key
    /// `signals:{name}`, preserving the full time-series history.
    fn add_signal(&self, signal: &Signal) -> anyhow::Result<()> {
        let key = format!("{SIGNALS}{REDIS_DELIMITER}{}", signal.name);
        log::debug!("Adding signal '{}' to Redis", signal.name);
        let payload = DatabaseQueries::serialize_payload(self.encoding, signal)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_signal command: {e}"))
    }

    /// Loads all persisted signals by name from Redis.
    ///
    /// Reads the full LIST at key `signals:{name}` using LRANGE 0 -1,
    /// deserializes each entry, and returns them in insertion order.
    fn load_signals(&self, name: &str) -> anyhow::Result<Vec<Signal>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{SIGNALS}{REDIS_DELIMITER}{name}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(0i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Persists custom data to Redis.
    ///
    /// Appends the serialized `CustomData` (RPUSH) to the LIST at key
    /// `custom_data:{data_type}`, where `data_type` is the topic string
    /// representation of the data type.
    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let key = format!("{CUSTOM_DATA}{REDIS_DELIMITER}{}", data.data_type);
        log::debug!("Adding custom data '{}' to Redis", data.data_type);
        let payload = DatabaseQueries::serialize_payload(self.encoding, data)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send add_custom_data command: {e}"))
    }

    /// Loads all persisted custom data by data type from Redis.
    ///
    /// Reads the full LIST at key `custom_data:{data_type}` using
    /// LRANGE 0 -1, deserializes each entry, and returns them in insertion
    /// order.
    fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{CUSTOM_DATA}{REDIS_DELIMITER}{data_type}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(0i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        items
            .iter()
            .map(|raw| DatabaseQueries::deserialize_payload(self.encoding, raw))
            .collect()
    }

    /// Loads the most recent order snapshot from Redis.
    ///
    /// Reads the last element (LRANGE -1 -1) from the LIST at key
    /// `snapshots:orders:{client_order_id}` and deserializes it.
    /// Returns `Ok(None)` when no snapshots exist for this order.
    fn load_order_snapshot(
        &self,
        client_order_id: &ClientOrderId,
    ) -> anyhow::Result<Option<OrderSnapshot>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{SNAPSHOTS}{REDIS_DELIMITER}{ORDERS}{REDIS_DELIMITER}{client_order_id}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(-1i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        match items.first() {
            Some(raw) => {
                let snapshot: OrderSnapshot =
                    DatabaseQueries::deserialize_payload(self.encoding, raw)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Loads the most recent position snapshot from Redis.
    ///
    /// Reads the last element (LRANGE -1 -1) from the LIST at key
    /// `snapshots:positions:{position_id}` and deserializes it.
    /// Returns `Ok(None)` when no snapshots exist for this position.
    fn load_position_snapshot(
        &self,
        position_id: &PositionId,
    ) -> anyhow::Result<Option<PositionSnapshot>> {
        let key = format!(
            "{}{REDIS_DELIMITER}{SNAPSHOTS}{REDIS_DELIMITER}{POSITIONS}{REDIS_DELIMITER}{position_id}",
            self.database.trader_key,
        );

        let (tx, rx) = mpsc::sync_channel(1);
        let mut con = self.database.con.clone();

        get_runtime().spawn(async move {
            let result: Result<Vec<Vec<u8>>, _> = redis::cmd("LRANGE")
                .arg(&key)
                .arg(-1i64)
                .arg(-1i64)
                .query_async(&mut con)
                .await;
            let _ = tx.send(result);
        });

        let items: Vec<Vec<u8>> =
            blocking_recv(&rx).map_err(|e| anyhow::anyhow!("Channel closed: {e}"))??;

        match items.first() {
            Some(raw) => {
                let snapshot: PositionSnapshot =
                    DatabaseQueries::deserialize_payload(self.encoding, raw)?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Indexes a venue order ID against a client order ID in Redis.
    ///
    /// Stores in a HASH at key `index:order_ids` where the field is the
    /// client order ID and the value is the venue order ID. This allows
    /// efficient lookup of venue-assigned order IDs from internal ones.
    fn index_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        let key = INDEX_ORDER_IDS.to_string();
        log::debug!("Indexing venue order ID: {venue_order_id} for client order: {client_order_id}");
        let payload = vec![
            Bytes::from(client_order_id.to_string()),
            Bytes::from(venue_order_id.to_string()),
        ];
        let op = DatabaseCommand::new(DatabaseOperation::Insert, key, Some(payload));
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send index_venue_order_id command: {e}"))
    }

    /// Indexes an order-to-position mapping in Redis.
    ///
    /// Stores in a HASH at key `index:order_position` where the field is
    /// the client order ID and the value is the position ID. This allows
    /// efficient lookup of which position a given order belongs to.
    fn index_order_position(
        &self,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> anyhow::Result<()> {
        let key = INDEX_ORDER_POSITION.to_string();
        log::debug!("Indexing order position: {position_id} for client order: {client_order_id}");
        let payload = vec![
            Bytes::from(client_order_id.to_string()),
            Bytes::from(position_id.to_string()),
        ];
        let op = DatabaseCommand::new(DatabaseOperation::Insert, key, Some(payload));
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send index_order_position command: {e}"))
    }

    /// No-op: the Rust trait signature has no parameters so there is no
    /// actor state to persist.  In the Cython implementation, the `Actor`
    /// object is passed and its `save()` dict is serialized.  The Rust
    /// trait will need to be updated to accept a state payload before this
    /// method can do real work.
    fn update_actor(&self) -> anyhow::Result<()> {
        log::debug!("update_actor called (no-op: trait has no state parameter)");
        Ok(())
    }

    /// No-op: the Rust trait signature has no parameters so there is no
    /// strategy state to persist.  See `update_actor` for rationale.
    fn update_strategy(&self) -> anyhow::Result<()> {
        log::debug!("update_strategy called (no-op: trait has no state parameter)");
        Ok(())
    }

    /// Updates an account state in Redis by appending the latest event.
    ///
    /// Stores the last `AccountState` event under key `accounts:{account_id}`.
    /// Uses the Update operation which routes to RPUSH_EXISTS, meaning
    /// the append only succeeds if the key already exists in Redis.
    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let account_id = account.id();
        let key = format!("{ACCOUNTS}{REDIS_DELIMITER}{account_id}");
        log::debug!("Updating account: {account_id} in Redis");
        let last_event = account
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Account {account_id} has no events"))?;
        let payload = DatabaseQueries::serialize_payload(self.encoding, &last_event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Update,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send update_account command: {e}"))
    }

    /// Appends the latest order event and updates state-dependent indexes.
    /// Appends the latest order event and updates state-dependent indexes.
    ///
    /// Called on every order state change. Performs these operations:
    /// 1. Appends the new event to the order's event list (RPUSH_EXISTS)
    /// 2. Updates venue order ID index if assigned (HSET)
    /// 3. Manages inflight index via `order.is_inflight()` (SADD/SREM)
    /// 4. Manages open/closed indexes via `order.is_open()`/`order.is_closed()` (SADD+SREM)
    /// 5. Manages emulated index via `order.emulation_trigger()` (SADD/SREM)
    ///
    /// Receives the full `OrderAny`, enabling accurate state queries for index
    /// management. Uses `order.is_open()`, `order.is_closed()`, `order.is_inflight()`,
    /// and `order.emulation_trigger()` to determine index transitions, matching
    /// the behavior of the in-memory Cache and the Cython adapter. This correctly
    /// handles partial fills (which remain open, not closed).
    fn update_order(&self, order: &OrderAny) -> anyhow::Result<()> {
        let client_order_id = order.client_order_id();
        let client_order_id_str = client_order_id.to_string();
        let client_order_id_bytes = Bytes::from(client_order_id_str);
        let event = order.last_event();

        log::debug!("Updating order: {client_order_id} in Redis");

        // 1. Append event to order list (RPUSH_EXISTS)
        let key = format!("{ORDERS}{REDIS_DELIMITER}{client_order_id}");
        let payload = DatabaseQueries::serialize_payload(self.encoding, event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Update,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send update_order command: {e}"))?;

        // 2. Index venue order ID if assigned
        if let Some(venue_order_id) = order.venue_order_id() {
            self.index_venue_order_id(client_order_id, venue_order_id)?;
        }

        // 3. Inflight index (order.is_inflight() checks status + emulation trigger)
        if order.is_inflight() {
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_ORDERS_INFLIGHT.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send inflight index command: {e}"))?;
        } else {
            let op = DatabaseCommand::new(
                DatabaseOperation::Delete,
                INDEX_ORDERS_INFLIGHT.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send inflight index remove: {e}"))?;
        }

        // 4. Open/closed indexes (mutually exclusive, order.is_open() handles partial fills correctly)
        if order.is_open() {
            let op = DatabaseCommand::new(
                DatabaseOperation::Delete,
                INDEX_ORDERS_CLOSED.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send closed index remove: {e}"))?;
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_ORDERS_OPEN.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send open index command: {e}"))?;
        } else if order.is_closed() {
            let op = DatabaseCommand::new(
                DatabaseOperation::Delete,
                INDEX_ORDERS_OPEN.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send open index remove: {e}"))?;
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_ORDERS_CLOSED.to_string(),
                Some(vec![client_order_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| anyhow::anyhow!("Failed to send closed index command: {e}"))?;
        }

        // 5. Emulated index (uses full order state, matching Cython behavior)
        if let Some(trigger) = order.emulation_trigger() {
            if trigger != TriggerType::NoTrigger && !order.is_closed() {
                let op = DatabaseCommand::new(
                    DatabaseOperation::Insert,
                    INDEX_ORDERS_EMULATED.to_string(),
                    Some(vec![client_order_id_bytes]),
                );
                self.database
                    .tx
                    .send(op)
                    .map_err(|e| anyhow::anyhow!("Failed to send emulated index: {e}"))?;
            } else {
                let op = DatabaseCommand::new(
                    DatabaseOperation::Delete,
                    INDEX_ORDERS_EMULATED.to_string(),
                    Some(vec![client_order_id_bytes]),
                );
                self.database
                    .tx
                    .send(op)
                    .map_err(|e| anyhow::anyhow!("Failed to send emulated index remove: {e}"))?;
            }
        }

        Ok(())
    }

    /// Appends a fill event to a position and updates open/closed indexes.
    ///
    /// Called on every position state change (new fill). Performs:
    /// 1. RPUSH_EXISTS the position's last event (append fill to event list)
    /// 2. Manages open/closed indexes (mutually exclusive transition)
    ///
    /// Open and closed are exclusive states: when a position transitions to
    /// closed (flat), it is removed from `index:positions_open` and added to
    /// `index:positions_closed`, and vice versa.
    fn update_position(&self, position: &Position) -> anyhow::Result<()> {
        let position_id = position.id;
        let position_id_str = position_id.to_string();
        let position_id_bytes = Bytes::from(position_id_str);

        log::debug!("Updating position: {position_id} in Redis");

        // Append fill event to position list (RPUSH_EXISTS)
        let key = format!("{POSITIONS}{REDIS_DELIMITER}{position_id}");
        let event = position
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Position {position_id} has no events"))?;
        let payload = DatabaseQueries::serialize_payload(self.encoding, &event)?;
        let op = DatabaseCommand::new(
            DatabaseOperation::Update,
            key,
            Some(vec![Bytes::from(payload)]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send update_position command: {e}"))?;

        // Note: The Cython implementation (database.pyx) reuses serialized event bytes
        // for index SADD/SREM operations, which is incorrect — indexes should contain
        // position ID strings. This implementation correctly uses position_id_bytes.

        // Index: open/closed state (mutually exclusive)
        if position.is_open() {
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to send position open index command: {e}")
                })?;
            let op = DatabaseCommand::new(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to send position closed index remove command: {e}")
                })?;
        } else if position.is_closed() {
            let op = DatabaseCommand::new(
                DatabaseOperation::Insert,
                INDEX_POSITIONS_CLOSED.to_string(),
                Some(vec![position_id_bytes.clone()]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to send position closed index command: {e}")
                })?;
            let op = DatabaseCommand::new(
                DatabaseOperation::Delete,
                INDEX_POSITIONS_OPEN.to_string(),
                Some(vec![position_id_bytes]),
            );
            self.database
                .tx
                .send(op)
                .map_err(|e| {
                    anyhow::anyhow!("Failed to send position open index remove command: {e}")
                })?;
        }

        Ok(())
    }

    /// Creates a point-in-time snapshot of an order's full state.
    ///
    /// Converts the `OrderAny` to an `OrderSnapshot`, serializes it, and
    /// appends (RPUSH) to the LIST at `snapshots:orders:{client_order_id}`.
    /// This mirrors the Cython `snapshot_order_state` which serializes
    /// `order.to_dict()`.
    fn snapshot_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        let snapshot = OrderSnapshot::from(order.clone());
        self.add_order_snapshot(&snapshot)
    }

    /// Creates a point-in-time snapshot of a position's full state.
    ///
    /// Converts the `Position` to a `PositionSnapshot` (with no unrealized
    /// PnL — the trait does not provide it), serializes it, and appends
    /// (RPUSH) to the LIST at `snapshots:positions:{position_id}`.
    fn snapshot_position_state(&self, position: &Position) -> anyhow::Result<()> {
        let snapshot = PositionSnapshot::from(position, None);
        self.add_position_snapshot(&snapshot)
    }

    /// Writes a heartbeat timestamp to Redis.
    ///
    /// Stores the timestamp as a STRING at key `health:heartbeat`.
    /// Each heartbeat overwrites the previous value (SET, not RPUSH).
    ///
    /// Note: Stores timestamp as `UnixNanos.to_string()` (integer nanoseconds string).
    /// The Cython implementation uses ISO8601 format via `format_iso8601()`.
    fn heartbeat(&self, timestamp: UnixNanos) -> anyhow::Result<()> {
        let key = format!("{HEALTH}{REDIS_DELIMITER}heartbeat");
        let ts_str = timestamp.to_string();
        log::debug!("Heartbeat: {ts_str}");
        let op = DatabaseCommand::new(
            DatabaseOperation::Insert,
            key,
            Some(vec![Bytes::from(ts_str.into_bytes())]),
        );
        self.database
            .tx
            .send(op)
            .map_err(|e| anyhow::anyhow!("Failed to send heartbeat command: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_get_trader_key_with_prefix_and_instance_id() {
        let trader_id = TraderId::from("tester-123");
        let instance_id = UUID4::new();
        let config = CacheConfig {
            use_instance_id: true,
            ..Default::default()
        };

        let key = get_trader_key(trader_id, instance_id, &config);
        assert!(key.starts_with("trader-tester-123:"));
        assert!(key.ends_with(&instance_id.to_string()));
    }

    #[rstest]
    fn test_get_collection_key_valid() {
        let key = "collection:123";
        assert_eq!(get_collection_key(key).unwrap(), "collection");
    }

    #[rstest]
    fn test_get_collection_key_invalid() {
        let key = "no_delimiter";
        assert!(get_collection_key(key).is_err());
    }
}
