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

use std::{
    cell::{Ref, RefCell},
    rc::Rc,
    time::Duration,
};

use nautilus_common::{
    cache::{Cache, CacheConfig, database::CacheDatabaseAdapter},
    clock::{Clock, TestClock},
    component::Component,
    enums::Environment,
    logging::{
        headers, init_logging,
        logger::{LogGuard, LoggerConfig},
        writer::FileWriterConfig,
    },
    msgbus::{MessageBus, set_message_bus},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_data::engine::DataEngine;
use nautilus_execution::{engine::ExecutionEngine, order_emulator::adapter::OrderEmulatorAdapter};
use nautilus_model::identifiers::{ClientId, TraderId};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_risk::engine::RiskEngine;
use ustr::Ustr;

use crate::{builder::NautilusKernelBuilder, config::NautilusKernelConfig, trader::Trader};

/// Core Nautilus system kernel.
///
/// Orchestrates data and execution engines, cache, clock, and messaging across environments.
#[derive(Debug)]
pub struct NautilusKernel {
    /// The kernel name (for logging and identification).
    pub name: String,
    /// The unique instance identifier for this kernel.
    pub instance_id: UUID4,
    /// The machine identifier (hostname or similar).
    pub machine_id: String,
    /// The kernel configuration.
    pub config: Box<dyn NautilusKernelConfig>,
    /// The shared in-memory cache.
    pub cache: Rc<RefCell<Cache>>,
    /// The clock driving the kernel.
    pub clock: Rc<RefCell<dyn Clock>>,
    /// The portfolio manager.
    pub portfolio: Rc<RefCell<Portfolio>>,
    /// Guard for the logging subsystem (keeps logger thread alive).
    pub log_guard: LogGuard,
    /// The data engine instance.
    pub data_engine: Rc<RefCell<DataEngine>>,
    /// The risk engine instance.
    pub risk_engine: Rc<RefCell<RiskEngine>>,
    /// The execution engine instance.
    pub exec_engine: Rc<RefCell<ExecutionEngine>>,
    /// The order emulator for handling emulated orders.
    pub order_emulator: OrderEmulatorAdapter,
    /// The trader component.
    pub trader: Trader,
    /// The UNIX timestamp (nanoseconds) when the kernel was created.
    pub ts_created: UnixNanos,
    /// The UNIX timestamp (nanoseconds) when the kernel was last started.
    pub ts_started: Option<UnixNanos>,
    /// The UNIX timestamp (nanoseconds) when the kernel was last shutdown.
    pub ts_shutdown: Option<UnixNanos>,
}

impl NautilusKernel {
    /// Create a new [`NautilusKernelBuilder`] for fluent configuration.
    #[must_use]
    pub const fn builder(
        name: String,
        trader_id: TraderId,
        environment: Environment,
    ) -> NautilusKernelBuilder {
        NautilusKernelBuilder::new(name, trader_id, environment)
    }

    /// Create a new [`NautilusKernel`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel fails to initialize.
    pub fn new<T: NautilusKernelConfig + 'static>(name: String, config: T) -> anyhow::Result<Self> {
        let instance_id = config.instance_id().unwrap_or_default();
        let machine_id = Self::determine_machine_id()?;

        let logger_config = config.logging();
        let log_guard = Self::initialize_logging(config.trader_id(), instance_id, logger_config)?;
        headers::log_header(
            config.trader_id(),
            &machine_id,
            instance_id,
            Ustr::from(stringify!(LiveNode)),
        );

        log::info!("Building system kernel");

        let clock = Self::initialize_clock(&config.environment());
        let cache = Self::initialize_cache(config.cache());

        let msgbus = Rc::new(RefCell::new(MessageBus::new(
            config.trader_id(),
            instance_id,
            Some(name.clone()),
            None,
        )));
        set_message_bus(msgbus);

        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            cache.clone(),
            clock.clone(),
            config.portfolio(),
        )));

        let risk_engine = RiskEngine::new(
            config.risk_engine().unwrap_or_default(),
            portfolio.borrow().clone_shallow(),
            clock.clone(),
            cache.clone(),
        );
        let risk_engine = Rc::new(RefCell::new(risk_engine));

        let exec_engine = ExecutionEngine::new(clock.clone(), cache.clone(), config.exec_engine());
        let exec_engine = Rc::new(RefCell::new(exec_engine));

        let order_emulator =
            OrderEmulatorAdapter::new(config.trader_id(), clock.clone(), cache.clone());

        let data_engine = DataEngine::new(clock.clone(), cache.clone(), config.data_engine());
        let data_engine = Rc::new(RefCell::new(data_engine));

        DataEngine::register_msgbus_handlers(data_engine.clone());
        RiskEngine::register_msgbus_handlers(risk_engine.clone());
        ExecutionEngine::register_msgbus_handlers(exec_engine.clone());

        // Setup streaming to feather files (if configured).
        // Uses a dedicated writer thread to avoid blocking the trading event loop.
        #[cfg(feature = "streaming")]
        if let Some(streaming_config) = config.streaming()
            && let Err(e) = Self::setup_streaming(
                &streaming_config,
                config.environment(),
                instance_id,
            )
        {
            log::error!("Failed to setup streaming: {e}");
        }

        let trader = Trader::new(
            config.trader_id(),
            instance_id,
            config.environment(),
            clock.clone(),
            cache.clone(),
            portfolio.clone(),
        );

        let ts_created = clock.borrow().timestamp_ns();

        Ok(Self {
            name,
            instance_id,
            machine_id,
            config: Box::new(config),
            cache,
            clock,
            portfolio,
            log_guard,
            data_engine,
            risk_engine,
            exec_engine,
            order_emulator,
            trader,
            ts_created,
            ts_started: None,
            ts_shutdown: None,
        })
    }

    fn determine_machine_id() -> anyhow::Result<String> {
        sysinfo::System::host_name().ok_or_else(|| anyhow::anyhow!("Failed to determine hostname"))
    }

    fn initialize_logging(
        trader_id: TraderId,
        instance_id: UUID4,
        config: LoggerConfig,
    ) -> anyhow::Result<LogGuard> {
        #[cfg(feature = "tracing-bridge")]
        let use_tracing = config.use_tracing;

        let log_guard = match init_logging(
            trader_id,
            instance_id,
            config,
            FileWriterConfig::default(), // TODO: Properly incorporate file writer config
        ) {
            Ok(guard) => guard,
            Err(e) => {
                // Only recover from SetLoggerError (logger already registered).
                // This is common in tests where multiple kernels are created and
                // the log crate's global logger persists after LogGuard teardown.
                // Any other error (e.g. thread spawn failure) is propagated.
                if e.downcast_ref::<log::SetLoggerError>().is_some() {
                    if let Some(guard) = LogGuard::new() {
                        guard
                    } else {
                        return Err(e.context(
                            "A non-Nautilus logger is already registered; \
                             cannot initialize Nautilus logging",
                        ));
                    }
                } else {
                    return Err(e);
                }
            }
        };

        // Initialize tracing subscriber if enabled (idempotent)
        #[cfg(feature = "tracing-bridge")]
        if use_tracing && !nautilus_common::logging::bridge::tracing_is_initialized() {
            nautilus_common::logging::bridge::init_tracing()?;
        }

        Ok(log_guard)
    }

    fn initialize_clock(environment: &Environment) -> Rc<RefCell<dyn Clock>> {
        match environment {
            Environment::Backtest => {
                let test_clock = TestClock::new();
                Rc::new(RefCell::new(test_clock))
            }
            #[cfg(feature = "live")]
            Environment::Live | Environment::Sandbox => {
                let live_clock = nautilus_common::live::clock::LiveClock::default(); // nautilus-import-ok
                Rc::new(RefCell::new(live_clock))
            }
            #[cfg(not(feature = "live"))]
            Environment::Live | Environment::Sandbox => {
                panic!(
                    "Live/Sandbox environment requires the 'live' feature to be enabled. \
                     Build with `--features live` or add `features = [\"live\"]` to your dependency."
                );
            }
        }
    }

    fn initialize_cache(cache_config: Option<CacheConfig>) -> Rc<RefCell<Cache>> {
        let cache_config = cache_config.unwrap_or_default();

        // TODO: Placeholder: persistent database adapter can be initialized here (e.g., Redis)
        let cache_database: Option<Box<dyn CacheDatabaseAdapter>> = None;
        let cache = Cache::new(Some(cache_config), cache_database);

        Rc::new(RefCell::new(cache))
    }

    /// Wires the Feather/Parquet streaming writer onto the message bus.
    ///
    /// Uses a dedicated writer thread to avoid blocking the trading event loop.
    /// The message bus handler sends events through an mpsc channel (~100ns per
    /// event). A separate OS thread with its own tokio runtime receives events,
    /// encodes them to Arrow RecordBatches, buffers in memory, and flushes to
    /// the object store (local filesystem or S3) on a configurable interval.
    ///
    /// This improves on the Python kernel's `_setup_streaming()` which blocks
    /// the event loop during serialization and I/O. The Rust implementation
    /// achieves zero event-loop blocking, making it suitable for HFT workloads.
    #[cfg(feature = "streaming")]
    fn setup_streaming(
        config: &crate::config::StreamingConfig,
        environment: Environment,
        instance_id: UUID4,
    ) -> anyhow::Result<()> {
        use std::any::Any;
        use std::sync::mpsc::{self, RecvTimeoutError};

        use nautilus_common::live::clock::LiveClock;
        use nautilus_common::msgbus::{subscribe_any, MStr, ShareableMessageHandler};
        use nautilus_model::{
            data::{
                Bar, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta,
                OrderBookDeltas, OrderBookDepth10, QuoteTick, TradeTick, close::InstrumentClose,
            },
            events::{
                AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied,
                OrderEmulated, OrderExpired, OrderFilled, OrderInitialized, OrderModifyRejected,
                OrderPendingCancel, OrderPendingUpdate, OrderRejected, OrderReleased,
                OrderSubmitted, OrderTriggered, OrderUpdated, PositionAdjusted, PositionChanged,
                PositionClosed, PositionOpened,
            },
            instruments::InstrumentAny,
        };
        use nautilus_persistence::backend::feather::{
            FeatherWriter, RotationConfig as FeatherRotation,
        };
        use nautilus_persistence::parquet::create_object_store_from_path;

        let catalog_path = format!(
            "{}/{}/{}",
            config.catalog_path,
            environment,
            instance_id,
        );

        // Ensure the catalog directory exists for LocalFileSystem (requires canonical path).
        if config.fs_protocol == "file" {
            std::fs::create_dir_all(&catalog_path)
                .map_err(|e| anyhow::anyhow!(
                    "failed to create streaming catalog directory '{catalog_path}': {e}"
                ))?;
        }

        let flush_interval_ms = config.flush_interval_ms;

        // Pre-create object store and rotation config (validated on main thread).
        let (store, base_path, _scheme) = create_object_store_from_path(&catalog_path, None)?;

        let rotation = match &config.rotation_config {
            crate::config::RotationConfig::Size { max_size } => {
                FeatherRotation::Size { max_size: *max_size }
            }
            crate::config::RotationConfig::Interval { interval_ns } => {
                FeatherRotation::Interval { interval_ns: *interval_ns }
            }
            crate::config::RotationConfig::ScheduledDates { .. } => {
                log::warn!(
                    "ScheduledDates rotation not supported in Rust streaming, using NoRotation"
                );
                FeatherRotation::NoRotation
            }
            crate::config::RotationConfig::NoRotation => FeatherRotation::NoRotation,
        };

        // Channel for sending events from the event loop to the writer thread.
        // Unbounded to avoid backpressure on the trading event loop.
        let (tx, rx) = mpsc::channel::<Box<dyn Any + Send>>();

        // Spawn dedicated writer thread with its own tokio runtime.
        // FeatherWriter uses Rc<RefCell<>> internally (not Send), so it must be
        // created and used entirely within this thread.
        std::thread::Builder::new()
            .name("streaming-writer".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build streaming tokio runtime");

                rt.block_on(async move {
                    let clock: Rc<RefCell<dyn Clock>> =
                        Rc::new(RefCell::new(LiveClock::default()));

                    let mut writer = FeatherWriter::new(
                        base_path,
                        store,
                        clock,
                        rotation,
                        None,
                        None,
                        Some(flush_interval_ms),
                    );

                    let mut last_flush = std::time::Instant::now();
                    let flush_interval = std::time::Duration::from_millis(flush_interval_ms);

                    loop {
                        match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                            Ok(data) => {
                                Self::dispatch_write(&mut writer, data).await;
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => {
                                log::info!("Streaming channel disconnected, flushing and exiting");
                                break;
                            }
                        }

                        // Periodic flush based on configured interval
                        if last_flush.elapsed() >= flush_interval {
                            if let Err(e) = writer.flush().await {
                                log::warn!("Streaming flush error: {e}");
                            }
                            last_flush = std::time::Instant::now();
                        }
                    }

                    if let Err(e) = writer.close().await {
                        log::warn!("Streaming close error: {e}");
                    }
                    log::info!("Streaming writer thread stopped");
                });
            })
            .map_err(|e| anyhow::anyhow!("failed to spawn streaming writer thread: {e}"))?;

        // Subscribe a lightweight handler to the message bus that forwards events
        // through the channel. Each send is ~100ns — zero I/O on the event loop.
        let handler = ShareableMessageHandler::from_any(move |message: &dyn Any| {
            // Market data (Copy types)
            if let Some(q) = message.downcast_ref::<QuoteTick>() {
                let _ = tx.send(Box::new(*q));
            } else if let Some(t) = message.downcast_ref::<TradeTick>() {
                let _ = tx.send(Box::new(*t));
            } else if let Some(b) = message.downcast_ref::<Bar>() {
                let _ = tx.send(Box::new(*b));
            } else if let Some(d) = message.downcast_ref::<OrderBookDelta>() {
                let _ = tx.send(Box::new(*d));
            } else if let Some(d) = message.downcast_ref::<OrderBookDepth10>() {
                let _ = tx.send(Box::new(*d));
            } else if let Some(d) = message.downcast_ref::<OrderBookDeltas>() {
                for delta in &d.deltas {
                    let _ = tx.send(Box::new(*delta));
                }
            } else if let Some(p) = message.downcast_ref::<IndexPriceUpdate>() {
                let _ = tx.send(Box::new(*p));
            } else if let Some(p) = message.downcast_ref::<MarkPriceUpdate>() {
                let _ = tx.send(Box::new(*p));
            } else if let Some(c) = message.downcast_ref::<InstrumentClose>() {
                let _ = tx.send(Box::new(*c));
            } else if let Some(i) = message.downcast_ref::<InstrumentAny>() {
                let _ = tx.send(Box::new(i.clone()));
            // Order events (Copy types)
            } else if let Some(e) = message.downcast_ref::<OrderFilled>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderAccepted>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderCanceled>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderRejected>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderSubmitted>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderDenied>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderExpired>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderTriggered>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderUpdated>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderPendingCancel>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderPendingUpdate>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderCancelRejected>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderModifyRejected>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderEmulated>() {
                let _ = tx.send(Box::new(*e));
            } else if let Some(e) = message.downcast_ref::<OrderReleased>() {
                let _ = tx.send(Box::new(*e));
            // Order events (Clone only — OrderInitialized has IndexMap)
            } else if let Some(e) = message.downcast_ref::<OrderInitialized>() {
                let _ = tx.send(Box::new(e.clone()));
            // Position events (Clone only)
            } else if let Some(e) = message.downcast_ref::<PositionOpened>() {
                let _ = tx.send(Box::new(e.clone()));
            } else if let Some(e) = message.downcast_ref::<PositionChanged>() {
                let _ = tx.send(Box::new(e.clone()));
            } else if let Some(e) = message.downcast_ref::<PositionClosed>() {
                let _ = tx.send(Box::new(e.clone()));
            } else if let Some(e) = message.downcast_ref::<PositionAdjusted>() {
                let _ = tx.send(Box::new(*e));
            // Account events (Clone only)
            } else if let Some(e) = message.downcast_ref::<AccountState>() {
                let _ = tx.send(Box::new(e.clone()));
            } else if let Some(e) = message.downcast_ref::<FundingRateUpdate>() {
                let _ = tx.send(Box::new(*e));
            }
        });

        subscribe_any(MStr::pattern("*"), handler, None);

        log::info!(
            "Streaming enabled: {catalog_path} (flush={flush_interval_ms}ms, dedicated writer thread)",
        );

        Ok(())
    }

    /// Dispatches a boxed event to the appropriate FeatherWriter write method.
    #[cfg(feature = "streaming")]
    async fn dispatch_write(writer: &mut nautilus_persistence::backend::feather::FeatherWriter, data: Box<dyn std::any::Any + Send>) {
        use nautilus_model::{
            data::{
                Bar, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, OrderBookDelta,
                OrderBookDepth10, QuoteTick, TradeTick, close::InstrumentClose,
            },
            events::{
                AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied,
                OrderEmulated, OrderExpired, OrderFilled, OrderInitialized, OrderModifyRejected,
                OrderPendingCancel, OrderPendingUpdate, OrderRejected, OrderReleased,
                OrderSubmitted, OrderTriggered, OrderUpdated, PositionAdjusted, PositionChanged,
                PositionClosed, PositionOpened,
            },
            instruments::InstrumentAny,
        };

        macro_rules! try_write {
            ($data:expr, $writer:expr, $($ty:ty),+ $(,)?) => {
                $(
                    if let Some(event) = $data.downcast_ref::<$ty>() {
                        if let Err(e) = $writer.write(event.clone()).await {
                            log::warn!("Failed to write {}: {e}", stringify!($ty));
                        }
                        return;
                    }
                )+
            };
        }

        // Market data
        try_write!(data, writer,
            QuoteTick, TradeTick, Bar, OrderBookDelta, OrderBookDepth10,
            IndexPriceUpdate, MarkPriceUpdate, InstrumentClose,
        );

        // Instruments (special write path)
        if let Some(instrument) = data.downcast_ref::<InstrumentAny>() {
            if let Err(e) = writer.write_instrument(instrument.clone()).await {
                log::warn!("Failed to write InstrumentAny: {e}");
            }
            return;
        }

        // Order events
        try_write!(data, writer,
            OrderFilled, OrderAccepted, OrderCanceled, OrderRejected,
            OrderInitialized, OrderSubmitted, OrderDenied, OrderExpired,
            OrderTriggered, OrderUpdated, OrderPendingCancel, OrderPendingUpdate,
            OrderCancelRejected, OrderModifyRejected, OrderEmulated, OrderReleased,
        );

        // Position events
        try_write!(data, writer,
            PositionOpened, PositionChanged, PositionClosed, PositionAdjusted,
        );

        // Account + funding
        try_write!(data, writer, AccountState, FundingRateUpdate);
    }

    fn cancel_timers(&self) {
        self.clock.borrow_mut().cancel_timers();
    }

    #[must_use]
    pub fn generate_timestamp_ns(&self) -> UnixNanos {
        self.clock.borrow().timestamp_ns()
    }

    /// Returns the kernel's environment context (Backtest, Sandbox, Live).
    #[must_use]
    pub fn environment(&self) -> Environment {
        self.config.environment()
    }

    /// Returns the kernel's name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns the kernel's trader ID.
    #[must_use]
    pub fn trader_id(&self) -> TraderId {
        self.config.trader_id()
    }

    /// Returns the kernel's machine ID.
    #[must_use]
    pub fn machine_id(&self) -> &str {
        &self.machine_id
    }

    /// Returns the kernel's instance ID.
    #[must_use]
    pub const fn instance_id(&self) -> UUID4 {
        self.instance_id
    }

    /// Returns the delay after stopping the node to await residual events before final shutdown.
    #[must_use]
    pub fn delay_post_stop(&self) -> Duration {
        self.config.delay_post_stop()
    }

    /// Returns the UNIX timestamp (ns) when the kernel was created.
    #[must_use]
    pub const fn ts_created(&self) -> UnixNanos {
        self.ts_created
    }

    /// Returns the UNIX timestamp (ns) when the kernel was last started.
    #[must_use]
    pub const fn ts_started(&self) -> Option<UnixNanos> {
        self.ts_started
    }

    /// Returns the UNIX timestamp (ns) when the kernel was last shutdown.
    #[must_use]
    pub const fn ts_shutdown(&self) -> Option<UnixNanos> {
        self.ts_shutdown
    }

    /// Returns whether the kernel has been configured to load state.
    #[must_use]
    pub fn load_state(&self) -> bool {
        self.config.load_state()
    }

    /// Returns whether the kernel has been configured to save state.
    #[must_use]
    pub fn save_state(&self) -> bool {
        self.config.save_state()
    }

    /// Returns the kernel's clock.
    #[must_use]
    pub fn clock(&self) -> Rc<RefCell<dyn Clock>> {
        self.clock.clone()
    }

    /// Returns the kernel's cache.
    #[must_use]
    pub fn cache(&self) -> Rc<RefCell<Cache>> {
        self.cache.clone()
    }

    /// Returns the kernel's portfolio.
    #[must_use]
    pub fn portfolio(&self) -> Ref<'_, Portfolio> {
        self.portfolio.borrow()
    }

    /// Returns the kernel's data engine.
    #[must_use]
    pub fn data_engine(&self) -> Ref<'_, DataEngine> {
        self.data_engine.borrow()
    }

    /// Returns the kernel's risk engine.
    #[must_use]
    pub const fn risk_engine(&self) -> &Rc<RefCell<RiskEngine>> {
        &self.risk_engine
    }

    /// Returns the kernel's execution engine.
    #[must_use]
    pub const fn exec_engine(&self) -> &Rc<RefCell<ExecutionEngine>> {
        &self.exec_engine
    }

    /// Returns the kernel's trader.
    #[must_use]
    pub const fn trader(&self) -> &Trader {
        &self.trader
    }

    /// Starts the Nautilus system kernel synchronously (for backtest use).
    pub fn start(&mut self) {
        log::info!("Starting");
        self.start_engines();

        log::info!("Initializing trader");
        if let Err(e) = self.trader.initialize() {
            log::error!("Error initializing trader: {e:?}");
            return;
        }

        log::info!("Starting clients...");

        if let Err(e) = self.start_clients() {
            log::error!("Error starting clients: {e:?}");
        }
        log::info!("Clients started");

        self.ts_started = Some(self.clock.borrow().timestamp_ns());
        log::info!("Started");
    }

    /// Starts the Nautilus system kernel asynchronously.
    pub async fn start_async(&mut self) {
        self.start();
    }

    /// Starts the trader (strategies and actors).
    ///
    /// This should be called after clients are connected and instruments are cached.
    pub fn start_trader(&mut self) {
        log::info!("Starting trader...");
        if let Err(e) = self.trader.start() {
            log::error!("Error starting trader: {e:?}");
        }
        log::info!("Trader started");
    }

    /// Stops the trader and its registered components.
    ///
    /// This method initiates a graceful shutdown of trading components (strategies, actors)
    /// which may trigger residual events such as order cancellations. The caller should
    /// continue processing events after calling this method to handle these residual events.
    pub fn stop_trader(&mut self) {
        if !self.trader.is_running() {
            return;
        }

        log::info!("Stopping trader...");

        if let Err(e) = self.trader.stop() {
            log::error!("Error stopping trader: {e}");
        }
    }

    /// Finalizes the kernel shutdown after the grace period.
    ///
    /// This method should be called after the residual events grace period has elapsed
    /// and all remaining events have been processed. It disconnects clients and stops engines.
    pub async fn finalize_stop(&mut self) {
        // Stop all adapter clients
        if let Err(e) = self.stop_all_clients() {
            log::error!("Error stopping clients: {e:?}");
        }

        self.stop_engines();
        self.cancel_timers();

        self.ts_shutdown = Some(self.clock.borrow().timestamp_ns());
        log::info!("Stopped");
    }

    /// Resets the Nautilus system kernel to its initial state.
    pub fn reset(&mut self) {
        log::info!("Resetting");

        if let Err(e) = self.trader.reset() {
            log::error!("Error resetting trader: {e:?}");
        }

        self.data_engine.borrow_mut().reset();
        self.exec_engine.borrow_mut().reset();
        self.risk_engine.borrow_mut().reset();

        self.ts_started = None;
        self.ts_shutdown = None;

        log::info!("Reset");
    }

    /// Disposes of the Nautilus system kernel, releasing resources.
    pub fn dispose(&mut self) {
        log::info!("Disposing");

        if let Err(e) = self.trader.dispose() {
            log::error!("Error disposing trader: {e:?}");
        }

        self.stop_engines();

        self.data_engine.borrow_mut().dispose();
        self.exec_engine.borrow_mut().dispose();
        self.risk_engine.borrow_mut().dispose();

        log::info!("Disposed");
    }

    /// Starts all engine components.
    fn start_engines(&self) {
        self.data_engine.borrow_mut().start();
        self.exec_engine.borrow_mut().start();
        self.risk_engine.borrow_mut().start();
    }

    /// Stops all engine components.
    fn stop_engines(&self) {
        self.data_engine.borrow_mut().stop();
        self.exec_engine.borrow_mut().stop();
        self.risk_engine.borrow_mut().stop();
    }

    /// Starts all engine clients.
    ///
    /// Note: Async connection (connect/disconnect) is handled by LiveNode for live clients.
    /// This method only handles synchronous start operations on execution clients.
    fn start_clients(&mut self) -> Result<(), Vec<anyhow::Error>> {
        let mut errors = Vec::new();

        {
            let mut exec_engine = self.exec_engine.borrow_mut();
            let exec_adapters = exec_engine.get_clients_mut();

            for adapter in exec_adapters {
                if let Err(e) = adapter.start() {
                    log::error!("Error starting execution client {}: {e}", adapter.client_id);
                    errors.push(e);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Stops all engine clients.
    ///
    /// Note: Async disconnection is handled by LiveNode for live clients.
    /// This method only handles synchronous stop operations on execution clients.
    fn stop_all_clients(&mut self) -> Result<(), Vec<anyhow::Error>> {
        let mut errors = Vec::new();

        {
            let mut exec_engine = self.exec_engine.borrow_mut();
            let exec_adapters = exec_engine.get_clients_mut();

            for adapter in exec_adapters {
                if let Err(e) = adapter.stop() {
                    log::error!("Error stopping execution client {}: {e}", adapter.client_id);
                    errors.push(e);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Connects all engine clients.
    ///
    /// Connection failures are logged but do not prevent the node from running.
    #[allow(clippy::await_holding_refcell_ref)] // Single-threaded runtime, intentional design
    pub async fn connect_clients(&mut self) {
        log::info!("Connecting clients...");
        self.data_engine.borrow_mut().connect().await;
        self.exec_engine.borrow_mut().connect().await;
    }

    /// Disconnects all engine clients.
    ///
    /// # Errors
    ///
    /// Returns an error if any client fails to disconnect.
    #[allow(clippy::await_holding_refcell_ref)] // Single-threaded runtime, intentional design
    pub async fn disconnect_clients(&mut self) -> anyhow::Result<()> {
        log::info!("Disconnecting clients...");
        self.data_engine.borrow_mut().disconnect().await?;
        self.exec_engine.borrow_mut().disconnect().await?;
        Ok(())
    }

    /// Returns `true` if all engine clients are connected.
    #[must_use]
    pub fn check_engines_connected(&self) -> bool {
        self.data_engine.borrow().check_connected() && self.exec_engine.borrow().check_connected()
    }

    /// Returns `true` if all engine clients are disconnected.
    #[must_use]
    pub fn check_engines_disconnected(&self) -> bool {
        self.data_engine.borrow().check_disconnected()
            && self.exec_engine.borrow().check_disconnected()
    }

    /// Returns connection status for all data clients.
    #[must_use]
    pub fn data_client_connection_status(&self) -> Vec<(ClientId, bool)> {
        self.data_engine.borrow().client_connection_status()
    }

    /// Returns connection status for all execution clients.
    #[must_use]
    pub fn exec_client_connection_status(&self) -> Vec<(ClientId, bool)> {
        self.exec_engine.borrow().client_connection_status()
    }
}
