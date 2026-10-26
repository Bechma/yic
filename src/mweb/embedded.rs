use super::{Failure, Result, USER_AGENT};
use deno_core::{JsRuntime, JsRuntimeForSnapshot, RuntimeOptions, v8};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tracing::{Dispatch, Instrument, Span, instrument::WithSubscriber};
use uuid::Uuid;

const MAX_BYTES: usize = 32 * 1024 * 1024;
const HEAP_LIMIT: usize = 256 * 1024 * 1024;
const SCRIPT_TIMEOUT: Duration = Duration::from_secs(15);

struct Pool {
    idle: Mutex<Vec<Worker>>,
    slots: Arc<Semaphore>,
}

// Idle workers have already disposed their isolate, so joining them on pool
// destruction only waits for the command receiver to close, never for JS.
impl Drop for Pool {
    fn drop(&mut self) {
        if let Ok(idle) = self.idle.get_mut() {
            for mut worker in idle.drain(..) {
                worker.stop();
                if let Some(join) = worker.join.take() {
                    let _ = join.join();
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct EmbeddedPool(Arc<Pool>);

impl Default for EmbeddedPool {
    fn default() -> Self {
        Self::new(2)
    }
}

impl EmbeddedPool {
    fn new(capacity: usize) -> Self {
        Self(Arc::new(Pool {
            idle: Mutex::new(Vec::new()),
            slots: Arc::new(Semaphore::new(capacity)),
        }))
    }

    pub(super) async fn acquire(&self) -> Result<Session> {
        let permit = Arc::new(
            self.0
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Failure::Busy)?,
        );
        let idle = self.0.idle.lock().map_err(|_| Failure::Runtime)?.pop();
        let worker = match idle {
            Some(worker) if !worker.control.finished.load(Ordering::Acquire) => {
                tracing::debug!(worker_id = %worker.id, "Reusing embedded V8 worker");
                worker
            }
            _ => Worker::spawn().await?,
        };
        let mut session = Session {
            pool: self.clone(),
            worker: Some(worker),
            healthy: true,
            _permit: permit.clone(),
        };
        session
            .call(Operation::Begin(permit), SCRIPT_TIMEOUT)
            .await?;
        Ok(session)
    }
}

#[derive(Default)]
struct Control {
    isolate: Mutex<Option<v8::IsolateHandle>>,
    cancelled: AtomicBool,
    heap_exceeded: AtomicBool,
    finished: AtomicBool,
}

impl Control {
    fn terminate(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Ok(handle) = self.isolate.lock()
            && let Some(handle) = handle.as_ref()
        {
            handle.terminate_execution();
        }
    }
    fn attach(&self, handle: v8::IsolateHandle) {
        if let Ok(mut isolate) = self.isolate.lock() {
            if self.cancelled.load(Ordering::Acquire) {
                handle.terminate_execution();
            }
            *isolate = Some(handle);
        }
    }
    fn detach(&self) {
        if let Ok(mut isolate) = self.isolate.lock() {
            *isolate = None;
        }
    }
}

enum Operation {
    Begin(Arc<OwnedSemaphorePermit>),
    Reset,
    Eval(String),
    Dispose,
}
struct Command {
    op: Operation,
    reply: oneshot::Sender<Result<String>>,
    span: Span,
    dispatch: Dispatch,
}

struct Worker {
    id: Uuid,
    sender: Option<mpsc::Sender<Command>>,
    control: Arc<Control>,
    join: Option<thread::JoinHandle<()>>,
}
impl Worker {
    async fn spawn() -> Result<Self> {
        let id = Uuid::new_v4();
        let control = Arc::new(Control::default());
        let (sender, commands) = mpsc::channel();
        let (ready, receive) = oneshot::channel();
        let mut worker = Self {
            id,
            sender: Some(sender),
            control: control.clone(),
            join: None,
        };
        host()?
            .send(Spawn {
                id,
                control,
                commands,
                ready,
                span: Span::current(),
                dispatch: tracing::dispatcher::get_default(Clone::clone),
            })
            .map_err(|_| Failure::Runtime)?;
        worker.join = Some(
            tokio::time::timeout(SCRIPT_TIMEOUT, receive)
                .await
                .map_err(|_| Failure::Timeout)?
                .map_err(|_| Failure::Runtime)??,
        );
        tracing::info!(worker_id = %id, "Embedded V8 worker ready");
        Ok(worker)
    }
    fn stop(&mut self) {
        self.control.terminate();
        self.sender.take();
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop();
    }
}

// A single common parent initializes V8 and builds one immutable startup
// snapshot. Runtime-owning workers are spawned from that same parent thread.
struct Spawn {
    id: Uuid,
    control: Arc<Control>,
    commands: mpsc::Receiver<Command>,
    ready: oneshot::Sender<Result<thread::JoinHandle<()>>>,
    span: Span,
    dispatch: Dispatch,
}
fn host() -> Result<&'static mpsc::Sender<Spawn>> {
    static HOST: OnceLock<Option<mpsc::Sender<Spawn>>> = OnceLock::new();
    HOST.get_or_init(|| {
        let (sender, requests) = mpsc::channel::<Spawn>();
        thread::Builder::new()
            .name("yic-v8-host".into())
            .spawn(move || {
                let Ok(executor) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                let _entered = executor.enter();
                JsRuntime::init_platform(None);
                let mut snapshot = None;
                for request in requests {
                    if request.ready.is_closed() {
                        continue;
                    }
                    if snapshot.is_none() {
                        let built = tracing::dispatcher::with_default(&request.dispatch, || {
                            request.span.in_scope(|| {
                                catch_unwind(AssertUnwindSafe(build_snapshot))
                                    .unwrap_or(Err(Failure::Runtime))
                            })
                        });
                        match built {
                            Ok(bytes) => snapshot = Some(Box::leak(bytes) as &'static [u8]),
                            Err(error) => {
                                let _ = request.ready.send(Err(error));
                                continue;
                            }
                        }
                    }
                    if request.ready.is_closed() {
                        continue;
                    }
                    let Spawn {
                        id,
                        control,
                        commands,
                        ready,
                        ..
                    } = request;
                    let bytes = snapshot.expect("snapshot initialized");
                    let spawned = thread::Builder::new()
                        .name(format!("yic-v8-{id}"))
                        .stack_size(8 * 1024 * 1024)
                        .spawn(move || worker_loop(bytes, control, commands))
                        .map_err(|_| Failure::Runtime);
                    if let Err(Ok(join)) = ready.send(spawned) {
                        let _ = join.join();
                    }
                }
            })
            .ok()?;
        Some(sender)
    })
    .as_ref()
    .ok_or(Failure::Runtime)
}

fn options(snapshot: Option<&'static [u8]>) -> RuntimeOptions {
    RuntimeOptions {
        startup_snapshot: snapshot,
        create_params: Some(v8::CreateParams::default().heap_limits(0, HEAP_LIMIT)),
        extensions: vec![
            deno_webidl::deno_webidl::init(),
            deno_web::deno_web::init(
                Arc::new(deno_web::BlobStore::default()),
                None,
                false,
                Default::default(),
            ),
        ],
        ..Default::default()
    }
}

fn build_snapshot() -> Result<Box<[u8]>> {
    tracing::info!("Preparing embedded V8 startup snapshot");
    let mut runtime = JsRuntimeForSnapshot::new(options(None));
    for (name, code) in [
        ("yic:web-globals", include_str!("web_globals.js")),
        ("yic:browser", include_str!("browser.js")),
        ("yic:dom", include_str!("vendor/dom.js")),
        ("yic:meriyah", include_str!("vendor/meriyah.js")),
        ("yic:helpers", include_str!("vendor/helpers.js")),
        ("yic:matchers", include_str!("vendor/matchers.js")),
        ("yic:analyzer", include_str!("vendor/JsAnalyzer.js")),
        ("yic:extractor", include_str!("vendor/JsExtractor.js")),
        ("yic:decipher", include_str!("decipher.js")),
    ] {
        runtime.execute_script(name, code).map_err(|_| {
            tracing::warn!(module = name, "Embedded runtime bootstrap failed");
            Failure::Runtime
        })?;
    }
    Ok(runtime.snapshot())
}

struct State {
    runtime: Option<JsRuntime>,
    // Kept until the isolate is actually disposed, even if the caller cancels.
    permit: Option<Arc<OwnedSemaphorePermit>>,
    control: Arc<Control>,
}
impl State {
    fn dispose(&mut self) {
        self.control.detach();
        self.runtime.take();
        self.permit.take();
    }
}
impl Drop for State {
    fn drop(&mut self) {
        self.dispose();
        self.control.finished.store(true, Ordering::Release);
    }
}

fn worker_loop(snapshot: &'static [u8], control: Arc<Control>, commands: mpsc::Receiver<Command>) {
    let Ok(executor) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };
    let _entered = executor.enter();
    let mut state = State {
        runtime: None,
        permit: None,
        control,
    };
    while let Ok(command) = commands.recv() {
        if state.control.cancelled.load(Ordering::Acquire) {
            break;
        }
        let Command {
            op,
            reply,
            span,
            dispatch,
        } = command;
        let result = catch_unwind(AssertUnwindSafe(|| {
            executor.block_on(
                perform(&mut state, snapshot, op)
                    .instrument(span)
                    .with_subscriber(dispatch),
            )
        }))
        .unwrap_or(Err(Failure::Runtime));
        if reply.send(result).is_err() || state.control.cancelled.load(Ordering::Acquire) {
            break;
        }
    }
}

async fn perform(state: &mut State, snapshot: &'static [u8], op: Operation) -> Result<String> {
    match op {
        Operation::Begin(permit) => {
            state.permit = Some(permit);
            let mut runtime = JsRuntime::new(options(Some(snapshot)));
            let handle = runtime.v8_isolate().thread_safe_handle();
            state.control.attach(handle.clone());
            let control = state.control.clone();
            runtime.add_near_heap_limit_callback(move |limit, _| {
                control.heap_exceeded.store(true, Ordering::Release);
                handle.terminate_execution();
                limit.saturating_add(16 * 1024 * 1024)
            });
            state.runtime = Some(runtime);
            Ok(String::new())
        }
        Operation::Reset => {
            let runtime = state.runtime.as_mut().ok_or(Failure::Runtime)?;
            evaluate(
                runtime,
                format!("globalThis.__userAgent={};", super::argument(USER_AGENT)),
            )
            .await?;
            evaluate(runtime, include_str!("runtime.js").to_owned()).await
        }
        Operation::Eval(code) => {
            evaluate(state.runtime.as_mut().ok_or(Failure::Runtime)?, code).await
        }
        Operation::Dispose => {
            if let Some(runtime) = state.runtime.as_mut() {
                evaluate(
                    runtime,
                    "if(typeof __shutdown==='function')__shutdown();".into(),
                )
                .await?;
            }
            state.dispose();
            Ok(String::new())
        }
    }
}

async fn evaluate(runtime: &mut JsRuntime, code: String) -> Result<String> {
    let value = runtime
        .execute_script("yic:execute", code)
        .map_err(|error| {
            let kind = match error.name.as_deref() {
                Some("Error") => "Error",
                Some("TypeError") => "TypeError",
                Some("ReferenceError") => "ReferenceError",
                Some("SyntaxError") => "SyntaxError",
                Some("RangeError") => "RangeError",
                _ => "other",
            };
            tracing::debug!(kind, "Embedded JavaScript evaluation failed");
            Failure::Runtime
        })?;
    runtime
        .run_event_loop(Default::default())
        .await
        .map_err(|_| Failure::Runtime)?;
    deno_core::scope!(scope, runtime);
    v8::tc_scope!(let scope, scope);
    let value = v8::Local::new(scope, value);
    let string = value.to_string(scope).ok_or(Failure::Runtime)?;
    if string.utf8_length(scope) > MAX_BYTES {
        return Err(Failure::Runtime);
    }
    Ok(string.to_rust_string_lossy(scope))
}

// If an individual call future is dropped while the session remains alive,
// interrupt its isolate too. Late replies can never be mistaken for another job.
struct Pending {
    control: Arc<Control>,
    active: bool,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if self.active {
            self.control.terminate();
        }
    }
}

pub(super) struct Session {
    pool: EmbeddedPool,
    worker: Option<Worker>,
    healthy: bool,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl Session {
    async fn call(&mut self, op: Operation, deadline: Duration) -> Result<String> {
        let worker = self.worker.as_ref().ok_or(Failure::Runtime)?;
        if worker.control.cancelled.load(Ordering::Acquire) {
            return Err(Failure::Runtime);
        }
        let mut pending = Pending {
            control: worker.control.clone(),
            active: true,
        };
        let (reply, receive) = oneshot::channel();
        worker
            .sender
            .as_ref()
            .ok_or(Failure::Runtime)?
            .send(Command {
                op,
                reply,
                span: Span::current(),
                dispatch: tracing::dispatcher::get_default(Clone::clone),
            })
            .map_err(|_| Failure::Runtime)?;
        let result = match tokio::time::timeout(deadline, receive).await {
            Ok(reply) => {
                pending.active = false;
                reply.map_err(|_| Failure::Runtime)?
            }
            Err(_) => Err(Failure::Timeout),
        };
        self.healthy &= result.is_ok();
        result
    }
    pub(super) async fn reset(&mut self) -> Result<()> {
        self.call(Operation::Reset, SCRIPT_TIMEOUT)
            .await
            .map(|_| ())
    }
    pub(super) async fn js(&mut self, script: &str) -> Result<String> {
        if script.len() > MAX_BYTES {
            return Err(Failure::Runtime);
        }
        self.call(Operation::Eval(script.to_owned()), SCRIPT_TIMEOUT)
            .await
    }
    pub(super) async fn recycle(mut self) -> Result<()> {
        if !self.healthy {
            return Ok(());
        }
        self.call(Operation::Dispose, SCRIPT_TIMEOUT).await?;
        let worker = self.worker.take().ok_or(Failure::Runtime)?;
        self.pool
            .0
            .idle
            .lock()
            .map_err(|_| Failure::Runtime)?
            .push(worker);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
