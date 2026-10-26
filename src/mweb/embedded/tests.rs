use super::*;

async fn acquire_after_cleanup(pool: &EmbeddedPool) -> Session {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match pool.acquire().await {
                Ok(session) => return session,
                Err(Failure::Busy) => tokio::time::sleep(Duration::from_millis(5)).await,
                Err(error) => panic!("unexpected worker failure: {error:?}"),
            }
        }
    })
    .await
    .expect("worker capacity should return after isolate disposal")
}

#[tokio::test]
async fn reuses_thread_with_a_fresh_isolate_and_drains_promise_jobs() {
    let pool = EmbeddedPool::default();
    let mut session = pool.acquire().await.unwrap();
    let id = session.worker.as_ref().unwrap().id;
    session.reset().await.unwrap();
    session.js("globalThis.privateValue=41;Promise.resolve(privateValue).then(v=>{globalThis.answer=v+1});").await.unwrap();
    assert_eq!(session.js("answer").await.unwrap(), "42");
    assert_eq!(session.js("typeof process+':'+typeof require+':'+typeof fetch+':'+typeof Deno+':'+typeof __bootstrap").await.unwrap(), "undefined:undefined:undefined:undefined:undefined");
    assert_eq!(
        session
            .js("TextEncoder.constructor('return typeof process')()")
            .await
            .unwrap(),
        "undefined"
    );
    assert_eq!(
        session
            .js("new TextDecoder().decode(new TextEncoder().encode('Jesús'))")
            .await
            .unwrap(),
        "Jesús"
    );
    assert_eq!(
        session
            .js("structuredClone({value:42}).value")
            .await
            .unwrap(),
        "42"
    );
    session.recycle().await.unwrap();
    let mut session = pool.acquire().await.unwrap();
    assert_eq!(session.worker.as_ref().unwrap().id, id);
    session.reset().await.unwrap();
    assert_eq!(
        session
            .js("typeof privateValue+':'+typeof answer")
            .await
            .unwrap(),
        "undefined:undefined"
    );
    session.recycle().await.unwrap();
}

#[tokio::test]
async fn bounds_workers_and_replaces_a_failed_runtime() {
    let pool = EmbeddedPool::default();
    let mut first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();
    assert!(matches!(pool.acquire().await, Err(Failure::Busy)));
    let id = first.worker.as_ref().unwrap().id;
    first.reset().await.unwrap();
    assert!(matches!(
        first.js("throw new Error('secret signed URL')").await,
        Err(Failure::Runtime)
    ));
    drop(first);
    let mut replacement = acquire_after_cleanup(&pool).await;
    assert_ne!(replacement.worker.as_ref().unwrap().id, id);
    replacement.reset().await.unwrap();
    assert_eq!(replacement.js("6*7").await.unwrap(), "42");
    replacement.recycle().await.unwrap();
    second.recycle().await.unwrap();
}

#[tokio::test]
async fn script_timeout_interrupts_v8_and_recovers_capacity() {
    let pool = EmbeddedPool::new(1);
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let control = session.worker.as_ref().unwrap().control.clone();
    assert!(matches!(
        session
            .call(
                Operation::Eval("while(true){}".into()),
                Duration::from_millis(20)
            )
            .await,
        Err(Failure::Timeout)
    ));
    assert!(control.cancelled.load(Ordering::Acquire));
    drop(session);
    let mut session = acquire_after_cleanup(&pool).await;
    assert!(control.finished.load(Ordering::Acquire));
    session.reset().await.unwrap();
    assert_eq!(session.js("21*2").await.unwrap(), "42");
    session.recycle().await.unwrap();
}

#[tokio::test]
async fn cancellation_interrupts_microtasks_and_keeps_capacity_until_cleanup() {
    let pool = EmbeddedPool::new(1);
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let control = session.worker.as_ref().unwrap().control.clone();
    let task = tokio::spawn(async move {
        session
            .js("Promise.resolve().then(()=>{while(true){}})")
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let replacement = acquire_after_cleanup(&pool).await;
    assert!(control.finished.load(Ordering::Acquire));
    replacement.recycle().await.unwrap();
}

#[tokio::test]
async fn dropping_only_a_call_future_interrupts_its_runtime() {
    let pool = EmbeddedPool::new(1);
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let control = session.worker.as_ref().unwrap().control.clone();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), session.js("while(true){}"))
            .await
            .is_err()
    );
    assert!(control.cancelled.load(Ordering::Acquire));
    drop(session);
    let replacement = acquire_after_cleanup(&pool).await;
    replacement.recycle().await.unwrap();
}

#[tokio::test]
async fn dropping_pool_stops_and_joins_idle_workers() {
    let pool = EmbeddedPool::new(1);
    let session = pool.acquire().await.unwrap();
    let control = session.worker.as_ref().unwrap().control.clone();
    session.recycle().await.unwrap();
    drop(pool);
    assert!(control.finished.load(Ordering::Acquire));
}

#[tokio::test]
async fn heap_limit_interrupts_allocation_and_runtime_is_replaced() {
    let pool = EmbeddedPool::new(1);
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let control = session.worker.as_ref().unwrap().control.clone();
    assert!(
        session
            .js("globalThis.values=[];while(true){values.push(new Array(32768).fill(1));}")
            .await
            .is_err()
    );
    assert!(control.heap_exceeded.load(Ordering::Acquire));
    drop(session);
    let mut replacement = acquire_after_cleanup(&pool).await;
    replacement.reset().await.unwrap();
    assert_eq!(replacement.js("42").await.unwrap(), "42");
    replacement.recycle().await.unwrap();
}

#[tokio::test]
async fn result_conversion_is_bounded_and_handles_throwing_to_string() {
    let pool = EmbeddedPool::new(1);
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    assert!(matches!(
        session
            .js("({toString(){throw new Error('secret')}})")
            .await,
        Err(Failure::Runtime)
    ));
    drop(session);
    let mut session = acquire_after_cleanup(&pool).await;
    session.reset().await.unwrap();
    assert!(matches!(
        session.js(&format!("'x'.repeat({})", MAX_BYTES + 1)).await,
        Err(Failure::Runtime)
    ));
    drop(session);
    let replacement = acquire_after_cleanup(&pool).await;
    replacement.recycle().await.unwrap();
}
