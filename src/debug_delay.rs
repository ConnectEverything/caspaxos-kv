use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use rand::Rng;

pub async fn debug_delay() {
    let delay = {
        let mut thread_rng = rand::thread_rng();
        thread_rng.gen_ratio(1, 10)
    };

    if delay {
        smol::Timer::after(std::time::Duration::from_millis(2)).await;
    }

    DebugDelay.await
}

struct DebugDelay;

impl Future for DebugDelay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut thread_rng = rand::thread_rng();
        if thread_rng.gen_ratio(99, 100) {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
