use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_core::{stream::Stream, Future};
use pin_project_lite::pin_project;
use tokio::time::Sleep;

pub trait ChunksWithinStream: Stream {
    fn chunks_within(self, capacity: usize, timeout: Duration) -> ChunksWithin<Self>
    where
        Self: Sized,
    {
        ChunksWithin::new(self, capacity, timeout)
    }
}

impl<T: ?Sized> ChunksWithinStream for T where T: Stream {}

pin_project! {
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct ChunksWithin<St: Stream> {
        #[pin]
        stream: St,

        items: Vec<St::Item>,
        cap: usize, // https://github.com/rust-lang/futures-rs/issues/1475

        #[pin]
        timer: Sleep,
        duration: Duration,
    }
}

impl<St: Stream> ChunksWithin<St>
where
    St: Stream,
{
    fn new(stream: St, capacity: usize, duration: Duration) -> Self {
        assert!(capacity > 0);
        assert!(duration > Duration::from_millis(0));

        Self {
            stream,
            items: Vec::with_capacity(capacity),
            cap: capacity,

            timer: tokio::time::sleep(duration),
            duration,
        }
    }

    fn take(mut self: Pin<&mut Self>) -> Vec<St::Item> {
        let duration = self.duration;
        let cap = self.cap;
        let this = self.as_mut().project();
        this.timer.reset(tokio::time::Instant::now() + duration);

        std::mem::replace(this.items, Vec::with_capacity(cap))
    }
}

impl<St: Stream> Stream for ChunksWithin<St> {
    type Item = Vec<St::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        loop {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => {}
                Poll::Ready(Some(item)) => {
                    this.items.push(item);
                    if this.items.len() >= *this.cap {
                        return Poll::Ready(Some(self.take()));
                    }
                }

                Poll::Ready(None) => {
                    let last = if this.items.is_empty() {
                        None
                    } else {
                        let full_buf = std::mem::replace(this.items, Vec::new());
                        Some(full_buf)
                    };

                    return Poll::Ready(last);
                }
            }

            match this.timer.as_mut().poll(cx) {
                Poll::Pending => {}
                Poll::Ready(()) => {
                    return Poll::Ready(Some(self.take()));
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, FutureExt, StreamExt};
    use std::iter;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn messages_pass_through() {
        let results = stream::iter(iter::once(5))
            .chunks_within(5, Duration::from_secs(1))
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![5]], results.await);
    }

    #[tokio::test]
    async fn message_chunks() {
        let iter = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        let stream = stream::iter(iter).chunks_within(5, Duration::from_secs(1));

        assert_eq!(
            vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9]],
            stream.collect::<Vec<_>>().await
        );
    }

    #[tokio::test]
    async fn message_early_exit() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream = stream::iter(iter);

        let chunk_stream = ChunksWithin::new(stream, 5, Duration::from_secs(5));
        assert_eq!(
            vec![vec![1, 2, 3, 4]],
            chunk_stream.collect::<Vec<_>>().await
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn message_timeout() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream0 = stream::iter(iter);

        let iter = vec![5].into_iter();
        let stream1 = stream::iter(iter)
            .then(move |n| tokio::time::sleep(Duration::from_secs(3)).map(move |_| n));

        let iter = vec![6, 7, 8].into_iter();
        let stream2 = stream::iter(iter);

        let chunk_stream = stream0
            .chain(stream1)
            .chain(stream2)
            .chunks_within(5, Duration::from_secs(2));

        let now = Instant::now();
        let min_times = [Duration::from_millis(800), Duration::from_millis(1500)];
        let max_times = [Duration::from_millis(3500), Duration::from_millis(5000)];
        let expected = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]];
        let mut i = 0;

        let results = chunk_stream
            .map(move |s| {
                let now2 = Instant::now();
                assert!((now2 - now) < max_times[i]);
                assert!((now2 - now) > min_times[i]);
                i += 1;
                s
            })
            .collect::<Vec<_>>();

        assert_eq!(results.await, expected);
    }
}
