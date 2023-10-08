use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::{select, AsyncReadExt, AsyncWriteExt, FutureExt};
use libp2p::swarm::Stream;
use replace_with::replace_with_or_abort;

use super::super::DataBound;
use crate::messages::write_message;

pub(super) struct InboundSession<Data: DataBound> {
    pending_messages: VecDeque<Data>,
    current_task: WriteMessageTask,
    buffer: [u8; 1],
}

pub(super) enum InboundSessionError {
    IO(io::Error),
    OtherPeerSentData,
}

pub(super) enum FinishReason {
    Error(InboundSessionError),
    Closed,
    OtherPeerClosed,
}

enum RunningFinishReason {
    Error(InboundSessionError),
    OtherPeerClosed,
}

enum WriteMessageTask {
    Waiting(Stream),
    Running(BoxFuture<'static, Result<Stream, RunningFinishReason>>),
    Closing(BoxFuture<'static, Result<(), io::Error>>),
}

impl<Data: DataBound> InboundSession<Data> {
    #[allow(dead_code)]
    // TODO(shahak) remove allow dead code.
    pub fn new(stream: Stream) -> Self {
        Self {
            pending_messages: Default::default(),
            current_task: WriteMessageTask::Waiting(stream),
            buffer: Default::default(),
        }
    }

    #[allow(dead_code)]
    // TODO(shahak) remove allow dead code.
    pub fn add_message_to_queue(&mut self, data: Data) {
        self.pending_messages.push_back(data);
    }

    #[allow(dead_code)]
    // TODO(shahak) remove allow dead code.
    pub fn is_waiting(&self) -> bool {
        matches!(self.current_task, WriteMessageTask::Waiting(_))
            && self.pending_messages.is_empty()
    }

    pub fn start_closing(&mut self) {
        replace_with_or_abort(&mut self.current_task, |current_task| {
            let WriteMessageTask::Waiting(mut stream) = current_task else {
                panic!("Called start_closing while not waiting.");
            };
            WriteMessageTask::Closing(async move { stream.close().await }.boxed())
        })
    }

    fn handle_waiting(&mut self, cx: &mut Context<'_>) -> Option<FinishReason> {
        if let Some(data) = self.pending_messages.pop_front() {
            replace_with_or_abort(&mut self.current_task, |current_task| {
                let WriteMessageTask::Waiting(mut stream) = current_task else {
                    panic!("Called handle_waiting while not waiting.");
                };
                WriteMessageTask::Running(
                    async move {
                        select! {
                            result = write_message(data, &mut stream)
                                .map(|result| result.map_err(|io_error| {
                                    RunningFinishReason::Error(InboundSessionError::IO(io_error))
                                })).fuse() => result,
                            size_result = stream.read(&mut self.buffer).fuse() => {
                                match size_result {
                                    Err(io_error) => {
                                        Err(RunningFinishReason::Error(
                                            InboundSessionError::IO(io_error)
                                        ))
                                    },
                                    Ok(0) => {
                                        Ok(())
                                    },
                                    _ => {
                                        Err(RunningFinishReason::Error(
                                            InboundSessionError::OtherPeerSentData
                                        ))
                                    }
                                }
                            }
                        }?;
                        Ok(stream)
                    }
                    .boxed(),
                )
            });
            return self.handle_running(cx);
        }
        None
    }

    fn handle_running(&mut self, cx: &mut Context<'_>) -> Option<FinishReason> {
        let WriteMessageTask::Running(fut) = &mut self.current_task else {
            panic!("Called handle_running while not running.");
        };
        match fut.poll_unpin(cx) {
            Poll::Pending => None,
            Poll::Ready(Ok(stream)) => {
                self.current_task = WriteMessageTask::Waiting(stream);
                self.handle_waiting(cx)
            }
            Poll::Ready(Err(RunningFinishReason::Error(error))) => Some(FinishReason::Error(error)),
            Poll::Ready(Err(RunningFinishReason::OtherPeerClosed)) => {
                Some(FinishReason::OtherPeerClosed)
            }
        }
    }

    fn handle_closing(&mut self, cx: &mut Context<'_>) -> Option<FinishReason> {
        let WriteMessageTask::Closing(fut) = &mut self.current_task else {
            panic!("Called handle_closing while not closing.");
        };
        match fut.poll_unpin(cx) {
            Poll::Pending => None,
            Poll::Ready(Ok(())) => Some(FinishReason::Closed),
            Poll::Ready(Err(io_error)) => {
                Some(FinishReason::Error(InboundSessionError::IO(io_error)))
            }
        }
    }
}

impl<Data: DataBound> Future for InboundSession<Data> {
    type Output = FinishReason;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let unpinned_self = Pin::into_inner(self);
        let result = match &mut unpinned_self.current_task {
            WriteMessageTask::Running(_) => unpinned_self.handle_running(cx),
            WriteMessageTask::Waiting(_) => unpinned_self.handle_waiting(cx),
            WriteMessageTask::Closing(_) => unpinned_self.handle_closing(cx),
        };
        match result {
            Some(finish_reason) => Poll::Ready(finish_reason),
            None => Poll::Pending,
        }
    }
}
