use futures_util::StreamExt;
use crate::{
    GrpcChannel, GrpcReadError, RequestWithInputStreamGrpcExecutor,
    RequestWithInputStreamWithResponseAsStreamGrpcExecutor, StreamedResponse,
};

pub struct RequestBuilderWithStreamWithRetries<
    TService: Send + Sync + 'static,
    TRequest: Clone + Send + Sync + 'static,
> {
    input_stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = TRequest> + Send + Sync + 'static>,
    >,
    channel: GrpcChannel<TService>,
    max_attempts_amount: usize,
}

impl<TService: Send + Sync + 'static, TRequest: Clone + Send + Sync + 'static>
    RequestBuilderWithStreamWithRetries<TService, TRequest>
{
    pub fn new(
        input_stream: std::pin::Pin<
            Box<dyn futures_core::Stream<Item = TRequest> + Send + Sync + 'static>,
        >,
        channel: GrpcChannel<TService>,
        max_attempts_amount: usize,
    ) -> Self {
        Self {
            input_stream,
            channel,
            max_attempts_amount,
        }
    }

    pub async fn get_response<
        TResponse,
        TExecutor: RequestWithInputStreamGrpcExecutor<TService, TRequest, TResponse> + Send + Sync + 'static,
    >(
        mut self,
        grpc_executor: &TExecutor,
    ) -> Result<TResponse, GrpcReadError>
    where
        TResponse: Send + Sync + 'static,
    {
        // Collect stream into Vec for retry support
        // Note: This means the first attempt will collect all items, but retries can work
        let mut items = Vec::new();
        while let Some(item) = self.input_stream.next().await {
            items.push(item);
        }

        let mut attempt_no = 0;
        loop {
            // Convert Vec back to stream for execution
            let stream = Box::pin(futures::stream::iter(items.clone()));
            
            let result = self
                .channel
                .execute_with_input_stream(stream, grpc_executor)
                .await;

            match result {
                Ok(response) => return Ok(response),
                Err(err) => {
                    if attempt_no >= self.max_attempts_amount {
                        return Err(err);
                    }
                }
            }

            attempt_no += 1;
        }
    }

    pub async fn get_streamed_response<
        TResponse,
        TExecutor: RequestWithInputStreamWithResponseAsStreamGrpcExecutor<TService, TRequest, TResponse>
            + Send
            + Sync
            + 'static,
    >(
        mut self,
        grpc_executor: &TExecutor,
    ) -> Result<StreamedResponse<TResponse>, GrpcReadError>
    where
        TResponse: Send + Sync + 'static,
    {
        // Collect stream into Vec for retry support
        let mut items = Vec::new();
        while let Some(item) = self.input_stream.next().await {
            items.push(item);
        }

        let mut attempt_no = 0;
        loop {
            // Convert Vec back to stream for execution
            let stream = Box::pin(futures::stream::iter(items.clone()));
            
            let result = self
                .channel
                .execute_with_input_stream_response_as_stream(stream, grpc_executor)
                .await;

            match result {
                Ok(stream_to_read) => {
                    return Ok(StreamedResponse::new(
                        stream_to_read,
                        self.channel.request_timeout,
                    ));
                }
                Err(err) => {
                    if attempt_no >= self.max_attempts_amount {
                        return Err(err);
                    }
                }
            }

            attempt_no += 1;
        }
    }
}



