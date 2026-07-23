use crate::{
    GrpcChannel, GrpcReadError, RequestBuilderWithStreamWithRetries,
    RequestWithInputStreamGrpcExecutor, RequestWithInputStreamWithResponseAsStreamGrpcExecutor,
    StreamedResponse,
};

pub struct RequestBuilderWithStream<
    TService: Send + Sync + 'static,
    TRequest: Clone + Send + Sync + 'static,
> {
    input_stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = TRequest> + Send + Sync + 'static>,
    >,
    channel: GrpcChannel<TService>,
}

impl<TService: Send + Sync + 'static, TRequest: Clone + Send + Sync + 'static>
    RequestBuilderWithStream<TService, TRequest>
{
    pub fn new(
        input_stream: std::pin::Pin<
            Box<dyn futures_core::Stream<Item = TRequest> + Send + Sync + 'static>,
        >,
        channel: GrpcChannel<TService>,
    ) -> Self {
        Self {
            input_stream,
            channel,
        }
    }

    pub fn with_retries(
        self,
        attempts_amount: usize,
    ) -> RequestBuilderWithStreamWithRetries<TService, TRequest> {
        RequestBuilderWithStreamWithRetries::new(
            self.input_stream,
            self.channel,
            attempts_amount,
        )
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
        self.channel
            .execute_with_input_stream(self.input_stream, grpc_executor)
            .await
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
        let stream_to_read = self
            .channel
            .execute_with_input_stream_response_as_stream(self.input_stream, grpc_executor)
            .await?;

        Ok(StreamedResponse::new(
            stream_to_read,
            self.channel.request_timeout,
        ))
    }
}



