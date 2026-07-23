use crate::{
    GrpcChannel, GrpcReadError, RequestBuilderWithRetries, RequestResponseGrpcExecutor,
    RequestWithResponseAsStreamGrpcExecutor, StreamedResponse,
};

pub struct RequestBuilder<TService: Send + Sync + 'static, TRequest: Clone + Send + Sync + 'static>
{
    input_contract: TRequest,
    channel: GrpcChannel<TService>,
}

impl<TService: Send + Sync + 'static, TRequest: Clone + Send + Sync + 'static>
    RequestBuilder<TService, TRequest>
{
    pub fn new(input_contract: TRequest, channel: GrpcChannel<TService>) -> Self {
        Self {
            input_contract,
            channel,
        }
    }

    pub fn with_retries(
        self,
        attempts_amount: usize,
    ) -> RequestBuilderWithRetries<TService, TRequest> {
        RequestBuilderWithRetries::new(self.input_contract, self.channel, attempts_amount)
    }

    pub async fn get_response<
        TResponse,
        TExecutor: RequestResponseGrpcExecutor<TService, TRequest, TResponse> + Send + Sync + 'static,
    >(
        mut self,
        grpc_executor: &TExecutor,
    ) -> Result<TResponse, GrpcReadError>
    where
        TResponse: Send + Sync + 'static,
    {
        let result = self
            .channel
            .execute(self.input_contract, grpc_executor)
            .await;

        if let Err(err) = &result {
            let service_name = self.channel.get_service_name();
            tracing::warn!(?err, %service_name, "Grpc read error during get_response.");
        }

        result
    }

    pub async fn get_streamed_response<
        TResponse,
        TExecutor: RequestWithResponseAsStreamGrpcExecutor<TService, TRequest, TResponse>
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
        let result = self
            .channel
            .execute_with_response_as_stream(self.input_contract.clone(), grpc_executor)
            .await;

        if let Err(err) = &result {
            let service_name = self.channel.get_service_name();
            tracing::warn!(?err, %service_name, "Grpc read error during get_streamed_response.");
        }

        let result = result?;

        Ok(StreamedResponse::new(result, self.channel.request_timeout))
    }
}
