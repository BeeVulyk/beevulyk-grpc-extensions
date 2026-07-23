use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::Channel;
use beevulyk_rust_extensions::remote_endpoint::RemoteEndpoint;

use crate::GrpcReadError;

pub struct ChannelData {
    pub channel: Channel,
    pub host: String,
    pub service_name: &'static str,
}

pub struct GrpcChannelHolder {
    pub channel: Mutex<Option<ChannelData>>,
}

impl Default for GrpcChannelHolder {
    fn default() -> Self {
        Self::new()
    }
}

impl GrpcChannelHolder {
    pub fn new() -> Self {
        Self {
            channel: Mutex::new(None),
        }
    }

    async fn set(&self, service_name: &'static str, host: String, channel: Channel) {
        tracing::info!(%service_name, %host, "GrpcChannelPoolInner::set - GRPC Connection is established");

        let mut channel_access = self.channel.lock().await;
        *channel_access = Some(ChannelData {
            channel,
            host,
            service_name,
        });
    }

    pub async fn reuse_existing_channel(&self) -> Option<Channel> {
        let channel_access = self.channel.lock().await;

        let channel = channel_access.as_ref()?;
        Some(channel.channel.clone())
    }
    pub async fn drop_channel(&self, err: String) {
        let disconnected_channel = {
            let mut channel_access = self.channel.lock().await;
            channel_access.take()
        };

        if let Some(disconnected_channel) = disconnected_channel {
            tracing::warn!(%disconnected_channel.service_name, %disconnected_channel.host, ?err, "GrpcChannel::ping_channel err");
        }
    }

    pub async fn create_channel(
        &self,
        connect_url: String,
        service_name: &'static str,
        request_timeout: Duration,
    ) -> Result<Channel, GrpcReadError> {
        let grpc_service_endpoint = RemoteEndpoint::try_parse(&connect_url);

        if grpc_service_endpoint.is_err() {
            panic!(
                "Failed to parse grpc service endpoint: {connect_url} for service {service_name}"
            );
        }

        let _grpc_service_endpoint = grpc_service_endpoint.unwrap();

        let mut attempt_no = 0;
        loop {
            let end_point = Channel::from_shared(connect_url.clone());

            if let Err(err) = end_point {
                panic!(
                    "Failed to create channel with url:{connect_url}. Err: {err:?}"
                )
            }

            let end_point = end_point.unwrap();

            match tokio::time::timeout(request_timeout, end_point.connect()).await {
                Ok(channel) => match channel {
                    Ok(channel) => {
                        {
                            self.set(service_name, connect_url, channel.clone()).await;
                        }
                        return Ok(channel);
                    }
                    Err(err) => {
                        if attempt_no > 3 {
                            return Err(err.into());
                        }
                    }
                },
                Err(_) => {
                    if attempt_no > 3 {
                        return Err(GrpcReadError::Timeout);
                    }
                }
            }

            attempt_no += 1;
        }
    }
}
