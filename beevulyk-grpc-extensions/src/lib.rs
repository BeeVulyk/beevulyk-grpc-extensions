mod grpc_channel;
pub mod grpc_server;
pub mod read_grpc_stream;
pub use grpc_channel::*;

pub extern crate external_dependencies as prelude;

#[cfg(feature = "grpc-client")]
pub extern crate beevulyk_grpc_client_macros as client;

#[cfg(feature = "grpc-server")]
pub extern crate beevulyk_grpc_server_macros as server;

pub extern crate hyper;
pub extern crate tonic;

#[cfg(feature = "grpc-server")]
pub mod server_stream_result;
