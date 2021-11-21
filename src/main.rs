use std::pin::Pin;

use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
use foundationdb::{Database, Transaction};
use futures::{Stream, StreamExt};
use protos::fdb_service_server::FdbService;
use protos::fdb_service_server::FdbServiceServer;
use protos::{GetResponse, OperationRequest, OperationResponse};
use tokio::time::timeout;
use tonic::codegen::http::StatusCode;
use tonic::Streaming;
use tonic::{transport::Server, Request, Response, Status};
use protos::operation_request::Operation as RequestOperation;
use protos::operation_response::Operation as ResponseOperation;
use std::sync::Arc;

pub struct Client {
    pub db: Database,
}

impl Client {
    /// # Safety
    ///
    /// This function should only be called once at startup and the
    /// returned guard should be dropped once fdb is no longer used.
    pub unsafe fn start_network() -> Result<NetworkAutoStop, Box<dyn std::error::Error>> {
        let network_builder = FdbApiBuilder::default().build()?;

        network_builder.boot().map_err(Into::into)
    }

    pub fn new(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::new(Some(path))?;
        Ok(Self { db })
    }

    pub async fn begin_tx(&self) -> Result<Transaction, Box<dyn std::error::Error>> {
        self.db.create_trx().map_err(Into::into)
    }
}

pub mod protos {
    tonic::include_proto!("fdb");
}

pub struct FdbServer {
    client: Arc<Client>,
}

#[tonic::async_trait]
impl FdbService for FdbServer {
    type TransactionStream =
        Pin<Box<dyn Stream<Item = Result<OperationResponse, Status>> + Send + Sync + 'static>>;

    async fn transaction(
        &self,
        request: tonic::Request<tonic::Streaming<OperationRequest>>,
    ) -> Result<tonic::Response<Self::TransactionStream>, tonic::Status> {
        let mut stream = request.into_inner();

        let mut count = 0;

        let client = self.client.clone();

        let tx = client.begin_tx().await.expect("unable to begin tx");

        let mut commit = false;

        let output = async_stream::try_stream! {
            while let Some(req) = stream.next().await {
                if commit {
                    break;
                }

                count += 1;

                let req = req?;

                let operation  = match req.operation {
                    Some(operation) => Ok(operation),
                    None => Err(Status::invalid_argument("Missing operation")),
                }?;

                println!("{} {:?}", count, operation);

                let res = match operation {
                    RequestOperation::Get(get) => {
                        let key = get.key;

                        let opt_val = tx.get(key.as_bytes(), false).await.expect("getting key");
                        let val = match opt_val {
                            Some(val) => (*val).to_vec(),
                            None => vec![],
                        };

                        let mut res = OperationResponse::default();
                        let res_op = ResponseOperation::Get(GetResponse {
                            value: val,
                        });
                        res.operation = Some(res_op);

                        Ok(res)
                    },
                    RequestOperation::Set(set) => {
                        let key = set.key;
                        let value = set.value;

                        tx.set(key.as_bytes(), &value);

                        Ok(OperationResponse::default())
                    },
                    RequestOperation::Commit(_) => {
                        commit = true;

                        Ok(OperationResponse::default())
                    }
                    _ => Err(Status::invalid_argument("Unknown operation")),
                }?;

                if commit {
                    tx.commit().await.expect("commit failed");
                    yield res;
                    break;
                }

                yield res;
            }
        };

        println!("HERE");

        Ok(Response::new(Box::pin(output) as Self::TransactionStream))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hello, world!");

    // guard has to be in scope so it is dropped on shutdown
    #[allow(unused)]
    let guard = unsafe { Client::start_network() }.expect("unable to start network");

    let fdb_client = Client::new("/etc/foundationdb/fdb.cluster").expect("creating fdb client");

    let addr = "0.0.0.0:50051".parse()?;

    let server = FdbServer { client: Arc::new(fdb_client) };

    Server::builder()
        .add_service(FdbServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
