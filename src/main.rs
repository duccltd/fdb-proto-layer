use std::pin::Pin;

use foundationdb::api::{FdbApiBuilder, NetworkAutoStop};
use foundationdb::{Database, RangeOption, Transaction};
use futures::{Stream, StreamExt};
use protos::fdb_service_server::FdbService;
use protos::fdb_service_server::FdbServiceServer;
use protos::get_range_response::Pair as GetRangePair;
use protos::operation_request::Operation as RequestOperation;
use protos::operation_response::Operation as ResponseOperation;
use protos::{
    ClearResponse, CommitResponse, GetRangeResponse, GetResponse, OperationRequest,
    OperationResponse, SetResponse,
};
use std::sync::Arc;
use tonic::{transport::Server, Response, Status};
use tracing::debug;

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
        // todo use Database#transact for retry stategy + timeouts
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

        let output = async_stream::try_stream! {
            while let Some(req) = stream.next().await {
                count += 1;

                let req = req?;

                let operation = match req.operation {
                    Some(operation) => Ok(operation),
                    None => Err(Status::invalid_argument("Missing operation")),
                }?;

                debug!("{} {:?}", count, operation);

                if let RequestOperation::Commit(_) = operation {
                    tx.commit().await.expect("commit failed");
                    yield OperationResponse{
                        operation: Some(ResponseOperation::Commit(CommitResponse{
                            ..Default::default()
                        })),
                        ..Default::default()
                    };
                    break;
                }

                let res = handle_op(&tx, operation).await?;

                yield res;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::TransactionStream))
    }
}

async fn handle_op(
    tx: &Transaction,
    operation: RequestOperation,
) -> Result<OperationResponse, tonic::Status> {
    match operation {
        RequestOperation::Get(get) => {
            let key = get.key;

            let opt_val = tx.get(&key, false).await.expect("getting key");
            let val = match opt_val {
                Some(val) => (*val).to_vec(),
                None => return Err(Status::not_found("key not found")),
            };

            Ok(OperationResponse {
                operation: Some(ResponseOperation::Get(GetResponse {
                    value: val,
                    ..Default::default()
                })),
                ..Default::default()
            })
        }
        RequestOperation::GetRange(range) => {
            let limit = if range.limit == 0 {
                None
            } else {
                Some(range.limit as usize)
            };

            let mut kvs = tx.get_ranges(
                RangeOption {
                    limit: limit,
                    reverse: range.reverse,
                    ..RangeOption::from((range.start_key, range.end_key))
                },
                false,
            );

            let mut pairs = vec![];

            while let Some(kv) = kvs.next().await {
                let kv = match kv {
                    Ok(kv) => kv,
                    Err(e) => return Err(Status::internal(e.to_string())),
                };

                // todo yield each set of values instead of grouping them into a single response

                for value in (*kv).into_iter() {
                    let k = value.key();
                    let v = value.value();

                    pairs.push(GetRangePair {
                        key: k.to_vec(),
                        value: v.to_vec(),
                    });
                }
            }

            Ok(OperationResponse {
                operation: Some(ResponseOperation::GetRange(GetRangeResponse {
                    pairs: pairs,
                    ..Default::default()
                })),
                ..Default::default()
            })
        }
        RequestOperation::Set(set) => {
            let key = set.key;
            let value = set.value;

            tx.set(&key, &value);

            Ok(OperationResponse {
                operation: Some(ResponseOperation::Set(SetResponse {
                    ..Default::default()
                })),
                ..Default::default()
            })
        }
        RequestOperation::Clear(clear) => {
            let key = clear.key;

            tx.clear(&key);

            Ok(OperationResponse {
                operation: Some(ResponseOperation::Clear(ClearResponse {
                    ..Default::default()
                })),
                ..Default::default()
            })
        }
        _ => Err(Status::invalid_argument("Unknown operation")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // guard has to be in scope so it is dropped on shutdown
    #[allow(unused)]
    let guard = unsafe { Client::start_network() }.expect("unable to start network");

    let fdb_client = Client::new("/etc/foundationdb/fdb.cluster").expect("creating fdb client");

    let addr = "0.0.0.0:50051".parse()?;

    let server = FdbServer {
        client: Arc::new(fdb_client),
    };

    Server::builder()
        .add_service(FdbServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
