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
    ClearRequest, ClearResponse, CommitResponse, GetProtoRequest, GetProtoResponse,
    GetRangeRequest, GetRangeResponse, GetRequest, GetResponse, OperationRequest,
    OperationResponse, SetProtoRequest, SetRequest, SetResponse, SetProtoResponse,
    ClearProtoRequest, ClearProtoResponse, GetRangeProtoRequest, GetRangeProtoResponse,
};
use std::sync::Arc;
use tonic::{transport::Server, Response, Status};
use tracing::debug;

mod schema;
mod pb;

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
    s: schema::Schema,
}

#[tonic::async_trait]
impl FdbService for FdbServer {
    type TransactionStream =
        Pin<Box<dyn Stream<Item = Result<OperationResponse, Status>> + Send + Sync + 'static>>;

    async fn transaction(
        &self,
        request: tonic::Request<tonic::Streaming<OperationRequest>>,
    ) -> Result<tonic::Response<Self::TransactionStream>, tonic::Status> {
        let handler = Handler {
            s: self.s.clone(),
            c: pb::load().await.unwrap(),
        };

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

                
                let res = handler.handle_op(&tx, operation).await?;

                yield res;
            }
        };

        Ok(Response::new(Box::pin(output) as Self::TransactionStream))
    }
}

fn build_key(index_name: &str, values: Vec<Vec<u8>>) -> Vec<u8> {
    let mut subspace = foundationdb::tuple::Subspace::from_bytes(index_name.as_bytes());

    for value in &values[0..values.len() - 1] {
        subspace = subspace.subspace(&value);
    }

    subspace.pack(&values.into_iter().last().unwrap())
}

struct Handler {
    s: schema::Schema,
    c: protofish::context::Context,
}

impl Handler {
    async fn handle_op(
        &self,
        tx: &Transaction,
        operation: RequestOperation,
    ) -> Result<OperationResponse, tonic::Status> {
        match operation {
            RequestOperation::Get(req) => self.get(tx, req).await,
            RequestOperation::GetProto(req) => self.get_proto(&tx, req).await,
            RequestOperation::GetRange(req) => self.get_range(&tx, req).await,
            RequestOperation::GetRangeProto(req) => self.get_range_proto(&tx, req).await,
            RequestOperation::Set(req) => self.set(&tx, req).await,
            RequestOperation::SetProto(req) => self.set_proto(&tx, req).await,
            RequestOperation::Clear(req) => self.clear(&tx, req).await,
            _ => Err(Status::invalid_argument("Unknown operation")),
        }
    }

    async fn get(
        &self,
        tx: &Transaction,
        get: GetRequest,
    ) -> Result<OperationResponse, tonic::Status> {
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

    async fn get_proto(
        &self,
        tx: &Transaction,
        get_proto: GetProtoRequest,
    ) -> Result<OperationResponse, tonic::Status> {
        let keys = get_proto.keys;

        let index_name = format!("{}_{}", get_proto.name.to_lowercase(), keys
            .iter()
            .map(|k| k.name.clone())
            .collect::<Vec<String>>()
            .join("_"));

        let key = build_key(&index_name, keys.into_iter().map(|k| k.value).collect());

        debug!("get_proto: key={}", String::from_utf8_lossy(&key));

        let opt_val = tx.get(&key, false).await.expect("getting key");
        let val = match opt_val {
            Some(val) => (*val).to_vec(),
            None => return Err(Status::not_found("key not found")),
        };

        Ok(OperationResponse {
            operation: Some(ResponseOperation::GetProto(GetProtoResponse {
                value: val,
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    async fn get_range(
        &self,
        tx: &Transaction,
        range: GetRangeRequest,
    ) -> Result<OperationResponse, tonic::Status> {
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

    async fn get_range_proto(
        &self,
        tx: &Transaction,
        range: GetRangeProtoRequest,
    ) -> Result<OperationResponse, tonic::Status> {
        let table = self.s.get_table_by_proto(&range.name).expect("could not find table by proto name");
        let index = table.get_index_by_keys(range.start_key.iter().map(|k| k.name.to_string()).collect()).expect("could not get index by keys");

        let limit = if range.limit == 0 {
            None
        } else {
            Some(range.limit as usize)
        };

        let index_name = format!("{}_{}", range.name.to_lowercase(), range.start_key
            .iter()
            .map(|k| k.name.clone())
            .collect::<Vec<String>>()
            .join("_"));

        let start_key = build_key(&index_name, range.start_key.into_iter().map(|k| k.value).collect());
        let end_key = build_key(&index_name, range.end_key.into_iter().map(|k| k.value).collect());

        let mut kvs = tx.get_ranges(
            RangeOption {
                limit: limit,
                reverse: range.reverse,
                ..RangeOption::from((start_key, end_key))
            },
            false,
        );

        let mut values = vec![];

        while let Some(kv) = kvs.next().await {
            let kv = match kv {
                Ok(kv) => kv,
                Err(e) => return Err(Status::internal(e.to_string())),
            };

            // todo yield each set of values instead of grouping them into a single response

            for value in (*kv).into_iter() {
                let k = value.key();
                let v = value.value();

                if index.type_field == "alias" {
                    // TODO: do another call to get the actual value!
                    // 1. find which fields should be in this index's key
                    // 2. extract each field
                    // 3. using those fields, build the key for the "value" index
                    // 4. perform read for each value
                    // 5. return those values instead
                }

                values.push(v.to_vec());
            }
        }

        Ok(OperationResponse {
            operation: Some(ResponseOperation::GetRangeProto(GetRangeProtoResponse {
                values,
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    async fn set(
        &self,
        tx: &Transaction,
        set: SetRequest,
    ) -> Result<OperationResponse, tonic::Status> {
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

    async fn set_proto(
        &self,
        tx: &Transaction,
        set_proto: SetProtoRequest,
    ) -> Result<OperationResponse, tonic::Status> {
        let table = match self.s.tables.iter().find(|t| t.proto == set_proto.name) {
            Some(t) => t,
            None => panic!("oopsie woopsie"),
        };

        let message = self.c.get_message(&set_proto.name).unwrap();
        let decoded = message.decode(&set_proto.value, &self.c);

        for index in &table.indexes {
            let index_name = format!("{}_{}", table.proto.to_lowercase(), index.keys.iter().map(|k| k.to_string()).collect::<Vec<String>>().join("_"));

            let mut values = vec![];

            for key_field in &index.keys {
                let field = message.get_field_by_name(&key_field).unwrap();
                let value = decoded.fields.iter().find(|f| f.number == field.number).unwrap();
                match &value.value {
                    protofish::decode::Value::String(val) => values.push(val.as_bytes().to_vec()),
                    _ => panic!("expected bytes: {:?}", value),
                }
            }

            let key = build_key(&index_name, values);
            
            match index.type_field.as_ref() {
                "value" => {
                    tx.set(&key, &set_proto.value);
                }
                "alias" => {
                    tx.set(&key, &[]);
                }
                _ => panic!("unknown type")
            }
        }

        Ok(OperationResponse {
            operation: Some(ResponseOperation::SetProto(SetProtoResponse {
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    async fn clear(
        &self,
        tx: &Transaction,
        clear: ClearRequest,
    ) -> Result<OperationResponse, tonic::Status> {
        let key = clear.key;

        tx.clear(&key);

        Ok(OperationResponse {
            operation: Some(ResponseOperation::Clear(ClearResponse {
                ..Default::default()
            })),
            ..Default::default()
        })
    }

    async fn clear_proto(
        &self,
        tx: &Transaction,
        clear: ClearProtoRequest,
    ) -> Result<OperationResponse, tonic::Status> {
        // let key = clear.key;

        // tx.clear(&key);

        Ok(OperationResponse {
            operation: Some(ResponseOperation::ClearProto(ClearProtoResponse {
                ..Default::default()
            })),
            ..Default::default()
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let s = schema::load().await.unwrap();
    let p = pb::load().await.unwrap();

    // guard has to be in scope so it is dropped on shutdown
    #[allow(unused)]
    let guard = unsafe { Client::start_network() }.expect("unable to start network");

    let fdb_client = Client::new("/etc/foundationdb/fdb.cluster").expect("creating fdb client");

    let addr = "0.0.0.0:50051".parse()?;

    let server = FdbServer {
        client: Arc::new(fdb_client),
        s,
    };

    Server::builder()
        .add_service(FdbServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
