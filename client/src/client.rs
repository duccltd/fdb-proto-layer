use protos::fdb_service_client::FdbServiceClient;
use protos::get_range_response::Pair as GetRangePair;
use protos::operation_request::Operation as RequestOperation;
use protos::operation_response::Operation as ResponseOperation;
use protos::{
    CommitRequest, GetRangeRequest, GetRequest, SetProtoRequest, GetProtoRequest, GetProtoResponse, OperationRequest, OperationResponse, SetRequest,
};

pub mod protos {
    tonic::include_proto!("fdb");
}

pub struct Client {
    inner: FdbServiceClient<tonic::transport::Channel>,
}

impl Client {
    pub async fn new(addr: String) -> Result<Client, Box<dyn std::error::Error>> {
        let client = FdbServiceClient::connect(addr).await?;

        Ok(Client { inner: client })
    }

    pub async fn transaction(&mut self) -> Result<Transaction, Box<dyn std::error::Error>> {
        let (sender, receiver) = futures::channel::mpsc::unbounded::<OperationRequest>();

        let response = self.inner.transaction(receiver).await?;
        let stream = response.into_inner();

        Ok(Transaction { stream, sender })
    }
}

pub struct Transaction {
    stream: tonic::codec::Streaming<OperationResponse>,
    sender: futures::channel::mpsc::UnboundedSender<OperationRequest>,
}

impl Transaction {
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::Get(GetRequest {
                key: key.to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        })?;

        // todo timeouts
        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::Get(get)) => {
                    return Ok(Some(get.value));
                }
                _ => return Err("Expected get response".into()),
            }
        }

        Ok(None)
    }

    pub async fn get_proto(&mut self, name: impl Into<String>, keys: KeySet) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::GetProto(GetProtoRequest {
                name: name.into(),
                keys: keys.keys,
                ..Default::default()
            })),
            ..Default::default()
        })?;

        // todo timeouts
        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::GetProto(get_proto)) => {
                    return Ok(Some(get_proto.value));
                }
                _ => return Err("Expected get response".into()),
            }
        }

        Ok(None)
    }

    pub async fn get_range(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        opts: GetRangeOpts,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::GetRange(GetRangeRequest {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                limit: opts.limit.unwrap_or(0),
                reverse: opts.reverse.unwrap_or(false),
                ..Default::default()
            })),
            ..Default::default()
        })?;

        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::GetRange(get_range)) => {
                    return Ok(get_range
                        .pairs
                        .into_iter()
                        .map(|GetRangePair { key, value }| (key, value))
                        .collect());
                }
                _ => return Err("Expected get range response".into()),
            }
        }

        Ok(vec![])
    }

    pub async fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::Set(SetRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        })?;

        // todo timeouts
        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::Set(_)) => {
                    return Ok(());
                }
                _ => return Err("Expected set response".into()),
            }
        }

        Ok(())
    }

    pub async fn set_proto(
        &mut self,
        name: impl Into<String>,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::SetProto(SetProtoRequest {
                name: name.into(),
                value: value.to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        })?;

        // todo timeouts
        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::SetProto(_)) => {
                    return Ok(());
                }
                _ => return Err("Expected set response".into()),
            }
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.sender.unbounded_send(OperationRequest {
            operation: Some(RequestOperation::Commit(CommitRequest {})),
            ..Default::default()
        })?;

        if let Some(message) = self.stream.message().await.expect("getting next message") {
            match message.operation {
                Some(ResponseOperation::Commit(_)) => {
                    return Ok(());
                }
                _ => return Err("Expected commit response".into()),
            }
        }

        Ok(())
    }
}

#[derive(Default)]
pub struct GetRangeOpts {
    pub reverse: Option<bool>,
    pub limit: Option<i32>,
}

#[derive(Default)]
pub struct KeySet {
    keys: Vec<protos::get_proto_request::Key>,
}

impl KeySet {
    pub fn builder() -> KeySet {
        Default::default()
    }

    pub fn key(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.keys.push(protos::get_proto_request::Key { name: name.into(), value: value.into() });
        
        Self {
            ..self
        }
    }
}