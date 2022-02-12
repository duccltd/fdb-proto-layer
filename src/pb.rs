use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use protofish::{context::Context};
use bytes::Bytes;

pub async fn load() -> Result<Context, Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::open("./protos.proto").await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await.unwrap();

    let context = Context::parse(&[buf]).unwrap();
    Ok(context)
}
