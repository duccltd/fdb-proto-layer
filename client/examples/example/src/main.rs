use fdb_proto_client::client::{Client, GetRangeOpts};

#[tokio::main]
async fn main() {
    let mut client = Client::new("http://0.0.0.0:50051".into())
        .await
        .expect("Unable to create client");

    let mut tx = client
        .transaction()
        .await
        .expect("Unable to create transaction");

    for (k, v) in tx
        .get_range(b"", b"kalJWDwajdwajdwadaw", GetRangeOpts::default())
        .await
        .expect("Gettingr ange")
    {
        println!(
            "{} = '{}'",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }

    tx.commit().await.expect("Unable to commit transaction");
}
