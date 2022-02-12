use fdb_proto_client::client::{Client, GetRangeOpts, KeySet};
use prost::Message;
use protos::Foo;

pub mod protos {
    tonic::include_proto!("protos");
}

#[tokio::main]
async fn main() {
    let mut client = Client::new("http://0.0.0.0:50051".into())
        .await
        .expect("Unable to create client");

    let mut tx = client
        .transaction()
        .await
        .expect("Unable to create transaction");

    let foo = Foo {
        foo_id: "123".into(),
        bar: "456".into(),
        baz: "789".into(),
    };
    let mut encoded_foo = vec![];
    foo.encode(&mut encoded_foo).unwrap();

    tx.set_proto("protos.Foo", &encoded_foo)
        .await
        .expect("setting proto");

    let proto = tx
        .get_proto(
            "protos.Foo",
            KeySet::builder().key("foo_id", "123".as_bytes()),
        )
        .await
        .expect("getting proto");

    println!("{:?}", proto);

    tx.commit().await.expect("Unable to commit transaction");
}
