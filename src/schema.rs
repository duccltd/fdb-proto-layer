use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub version: i64,
    pub namespace: String,
    pub resources: Vec<Resource>,
    pub tables: Vec<Table>,
}

impl Schema {
    pub fn get_table_by_proto(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.resource == name)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    #[serde(rename = "type")]
    pub type_field: String,
    pub name: String,
    pub location: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    #[serde(rename = "type")]
    pub type_field: String,
    pub resource: String,
    pub proto: String,
    pub indexes: Vec<Index>,
}

impl Table {
    // TODO: warning about having multiple value indexes
    pub fn get_value_index(&self) -> Option<&Index> {
        self.indexes.iter().find(|idx| idx.type_field == "value")
    }

    pub fn get_index_by_keys(&self, keys: Vec<String>) -> Option<&Index> {
        let key_hash = keys.join("_");
        self.indexes.iter().find(|idx| idx.keys.join("_") == key_hash)
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Index {
    #[serde(rename = "type")]
    pub type_field: String,
    pub keys: Vec<String>,
}

pub async fn load() -> Result<Schema, Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::open("schema.json").await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await.unwrap();

    let v: Schema = serde_json::from_str(&buf)?;

    // todo validate schema

    Ok(v)
}
