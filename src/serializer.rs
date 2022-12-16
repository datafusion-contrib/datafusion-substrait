use crate::producer;

use datafusion::error::Result;
use datafusion::prelude::*;

use prost::Message;
use substrait::protobuf::Plan;

use std::fs::OpenOptions;
use std::io::{Read, Write};

pub async fn serialize(sql: &str, ctx: &SessionContext, path: &str) -> Result<()> {
    let df = ctx.sql(sql).await?;
    let plan = df.to_logical_plan()?;
    let proto = producer::to_substrait_plan(&plan)?;

    let mut protobuf_out = Vec::<u8>::new();
    proto.encode(&mut protobuf_out).unwrap();
    let mut file = OpenOptions::new().create(true).write(true).open(path)?;
    file.write_all(&protobuf_out)?;
    Ok(())
}

pub async fn deserialize(path: &str) -> Result<Box<Plan>> {
    let mut protobuf_in = Vec::<u8>::new();

    let mut file = OpenOptions::new().read(true).open(path)?;

    file.read_to_end(&mut protobuf_in)?;
    deserialize_bytes(protobuf_in).await
}

pub async fn deserialize_bytes(proto_bytes: Vec<u8>) -> Result<Box<Plan>> {
    Ok(Box::new(Message::decode(&*proto_bytes).unwrap()))
}
