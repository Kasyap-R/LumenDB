use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

use super::construct_rdb;
use crate::resp::{resp_deserializer::RespParser, resp_serializer::serialize_resp_data, RespType};
use crate::Redis;

pub async fn handle_replconf(stream: Arc<RwLock<TcpStream>>) {
    let response = String::from("+OK\r\n");
    let mut stream = stream.write().await;
    let _ = stream.write_all(response.as_bytes()).await;
}

pub async fn handle_replconf_getack(stream: Arc<RwLock<TcpStream>>) {
    let response = RespType::Array(vec![
        RespType::BulkString(Some(String::from("REPLCONF"))),
        RespType::BulkString(Some(String::from("ACK"))),
        RespType::BulkString(Some(String::from("0"))),
    ]);

    let serialized_response = serialize_resp_data(response);

    let mut stream = stream.write().await;
    let _ = stream.write_all(serialized_response.as_bytes()).await;
}

pub async fn handle_psync(
    _replication_id: String,
    _offset: String,
    stream: Arc<RwLock<TcpStream>>,
    db: Arc<Mutex<HashMap<String, String>>>,
) {
    {
        let mut stream = stream.write().await;

        let repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        let response =
            serialize_resp_data(RespType::SimpleString(format!("FULLRESYNC {} 0", repl_id)));
        let (length, binary) = construct_rdb(Arc::clone(&db));

        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.write_all(length.as_bytes()).await;
        let _ = stream.write_all(&binary).await;
    }
}

async fn send_and_recieve(
    stream: Arc<RwLock<TcpStream>>,
    message: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = stream.write().await;
    // Write the message to the stream
    stream.write_all(message.as_bytes()).await?;
    stream.flush().await?;

    // Buffer to store the response
    let mut buf = [0; 1024];
    let n = stream.read(&mut buf).await?;

    // Convert the response to a String
    let response = String::from_utf8_lossy(&buf[..n]).to_string();
    Ok(response)
}
pub async fn perform_handshake(redis: &mut Redis) -> RespParser {
    use crate::resp::RespType;
    let ping: RespType = RespType::Array(vec![RespType::BulkString(Some(String::from("PING")))]);
    let repl_port = RespType::Array(vec![
        RespType::BulkString(Some(String::from("REPLCONF"))),
        RespType::BulkString(Some(String::from("listening-port"))),
        RespType::BulkString(Some(String::from(&format!("{}", redis.config.port)))),
    ]);
    let repl_capa = RespType::Array(vec![
        RespType::BulkString(Some(String::from("REPLCONF"))),
        RespType::BulkString(Some(String::from("capa"))),
        RespType::BulkString(Some(String::from("psync2"))),
    ]);
    let psync = RespType::Array(vec![
        RespType::BulkString(Some(String::from("PSYNC"))),
        RespType::BulkString(Some(String::from("?"))),
        RespType::BulkString(Some(String::from("-1"))),
    ]);

    let serialized_ping = serialize_resp_data(ping);
    let serialized_repl_port = serialize_resp_data(repl_port);
    let serialized_repl_capa = serialize_resp_data(repl_capa);
    let serialized_psync = serialize_resp_data(psync);
    redis.master_connection = match TcpStream::connect(format!(
        "{}:{}",
        redis.config.master_host.as_ref().unwrap(),
        redis.config.master_port.as_ref().unwrap()
    ))
    .await
    {
        Ok(x) => Some(Arc::new(RwLock::new(x))),
        Err(e) => panic!("{}", e),
    };

    let stream = Arc::clone(&redis.master_connection.as_ref().unwrap());
    let _ = send_and_recieve(Arc::clone(&stream), &serialized_ping).await;
    let _ = send_and_recieve(Arc::clone(&stream), &serialized_repl_port).await;
    let _ = send_and_recieve(Arc::clone(&stream), &serialized_repl_capa).await;
    let stream_data = send_and_recieve(Arc::clone(&stream), &serialized_psync)
        .await
        .expect("Failed to recieve repl_id from master at the end of handshake");
    // We read up to CRLF and then everything after is the contents of the RDB file
    // if we don't read as much as we expect, we read again, until we do
    // then the stream is empty enough
    println!("====== Recieiving Psync Response from Master ======");
    println!("{}", stream_data);
    println!("====== End of Psync Response from Master ==========");
    let mut parser = RespParser::new(stream_data, Arc::clone(&stream));
    let resync: String;
    let rdb: Vec<u8>;
    (resync, rdb) = parser.parse_handshake().await;
    parser
}
