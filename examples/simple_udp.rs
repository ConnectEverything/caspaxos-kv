const SERVER_ADDRS: &[&str] = &["127.0.0.1:9999"];

async fn client() {
    // use any available local port
    let client_addr = "0.0.0.0:0";
    let timeout = std::time::Duration::from_millis(5);
    let mut client =
        caspaxos_kv::start_udp_client(client_addr, SERVER_ADDRS, timeout)
            .unwrap();
    client.get(b"k1".to_vec()).await.unwrap();
    client.set(b"k1".to_vec(), b"v1".to_vec()).await.unwrap();
    let current = client.get(b"k1".to_vec()).await.unwrap();
    client
        .cas(b"k1".to_vec(), current, Some(b"v2".to_vec()))
        .await
        .unwrap()
        .unwrap();
    client.get(b"k1".to_vec()).await.unwrap();
    client.del(b"k1".to_vec()).await.unwrap();
    client.get(b"k1".to_vec()).await.unwrap();
}

async fn server() {
    let timeout = std::time::Duration::from_millis(5);
    let mut server =
        caspaxos_kv::start_udp_server(SERVER_ADDRS[0], "storage_dir", timeout)
            .unwrap();
    server.run().await;
}

fn main() {
    smol::block_on(async {
        let server = smol::spawn(server());
        let client = smol::spawn(client());
        client.await;
        drop(server);
    });
}
