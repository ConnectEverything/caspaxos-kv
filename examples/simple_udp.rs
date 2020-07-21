use smol::Task;

const SERVER_ADDRS: &[&str] = &["127.0.0.1:9999"];

async fn client() {
    // use any available local port
    let client_addr = "0.0.0.0:0";
    let mut client =
        caspaxos::start_udp_client(client_addr, SERVER_ADDRS).unwrap();
    dbg!(client.get(b"k1".to_vec()).await.unwrap());
    dbg!(client.set(b"k1".to_vec(), b"v1".to_vec()).await.unwrap());
    let current = dbg!(client.get(b"k1".to_vec()).await.unwrap());
    dbg!(client
        .compare_and_swap(b"k1".to_vec(), current, Some(b"v2".to_vec()))
        .await
        .unwrap());
    let current = dbg!(client.get(b"k1".to_vec()).await.unwrap());
    let current = dbg!(client.del(b"k1".to_vec()).await.unwrap());
    let current = dbg!(client.get(b"k1".to_vec()).await.unwrap());
}

async fn server() {
    let mut server =
        caspaxos::start_udp_server(SERVER_ADDRS[0], "storage_dir").unwrap();
    server.run().await;
}

fn main() {
    smol::run(async {
        let server = Task::spawn(server());
        let client = Task::spawn(client());
        client.await;
    });
}
