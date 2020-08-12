use smol::Task;

const SERVER_ADDRS: &[&str] = &["127.0.0.1:9999"];
const OPS: u128 = 200_000;

async fn client(key: Vec<u8>) -> usize {
    // use any available local port
    let client_addr = "0.0.0.0:0";
    let timeout = std::time::Duration::from_millis(5);
    let mut client =
        caspaxos_kv::start_udp_client(client_addr, SERVER_ADDRS, timeout)
            .unwrap();

    let mut errors = 0_usize;
    let value = b"v1".to_vec();
    for _ in 0..OPS {
        if client.set(&key, &value).await.is_err() {
            errors += 1;
        }
    }
    errors
}

async fn server() {
    let timeout = std::time::Duration::from_millis(5);
    let mut server =
        caspaxos_kv::start_udp_server(SERVER_ADDRS[0], "storage_dir", timeout)
            .unwrap();
    server.run().await;
}

fn main() {
    let start = std::time::Instant::now();
    smol::run(async {
        let server = Task::spawn(server());
        let clients = vec![
            Task::spawn(client(b"k1".to_vec())),
            //Task::spawn(client(b"k1".to_vec())),
            //Task::spawn(client(b"k3".to_vec())),
            //Task::spawn(client(b"k4".to_vec())),
            //Task::spawn(client(b"k5".to_vec())),
        ];

        let mut errors = 0;
        for client in clients {
            errors += client.await;
        }
        let throughput = OPS * 1000 / start.elapsed().as_millis();

        println!("throughput: {}/s, {} errors", throughput, errors);
        drop(server);
    });
}
