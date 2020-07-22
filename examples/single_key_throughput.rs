use smol::Task;

const SERVER_ADDRS: &[&str] = &["127.0.0.1:9999"];
const OPS: u128 = 200_000;

async fn client() {
    // use any available local port
    let client_addr = "0.0.0.0:0";
    let mut client =
        caspaxos::start_udp_client(client_addr, SERVER_ADDRS).unwrap();
    let start = std::time::Instant::now();

    let mut errors = 0_usize;
    let key = b"k1".to_vec();
    let value = b"v1".to_vec();
    for _ in 0..OPS {
        if client.set(&key, &value).await.is_err() {
            errors += 1;
        }
    }

    let throughput = OPS * 1000 / start.elapsed().as_millis();

    println!("throughput: {}/s, {} errors", throughput, errors);
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
