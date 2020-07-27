use caspaxos::{simulate, Client, VersionedValue};
use smol::Task;

fn increment(vv: &VersionedValue) -> Vec<u8> {
    let current = if let Some(ref value) = vv.value {
        *value.last().unwrap()
    } else {
        0
    };
    vec![current + 1]
}

fn cas_client(mut client: Client) -> Task<Vec<VersionedValue>> {
    Task::local(async move {
        let key = || b"k1".to_vec();

        let mut last_known = VersionedValue {
            ballot: 0,
            value: None,
        };

        // last_knownize in a loop until successful (nested result may be Ok or Err depending on CAS
        // success)
        loop {
            if let Ok(actual) = client
                .compare_and_swap(key(), last_known.clone(), Some(vec![0]))
                .await
            {
                // we got a majority of responses, possibly successfully initialized
                // or someone else initialized the value before us.
                match actual {
                    Ok(new_vv) => {
                        last_known = new_vv;
                    }
                    Err(a) => last_known = a,
                }
                break;
            }
        }

        let mut witnessed: Vec<VersionedValue> = vec![last_known.clone()];

        let mut successes = 0;

        while successes < 10 {
            let incremented = increment(&last_known);

            let res = client
                .compare_and_swap(
                    key(),
                    last_known.clone(),
                    Some(incremented.clone()),
                )
                .await;

            match res {
                Ok(Ok(new_vv)) => {
                    witnessed.push(new_vv.clone());
                    last_known = new_vv;
                    successes += 1;
                }
                Ok(Err(current_vv)) => {
                    witnessed.push(current_vv.clone());
                    last_known = current_vv;
                }
                _ => {}
            }
        }

        witnessed
    })
}

#[test]
fn cas_linearizability() {
    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

    let n_servers = 3;
    let n_clients = 15;

    // drop 1 in 10 messages
    let lossiness = Some(10);

    let clients =
        vec![cas_client as fn(Client) -> Task<Vec<VersionedValue>>; n_clients];

    let client_witnessed_values = simulate(lossiness, n_servers, clients);

    for history in client_witnessed_values {
        for xs in history.windows(2) {
            let (x1, x2): (&VersionedValue, &VersionedValue) = (&xs[0], &xs[1]);
            assert!(x1 < x2, "non-linearizable history: {:?}", history);
        }
    }
}

#[test]
fn basic() {
    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

    fn basic_client_ops(mut client: Client) -> Task<()> {
        Task::local(async move {
            let responses = client.ping().await;
            println!("majority pinger got {} responses", responses);

            let set = client.set(b"k1".to_vec(), b"v1".to_vec()).await;
            println!("set response: {:?}", set);
        })
    }

    let n_servers = 3;

    // drop no messages
    let lossiness = None;

    let clients = vec![basic_client_ops as fn(Client) -> Task<()>];

    simulate(lossiness, n_servers, clients);
}
