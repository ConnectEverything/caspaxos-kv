use caspaxos::{simulate, Client, VersionedValue};
use smol::Task;

const N_SUCCESSES: usize = 10;

fn increment(vv: &VersionedValue) -> Vec<u8> {
    let current = if let Some(ref value) = vv.value {
        *value.last().unwrap()
    } else {
        0
    };
    vec![current + 1]
}

// this client reads the previous value and tries to cas += 1 to it `N_SUCCESSES` times.
fn cas_client(mut client: Client) -> Task<Vec<VersionedValue>> {
    Task::spawn(async move {
        let key = || b"k1".to_vec();

        // assume initial value of 0:None, which is the value for all non-set items
        let mut last_known = VersionedValue {
            ballot: 0,
            value: None,
        };

        let mut witnessed: Vec<VersionedValue> = vec![last_known.clone()];

        let mut successes = 0;

        while successes < N_SUCCESSES {
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
                    log::debug!(
                        "{} successful cas from {:?} to {:?}",
                        client.net.address.port(),
                        last_known.value,
                        new_vv.value
                    );
                    last_known = new_vv;
                    successes += 1;
                }
                Ok(Err(current_vv)) => {
                    log::debug!(
                        "{} failure to cas from {:?} to {:?}",
                        client.net.address.port(),
                        last_known.value,
                        incremented
                    );

                    witnessed.push(current_vv.clone());
                    last_known = current_vv;
                }
                _ => {
                    log::trace!(
                        "{} io error with request, retrying",
                        client.net.address.port()
                    );
                }
            }
            smol::Timer::new(std::time::Duration::from_millis(10)).await;
        }

        log::trace!(
            "{} witnessed: {:?}",
            client.net.address.port(),
            witnessed
                .iter()
                .map(|vv| vv.value.clone())
                .collect::<Vec<_>>()
        );
        witnessed
    })
}

#[test]
fn cas_exact_writes() {
    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

    env_logger::init();

    let n_servers = 5;
    let n_clients = 15;

    // network never loses messages
    let lossiness = None;

    // network never times requests out
    let timeout = None;

    let clients =
        vec![cas_client as fn(Client) -> Task<Vec<VersionedValue>>; n_clients];

    let client_witnessed_values =
        simulate(lossiness, n_servers, clients, timeout);

    let last_value = &client_witnessed_values
        .into_iter()
        .map(|mut wv| wv.last_mut().unwrap().value.take())
        .max()
        .unwrap();

    let expected_last_value = &Some(vec![n_clients as u8 * N_SUCCESSES as u8]);

    assert_eq!(
        last_value, expected_last_value,
        "last value did not equal {:?}: {:?}",
        expected_last_value, last_value,
    );
}

#[test]
fn cas_monotonicity() {
    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

    env_logger::init();

    let n_servers = 3;
    let n_clients = 15;

    // drop 1 in 10 messages
    let lossiness = Some(10);

    // time-out requests after 10 ms
    let timeout = Some(std::time::Duration::from_millis(10));

    let clients =
        vec![cas_client as fn(Client) -> Task<Vec<VersionedValue>>; n_clients];

    let client_witnessed_values =
        simulate(lossiness, n_servers, clients, timeout);

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

    env_logger::init();

    fn basic_client_ops(mut client: Client) -> Task<()> {
        Task::local(async move {
            let responses = client.ping().await;
            log::trace!("majority pinger got {} responses", responses);

            let set = client.set(b"k1".to_vec(), b"v1".to_vec()).await;
            log::trace!("set response: {:?}", set);
        })
    }

    let n_servers = 3;

    // drop no messages
    let lossiness = None;

    // network never times requests out
    let timeout = None;

    let clients = vec![basic_client_ops as fn(Client) -> Task<()>];

    simulate(lossiness, n_servers, clients, timeout);
}
