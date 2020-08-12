use caspaxos_kv::{simulate, Client, VersionedValue};
use smol::Task;

const N_SUCCESSES: usize = 3;

fn increment(vv: &VersionedValue) -> Vec<u8> {
    let current = if let Some(ref value) = vv.value {
        *value.last().unwrap()
    } else {
        0
    };
    vec![current + 1]
}

// this client reads the previous value and tries to cas += 1 to it `N_SUCCESSES` times.
fn cas_client(mut client: Client) -> Task<(Vec<VersionedValue>, usize)> {
    Task::local(async move {
        let key = || b"k1".to_vec();

        // assume initial value of 0:None, which is the value for all non-set items
        let mut last_known = VersionedValue {
            ballot: 0,
            value: None,
        };

        let mut witnessed: Vec<VersionedValue> = vec![last_known.clone()];

        let mut successes = 0;
        let mut timeouts = 0;

        while successes < N_SUCCESSES {
            let incremented = increment(&last_known);

            log::debug!(
                "{} trying to cas from {:?} to {:?}",
                client.net.address.port(),
                last_known.value,
                incremented
            );

            let res = client
                .cas(key(), last_known.clone(), Some(incremented.clone()))
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
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    log::trace!(
                        "{} timeout io error with request, retrying",
                        client.net.address.port()
                    );
                    timeouts += 1;
                }
                Err(other) => {
                    log::trace!(
                        "{} non-timeout io error with request, retrying: {:?}",
                        client.net.address.port(),
                        other
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
        (witnessed, timeouts)
    })
}

#[test]
fn cas_exact_writes() {
    #[cfg(feature = "pretty_backtrace")]
    color_backtrace::install();

    env_logger::init();

    // NB If this test ever fails, try reducing each
    // of these variables to simplify the network trace
    // to figure out what is going on. It's effective to
    // reduce concurrency to 1, but increase tests_per_thread
    // while reducing n_clients to 2 and n_servers to 2 or 3.
    // This will run a bunch of tests with a single thread
    // while producing relatively short histories to try
    // to understand and debug. Keep these numbers high
    // before a bug is known to exist though, since
    // it will cause a lot more testing to happen.
    let n_servers = 3;
    let n_clients = 15;
    let concurrency = 30;
    let tests_per_thread = 10;

    let mut threads = vec![];
    for _ in 0..concurrency {
        let thread = std::thread::spawn(move || {
            for _ in 0..tests_per_thread {
                log::info!("running new test");
                // network never loses messages
                let lossiness = None;

                // network never times requests out
                let timeout = None;

                let clients =
                    vec![
                        cas_client
                            as fn(Client) -> Task<(Vec<VersionedValue>, usize)>;
                        n_clients
                    ];

                let client_witnessed_values =
                    simulate(lossiness, n_servers, clients, timeout);

                let last_value = &client_witnessed_values
                    .iter()
                    .map(|wv| wv.0.last().unwrap().clone())
                    .max()
                    .unwrap();

                let timeouts = &client_witnessed_values
                    .iter()
                    .map(|wv| wv.1)
                    .sum::<usize>();

                let max_expected_last_value = &Some(vec![
                    (n_clients as u8 * N_SUCCESSES as u8) + (*timeouts as u8),
                ]);

                assert!(
                    &last_value.value <= max_expected_last_value,
                    "last value was not less than or equal to {:?}: {:?}",
                    max_expected_last_value,
                    last_value,
                );
            }
        });
        threads.push(thread);
    }

    for thread in threads.into_iter() {
        thread.join().unwrap();
    }
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

    let clients = vec![
        cas_client
            as fn(Client) -> Task<(Vec<VersionedValue>, usize)>;
        n_clients
    ];

    let client_witnessed_values =
        simulate(lossiness, n_servers, clients, timeout);

    for (history, _timeouts) in client_witnessed_values {
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
