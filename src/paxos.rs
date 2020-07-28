use std::{collections::HashMap, io, net::SocketAddr, time::Duration};

use rand::{thread_rng, Rng};
use smol::{Task, Timer};

use super::*;

fn backoff_generator() -> impl FnMut() -> Timer {
    let mut backoff = 0;

    move || {
        backoff += 1;
        // Exponential backoff up to 1<<5 ms = 32 ms
        let truncated_exponential_backoff =
            Duration::from_millis(1 << backoff.min(5));
        let mut rng = thread_rng();
        let randomized_amount = Duration::from_micros(rng.gen_range(0, 3200));
        let total_backoff = truncated_exponential_backoff + randomized_amount;
        Timer::new(total_backoff)
    }
}

fn timeout() -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::TimedOut, "request timed out"))
}

#[derive(Debug, Default)]
pub struct CacheEntry {
    value: VersionedValue,
    last_attempt_was_successful: bool,
}

#[derive(Debug)]
pub struct Client {
    pub known_servers: Vec<SocketAddr>,
    pub net: Net,
    pub cache: HashMap<Vec<u8>, CacheEntry>,
    pub processor: Option<Task<io::Result<()>>>,
}

impl Client {
    /// Send a `Ping` to the `Server`, which
    /// hopefully will respond with a `Pong`.
    pub async fn ping(&mut self) -> usize {
        self.majority(Request::Ping, Response::is_pong).await.len()
    }

    /// Get the value associated with a key, if any.
    pub async fn get<K: AsRef<[u8]>>(
        &mut self,
        key: K,
    ) -> io::Result<VersionedValue> {
        let (_old_vv, new_vv) =
            self.consensus(key.as_ref(), |v| v.value.clone()).await?;
        Ok(new_vv)
    }

    /// Set a key to a new value. Returns the previous set value.
    pub async fn set<K, V>(
        &mut self,
        key: K,
        value: V,
    ) -> io::Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let (old_vv, _new_vv) = self
            .consensus(key.as_ref(), |_| Some(value.as_ref().to_vec()))
            .await?;
        Ok(old_vv.value)
    }

    /// Delete a value associated with a key. Returns the previously set value, if any.
    pub async fn del<K: AsRef<[u8]>>(
        &mut self,
        key: K,
    ) -> io::Result<Option<Vec<u8>>> {
        let (old_vv, _new_vv) = self.consensus(key.as_ref(), |_| None).await?;
        Ok(old_vv.value)
    }

    /// Given a previous versioned value, either set (with `Some`) or
    /// delete (with `None`) a new value. Returns either `Ok(new_version)`
    /// or `Err(current_versioned_value)`. Returns an error if the old value is not
    /// correctly guessed.
    pub async fn compare_and_swap<K: AsRef<[u8]>>(
        &mut self,
        key: K,
        old: VersionedValue,
        new: Option<Vec<u8>>,
    ) -> io::Result<Result<VersionedValue, VersionedValue>> {
        let (old_vv, new_vv) = self
            .consensus(key.as_ref(), |old_vv| {
                if &old == old_vv {
                    new.clone()
                } else {
                    old_vv.value.clone()
                }
            })
            .await?;

        if old_vv == old {
            Ok(Ok(new_vv))
        } else {
            Ok(Err(new_vv))
        }
    }

    async fn majority<F, B>(&mut self, request: Request, transform: F) -> Vec<B>
    where
        F: Fn(Response) -> B,
    {
        let responses = self
            .net
            .request_multi(
                &self.known_servers,
                request,
                (self.known_servers.len() / 2) + 1,
            )
            .await;

        responses.into_iter().map(transform).collect()
    }

    async fn prepare(
        &mut self,
        key: &[u8],
    ) -> io::Result<(VersionedValue, u64)> {
        // may be skipped in subsequent rounds if `last_attempt_was_successful`
        let mut backoff = backoff_generator();
        loop {
            let last_known_entry = self.cache.entry(key.to_vec()).or_default();

            if last_known_entry.last_attempt_was_successful {
                let last_vv = last_known_entry.value.clone();
                // add 1 to the previous successful ballot
                let next_ballot = last_known_entry.value.ballot + 1;
                return Ok((last_vv, next_ballot));
            }

            // must gather a majority of successful Promise responses
            // before we can correctly move on to phase 2

            // propose the last ballot + 1
            let proposed_ballot = last_known_entry.value.ballot + 1;

            let promises = self
                .majority(
                    Request::Prepare {
                        ballot: proposed_ballot,
                        key: key.to_vec(),
                    },
                    Response::to_promise,
                )
                .await;

            let success = promises.iter().filter(|p| p.is_ok()).count()
                > self.known_servers.len() / 2;

            if success {
                let last_vv = promises
                    .into_iter()
                    .filter_map(|p| p.ok())
                    .max()
                    .unwrap()
                    .clone();

                // we can now safely move on to phase 2 after gathering
                // a majority of promises for our proposed ballot

                return Ok((last_vv, proposed_ballot));
            }

            let was_retryable = promises.len() > self.known_servers.len() / 2;

            if !was_retryable {
                timeout()?;
            }

            // retry
            let last_err_ballot =
                promises.into_iter().filter_map(|p| p.err()).max();

            if let Some(ballot) = last_err_ballot {
                if ballot >= proposed_ballot {
                    let cache_entry = CacheEntry {
                        last_attempt_was_successful: false,
                        value: VersionedValue {
                            ballot,
                            value: None,
                        },
                    };
                    self.cache.insert(key.to_vec(), cache_entry);
                }
            }

            backoff().await;
        }
    }

    // if successful in applying a function to some state,
    // returns `Ok((old_version, new_version))`.
    async fn consensus<F>(
        &mut self,
        key: &[u8],
        transform: F,
    ) -> io::Result<(VersionedValue, VersionedValue)>
    where
        F: Fn(&VersionedValue) -> Option<Vec<u8>>,
    {
        let mut backoff = backoff_generator();

        loop {
            // phase 1: prepare
            let (last_vv, proposed_ballot): (VersionedValue, u64) =
                self.prepare(key).await?;

            // phase 2: accept
            let new_value = transform(&last_vv);
            let new_vv = VersionedValue {
                ballot: proposed_ballot,
                value: new_value,
            };
            let accepts = self
                .majority(
                    Request::Accept {
                        key: key.to_vec(),
                        value: new_vv.clone(),
                    },
                    Response::to_accepted,
                )
                .await;

            if accepts.is_empty() {
                continue;
            }

            let successes = accepts.iter().filter(|p| p.is_ok()).count();
            let was_successful = successes > self.known_servers.len() / 2;

            if was_successful {
                let cache_entry = CacheEntry {
                    last_attempt_was_successful: true,
                    value: new_vv.clone(),
                };
                self.cache.insert(key.to_vec(), cache_entry);
                return Ok((last_vv, new_vv));
            }

            // retry
            let was_retryable = accepts.len() > self.known_servers.len() / 2;

            if !was_retryable {
                timeout()?;
            }

            let last_err_vv = accepts
                .into_iter()
                .filter(|p| p.is_err())
                .map(|p| p.unwrap_err())
                .max();

            if let Some(last_err_vv) = last_err_vv {
                if last_err_vv.ballot >= proposed_ballot {
                    let cache_entry = CacheEntry {
                        last_attempt_was_successful: false,
                        value: last_err_vv,
                    };
                    self.cache.insert(key.to_vec(), cache_entry);
                }
            }

            backoff().await;
        }
    }
}

#[derive(Debug)]
pub struct Server {
    pub net: Net,
    pub db: versioned_storage::VersionedStorage,
    pub processor: Option<Task<io::Result<()>>>,
    pub promises: HashMap<Vec<u8>, u64>,
}

impl Server {
    /// Respond to received messages in a loop
    pub async fn run(&mut self) {
        loop {
            let (from, uuid, request) = self.net.receive().await.unwrap();
            let response = match request {
                Request::Ping => Response::Pong,
                Request::Prepare { key, ballot } => {
                    let current_promise: u64 =
                        self.promises.get(&key).cloned().unwrap_or(0);
                    let current_value: Option<VersionedValue> =
                        self.db.get(&key);
                    let current_ballot = current_value
                        .as_ref()
                        .map(|vv| vv.ballot + 1)
                        .unwrap_or(0)
                        .max(current_promise);

                    let successful = current_ballot < ballot;

                    if successful {
                        self.promises.insert(key.clone(), ballot);
                    }

                    let current_value = current_value.unwrap_or_default();

                    let success = if successful {
                        println!(
                            "{} returning successful promise to {} (promised: {}, proposed: {}) with value {:?}",
                            self.net.address.port(),
                            from.port(),
                            current_ballot, ballot,
                            current_value
                        );
                        Ok(current_value)
                    } else {
                        println!(
                            "{} returning failed promise to {} (promised: {}, proposed: {}) with value {:?}",
                            self.net.address.port(),
                            from.port(),
                            current_ballot, ballot,
                            current_value
                        );

                        Err(current_ballot)
                    };

                    Response::Promise { success }
                }
                Request::Accept { key, value } => {
                    let promise = self.promises.get(&key).unwrap_or(&0);
                    let success = if *promise > value.ballot {
                        let current = self.db.get(&key).unwrap_or_default();
                        Err(current.clone())
                    } else {
                        self.db.update_if_newer(&key, value)
                    };
                    Response::Accepted { success }
                }
            };
            if let Err(e) = self.net.respond(from, uuid, response).await {
                println!("failed to respond to client: {:?}", e);
            }
        }
    }
}
