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

#[derive(Debug)]
pub struct Client {
    pub known_servers: Vec<SocketAddr>,
    pub net: Net,
    pub cache: HashMap<Vec<u8>, VersionedValue>,
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

        // phase 1: prepare
        // may be skipped in subsequent rounds
        while !self.cache.contains_key(key) {
            let promises = self
                .majority(
                    Request::Prepare {
                        ballot: 1,
                        key: key.to_vec(),
                    },
                    Response::to_promise,
                )
                .await;

            let success = promises.iter().filter(|p| p.0).count()
                > self.known_servers.len() / 2;

            if success {
                let last_vv = promises
                    .into_iter()
                    .filter(|p| p.0)
                    .map(|p| p.1)
                    .max()
                    .unwrap();

                self.cache.insert(key.to_vec(), last_vv);

                break;
            }

            let was_retryable = promises.len() > self.known_servers.len() / 2;

            if !was_retryable {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "request timed out",
                ));
            }

            // retry
            let last_err_vv =
                promises.into_iter().filter(|p| !p.0).map(|p| p.1).max();

            if let Some(last_err_vv) = last_err_vv {
                self.cache.insert(key.to_vec(), last_err_vv);
            }

            backoff().await;
        }

        // phase 2: accept
        loop {
            let last_vv = self.cache.get(key).unwrap().clone();
            let new_value = transform(&last_vv);
            let ballot = last_vv.ballot + 1;
            let new_vv = VersionedValue {
                ballot,
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
                self.cache.insert(key.to_vec(), new_vv.clone());
                return Ok((last_vv, new_vv));
            }

            // retry
            let was_retryable = accepts.len() > self.known_servers.len() / 2;

            if !was_retryable {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "request timed out",
                ));
            }

            let last_err_vv = accepts
                .into_iter()
                .filter(|p| p.is_err())
                .map(|p| p.unwrap_err())
                .max();

            if let Some(last_err_vv) = last_err_vv {
                if last_err_vv.ballot >= ballot {
                    self.cache.insert(key.to_vec(), last_err_vv);
                } else {
                    println!("not updating ballot");
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
}

impl Server {
    /// Respond to received messages in a loop
    pub async fn run(&mut self) {
        loop {
            let (from, uuid, request) = self.net.receive().await.unwrap();
            let response = match request {
                Request::Ping => Response::Pong,
                Request::Prepare { key, ballot } => {
                    let current_value: Option<VersionedValue> =
                        self.db.get(&key);
                    let success =
                        current_value.as_ref().map(|cv| cv.ballot).unwrap_or(0)
                            < ballot;
                    Response::Promise {
                        success,
                        current_value: current_value.unwrap_or_default(),
                    }
                }
                Request::Accept { key, value } => Response::Accepted {
                    success: self.db.update_if_newer(&key, value),
                },
            };
            if let Err(e) = self.net.respond(from, uuid, response).await {
                println!("failed to respond to client: {:?}", e);
            }
        }
    }
}
