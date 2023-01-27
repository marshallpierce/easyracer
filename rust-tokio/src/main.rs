use anyhow::anyhow;
use futures::stream::FuturesOrdered;
use futures::{stream::FuturesUnordered, StreamExt};
use log::{debug, warn};
use std::{fmt, time};

#[tokio::main]
async fn main() {
    env_logger::init();

    let scenarios = Scenarios::new(reqwest::Url::parse("http://localhost:8080").unwrap());

    println!("scenario 1: {:?}", scenarios.scenario1().await);
    println!("scenario 2: {:?}", scenarios.scenario2().await);
    println!("scenario 3: {:?}", scenarios.scenario3().await);
    println!("scenario 4: {:?}", scenarios.scenario4().await);
    println!("scenario 5: {:?}", scenarios.scenario5().await);
    println!("scenario 6: {:?}", scenarios.scenario6().await);
    println!("scenario 7: {:?}", scenarios.scenario7().await);
    println!("scenario 8: {:?}", scenarios.scenario8().await);
    println!("scenario 9: {:?}", scenarios.scenario9().await);
}

pub struct Scenarios {
    base_url: reqwest::Url,
    client: reqwest::Client,
}

impl Scenarios {
    fn new(base_url: reqwest::Url) -> Self {
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    /// Race 2 concurrent requests
    pub async fn scenario1(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        tokio::select! {
            v = http_call(self.client.clone(), self.url("1")) => v,
            v = http_call(self.client.clone(), self.url("1")) => v
        }
    }

    /// Race 2 concurrent requests, where one produces a connection error
    async fn scenario2(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        tokio::select! {
            Ok(r) = http_call(self.client.clone(), self.url("2")) => Ok(r),
            Ok(r) = http_call(self.client.clone(), self.url("2")) => Ok(r),
            else => Err(anyhow!("no successful requests"))
        }
    }

    /// Race 10,000 concurrent requests
    async fn scenario3(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        first_matching(
            (0..10_000).map(|_| {
                let client = self.client.clone();
                let url = self.url("3");

                tokio::spawn(async move { http_call(client, url).await })
            }),
            |_| true,
        )
        .await
    }

    /// Race 2 concurrent requests but 1 of them should have a 1 second timeout
    async fn scenario4(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        let max_wait = time::Duration::from_secs(1);
        tokio::select! {
            // the first request succeeded
            Ok(v) = http_call(self.client.clone(), self.url("4")) => Ok(v),
            // the second request succeeded inside its timeout
            Ok(Ok(v)) = tokio::time::timeout(max_wait, http_call(self.client.clone(), self.url("4"))) => Ok(v),
            else => Err(anyhow!("no successful requests"))
        }
    }

    /// Race 2 concurrent requests where the winner is a 20x response
    async fn scenario5(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        self.first_2xx(2, "5").await
    }

    /// Race 3 concurrent requests where the winner is a 20x response
    async fn scenario6(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        self.first_2xx(3, "6").await
    }

    /// Start a request, wait at least 3 seconds then start a second request (hedging)
    async fn scenario7(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        tokio::select! {
            v = http_call(self.client.clone(), self.url("7")) => v,
            v = {
                    tokio::time::sleep(time::Duration::from_secs(3)).await;
                    http_call(self.client.clone(), self.url("7"))
                } => v
        }
    }
    /// Race 2 concurrent requests that "use" a resource which is obtained and released through
    /// other requests.
    async fn scenario8(&self) -> anyhow::Result<(String, reqwest::StatusCode)> {
        async fn use_flow(
            base_url: reqwest::Url,
            client: reqwest::Client,
        ) -> anyhow::Result<(String, reqwest::StatusCode)> {
            let mut open_url = base_url.clone();
            open_url.set_query(Some("open"));
            let (open_resp, _) = http_call(client.clone(), open_url).await?;

            let mut use_url = base_url.clone();
            use_url.query_pairs_mut().append_pair("use", &open_resp);
            let (use_resp, use_status) = http_call(client.clone(), use_url).await?;

            let mut close_url = base_url.clone();
            close_url.query_pairs_mut().append_pair("close", &open_resp);
            let (_, _) = http_call(client.clone(), close_url).await?;
            if (200..300).contains(&use_status.as_u16()) {
                return Err(anyhow!("Use status: {}", use_status));
            }

            Ok((use_resp, use_status))
        }

        first_matching(
            (0..2).map(|_| {
                let url = self.url("8");
                let client = self.client.clone();
                tokio::spawn(async move { use_flow(url, client).await })
            }),
            |_| true,
        )
        .await
    }

    /// Make 10 concurrent requests where 5 return a 200 response with a letter, when assembled
    /// in order of when they responded, form the "right" answer
    async fn scenario9(&self) -> anyhow::Result<String> {
        let letters = (0..10)
            .map(|_| http_call(self.client.clone(), self.url("9")))
            .collect::<FuturesOrdered<_>>()
            .filter_map(|res| async {
                res.ok()
                    .filter(|(_resp, status)| (200..300).contains(&status.as_u16()))
                    .map(|(resp, _)| resp)
            })
            .collect::<Vec<_>>()
            .await;

        Ok(letters.join(""))
    }

    async fn first_2xx(
        &self,
        count: u32,
        path: &str,
    ) -> anyhow::Result<(String, reqwest::StatusCode)> {
        first_matching(
            (0..count).map(|_| {
                let client = self.client.clone();
                let url = self.url(path);

                tokio::spawn(async move {
                    http_call(client, url).await.and_then(|(text, status)| {
                        if (200..300).contains(&status.as_u16()) {
                            Ok((text, status))
                        } else {
                            Err(anyhow!("Bad status: {}", status))
                        }
                    })
                })
            }),
            |_| true,
        )
        .await
    }

    fn url(&self, path: &str) -> reqwest::Url {
        self.base_url.join(path).unwrap()
    }
}

async fn http_call(
    client: reqwest::Client,
    url: reqwest::Url,
) -> anyhow::Result<(String, reqwest::StatusCode)> {
    let resp = client.get(url).send().await?;
    let status = resp.status();
    Ok((resp.text().await?, status))
}

/// Return the first result that matches the predicate
async fn first_matching<
    T: fmt::Debug,
    I: Iterator<Item = tokio::task::JoinHandle<anyhow::Result<T>>>,
    P: Fn(&anyhow::Result<T>) -> bool,
>(
    iterator: I,
    predicate: P,
) -> anyhow::Result<T> {
    let mut futures = iterator.collect::<FuturesUnordered<_>>();

    loop {
        match futures.next().await.ok_or_else(||anyhow!("ran out of futures"))? {
            // returning will drop and thus cancel the remaining futures
            Ok(res) => {
                if predicate(&res) {
                    return res;
                } else {
                    debug!("Skipping {:?}", res)
                }
            }
            Err(e) => warn!("Task panicked: {:?}", e),
        }
    }
}
