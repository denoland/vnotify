// Copyright 2018-2025 the Deno authors. MIT license.

use std::{
  cmp::Reverse,
  collections::{BinaryHeap, HashSet},
  sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant, SystemTime},
};

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, StreamExt, TryFutureExt};
use rand::Rng;
use tokio::{sync::oneshot, task::JoinSet};

/// Max number of keys returned in an S3 ListObjectsV2 response
pub const LIST_OBJECT_LIMIT: u32 = 1000;

pub struct VnotifyCache {
  _join_set: JoinSet<()>,
  shards: Arc<[Shard]>,
  client: aws_sdk_s3::Client,
  config: Config,
  fencing_queue_tx: tokio::sync::mpsc::UnboundedSender<oneshot::Sender<()>>,
}

// Configuration for a `VnotifyCache`.
#[derive(Clone, Debug)]
pub struct Config {
  /// S3 bucket name
  pub bucket: String,

  /// S3 bucket prefix
  pub prefix: String,

  /// Cache revalidation interval. Defaults to 1s.
  pub refresh_interval: Duration,

  /// Number of shards. This defaults to 1000, and normally should not be changed.
  /// All `VnotifyCache` instances that share the same S3 bucket and prefix must
  /// use the same `shards` value.
  ///
  /// Must be between 1 and `LIST_OBJECT_LIMIT` (1000), inclusively.
  pub shards: u32,

  /// Max number of stale-while-revalidate entries per shard after invalidation.
  /// Sorted by last access time, descending. Entries exceeding this limit will be dropped from cache.
  ///
  /// Defaults to 3.
  pub revalidate_limit: usize,

  time_base: Instant,
}

struct Shard {
  entries: moka::future::Cache<Box<str>, Option<Arc<Entry>>>,
}

struct Entry {
  body: Bytes,
  object_etag: String,
  last_used_ms: AtomicU64,
}

impl Config {
  pub fn with_bucket_and_prefix(bucket: String, prefix: String) -> Self {
    Self {
      bucket,
      prefix,
      refresh_interval: Duration::from_secs(1),
      shards: LIST_OBJECT_LIMIT,
      revalidate_limit: 3,
      time_base: Instant::now(),
    }
  }
}

impl VnotifyCache {
  pub fn new(client: aws_sdk_s3::Client, config: Config) -> Self {
    assert!(config.shards >= 1 && config.shards <= LIST_OBJECT_LIMIT);
    tracing::info!(
      num_shards = config.shards,
      bucket = config.bucket,
      prefix = config.prefix,
      refresh_interval = ?config.refresh_interval,
      "created vnotify cache"
    );
    let shards = (0..config.shards)
      .map(|_| Shard {
        entries: moka::future::Cache::builder()
          .time_to_idle(Duration::from_secs(600))
          .support_invalidation_closures()
          .build(),
      })
      .collect::<Arc<[Shard]>>();
    let (fencing_queue_tx, fencing_queue_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut join_set: JoinSet<()> = JoinSet::new();
    join_set.spawn(refresh_worker(
      client.clone(),
      config.clone(),
      shards.clone(),
      fencing_queue_rx,
    ));
    Self {
      _join_set: join_set,
      client,
      shards,
      config,
      fencing_queue_tx,
    }
  }

  pub async fn try_get(&self, key: &str) -> Option<Bytes> {
    let shard_index = get_shard_index_for_key(key, self.config.shards);
    let shard = &self.shards[shard_index];
    shard
      .entries
      .get(key)
      .await
      .flatten()
      .map(|x| x.body.clone())
  }

  pub async fn get(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
    let shard_index = get_shard_index_for_key(key, self.config.shards);
    let shard = &self.shards[shard_index];
    shard
      .entries
      .try_get_with(Box::from(key), async {
        let start_time = Instant::now();
        let (tx, rx) = oneshot::channel::<()>();

        // Perform fencing to ensure that any potential dependency of the newly seen object is fresh.
        // See ARCHITECTURE.md for more details.
        self
          .fencing_queue_tx
          .send(tx)
          .map_err(|_| "fencing_queue_tx closed".to_string())?;
        let x = get_from_origin(&self.client, &self.config, key, None).await?;
        let origin_fetch_duration = start_time.elapsed();
        match x {
          OriginResponse::Success(x) => {
            rx.await
              .map_err(|_| "failed to wait for fencing_queue response".to_string())?;
            let total_duration = start_time.elapsed();
            tracing::info!(
              key,
              origin_fetch_duration = ?origin_fetch_duration,
              total_duration = ?total_duration,
              "fetched object from origin"
            );
            Ok::<_, String>(Some(x))
          }
          _ => Ok(None),
        }
      })
      .map_ok(|x| {
        x.map(|x| {
          x.last_used_ms.store(
            self.config.time_base.elapsed().as_millis() as u64,
            Ordering::Relaxed,
          );
          x.body.clone()
        })
      })
      .map_err(|e| anyhow::anyhow!("failed to load object from s3: {:?}", e))
      .await
  }

  pub async fn put(&self, key: &str, body: Bytes) -> anyhow::Result<()> {
    let shard_index = get_shard_index_for_key(key, self.config.shards);
    let shard = &self.shards[shard_index];
    let res = self
      .client
      .put_object()
      .bucket(&self.config.bucket)
      .key(key)
      .body(ByteStream::from(body.clone()))
      .send()
      .await?;
    let object_etag = res.e_tag.unwrap_or_default();
    shard
      .entries
      .insert(
        Box::from(key),
        Some(Arc::new(Entry {
          body,
          object_etag,
          last_used_ms: AtomicU64::new(self.config.time_base.elapsed().as_millis() as u64),
        })),
      )
      .await;
    let body = Bytes::from(
      format!(
        "{},{}",
        SystemTime::now()
          .duration_since(SystemTime::UNIX_EPOCH)
          .unwrap()
          .as_millis(),
        faster_hex::hex_string(&rand::thread_rng().gen::<[u8; 16]>())
      )
      .into_bytes(),
    );
    self
      .client
      .put_object()
      .bucket(&self.config.bucket)
      .key(format!("{}{}", self.config.prefix, shard_index))
      .body(ByteStream::from(body.clone()))
      .send()
      .await?;
    self
      .client
      .put_object()
      .bucket(&self.config.bucket)
      .key(format!("{}_", self.config.prefix))
      .body(ByteStream::from(body))
      .send()
      .await?;
    Ok(())
  }
}

enum OriginResponse {
  Success(Arc<Entry>),
  NotModified,
  Missing,
}

async fn get_from_origin(
  client: &aws_sdk_s3::Client,
  config: &Config,
  key: &str,
  if_none_match: Option<String>,
) -> Result<OriginResponse, String> {
  let mut req = client.get_object().bucket(&config.bucket).key(key);
  if let Some(x) = if_none_match {
    req = req.if_none_match(x);
  }
  let res = req.send().await;
  match res {
    Ok(x) => x
      .body
      .collect()
      .await
      .map(|x| x.into_bytes())
      .map_err(|e| e.to_string())
      .map(|body| {
        OriginResponse::Success(Arc::new(Entry {
          body,
          object_etag: x.e_tag.unwrap_or_default(),
          last_used_ms: AtomicU64::new(config.time_base.elapsed().as_millis() as u64),
        }))
      }),

    Err(aws_sdk_s3::error::SdkError::ServiceError(x)) if x.raw().status().as_u16() == 304 => {
      Ok(OriginResponse::NotModified)
    }
    Err(aws_sdk_s3::error::SdkError::ServiceError(x))
      if matches!(
        x.err(),
        aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(_),
      ) =>
    {
      Ok(OriginResponse::Missing)
    }
    Err(error) => Err(error.to_string()),
  }
}
fn get_shard_index_for_key(key: &str, shards: u32) -> usize {
  (u64::from_le_bytes(<[u8; 8]>::try_from(&blake3::hash(key.as_bytes()).as_bytes()[..8]).unwrap())
    % shards as u64) as usize
}

async fn refresh_worker(
  client: aws_sdk_s3::Client,
  config: Config,
  shards: Arc<[Shard]>,
  mut fencing_queue_rx: tokio::sync::mpsc::UnboundedReceiver<oneshot::Sender<()>>,
) {
  struct Revalidation {
    shard_index: usize,
    key: Arc<Box<str>>,
    object_etag: String,
  }
  let mut global_etag: String = String::new();

  let mut etags: Vec<Box<str>> = (0..shards.len()).map(|_| Box::from("")).collect();

  let mut background = JoinSet::new();

  // moka worker
  let shards_clone = shards.clone();
  background.spawn(async move {
    loop {
      tokio::time::sleep(Duration::from_secs(5)).await;
      for shard in &*shards_clone {
        shard.entries.run_pending_tasks().await;
      }
    }
  });

  loop {
    let mut fencing_requests: Vec<oneshot::Sender<()>> = vec![];
    match tokio::time::timeout(config.refresh_interval, fencing_queue_rx.recv()).await {
      Ok(Some(x)) => {
        fencing_requests.push(x);
        while let Ok(x) = fencing_queue_rx.try_recv() {
          fencing_requests.push(x);
        }
      }
      Ok(None) => return,
      Err(_) => {}
    }
    let new_global_etag = match client
      .head_object()
      .bucket(&config.bucket)
      .key(format!("{}_", config.prefix))
      .send()
      .await
    {
      Ok(x) => x.e_tag.unwrap_or_default(),
      Err(aws_sdk_s3::error::SdkError::ServiceError(x))
        if matches!(
          x.err(),
          aws_sdk_s3::operation::head_object::HeadObjectError::NotFound(_),
        ) =>
      {
        String::new()
      }
      Err(error) => {
        tracing::error!(?error, "head_object failed");
        continue;
      }
    };
    let objects = if new_global_etag == global_etag {
      vec![]
    } else {
      tracing::info!(
        old_global_etag = global_etag,
        new_global_etag,
        "global etag changed, scanning prefix"
      );
      global_etag = new_global_etag;
      let res = client
        .list_objects_v2()
        .bucket(&config.bucket)
        .prefix(&config.prefix)
        .max_keys(LIST_OBJECT_LIMIT as i32)
        .send()
        .await;
      let res = match res {
        Ok(x) => x,
        Err(error) => {
          tracing::error!(?error, "refresh failed");
          continue;
        }
      };
      res.contents.unwrap_or_default()
    };
    let mut revalidate_queue: Vec<Revalidation> = vec![];
    for object in &objects {
      let Some(etag) = object.e_tag() else {
        continue;
      };
      let Some(key) = object.key().and_then(|x| x.strip_prefix(&config.prefix)) else {
        continue;
      };
      if key == "_" {
        continue;
      }
      let Ok(shard_index) = key.parse::<usize>() else {
        continue;
      };
      if shard_index >= config.shards as usize {
        continue;
      }
      if etag == &*etags[shard_index] {
        continue;
      }
      etags[shard_index] = Box::from(etag);
      let shard = &shards[shard_index];
      let num_entries = shard.entries.entry_count();
      if num_entries != 0 {
        #[allow(clippy::type_complexity)]
        let mut most_recent_entries: BinaryHeap<Reverse<(u64, Arc<Box<str>>, String)>> =
          BinaryHeap::new();
        let now_ms = config.time_base.elapsed().as_millis() as u64;
        for (k, v) in shard.entries.iter() {
          let Some(v) = v else {
            continue;
          };
          let last_used_ms = v.last_used_ms.load(Ordering::Relaxed);
          if now_ms.saturating_sub(last_used_ms) > 30_000 {
            continue;
          }
          most_recent_entries.push(Reverse((
            v.last_used_ms.load(Ordering::Relaxed),
            k,
            v.object_etag.clone(),
          )));
          if most_recent_entries.len() > config.revalidate_limit {
            most_recent_entries.pop();
          }
        }
        for entry in &most_recent_entries {
          revalidate_queue.push(Revalidation {
            shard_index,
            key: entry.0 .1.clone(),
            object_etag: entry.0 .2.clone(),
          });
        }
        let num_most_recent_entries = most_recent_entries.len();
        let most_recent_keys = most_recent_entries
          .into_iter()
          .map(|x| x.0 .1)
          .collect::<HashSet<_>>();
        shard
          .entries
          .invalidate_entries_if(move |k, _| !most_recent_keys.contains(k))
          .unwrap();
        tracing::info!(
          shard_index,
          num_entries,
          num_most_recent_entries,
          "evicting entries in shard"
        );
      }
    }

    let revalidation_start_time = Instant::now();
    let num_updated = AtomicUsize::new(0usize);
    let num_deleted = AtomicUsize::new(0usize);
    let num_unmodified = AtomicUsize::new(0usize);
    let num_failed = AtomicUsize::new(0usize);
    let mut revalidate_futures = Vec::with_capacity(revalidate_queue.len());
    for revalidation in revalidate_queue {
      let client = &client;
      let config = &config;
      let shards = &*shards;
      let num_updated = &num_updated;
      let num_deleted = &num_deleted;
      let num_unmodified = &num_unmodified;
      let num_failed = &num_failed;
      revalidate_futures.push(async move {
        match tokio::time::timeout(
          Duration::from_secs(5),
          get_from_origin(
            client,
            config,
            &revalidation.key,
            Some(revalidation.object_etag),
          ),
        )
        .await
        {
          Ok(Ok(x)) => match x {
            OriginResponse::Success(x) => {
              tracing::info!(
                shard_index = revalidation.shard_index,
                key = &**revalidation.key,
                "revalidation updated object"
              );
              shards[revalidation.shard_index]
                .entries
                .insert((*revalidation.key).clone(), Some(x))
                .await;
              num_updated.fetch_add(1, Ordering::Relaxed);
            }
            OriginResponse::Missing => {
              tracing::info!(
                shard_index = revalidation.shard_index,
                key = &**revalidation.key,
                "revalidation deleted object"
              );
              shards[revalidation.shard_index]
                .entries
                .insert((*revalidation.key).clone(), None)
                .await;
              num_deleted.fetch_add(1, Ordering::Relaxed);
            }
            OriginResponse::NotModified => {
              tracing::info!(
                shard_index = revalidation.shard_index,
                key = &**revalidation.key,
                "revalidation returned 304"
              );
              num_unmodified.fetch_add(1, Ordering::Relaxed);
            }
          },
          Ok(Err(error)) => {
            tracing::error!(
              shard_index = revalidation.shard_index,
              key = &**revalidation.key,
              error,
              "error while revalidating entry, deleting from cache"
            );
            shards[revalidation.shard_index]
              .entries
              .invalidate(&*revalidation.key)
              .await;
            num_failed.fetch_add(1, Ordering::Relaxed);
          }
          Err(_) => {
            tracing::error!(
              shard_index = revalidation.shard_index,
              key = &**revalidation.key,
              "revalidation timeout, deleting from cache"
            );
            shards[revalidation.shard_index]
              .entries
              .invalidate(&*revalidation.key)
              .await;
            num_failed.fetch_add(1, Ordering::Relaxed);
          }
        }
      });
    }
    let num_revalidations = revalidate_futures.len();
    FuturesUnordered::from_iter(revalidate_futures.into_iter())
      .collect::<Vec<()>>()
      .await;
    if num_revalidations != 0 {
      tracing::info!(
        duration = ?revalidation_start_time.elapsed(),
        num_revalidations,
        num_updated = num_updated.load(Ordering::Relaxed),
        num_deleted = num_deleted.load(Ordering::Relaxed),
        num_unmodified = num_unmodified.load(Ordering::Relaxed),
        num_failed = num_failed.load(Ordering::Relaxed),
        "completed revalidations");
    }
    for fencing_request in fencing_requests {
      let _ = fencing_request.send(());
    }
  }
}
