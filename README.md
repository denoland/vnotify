# vnotify

`vnotify` ("vectorized notification") is a lightweight library for monitoring S3
changes efficiently without external dependencies. It allows to detect changes
on millions of objects with one `HeadObject` and sometimes one `ListObjectsV2`
call.

`vnotify` uses a fixed-size S3 keyspace as a "hash map" to signal bucket-wide
changes. For example, when an update is made to `path/to/object`:

- an index into the hashmap is calculated as
  `vnotify_index = uint64_le(blake3("path/to/object")[0..8]) % vnotify_keyspace_size`
- a random string is written into `vnotify_prefix/vnotify_index`.
- a random string is written into `vnotify_prefix/_`.

The S3 `ListObjectsV2` API returns the ETag of each object, and a change in ETag
indicates that this "hash map" slot has changed and clients' local cache needs
to be invalidated for that shard.

AWS S3 `ListObjectsV2` can return a maximum of 1000 keys per invocation, so
`vnotify_keyspace_size` is set to `1000`. A client that wishes to listen for
changes only needs to make 1 `HeadObject` call to `vnotify_prefix/_` to check if
any changes have been made, and then make 1 `ListObjectsV2` call in order to
revalidate its entire local cache.

```
/vnotify/_
/vnotify/0
/vnotify/1
/vnotify/2
/vnotify/3
...
/vnotify/999
```

## Causal consistency

Certain use cases require causal consistency. Specifically, if an object is
created based on the assumption that a set of updates have been made to other
objects, a reader must not observe inversed causality.

`vnotify` solves this problem by forcing a revalidation after a newly-seen
object is loaded from S3. The revalidation `HeadObject` call happens
concurrently with the `GetObject` call, so this operation does not incur
additional latency in the optimistic case.
