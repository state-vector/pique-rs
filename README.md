# Pique

Pique makes selective queries over Parquet datasets on object storage fast. Really fast.

If you've ever waited seconds for DuckDB to scan hundreds of Parquet file headers just to find one record, or watched your Lambda timeout while S3 processes a glob across thousands of files — Pique solves that. It builds compact, immutable indexes alongside your Parquet files that tell your query engine exactly which files to read, turning seconds of metadata scanning into milliseconds of targeted access.

## The problem

You have analytical data in Parquet on S3. Hundreds or thousands of files. You need to find a specific record, or query a narrow time range, or filter by a high-cardinality column.

Without an index, your query engine opens every file, reads every footer, evaluates column statistics, and then — finally — reads the few files that actually matter. For 1000 files on S3, that's 1000 HTTP requests just to figure out where to look.

Most teams solve this by moving hot data into a database. Now you have two systems, two copies of the data, two consistency models, and twice the cost. You chose object storage for its economics and durability — shouldn't you be able to query it efficiently too?

## What Pique does

Pique builds a small index file (`.pique`) that sits next to your Parquet files on object storage. When your query arrives, instead of scanning every file's metadata, you read the Pique index — one or two HTTP range requests — and it tells you exactly which files and row groups contain your data.

Your query engine then reads only those files. Same DuckDB, same Parquet, same data — just fewer wasted requests.

## How much faster

We benchmarked Pique against DuckDB querying 1000 Parquet files (100 million rows) on S3:

| Query | Without Pique | With Pique | Speedup |
|-------|--------------|-----------|---------|
| Point lookup by ID | 562ms | 0ms (bloom rejected) | ∞ |
| 1-hour time range (1 file needed) | 737ms | 88ms | 8× |
| 24-hour range (24 files needed) | 1,572ms | 305ms | 5× |
| 7-day range (168 files needed) | 7,237ms | 1,217ms | 6× |
| Combined predicates (time + category) | 19,447ms | 98ms | **198×** |
| Uniform filter (all files needed) | 36,905ms | 6,494ms | 6× |

The combined-predicate query is the headline: from 19.4 seconds to 98 milliseconds. Same query engine, same data, same result — just told which file to read.

Even when no files can be pruned (the uniform filter case), passing an explicit file list is 6× faster than letting DuckDB resolve the glob. The glob resolution itself is expensive on object storage.

## How it works

A Pique segment is a single immutable file containing sorted key-value pairs with a bloom filter and an FST (Finite State Transducer) directory. The lookup pattern uses exactly two HTTP range requests on a cold start, and one on subsequent lookups:

1. **Tail read** — grab the last 256KB of the segment (one request). This loads the footer, bloom filter, and FST directory into memory. Cached for the process lifetime.
2. **Block read** — the FST tells you which 64KB data block contains your key. Fetch that block (one request). Binary search within it for the exact answer.

If the bloom filter says your key isn't in the index, you skip the second request entirely — the answer is "not here" in under a microsecond, with zero I/O.

## Designed for serverless

Pique was built for Lambda. Each partition of the index is independent and fits in memory. There's no background process, no coordination, no state to manage. Build an index during compaction, upload it to S3, and queries get faster immediately.

For datasets with billions of keys, Pique partitions the index into segments of about one million keys each. A manifest file routes queries to the right partition by key range. Adding more data means adding more partitions — lookup latency stays constant.

## What it supports

**Point lookups** — find a record by its key in 23ms from a same-region Lambda.

**Bloom filters** — non-existent keys are rejected in under a microsecond with zero I/O. The XOR8 filter has a 0.39% false positive rate at 1.2 bytes per key.

**Prefix search** — the FST directory supports efficient prefix iteration, useful for path-based lookups.

**Layered segments** — a base segment plus delta segments for incremental updates. New data produces small deltas instead of rebuilding the full index. Background merges keep the layer count bounded.

**Stats manifests** — per-file, per-row-group column statistics (min/max/null count) that enable range-predicate pruning without opening Parquet footers. Built from Parquet footer metadata (no data scan), queried via interval overlap: `WHERE ts BETWEEN a AND b` resolves to only the row groups whose min/max range overlaps the predicate. Multi-column predicates intersect their candidate sets (AND semantics).

**Adjacency lists** — compressed edge lists for graph traversal patterns, stored as index values.

## Quick start

```rust,no_run
# fn main() -> Result<(), Box<dyn std::error::Error>> {
use pique::{SegmentWriter, SegmentWriterOptions, SegmentReader};

// Build an index from sorted key-value pairs
let mut writer = SegmentWriter::new(SegmentWriterOptions::default());
writer.add(b"user_001", b"file_a.parquet#rg0")?;
writer.add(b"user_002", b"file_b.parquet#rg3")?;
writer.add(b"user_003", b"file_a.parquet#rg1")?;
let output = writer.finish()?;

// Upload output.data to S3 as a single object...

// Query: point lookup
let reader = SegmentReader::open(output.data)?;
let location = reader.get(b"user_002")?;
assert_eq!(location, Some(b"file_b.parquet#rg3".to_vec()));

// Non-existent keys are rejected by the bloom filter (no I/O)
assert_eq!(reader.get(b"user_999")?, None);
# Ok(())
# }
```

For S3-backed reads with the two-request pattern:

```rust,ignore
use pique::{RemoteSegmentReader, S3Backend};

let reader = RemoteSegmentReader::open(
    Box::new(S3Backend::from_env("my-bucket".into()).await),
    "indexes/users.pique".into(),
    None, // default 256KB tail budget
).await?;

// First lookup: 1 S3 range read (metadata already cached from open)
let result = reader.get(b"user_002").await?;
```

## Range-predicate pruning with stats manifests

For range queries (`WHERE ts BETWEEN a AND b`), the bloom filter can't help — it only handles equality. The stats manifest fills this gap by storing per-(file, row_group) column min/max values and answering interval-overlap queries:

```rust
use pique::stats::{StatsManifest, StatsManifestBuilder, ColumnStats, RangePredicate};

// Build manifest from Parquet footer metadata (no data scan needed)
let manifest = StatsManifestBuilder::new()
    .add_row_group("data/2024-01-15.parquet", 0, vec![
        ("timestamp".into(), ColumnStats {
            min: b"2024-01-15T00:00:00Z".to_vec(),
            max: b"2024-01-15T07:59:59Z".to_vec(),
            null_count: 0,
        }),
    ])
    .add_row_group("data/2024-01-15.parquet", 1, vec![
        ("timestamp".into(), ColumnStats {
            min: b"2024-01-15T08:00:00Z".to_vec(),
            max: b"2024-01-15T15:59:59Z".to_vec(),
            null_count: 0,
        }),
    ])
    .add_row_group("data/2024-01-15.parquet", 2, vec![
        ("timestamp".into(), ColumnStats {
            min: b"2024-01-15T16:00:00Z".to_vec(),
            max: b"2024-01-15T23:59:59Z".to_vec(),
            null_count: 0,
        }),
    ])
    .build();

// Query: which row groups overlap 10:00–14:00?
let candidates = manifest.query_overlap(&[RangePredicate {
    column: "timestamp".into(),
    min: b"2024-01-15T10:00:00Z".to_vec(),
    max: b"2024-01-15T14:00:00Z".to_vec(),
}]);
// Result: only row group 1 (08:00–15:59) — row groups 0 and 2 pruned

// Serialize/deserialize for storage alongside .pique segments
let bytes = manifest.to_json().unwrap();
let loaded = StatsManifest::from_json(&bytes).unwrap();
```

Multiple predicates use AND semantics — a row group must overlap *all* predicates to be included. This pairs with the segment index: use bloom for equality, stats manifest for ranges, intersect the candidate file sets.

## Project status

Pique is in production use at [Superpositional](https://superpositional.io), powering code entity lookups and graph traversal over indexed codebases. The format is expected to evolve while we validate the API and performance characteristics at scale.

Contributions, benchmarking, and design discussion are welcome.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pique = { git = "https://github.com/state-vector/pique-rs" }

# With S3 support:
pique = { git = "https://github.com/state-vector/pique-rs", features = ["s3"] }
```

## Running the benchmarks

Pique includes a Criterion benchmark suite that measures build time, lookup latency, bloom rejection, and block size trade-offs across various dataset sizes.

```bash
# Clone the repo
git clone https://github.com/state-vector/pique-rs.git
cd pique-rs

# Run all benchmarks (builds in release mode)
cargo bench

# Run specific benchmark groups
cargo bench -- "point_lookup"
cargo bench -- "layered"
cargo bench -- "merge"
cargo bench -- "bloom"
```

To measure real S3 latency (requires AWS credentials and a bucket):

```bash
# Set your test bucket
export OSI_TEST_BUCKET=your-bucket-name
export AWS_REGION=us-east-1

# Run the S3 measurement example
cargo run --release --example measure_s3 --features s3
```

This uploads test segments of various sizes and measures range-read latency, connection warmup effects, and the difference between full-object reads vs targeted range requests.

## License

MIT
