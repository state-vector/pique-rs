# Pique

Pique is an object storage indexing library and format for analytical datasets.

It provides immutable secondary indexes that enable efficient point lookups, prefix searches and other highly selective queries over datasets stored in object storage.

Pique is intended to complement columnar storage formats such as Parquet and query engines such as DuckDB, DataFusion and Polars.

## Goals

- Reduce the number of object store requests required for highly selective queries.

- Eliminate file discovery and metadata scanning for common lookup patterns.

- Provide a common immutable index format for object storage.

- Remain serverless and object-store native.

- Be lightweight enough for serverless runtimes.

## Non-goals

Pique is not:

- a database

- a query engine

- a transaction layer

- a storage format for analytical data

- a replacement for Parquet, Iceberg or DuckLake

Pique indexes analytical data; it does not store it.

## Use cases

Typical applications include:

- accelerating point lookups over Parquet datasets

- graph adjacency indexes

- path and prefix indexes

- metadata indexes

- custom lookup structures

- serverless analytical applications

## Design principles

- Immutable segments

- Object-store first

- Optimised for HTTP Range requests

- Cheap to regenerate during compaction

- Cheap to publish atomically

- Minimal runtime dependencies

- Portable across object storage providers

## Architecture

A Pique index consists of one or more immutable segments.

Each segment maps keys to application-defined values.

```text
key  →  index segment  →  value
```

Values may represent:

- Parquet file locations

- row groups

- posting lists

- adjacency lists

- application-specific payloads

## Project status

Prototype.

The on-disk format is expected to evolve while performance characteristics and API design are validated.

Contributions, benchmarking and design discussion are encouraged.
