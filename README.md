# Athena Query Result Collector

[![npm version](https://img.shields.io/npm/v/athena-query-result-collector?style=flat-square)](https://www.npmjs.com/package/athena-query-result-collector)
[![license](https://img.shields.io/npm/l/athena-query-result-collector?style=flat-square)](https://www.npmjs.com/package/athena-query-result-collector)
[![Node.js](https://img.shields.io/node/v/athena-query-result-collector?style=flat-square)](https://www.npmjs.com/package/athena-query-result-collector)
[![build](https://img.shields.io/github/actions/workflow/status/gammarers-aws-sdk-extensions/athena-query-result-collector/build.yml?label=build&style=flat-square)](https://github.com/gammarers-aws-sdk-extensions/athena-query-result-collector/actions/workflows/build.yml)

A TypeScript library for collecting AWS Athena query results via pagination.
It supports full collection, streaming, and page-based batch processing, and it uses [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) (^0.6.x) internally.

## Features

- Collect all rows with `collect()` / `collectWith()` and metadata (`totalRows`, `pageCount`, `truncated`)
- Stream rows lazily with `stream()` as an `AsyncGenerator`
- Process rows per page with `processBatches()` without buffering the full result set
- Cap output with collector `maxRows` (cumulative across pages; distinct from pager/parser `maxRows` on one `ResultSet`)
- Report progress with `onPage` during `collect()` / `collectWith()`
- Forward pager settings (`maxResults`, `queryResultType`, `parseResultSetOptions`) while keeping collector-only options separate
- Cancel long-running work with `AbortSignal` (`CollectorOptions.signal`)
- Rely on the injected `AthenaClient` for transient `GetQueryResults` retries (for example `maxAttempts`)
- Typed collector errors (`AthenaQueryResultCollectorConcurrentUseError`, `AthenaQueryResultCollectorAbortError`)
- Access the underlying pager via `getPager()` for advanced pagination or pager iterators

## How it works

1. You pass an `AthenaClient` and optional `CollectorOptions` into `AthenaQueryResultCollector`.
2. Collection APIs call [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager), which sends paginated Athena `GetQueryResults` requests and parses each `ResultSet`.
3. The collector applies `maxRows`, `onPage`, and `signal` across pages. Transient API retries stay on the client; this package does not add another retry loop.

## Installation

### npm

```bash
npm install athena-query-result-collector @aws-sdk/client-athena
```

### yarn

```bash
yarn add athena-query-result-collector @aws-sdk/client-athena
```

### pnpm

```bash
pnpm add athena-query-result-collector @aws-sdk/client-athena
```

## Usage

### Basic collection

`collect()` loads the full result into memory. For large Athena results, prefer `stream()`, `processBatches()`, or pager iterators (see [Choosing an API](#choosing-an-api-memory)).

```typescript
import { AthenaClient } from '@aws-sdk/client-athena';
import { AthenaQueryResultCollector } from 'athena-query-result-collector';

// Region comes from the environment / shared config. Configure SDK retries on the client.
const client = new AthenaClient({
  maxAttempts: 3,
});
const collector = new AthenaQueryResultCollector(client);

const result = await collector.collect('query-execution-id');
console.log(result.rows);       // ParsedRow[]
console.log(result.totalRows);
console.log(result.pageCount);
console.log(result.truncated);  // true if limited by maxRows
```

### Collection with a custom parser

`collectWith()` also builds a full in-memory `rows` array. Prefer streaming or batch processing for large results.

```typescript
const result = await collector.collectWith(
  'query-execution-id',
  (row) => ({ id: row['id'], name: row['name'] }),
);
```

### Streaming

```typescript
for await (const row of collector.stream('query-execution-id', (row) => row)) {
  console.log(row);
}
```

### Batch processing

```typescript
await collector.processBatches(
  'query-execution-id',
  (row) => row,
  async (rows, pageIndex) => {
    console.log(`processing page ${pageIndex}, rows=${rows.length}`);
    await saveToDb(rows);
  },
);
```

### Choosing an API (memory)

| API | Memory behavior | Use when |
| --- | --- | --- |
| `collect()` / `collectWith()` | Accumulates **all** rows into one array; large results can **OOM** | Small-to-moderate results that must fit in memory, or when you need aggregated metadata (`totalRows`, `pageCount`, `truncated`) |
| `stream()` | Yields one row at a time; only the current page is retained | Large result sets consumed row-by-row |
| `processBatches()` | Passes one page at a time; does not accumulate all rows | Large result sets written or forwarded in page-sized chunks |
| `getPager().iterateRows()` / `iteratePages()` / `iteratePagesWith()` | Same paging model as the pager; collector options are **not** applied | Lower-level control without collector `maxRows` / `signal` |

### Concurrency

One `AthenaQueryResultCollector` is for **serial** use. Overlapping `collect()` / `collectWith()`, `stream()`, or `processBatches()` on the same instance throws `AthenaQueryResultCollectorConcurrentUseError`. For parallel queries, create one collector per execution (sharing the same `AthenaClient` is fine).

```typescript
const collectorA = new AthenaQueryResultCollector(client);
const collectorB = new AthenaQueryResultCollector(client);

await Promise.all([
  collectorA.collect('execution-a'),
  collectorB.collect('execution-b'),
]);
```

If you use `getPager()` directly, do not overlap pager iteration with collector methods on the same instance.

### Pager iterators via `getPager()`

Collector options such as `maxRows`, `onPage`, and `signal` are **not** applied when you drive the pager yourself.

```typescript
const pager = collector.getPager();

for await (const row of pager.iterateRows('query-execution-id')) {
  console.log(row);
}

for await (const page of pager.iteratePages('query-execution-id')) {
  console.log(page.rowCount, page.rows);
}
```

### Parser options (`parseResultSetOptions`)

Pager parser settings on `CollectorOptions` are forwarded to every page fetch:

```typescript
const collector = new AthenaQueryResultCollector(client, {
  maxResults: 500,
  parseResultSetOptions: {
    skipHeaderRow: 'auto',
    columnCountMismatchBehavior: 'warn',
  },
});

await collector.collect('query-execution-id');

const decision = collector.getPager().getLastHeaderRowDecision();
console.log(decision);
```

Collector-only options (`maxRows`, `onPage`, `signal`) are **not** passed to the internal pager.
Collector `maxRows` is a cumulative cap across pages; pager/parser `maxRows` (via `parseResultSetOptions`) applies only to one `ResultSet` (one page).

### Retries and throttling

This library does not add its own retry loop around page fetches. Configure retries on the `AthenaClient`:

```typescript
import { AthenaClient } from '@aws-sdk/client-athena';

const client = new AthenaClient({
  maxAttempts: 3, // total attempts per SDK call (SDK default is 3)
});
```

Omit `region` so the SDK resolves it from the environment (`AWS_REGION` / `AWS_DEFAULT_REGION`), shared config, or the runtime. Omit `retryMode` as well; the SDK default is `'standard'` (set `'adaptive'` only under sustained throttling).

### Cancellation (`AbortSignal`)

```typescript
import {
  AthenaQueryResultCollector,
  AthenaQueryResultCollectorAbortError,
} from 'athena-query-result-collector';

const controller = new AbortController();

const collector = new AthenaQueryResultCollector(client, {
  signal: controller.signal,
});

setTimeout(() => controller.abort(), 5_000);

try {
  await collector.collect('query-execution-id');
} catch (error) {
  if (error instanceof AthenaQueryResultCollectorAbortError || (error instanceof Error && error.name === 'AbortError')) {
    console.log('collection cancelled');
  } else {
    throw error;
  }
}
```

When aborted, the collector stops pagination loops and rejects waits for in-flight page fetches.
In-flight HTTP requests are not cancelled unless the pager or AWS SDK client honors the same signal.
Cancellation also applies to `stream()` and `processBatches()`.

### Error handling

| Class | `code` | `name` | When |
| --- | --- | --- | --- |
| `AthenaQueryResultCollectorConcurrentUseError` | `CONCURRENT_USE` | `AthenaQueryResultCollectorConcurrentUseError` | A second `collect` / `stream` / `processBatches` starts on a busy instance |
| `AthenaQueryResultCollectorAbortError` | `ABORT` | `AbortError` | `CollectorOptions.signal` aborts, or an upstream abort (`name === 'AbortError'`) is normalized |

Pager, parser, and AWS SDK `Error` instances are rethrown unchanged. Non-`Error` rejections are wrapped in `Error` with `cause` when available.

```typescript
import {
  AthenaQueryResultCollectorAbortError,
  AthenaQueryResultCollectorConcurrentUseError,
  AthenaQueryResultPagerInvalidMaxResultsError,
} from 'athena-query-result-collector';

try {
  await collector.collect('query-execution-id');
} catch (error) {
  if (error instanceof AthenaQueryResultPagerInvalidMaxResultsError) {
    throw error;
  }

  if (error instanceof AthenaQueryResultCollectorAbortError || (error instanceof Error && error.name === 'AbortError')) {
    return;
  }

  if (error instanceof AthenaQueryResultCollectorConcurrentUseError) {
    throw error;
  }

  throw error;
}
```

## Options

`CollectorOptions` extends `PagerOptions` from `athena-query-result-pager` 0.6.x.
Only pager fields are forwarded to the internal `AthenaQueryResultPager` instance.

### Collector options

| Option | Type | Description |
| --- | --- | --- |
| `maxRows` | `number` | Cumulative row limit **across all pages** for `collect()` / `stream()` / `processBatches()` (unlimited if omitted). Distinct from pager/parser `maxRows`, which caps rows inside a single Athena `ResultSet` (one page) |
| `onPage` | `function` | Callback after each fetched page in `collect()` / `collectWith()`; receives the page and cumulative row count |
| `signal` | `AbortSignal` | Cancel collection/streaming/batch loops and pending page-fetch waits (throws `AthenaQueryResultCollectorAbortError` with `name === 'AbortError'`) |

### Pager options (forwarded)

| Option | Type | Description |
| --- | --- | --- |
| `maxResults` | `number` | `MaxResults` per `GetQueryResults` request, integer `1`–`1000` (default: `1000`) |
| `queryResultType` | `QueryResultType` | Result type forwarded to Athena (default: `DATA_ROWS`) |
| `parseResultSetOptions` | `ParseResultSetOptions` | Parser options applied on every page |

### Re-exported types and values

The package re-exports `ParsedRow`, `RowParser`, `PageResult`, `PagerOptions`, `ParseResultSetOptions`, `HeaderRowDecision`, and `QueryResultType`, plus collector errors `AthenaQueryResultCollectorError`, `AthenaQueryResultCollectorConcurrentUseError`, and `AthenaQueryResultCollectorAbortError`, and pager errors `AthenaQueryResultPagerError`, `AthenaQueryResultPagerInvalidMaxResultsError`, and `AthenaQueryResultPagerEmptyQueryExecutionIdError`.

## Requirements

- Node.js >= 20.0.0
- AWS Athena access configured for your runtime (credentials and region via the environment or shared config)
- Runtime dependency: `@aws-sdk/client-athena` (pass an `AthenaClient` to the collector)
- Bundled dependency: `athena-query-result-pager` ^0.6.x

## License

This project is licensed under the Apache-2.0 License.
