# Athena Query Result Collector

![build](https://github.com/gammarers-aws-sdk-extensions/athena-query-result-collector/actions/workflows/build.yml/badge.svg)
![npm](https://img.shields.io/npm/v/athena-query-result-collector)
![license](https://img.shields.io/npm/l/athena-query-result-collector)
![node](https://img.shields.io/node/v/athena-query-result-collector)

A TypeScript library for collecting AWS Athena query results via pagination.  
It supports full collection, streaming, and page-based batch processing, and it uses [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) (^0.5.x) internally.

## Features

- Collect all rows with `collect()` and metadata (`totalRows`, `pageCount`, `truncated`) — **keeps the full result in memory**
- Transform rows with a custom parser using `collectWith()` — **also accumulates every row in memory**
- Stream rows lazily with `stream()` as an `AsyncGenerator` (memory-efficient for large results)
- Process rows per page using `processBatches()` without buffering the full result set
- Limit output with `maxRows` (strictly enforced for collection, streaming, and batch processing)
- Invoke an `onPage` callback after each page in `collect()` / `collectWith()` for progress reporting
- Forward pager settings (`maxResults`, `queryResultType`, `parseResultSetOptions`) while keeping collector-only options separate
- Retry transient page-fetch failures only (throttling, 5xx, timeouts) with `retryCount` / `retryDelayMs` (permanent errors fail fast)
- Normalize unknown rejections to `Error` while preserving intentional subclasses such as `RangeError` from pager validation
- Cancel long-running work via `AbortSignal` (`CollectorOptions.signal`): stop pagination loops, reject pending page-fetch waits, and interrupt retry backoff sleep (throws `AbortError`)
- Access the underlying pager via `getPager()` for advanced pagination, header-row diagnostics, or pager iterators (`iterateRows` / `iteratePages`)

## Choosing an API (memory)

| API | Memory behavior | Use when |
| --- | --- | --- |
| `collect()` / `collectWith()` | Accumulates **all** rows into one array; large results can **OOM** | Small-to-moderate results that must fit in memory, or when you need aggregated metadata (`totalRows`, `pageCount`, `truncated`) |
| `stream()` | Yields one row at a time via pager `iterateRows`; only the current page is retained | Large result sets consumed row-by-row |
| `processBatches()` | Passes one page at a time via pager `iteratePagesWith`; does not accumulate all rows | Large result sets written or forwarded in page-sized chunks |
| `getPager().iterateRows()` / `iteratePages()` / `iteratePagesWith()` | Same paging model as the pager (no full-set buffer); collector options are **not** applied | Lower-level control without collector retries/limits |

Prefer `stream()`, `processBatches()`, or the pager iterators for huge Athena result sets. Use `collect()` / `collectWith()` only when the full set is known to fit comfortably in memory (or bound it with `maxRows`).

## Concurrency (serial use per instance)

One `AthenaQueryResultCollector` is intended for **serial** use:

- Do **not** overlap `collect()` / `collectWith()`, `stream()`, or `processBatches()` on the same instance.
- The internal pager keeps parser state (for example header-row bookkeeping). Each collector method calls `pager.reset()` before a new execution, but concurrent calls can corrupt that state.
- Starting a second operation while one is in flight throws `CollectorConcurrentUseError`. For parallel queries, create one collector per execution (sharing the same `AthenaClient` is fine).
- If you use `getPager()` directly, do not overlap pager iteration with collector methods on the same instance. The pager auto-resets parser state when `queryExecutionId` changes (see [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) 0.5+).

```typescript
// Parallel work: separate collectors, shared client
const collectorA = new AthenaQueryResultCollector(client);
const collectorB = new AthenaQueryResultCollector(client);

await Promise.all([
  collectorA.collect('execution-a'),
  collectorB.collect('execution-b'),
]);
```

## Requirements

- Node.js >= 20.0.0
- AWS Athena access configured for your runtime (credentials/region)
- Runtime dependency: `@aws-sdk/client-athena` (pass an `AthenaClient` instance to the collector)
- Bundled dependency: `athena-query-result-pager` ^0.5.x (installed automatically with this package)

## Installation

```bash
npm install athena-query-result-collector @aws-sdk/client-athena
```

```bash
yarn add athena-query-result-collector @aws-sdk/client-athena
```

## Usage

### Basic collection (raw row data)

`collect()` loads the full result into memory. For large Athena results, use `stream()`, `processBatches()`, or pager iterators instead (see [Choosing an API (memory)](#choosing-an-api-memory)).

```typescript
import { AthenaClient } from '@aws-sdk/client-athena';
import { AthenaQueryResultCollector } from 'athena-query-result-collector';

const client = new AthenaClient({ region: 'ap-northeast-1' });
const collector = new AthenaQueryResultCollector(client, {
  // Retries are applied only to transient failures (throttling, 5xx, timeouts, etc.)
  retryCount: 3,
  retryDelayMs: 1000,
});

const result = await collector.collect('query-execution-id');
console.log(result.rows);       // ParsedRow[]
console.log(result.totalRows);
console.log(result.pageCount);
console.log(result.truncated);  // true if limited by maxRows
```

### Collection with custom parser

`collectWith()` (like `collect()`) builds a full in-memory `rows` array. Avoid it for unbounded or very large results — prefer [Streaming](#streaming-asyncgenerator), [Batch processing](#batch-processing), or [Pager iterators](#pager-iterators-via-getpager) below.

```typescript
const result = await collector.collectWith(
  'query-execution-id',
  (row) => ({ id: row['id'], name: row['name'] }),
);
// result.rows is an array of the type returned by the parser
```

### Streaming (AsyncGenerator)

Memory-efficient alternative to `collect()` / `collectWith()` for large results.  
Internally delegates to the pager's `iterateRows()` and adds `maxRows`, retries, and `signal` handling.

```typescript
for await (const row of collector.stream('query-execution-id', (row) => row)) {
  console.log(row);
}
```

### Batch processing

Processes one page at a time without buffering the full result set.  
Internally delegates to the pager's `iteratePagesWith()` and adds `maxRows`, retries, and `signal` handling.

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

### Pager iterators via `getPager()`

For lower-level, memory-efficient iteration, use the underlying [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) iterators. Collector options such as `maxRows`, `onPage`, retries, and `signal` are **not** applied when you drive the pager yourself.

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

Pager parser settings can be set on `CollectorOptions` and are forwarded to every page fetch:

```typescript
import { AthenaQueryResultCollector } from 'athena-query-result-collector';

const collector = new AthenaQueryResultCollector(client, {
  maxResults: 500,
  parseResultSetOptions: {
    skipHeaderRow: 'auto',
    columnCountMismatchBehavior: 'warn',
  },
});

await collector.collect('query-execution-id');

// When skipHeaderRow is 'auto', inspect the pager's last header decision:
const decision = collector.getPager().getLastHeaderRowDecision();
console.log(decision);
```

Collector-only options (`maxRows`, `onPage`, `retryCount`, `retryDelayMs`, `signal`) are **not** passed to the internal pager.

### Cancellation (AbortSignal)

Pass `CollectorOptions.signal` to cancel long-running collection, streaming, or batch processing.  
When aborted, the collector:

- stops pagination loops
- rejects waits for in-flight page fetches
- interrupts delay between retry attempts

It then throws an `AbortError` (`DOMException` on runtimes that provide it).  
In-flight HTTP requests are not cancelled unless the underlying pager or AWS SDK client honors the same signal.

```typescript
const controller = new AbortController();

const collector = new AthenaQueryResultCollector(client, {
  signal: controller.signal,
  retryCount: 3,
  retryDelayMs: 1000,
});

// e.g. server timeout or user cancellation
setTimeout(() => controller.abort(), 5_000);

try {
  await collector.collect('query-execution-id');
} catch (error) {
  if (error instanceof Error && error.name === 'AbortError') {
    console.log('collection cancelled');
  } else {
    throw error;
  }
}
```

Cancellation also applies to `stream()` and `processBatches()`.

### Error handling

Page-fetch failures are rethrown as `Error` instances suitable for caller-side handling:

- Existing `Error` subclasses (for example `RangeError` from invalid `maxResults` at construction time, AWS SDK service errors) are **rethrown unchanged**
- String or plain-object rejections are wrapped in `Error` with a derived message; non-primitive values are attached via `Error.cause` when available
- `AbortError` is never retried
- Overlapping `collect()` / `collectWith()`, `stream()`, or `processBatches()` on the same instance throws `CollectorConcurrentUseError`

```typescript
try {
  await collector.collect('query-execution-id');
} catch (error) {
  if (error instanceof RangeError) {
    // pager option validation (e.g. invalid maxResults)
    throw error;
  }

  if (error instanceof Error && error.name === 'AbortError') {
    return;
  }

  if (error instanceof Error && (error as Error & { cause?: unknown }).cause) {
    console.error('wrapped rejection', (error as Error & { cause?: unknown }).cause);
  }

  throw error;
}
```

## Options

`CollectorOptions` extends `PagerOptions` from `athena-query-result-pager` 0.5.x.  
Only pager fields are forwarded to the internal `AthenaQueryResultPager` instance.

### Collector options

| Option | Type | Description |
| --- | --- | --- |
| `maxRows` | `number` | Maximum number of rows to collect or process (unlimited if omitted) |
| `onPage` | `function` | Callback invoked after each fetched page in `collect()` / `collectWith()`; receives the page and cumulative row count |
| `retryCount` | `number` | Additional attempts after the first page-fetch failure, for transient errors only (default: `0`; invalid/negative values are normalized) |
| `retryDelayMs` | `number` | Delay in milliseconds between retries; interruptible when `signal` aborts (default: `1000`; invalid/negative values are normalized) |
| `signal` | `AbortSignal` | Cancel collection/streaming/batch loops, pending page-fetch waits, and retry backoff sleep (throws `AbortError` when aborted) |

### Pager options (forwarded to `athena-query-result-pager`)

| Option | Type | Description |
| --- | --- | --- |
| `maxResults` | `number` | `MaxResults` per `GetQueryResults` request, integer `1`–`1000` (default: `1000`) |
| `queryResultType` | `QueryResultType` | Result type forwarded to Athena (default: `DATA_ROWS`) |
| `parseResultSetOptions` | `ParseResultSetOptions` | Parser options applied on every page (for example `skipHeaderRow`, `columnCountMismatchBehavior`, `headerRowDetectionStrategy`, `unavailableResultBehavior`) |

### Re-exported types and values

The package re-exports `ParsedRow`, `RowParser`, `PageResult`, `PagerOptions`, `ParseResultSetOptions`, `HeaderRowDecision`, and `QueryResultType`.

## License

This project is licensed under the Apache-2.0 License.
