# Athena Query Result Collector

![build](https://github.com/gammarers-aws-sdk-extensions/athena-query-result-collector/actions/workflows/build.yml/badge.svg)
![npm](https://img.shields.io/npm/v/athena-query-result-collector)
![license](https://img.shields.io/npm/l/athena-query-result-collector)
![node](https://img.shields.io/node/v/athena-query-result-collector)

A TypeScript library for collecting AWS Athena query results via pagination.  
It supports full collection, streaming, and page-based batch processing, and it uses [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) (^0.4.x) internally.

## Features

- Collect all rows with `collect()` and metadata (`totalRows`, `pageCount`, `truncated`)
- Transform rows with a custom parser using `collectWith()`
- Stream rows lazily with `stream()` as an `AsyncGenerator`
- Process rows per page using `processBatches()` without buffering the full result set
- Limit output with `maxRows` (strictly enforced for collection, streaming, and batch processing)
- Invoke an `onPage` callback after each page in `collect()` / `collectWith()` for progress reporting
- Forward pager settings (`maxResults`, `queryResultType`, `parseResultSetOptions`) while keeping collector-only options separate
- Retry transient page-fetch failures only (throttling, 5xx, timeouts) with `retryCount` / `retryDelayMs` (permanent errors fail fast)
- Normalize unknown rejections to `Error` while preserving intentional subclasses such as `RangeError` from pager validation
- Cancel long-running work via `AbortSignal` (`CollectorOptions.signal`): stop pagination loops, reject pending page-fetch waits, and interrupt retry backoff sleep (throws `AbortError`)
- Access the underlying pager via `getPager()` for advanced pagination or header-row diagnostics

## Requirements

- Node.js >= 20.0.0
- AWS Athena access configured for your runtime (credentials/region)
- Runtime dependency: `@aws-sdk/client-athena` (pass an `AthenaClient` instance to the collector)
- Bundled dependency: `athena-query-result-pager` ^0.4.x (installed automatically with this package)

## Installation

```bash
npm install athena-query-result-collector @aws-sdk/client-athena
```

```bash
yarn add athena-query-result-collector @aws-sdk/client-athena
```

## Usage

### Basic collection (raw row data)

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

```typescript
const result = await collector.collectWith(
  'query-execution-id',
  (row) => ({ id: row['id'], name: row['name'] }),
);
// result.rows is an array of the type returned by the parser
```

### Streaming (AsyncGenerator)

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

`CollectorOptions` extends `PagerOptions` from `athena-query-result-pager` 0.4.x.  
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
| `parseResultSetOptions` | `ParseResultSetOptions` | Parser options applied on every page (for example `skipHeaderRow`, `columnCountMismatchBehavior`, `headerRowDetectionStrategy`) |

### Re-exported types and values

The package re-exports `ParsedRow`, `RowParser`, `PageResult`, `PagerOptions`, `ParseResultSetOptions`, `HeaderRowDecision`, and `QueryResultType`.

## License

This project is licensed under the Apache-2.0 License.
