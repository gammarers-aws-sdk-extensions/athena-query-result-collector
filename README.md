# Athena Query Result Collector

![npm](https://img.shields.io/npm/v/athena-query-result-collector)
![license](https://img.shields.io/npm/l/athena-query-result-collector)
![node](https://img.shields.io/node/v/athena-query-result-collector)

A TypeScript library for collecting AWS Athena query results via pagination.  
It supports full collection, streaming, and page-based batch processing, and it uses [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) internally.

## Features

- Collect all rows with `collect()` and metadata (`totalRows`, `pageCount`, `truncated`)
- Transform rows with a custom parser using `collectWith()`
- Stream rows lazily with `stream()` as an `AsyncGenerator`
- Process rows per page using `processBatches()`
- Limit output with `maxRows` (strictly enforced for collection, stream, and batch processing)
- Retry page fetches with safe retry options (`retryCount`, `retryDelayMs`)

## Requirements

- Node.js >= 20.0.0
- AWS Athena access configured for your runtime (credentials/region)
- Peer usage dependency: `@aws-sdk/client-athena`

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
const collector = new AthenaQueryResultCollector(client);

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
  (row) => ({ id: row['id'], name: row['name'] })
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
  }
);
```

## Options

`CollectorOptions` (extends `PagerOptions`):

| Option | Type | Description |
| --- | --- | --- |
| `maxRows` | `number` | Maximum number of rows to process (unlimited if omitted) |
| `onPage` | `function` | Callback called after each fetched page in `collect()` / `collectWith()` |
| `retryCount` | `number` | Retry count on page fetch failure (default: `0`; invalid/negative values are normalized) |
| `retryDelayMs` | `number` | Delay in milliseconds between retries (default: `1000`; invalid/negative values are normalized) |

You can also pass `PagerOptions` from `athena-query-result-pager` (e.g. page size).

### Re-exported types

The package re-exports `ParsedRow`, `RowParser`, `PageResult`, and `PagerOptions`.

## License

This project is licensed under the (Apache-2.0) License.
