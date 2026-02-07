# Athena Query Result Collector

A TypeScript library that fetches AWS Athena query results via paging and supports bulk collection, streaming, and batch processing. It uses [athena-query-result-pager](https://www.npmjs.com/package/athena-query-result-pager) under the hood.

## Requirements

- Node.js >= 20.0.0
- Dependencies: `@aws-sdk/client-athena`, `athena-query-result-pager`

## Installation

```bash
npm install athena-query-result-collector @aws-sdk/client-athena
# or
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
    await saveToDb(rows);
  }
);
```

### Options

`CollectorOptions` (extends `PagerOptions`):

| Option        | Type     | Description |
|---------------|----------|-------------|
| `maxRows`     | number   | Maximum rows to collect (unlimited if omitted) |
| `onPage`      | function | Callback per page (e.g. for progress) |
| `retryCount`  | number   | Number of retries on error (default: 0) |
| `retryDelayMs`| number   | Retry delay in ms (default: 1000) |

You can also pass `PagerOptions` from `athena-query-result-pager` (e.g. page size).

### Re-exported types

The package re-exports `ParsedRow`, `RowParser`, `PageResult`, and `PagerOptions`.

## License

This project is licensed under the Apache-2.0 License.
