export {
  AthenaQueryResultCollector,
  type CollectResult,
  type CollectorOptions,
} from './collector';

export {
  AthenaQueryResultCollectorError,
  AthenaQueryResultCollectorAbortError,
  AthenaQueryResultCollectorConcurrentUseError,
  type CollectorErrorOptions,
} from './core/errors';

export {
  AthenaQueryResultPagerEmptyQueryExecutionIdError,
  AthenaQueryResultPagerError,
  AthenaQueryResultPagerInvalidMaxResultsError,
} from 'athena-query-result-pager';

/** Re-exports pager and parser types from `athena-query-result-pager`. */
export type {
  HeaderRowDecision,
  PageResult,
  PagerOptions,
  ParsedRow,
  ParseResultSetOptions,
  RowParser,
} from 'athena-query-result-pager';
export { QueryResultType } from 'athena-query-result-pager';
