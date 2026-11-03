import {
  AthenaQueryExecutionPager,
  AthenaQueryResultPager,
  AthenaQueryResultPagerConcurrentAccessError,
  AthenaQueryResultPagerEmptyQueryExecutionIdError,
  QueryResultType,
} from '../src';

describe('package entry re-exports', () => {
  it('exposes the pager classes, pager errors, and QueryResultType', () => {
    expect(AthenaQueryExecutionPager).toBeDefined();
    expect(AthenaQueryResultPager).toBeDefined();
    expect(new AthenaQueryResultPagerEmptyQueryExecutionIdError()).toBeInstanceOf(
      AthenaQueryResultPagerEmptyQueryExecutionIdError,
    );
    expect(new AthenaQueryResultPagerConcurrentAccessError()).toBeInstanceOf(
      AthenaQueryResultPagerConcurrentAccessError,
    );
    expect(QueryResultType.DATA_ROWS).toBe('DATA_ROWS');
  });
});
