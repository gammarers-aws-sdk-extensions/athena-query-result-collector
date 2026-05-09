import { AthenaQueryResultCollector } from '../src';
import type { PageResult } from '../src';

const mockFetchPageWith = jest.fn();
const mockReset = jest.fn();

jest.mock('athena-query-result-pager', () => ({
  AthenaQueryResultPager: jest.fn().mockImplementation(() => ({
    fetchPageWith: mockFetchPageWith,
    reset: mockReset,
  })),
}));

describe('AthenaQueryResultCollector', () => {
  const mockClient = {} as any;
  const queryExecutionId = 'query-id-123';

  beforeEach(() => {
    // clearAllMocks does not reset mock implementations; we want isolation between tests
    mockFetchPageWith.mockReset();
    mockReset.mockReset();
  });

  describe('constructor', () => {
    it('should create an instance with client and options', () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      expect(collector).toBeDefined();
      expect(collector.getPager()).toBeDefined();
    });

    it('should use default retryCount=0 and retryDelayMs=1000 when options are omitted', async () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      mockFetchPageWith.mockResolvedValue({ rows: [], rowCount: 0 });
      mockReset.mockImplementation(() => {});
      await collector.collect(queryExecutionId);
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('getPager', () => {
    it('should return the internal Pager instance', () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      const pager = collector.getPager();
      expect(pager).toBeDefined();
      expect(pager.reset).toBe(mockReset);
      expect(pager.fetchPageWith).toBe(mockFetchPageWith);
    });
  });

  describe('collect', () => {
    it('should return results for a single page as-is', async () => {
      const rows = [
        { col1: 'a', col2: '1' },
        { col1: 'b', col2: '2' },
      ];
      mockFetchPageWith.mockResolvedValueOnce({
        rows,
        rowCount: rows.length,
      } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient);
      const result = await collector.collect(queryExecutionId);

      expect(result.rows).toEqual(rows);
      expect(result.totalRows).toBe(2);
      expect(result.pageCount).toBe(1);
      expect(result.truncated).toBe(false);
      expect(mockFetchPageWith).toHaveBeenCalledWith(queryExecutionId, expect.any(Function), undefined);
    });

    it('should fetch multiple pages via nextToken and return all rows', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ x: 1 }],
          nextToken: 'token1',
          rowCount: 1,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ x: 2 }],
          rowCount: 1,
        } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient);
      const result = await collector.collect(queryExecutionId);

      expect(result.rows).toEqual([{ x: 1 }, { x: 2 }]);
      expect(result.totalRows).toBe(2);
      expect(result.pageCount).toBe(2);
      expect(result.truncated).toBe(false);
      expect(mockFetchPageWith).toHaveBeenNthCalledWith(1, queryExecutionId, expect.any(Function), undefined);
      expect(mockFetchPageWith).toHaveBeenNthCalledWith(2, queryExecutionId, expect.any(Function), 'token1');
    });

    it('should call reset before collect runs', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      await collector.collect(queryExecutionId);
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('collectWith', () => {
    it('should return rows transformed by rowParser', async () => {
      const rawRows = [{ name: 'foo' }, { name: 'bar' }];
      mockFetchPageWith.mockImplementation((_q, rowParser: (r: any) => any) =>
        Promise.resolve({
          rows: rawRows.map((r) => rowParser(r)),
          rowCount: 2,
        } as PageResult<any>),
      );

      const collector = new AthenaQueryResultCollector(mockClient);
      const result = await collector.collectWith(queryExecutionId, (row) => ({
        label: (row as { name: string }).name.toUpperCase(),
      }));

      expect(result.rows).toEqual([{ label: 'FOO' }, { label: 'BAR' }]);
      expect(result.totalRows).toBe(2);
      expect(result.pageCount).toBe(1);
      expect(result.truncated).toBe(false);
    });
  });

  describe('maxRows', () => {
    it('should truncate when exceeding maxRows in collect and set truncated to true', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ i: 1 }, { i: 2 }, { i: 3 }],
          nextToken: 't',
          rowCount: 3,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ i: 4 }, { i: 5 }],
          rowCount: 2,
        } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, { maxRows: 4 });
      const result = await collector.collect(queryExecutionId);

      expect(result.rows).toHaveLength(4);
      expect(result.rows.map((r: any) => r.i)).toEqual([1, 2, 3, 4]);
      expect(result.totalRows).toBe(4);
      expect(result.truncated).toBe(true);
      expect(result.pageCount).toBe(2);
    });

    it('should stop yielding when maxRows is exceeded in stream', async () => {
      mockFetchPageWith.mockResolvedValueOnce({
        rows: [{ v: 1 }, { v: 2 }, { v: 3 }],
        rowCount: 3,
      } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, { maxRows: 2 });
      const yielded: any[] = [];
      for await (const row of collector.stream(queryExecutionId, (r) => r)) {
        yielded.push(row);
      }
      expect(yielded).toHaveLength(2);
      expect(yielded).toEqual([{ v: 1 }, { v: 2 }]);
    });

    it('should break the loop when maxRows is reached in processBatches', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ a: 1 }, { a: 2 }],
          nextToken: 't',
          rowCount: 2,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ a: 3 }, { a: 4 }],
          rowCount: 2,
        } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, { maxRows: 3 });
      const batches: any[][] = [];
      const result = await collector.processBatches(
        queryExecutionId,
        (r) => r,
        (rows) => { batches.push(rows); },
      );

      expect(batches).toHaveLength(2);
      expect(batches[0]).toHaveLength(2);
      expect(batches[1]).toHaveLength(1);
      expect(batches[1]).toEqual([{ a: 3 }]);
      expect(result.totalRows).toBe(3);
      expect(result.pageCount).toBe(2);
    });
  });

  describe('onPage', () => {
    it('should invoke onPage callback after each page is fetched', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ n: 1 }],
          nextToken: 't',
          rowCount: 1,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ n: 2 }],
          rowCount: 1,
        } as PageResult<any>);

      const onPage = jest.fn();
      const collector = new AthenaQueryResultCollector(mockClient, { onPage });
      await collector.collect(queryExecutionId);

      expect(onPage).toHaveBeenCalledTimes(2);
      expect(onPage).toHaveBeenNthCalledWith(
        1,
        expect.objectContaining({ rows: [{ n: 1 }], nextToken: 't' }),
        1,
      );
      expect(onPage).toHaveBeenNthCalledWith(
        2,
        expect.objectContaining({ rows: [{ n: 2 }] }),
        2,
      );
    });
  });

  describe('stream', () => {
    it('should yield rows one by one', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ id: 1 }, { id: 2 }],
          nextToken: 't',
          rowCount: 2,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ id: 3 }],
          rowCount: 1,
        } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient);
      const out: any[] = [];
      for await (const row of collector.stream(queryExecutionId, (r) => r)) {
        out.push(row);
      }
      expect(out).toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
    });

    it('should call reset before stream execution', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      const gen = collector.stream(queryExecutionId, (r) => r);
      await gen.next();
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('processBatches', () => {
    it('should call batchProcessor with each page rows and pageIndex', async () => {
      mockFetchPageWith
        .mockResolvedValueOnce({
          rows: [{ x: 'a' }],
          nextToken: 't',
          rowCount: 1,
        } as PageResult<any>)
        .mockResolvedValueOnce({
          rows: [{ x: 'b' }, { x: 'c' }],
          rowCount: 2,
        } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient);
      const batches: { pageIndex: number; rows: any[] }[] = [];
      const result = await collector.processBatches(
        queryExecutionId,
        (r) => r,
        (rows, pageIndex) => {
          batches.push({ pageIndex, rows });
        },
      );

      expect(batches).toEqual([
        { pageIndex: 0, rows: [{ x: 'a' }] },
        { pageIndex: 1, rows: [{ x: 'b' }, { x: 'c' }] },
      ]);
      expect(result.totalRows).toBe(3);
      expect(result.pageCount).toBe(2);
    });

    it('should call reset before processBatches execution', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      await collector.processBatches(queryExecutionId, (r) => r, () => {});
      expect(mockReset).toHaveBeenCalled();
    });

    it('should not fetch any pages when maxRows is 0', async () => {
      const collector = new AthenaQueryResultCollector(mockClient, { maxRows: 0 });
      const batchProcessor = jest.fn();

      const result = await collector.processBatches(queryExecutionId, (r) => r, batchProcessor);

      expect(mockFetchPageWith).not.toHaveBeenCalled();
      expect(batchProcessor).not.toHaveBeenCalled();
      expect(result).toEqual({ totalRows: 0, pageCount: 0 });
    });
  });

  describe('retry', () => {
    it('should retry after fetchPageWith failure and succeed', async () => {
      const timeoutError = Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' });

      mockFetchPageWith
        .mockRejectedValueOnce(timeoutError)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 10,
      });
      const result = await collector.collect(queryExecutionId);

      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
      expect(result.totalRows).toBe(1);
    });

    it('should retry on HTTP 429 (throttling)', async () => {
      const tooManyRequests = Object.assign(new Error('TooManyRequests'), {
        name: 'TooManyRequestsException',
        $metadata: { httpStatusCode: 429 },
      });

      mockFetchPageWith
        .mockRejectedValueOnce(tooManyRequests)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should retry when Smithy retry hint is present ($retryable)', async () => {
      const retryable = Object.assign(new Error('retryable'), {
        $retryable: { throttling: true },
      });

      mockFetchPageWith
        .mockRejectedValueOnce(retryable)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should retry on throttling error codes without HTTP metadata', async () => {
      const throttled = {
        message: 'throttled',
        Code: 'RequestLimitExceeded',
      };

      mockFetchPageWith
        .mockRejectedValueOnce(throttled)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should retry on timeout identified by name or message', async () => {
      const timeoutByName = Object.assign(new Error('timed out while reading'), {
        name: 'TimeoutError',
      });

      mockFetchPageWith
        .mockRejectedValueOnce(timeoutByName)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should retry on timeout identified by message only', async () => {
      const timeoutByMessage = { message: 'Operation timed out' };

      mockFetchPageWith
        .mockRejectedValueOnce(timeoutByMessage)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 1,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should not retry on permanent errors (e.g. 403 AccessDenied)', async () => {
      const accessDenied = Object.assign(new Error('AccessDenied'), {
        name: 'AccessDeniedException',
        $metadata: { httpStatusCode: 403 },
      });

      mockFetchPageWith.mockRejectedValue(accessDenied);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 3,
        retryDelayMs: 1,
      });

      await expect(collector.collect(queryExecutionId)).rejects.toThrow('AccessDenied');
      expect(mockFetchPageWith).toHaveBeenCalledTimes(1);
    });

    it('should retry on transient HTTP errors (e.g. 500)', async () => {
      const internalError = Object.assign(new Error('InternalError'), {
        name: 'InternalServerException',
        $metadata: { httpStatusCode: 500 },
      });

      mockFetchPageWith
        .mockRejectedValueOnce(internalError)
        .mockResolvedValueOnce({ rows: [{ ok: true }], rowCount: 1 } as PageResult<any>);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 2,
        retryDelayMs: 1,
      });

      const result = await collector.collect(queryExecutionId);
      expect(mockFetchPageWith).toHaveBeenCalledTimes(2);
      expect(result.rows).toEqual([{ ok: true }]);
    });

    it('should throw the last error when retryCount is exceeded', async () => {
      const internalError = Object.assign(new Error('InternalError'), {
        name: 'InternalServerException',
        $metadata: { httpStatusCode: 500 },
      });

      mockFetchPageWith.mockRejectedValue(internalError);

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 2,
        retryDelayMs: 1,
      });

      await expect(collector.collect(queryExecutionId)).rejects.toThrow('InternalError');
      expect(mockFetchPageWith).toHaveBeenCalledTimes(3);
    });

    it('should normalize invalid retry options and never throw undefined', async () => {
      mockFetchPageWith.mockRejectedValue(new Error('invalid retry options'));

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: -1,
        retryDelayMs: Number.NaN,
      });

      await expect(collector.collect(queryExecutionId)).rejects.toThrow('invalid retry options');
      expect(mockFetchPageWith).toHaveBeenCalledTimes(1);
    });
  });
});
