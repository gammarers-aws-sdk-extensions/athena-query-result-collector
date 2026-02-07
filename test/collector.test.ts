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
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('creates an instance with client and options', () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      expect(collector).toBeDefined();
      expect(collector.getPager()).toBeDefined();
    });

    it('uses default retryCount=0 and retryDelayMs=1000 when options are omitted', async () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      mockFetchPageWith.mockResolvedValue({ rows: [], rowCount: 0 });
      mockReset.mockImplementation(() => {});
      await collector.collect(queryExecutionId);
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('getPager', () => {
    it('returns the internal Pager instance', () => {
      const collector = new AthenaQueryResultCollector(mockClient);
      const pager = collector.getPager();
      expect(pager).toBeDefined();
      expect(pager.reset).toBe(mockReset);
      expect(pager.fetchPageWith).toBe(mockFetchPageWith);
    });
  });

  describe('collect', () => {
    it('returns results for a single page as-is', async () => {
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

    it('fetches multiple pages via nextToken and returns all rows', async () => {
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

    it('calls reset before collect runs', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      await collector.collect(queryExecutionId);
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('collectWith', () => {
    it('returns rows transformed by rowParser', async () => {
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
    it('truncates when exceeding maxRows in collect and sets truncated to true', async () => {
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

    it('stops yielding when maxRows is exceeded in stream', async () => {
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

    it('breaks the loop when maxRows is reached in processBatches', async () => {
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
      expect(batches[1]).toHaveLength(2);
      expect(result.totalRows).toBe(4);
      expect(result.pageCount).toBe(2);
    });
  });

  describe('onPage', () => {
    it('invokes onPage callback after each page is fetched', async () => {
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
    it('yields rows one by one', async () => {
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

    it('calls reset before stream execution', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      const gen = collector.stream(queryExecutionId, (r) => r);
      await gen.next();
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('processBatches', () => {
    it('calls batchProcessor with each page rows and pageIndex', async () => {
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

    it('calls reset before processBatches execution', async () => {
      mockFetchPageWith.mockResolvedValueOnce({ rows: [], rowCount: 0 } as PageResult<any>);
      const collector = new AthenaQueryResultCollector(mockClient);
      await collector.processBatches(queryExecutionId, (r) => r, () => {});
      expect(mockReset).toHaveBeenCalled();
    });
  });

  describe('retry', () => {
    it('retries after fetchPageWith failure and succeeds', async () => {
      mockFetchPageWith
        .mockRejectedValueOnce(new Error('network error'))
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

    it('throws the last error when retryCount is exceeded', async () => {
      mockFetchPageWith.mockRejectedValue(new Error('always fail'));

      const collector = new AthenaQueryResultCollector(mockClient, {
        retryCount: 2,
        retryDelayMs: 1,
      });

      await expect(collector.collect(queryExecutionId)).rejects.toThrow('always fail');
      expect(mockFetchPageWith).toHaveBeenCalledTimes(3);
    });
  });
});
