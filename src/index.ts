import { AthenaClient } from '@aws-sdk/client-athena';
import { AthenaQueryResultPager, type ParsedRow, type RowParser, type PageResult, type PagerOptions } from 'athena-query-result-pager';

/**
 * Collector options
 */
export interface CollectorOptions extends PagerOptions {
  /** Maximum rows to collect (default: unlimited) */
  maxRows?: number;
  /** Callback per page (e.g. for progress) */
  onPage?: <T>(page: PageResult<T>, totalCollected: number) => void | Promise<void>;
  /** Number of retries on error (default: 0) */
  retryCount?: number;
  /** Retry interval in ms (default: 1000) */
  retryDelayMs?: number;
}

/**
 * Type for collection result
 */
export interface CollectResult<T> {
  /** All collected row data */
  rows: T[];
  /** Total row count */
  totalRows: number;
  /** Number of pages fetched */
  pageCount: number;
  /** Whether collection was truncated by maxRows */
  truncated: boolean;
}

/**
 * AthenaQueryResultCollector
 * Collects all Athena query results.
 */
export class AthenaQueryResultCollector {

  private readonly pager: AthenaQueryResultPager;
  private readonly options: CollectorOptions;

  constructor(client: AthenaClient, options: CollectorOptions = {}) {
    this.pager = new AthenaQueryResultPager(client, options);
    this.options = {
      maxRows: options.maxRows,
      onPage: options.onPage,
      retryCount: options.retryCount ?? 0,
      retryDelayMs: options.retryDelayMs ?? 1000,
      ...options,
    };
  }

  /**
   * Collect all rows (raw ParsedRow format)
   * @param queryExecutionId - Query execution ID
   * @returns Collection result
   */
  async collect(queryExecutionId: string): Promise<CollectResult<ParsedRow>> {
    return this.collectWith(queryExecutionId, (row) => row);
  }

  /**
   * Collect all rows (transform with custom parser)
   * @param queryExecutionId - Query execution ID
   * @param rowParser - Row parser
   * @returns Collection result
   */
  async collectWith<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): Promise<CollectResult<T>> {
    const rows: T[] = [];
    let nextToken: string | undefined;
    let pageCount = 0;
    let truncated = false;

    // Reset parser for new query
    this.pager.reset();

    do {
      const page = await this.fetchPageWithRetry(
        queryExecutionId,
        rowParser,
        nextToken,
      );

      pageCount++;

      // maxRows check
      if (this.options.maxRows !== undefined) {
        const remaining = this.options.maxRows - rows.length;
        if (page.rows.length >= remaining) {
          rows.push(...page.rows.slice(0, remaining));
          truncated = true;
          break;
        }
      }

      rows.push(...page.rows);

      // onPage callback
      if (this.options.onPage) {
        await this.options.onPage(page, rows.length);
      }

      nextToken = page.nextToken;
    } while (nextToken);

    return {
      rows,
      totalRows: rows.length,
      pageCount,
      truncated,
    };
  }

  /**
   * Stream rows one-by-one via AsyncGenerator
   * @param queryExecutionId - Query execution ID
   * @param rowParser - Row parser
   */
  async *stream<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): AsyncGenerator<T> {
    let nextToken: string | undefined;
    let count = 0;

    this.pager.reset();

    do {
      const page = await this.fetchPageWithRetry(
        queryExecutionId,
        rowParser,
        nextToken,
      );

      for (const row of page.rows) {
        // maxRows check
        if (this.options.maxRows !== undefined && count >= this.options.maxRows) {
          return;
        }
        yield row;
        count++;
      }

      nextToken = page.nextToken;
    } while (nextToken);
  }

  /**
   * For batch processing: run callback per page
   * @param queryExecutionId - Query execution ID
   * @param rowParser - Row parser
   * @param batchProcessor - Batch processor function
   */
  async processBatches<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
    batchProcessor: (rows: T[], pageIndex: number) => void | Promise<void>,
  ): Promise<{ totalRows: number; pageCount: number }> {

    let nextToken: string | undefined;
    let pageCount = 0;
    let totalRows = 0;

    this.pager.reset();

    do {
      const page = await this.fetchPageWithRetry(
        queryExecutionId,
        rowParser,
        nextToken,
      );

      await batchProcessor(page.rows, pageCount);

      pageCount++;
      totalRows += page.rows.length;

      // maxRows check
      if (this.options.maxRows !== undefined && totalRows >= this.options.maxRows) {
        break;
      }

      nextToken = page.nextToken;
    } while (nextToken);

    return { totalRows, pageCount };
  }

  /**
   * Fetch a page with retry
   */
  private async fetchPageWithRetry<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
    nextToken?: string,
  ): Promise<PageResult<T>> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= this.options.retryCount!; attempt++) {
      try {
        return await this.pager.fetchPageWith(queryExecutionId, rowParser, nextToken);
      } catch (error) {
        lastError = error as Error;
        if (attempt < this.options.retryCount!) {
          await this.sleep(this.options.retryDelayMs!);
        }
      }
    }

    throw lastError;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get the internal Pager instance (for advanced use)
   */
  getPager(): AthenaQueryResultPager {
    return this.pager;
  }
}

// Re-export types
export type { ParsedRow, RowParser, PageResult, PagerOptions };