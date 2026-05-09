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
  /**
   * AbortSignal to cancel long-running collection / streaming / batch processing.
   *
   * When aborted, the collector will stop looping and throw an `AbortError`.
   * If the underlying pager/client supports aborting in-flight requests via the same signal
   * (e.g. through PagerOptions), it will also be propagated via the constructor options.
   */
  signal?: AbortSignal;
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
  private readonly retryCount: number;
  private readonly retryDelayMs: number;
  private readonly signal?: AbortSignal;

  constructor(client: AthenaClient, options: CollectorOptions = {}) {
    const retryCount = this.normalizeNonNegativeInteger(options.retryCount, 0);
    const retryDelayMs = this.normalizeNonNegativeInteger(options.retryDelayMs, 1000);

    this.pager = new AthenaQueryResultPager(client, options);
    this.retryCount = retryCount;
    this.retryDelayMs = retryDelayMs;
    this.options = {
      ...options,
      retryCount,
      retryDelayMs,
    };
    this.signal = options.signal;
  }

  /**
   * Determines whether an error is likely transient and safe to retry.
   *
   * This is intentionally conservative: authentication/authorization/validation errors
   * should fail fast rather than being retried.
   */
  private readonly isRetryableError = (error: unknown): boolean => {
    const err = error as any;

    // AWS SDK v3 / Smithy-style metadata (best signal)
    const httpStatusCode: number | undefined =
      err?.$metadata?.httpStatusCode ??
      err?.$response?.httpResponse?.statusCode;

    if (typeof httpStatusCode === 'number') {
      // Retry only on transient HTTP statuses
      if (httpStatusCode >= 500) {
        return true;
      }

      if (httpStatusCode === 408 || httpStatusCode === 429) {
        return true;
      }

      return false;
    }

    // Smithy retry hints
    if (err?.$retryable?.throttling === true || err?.$retryable === true) {
      return true;
    }

    // Common AWS / HTTP-like error identifiers
    const nameOrCode: string | undefined =
      (typeof err?.name === 'string' ? err.name : undefined) ??
      (typeof err?.Code === 'string' ? err.Code : undefined) ??
      (typeof err?.code === 'string' ? err.code : undefined);

    const throttlingCodes = new Set<string>([
      'Throttling',
      'ThrottlingException',
      'TooManyRequestsException',
      'RequestLimitExceeded',
      'RequestThrottled',
      'RequestThrottledException',
      'SlowDown',
      'ProvisionedThroughputExceededException',
    ]);

    if (nameOrCode && throttlingCodes.has(nameOrCode)) {
      return true;
    }

    // Node/network transient errors
    const nodeErrorCodes = new Set<string>([
      'ETIMEDOUT',
      'ECONNRESET',
      'EPIPE',
      'EAI_AGAIN',
      'ENOTFOUND',
      'ECONNREFUSED',
      'EHOSTUNREACH',
      'ENETUNREACH',
      'EADDRINUSE',
    ]);

    if (typeof err?.code === 'string' && nodeErrorCodes.has(err.code)) {
      return true;
    }

    const message = typeof err?.message === 'string' ? err.message : '';
    if (typeof nameOrCode === 'string' && nameOrCode.toLowerCase().includes('timeout')) {
      return true;
    }

    if (message.toLowerCase().includes('timeout') || message.toLowerCase().includes('timed out')) {
      return true;
    }

    return false;
  };

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
      this.throwIfAborted();
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
        this.throwIfAborted();
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
      this.throwIfAborted();
      const page = await this.fetchPageWithRetry(
        queryExecutionId,
        rowParser,
        nextToken,
      );

      for (const row of page.rows) {
        this.throwIfAborted();
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
      this.throwIfAborted();
      if (this.options.maxRows !== undefined && totalRows >= this.options.maxRows) {
        break;
      }

      const page = await this.fetchPageWithRetry(
        queryExecutionId,
        rowParser,
        nextToken,
      );

      let rowsToProcess = page.rows;
      if (this.options.maxRows !== undefined) {
        const remaining = Math.max(this.options.maxRows - totalRows, 0);
        rowsToProcess = page.rows.slice(0, remaining);
      }

      this.throwIfAborted();
      await batchProcessor(rowsToProcess, pageCount);

      pageCount++;
      totalRows += rowsToProcess.length;

      // maxRows check
      if (this.options.maxRows !== undefined && totalRows >= this.options.maxRows) {
        break;
      }

      nextToken = page.nextToken;
    } while (nextToken);

    return { totalRows, pageCount };
  }

  /**
   * Fetch a single result page with retry.
   *
   * Retries are performed only for transient failures (throttling, 5xx, timeouts, etc.).
   * Permanent failures (auth/permission/validation) fail fast.
   */
  private async fetchPageWithRetry<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
    nextToken?: string,
  ): Promise<PageResult<T>> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= this.retryCount; attempt++) {
      this.throwIfAborted();
      try {
        return await this.pager.fetchPageWith(queryExecutionId, rowParser, nextToken);
      } catch (error) {
        this.throwIfAborted();
        const err = error as Error;
        lastError = err;

        // Fail-fast on permanent errors (auth/permission/validation, etc.)
        if (!this.isRetryableError(error)) {
          throw err;
        }

        if (attempt < this.retryCount) {
          await this.sleep(this.retryDelayMs);
          continue;
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch page after retries');
  }

  private normalizeNonNegativeInteger(value: number | undefined, fallback: number): number {
    if (value === undefined) {
      return fallback;
    }

    if (!Number.isFinite(value) || Number.isNaN(value) || value < 0) {
      return fallback;
    }

    return Math.floor(value);
  }

  private throwIfAborted(): void {
    if (!this.signal) {
      return;
    }

    if (!this.signal.aborted) {
      return;
    }

    throw this.createAbortError();
  }

  private createAbortError(): Error {
    const error = new Error(this.signal?.reason instanceof Error ? this.signal.reason.message : 'Aborted');
    (error as unknown as { name: string }).name = 'AbortError';
    return error;
  }

  private sleep(ms: number): Promise<void> {
    if (!this.signal) {
      return new Promise((resolve) => setTimeout(resolve, ms));
    }

    const signal = this.signal;

    if (signal.aborted) {
      return Promise.reject(this.createAbortError());
    }

    return new Promise((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(this.createAbortError());
      };

      const timeout = setTimeout(() => {
        cleanup();
        resolve();
      }, ms);

      const cleanup = () => {
        clearTimeout(timeout);
        signal.removeEventListener('abort', onAbort);
      };

      signal.addEventListener('abort', onAbort, { once: true });
    });
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