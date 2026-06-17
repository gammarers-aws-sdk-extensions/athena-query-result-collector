import { AthenaClient } from '@aws-sdk/client-athena';
import { AthenaQueryResultPager, type ParsedRow, type RowParser, type PageResult, type PagerOptions } from 'athena-query-result-pager';

/**
 * Options for {@link AthenaQueryResultCollector}.
 *
 * Extends {@link PagerOptions} from `athena-query-result-pager` (for example `maxResults`,
 * `queryResultType`, and `parseResultSetOptions`), plus collection-specific limits, retries,
 * progress callbacks, and cancellation.
 */
export interface CollectorOptions extends PagerOptions {
  /**
   * Maximum number of rows to collect or process.
   * @defaultValue Unlimited when omitted.
   */
  maxRows?: number;
  /**
   * Callback invoked after each page is fetched in {@link AthenaQueryResultCollector.collect}
   * and {@link AthenaQueryResultCollector.collectWith}.
   *
   * @param page - The page just retrieved.
   * @param totalCollected - Cumulative row count after appending this page (respecting `maxRows`).
   */
  onPage?: <T>(page: PageResult<T>, totalCollected: number) => void | Promise<void>;
  /**
   * Number of additional attempts after the first page-fetch failure.
   * Only transient errors (throttling, 5xx, timeouts, and similar) are retried.
   * @defaultValue `0`
   */
  retryCount?: number;
  /**
   * Delay in milliseconds between retry attempts.
   * Interruptible when {@link CollectorOptions.signal} aborts.
   * @defaultValue `1000`
   */
  retryDelayMs?: number;
  /**
   * Optional `AbortSignal` to cancel long-running collection, streaming, or batch processing.
   *
   * When aborted, the collector stops pagination loops, rejects pending page-fetch waits,
   * and interrupts retry backoff sleep, then throws an `AbortError` (`DOMException` when available).
   * In-flight HTTP requests are not cancelled unless the underlying pager or AWS SDK client
   * honors the same signal.
   */
  signal?: AbortSignal;
}

/**
 * Result of {@link AthenaQueryResultCollector.collect} or {@link AthenaQueryResultCollector.collectWith}.
 *
 * @typeParam T - Row type produced by the collector or custom {@link RowParser}.
 */
export interface CollectResult<T> {
  /** All collected rows (possibly truncated when `maxRows` is set). */
  rows: T[];
  /** Same as `rows.length`. */
  totalRows: number;
  /** Number of pages fetched from Athena before stopping. */
  pageCount: number;
  /** `true` when collection stopped early because `maxRows` was reached. */
  truncated: boolean;
}

/**
 * Collects AWS Athena query results by paginating `GetQueryResults` through
 * {@link AthenaQueryResultPager}.
 *
 * Supports full in-memory collection, lazy row streaming, and per-page batch processing,
 * with optional row limits, transient-error retries, and cancellation via {@link AbortSignal}.
 */
export class AthenaQueryResultCollector {

  private readonly pager: AthenaQueryResultPager;
  private readonly options: CollectorOptions;
  private readonly retryCount: number;
  private readonly retryDelayMs: number;
  private readonly signal?: AbortSignal;

  /**
   * @param client - AWS SDK v3 `AthenaClient` used to fetch query results.
   * @param options - Collection limits, retries, pager settings, and optional abort signal.
   */
  constructor(client: AthenaClient, options: CollectorOptions = {}) {
    const retryCount = this.normalizeNonNegativeInteger(options.retryCount, 0);
    const retryDelayMs = this.normalizeNonNegativeInteger(options.retryDelayMs, 1000);

    this.pager = new AthenaQueryResultPager(client, this.toPagerOptions(options));
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
   * Picks pager-only fields from {@link CollectorOptions} for {@link AthenaQueryResultPager}.
   *
   * Collector-specific options (`maxRows`, `onPage`, retries, `signal`) are not forwarded.
   *
   * @param options - Full collector options from the constructor.
   */
  private toPagerOptions(options: CollectorOptions): PagerOptions {
    const pagerOptions: PagerOptions = {};

    if (options.maxResults !== undefined) {
      pagerOptions.maxResults = options.maxResults;
    }
    if (options.queryResultType !== undefined) {
      pagerOptions.queryResultType = options.queryResultType;
    }
    if (options.parseResultSetOptions !== undefined) {
      pagerOptions.parseResultSetOptions = options.parseResultSetOptions;
    }

    return pagerOptions;
  }

  /**
   * Returns whether a page-fetch error is likely transient and safe to retry.
   *
   * Intentionally conservative: authentication, authorization, and validation errors
   * should fail fast rather than being retried.
   *
   * @param error - Rejected or thrown value from the pager or AWS SDK.
   * @returns `true` for throttling, 5xx, timeout, and similar transient failures.
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
   * Collects all rows as dictionary-shaped {@link ParsedRow} objects.
   *
   * @param queryExecutionId - Athena query execution identifier.
   * @returns Aggregated rows and collection metadata.
   * @throws {Error} When `AbortSignal` aborts (name `AbortError`), page fetch fails permanently, or retries are exhausted.
   */
  async collect(queryExecutionId: string): Promise<CollectResult<ParsedRow>> {
    return this.collectWith(queryExecutionId, (row) => row);
  }

  /**
   * Collects all rows and maps each {@link ParsedRow} through `rowParser`.
   *
   * Respects {@link CollectorOptions.maxRows}, invokes {@link CollectorOptions.onPage} after each page,
   * and honors {@link CollectorOptions.signal} between pages and during retries.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @returns Aggregated transformed rows and collection metadata.
   * @throws {Error} When `AbortSignal` aborts (name `AbortError`), page fetch fails permanently, or retries are exhausted.
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
   * Lazily yields rows one at a time without buffering the full result set in memory.
   *
   * Stops after {@link CollectorOptions.maxRows} rows when set. Checks
   * {@link CollectorOptions.signal} before each page fetch and before each yielded row.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @yields Successive `T` values in execution order.
   * @throws {Error} When `AbortSignal` aborts (name `AbortError`), page fetch fails permanently, or retries are exhausted.
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
   * Processes each fetched page through `batchProcessor` without accumulating all rows in memory.
   *
   * Respects {@link CollectorOptions.maxRows} across pages and checks
   * {@link CollectorOptions.signal} before each fetch and before invoking `batchProcessor`.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @param batchProcessor - Receives the rows for one page and its zero-based index.
   * @returns Total rows processed and number of pages handled.
   * @throws {Error} When `AbortSignal` aborts (name `AbortError`), page fetch fails permanently, or retries are exhausted.
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
   * Fetches a single results page with transient-error retries and abort awareness.
   *
   * Checks {@link CollectorOptions.signal} before each attempt, races the in-flight fetch
   * against abort, and uses interruptible backoff between retries.
   * Permanent failures (auth, permission, validation) fail fast without retrying.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @param nextToken - Continuation token; omit on the first page.
   * @returns One page of transformed rows and pagination metadata.
   * @throws {Error} When `AbortSignal` aborts (name `AbortError`), a permanent error occurs, or retries are exhausted.
   *   Intentional `Error` subclasses (for example `RangeError`) are rethrown unchanged; other rejections are normalized to `Error`.
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
        return await this.raceWithAbort(
          this.pager.fetchPageWith(queryExecutionId, rowParser, nextToken),
        );
      } catch (error: unknown) {
        this.throwIfAborted();

        if (this.isAbortError(error)) {
          throw this.normalizeError(error);
        }

        if (!this.isRetryableError(error)) {
          throw this.normalizeError(error);
        }

        lastError = this.normalizeError(error);

        if (attempt < this.retryCount) {
          await this.sleep(this.retryDelayMs);
          continue;
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch page after retries');
  }

  /**
   * Returns whether `error` represents an abort/cancellation (not a page-fetch failure).
   *
   * @param error - Rejected or thrown value.
   */
  private isAbortError(error: unknown): boolean {
    return error instanceof Error && error.name === 'AbortError';
  }

  /**
   * Converts an unknown rejection into an `Error` without losing intentional subclasses.
   *
   * Existing `Error` instances (including `RangeError`, `TypeError`, and `AbortError`) are
   * returned as-is. Other values are wrapped in `Error` with a derived message; non-primitive
   * values are attached via `Error.cause` when available.
   *
   * @param error - Rejected or thrown value from the pager or AWS SDK.
   * @returns An `Error` suitable for rethrowing to callers.
   */
  private normalizeError(error: unknown): Error {
    if (error instanceof Error) {
      return error;
    }

    if (typeof error === 'string') {
      return new Error(error);
    }

    const message = this.extractErrorMessage(error);
    const normalized = new Error(message);
    if (error !== undefined) {
      (normalized as Error & { cause?: unknown }).cause = error;
    }
    return normalized;
  }

  /**
   * Derives a human-readable message from a non-`Error` rejection value.
   *
   * @param error - Rejected value that is not an `Error` instance.
   */
  private extractErrorMessage(error: unknown): string {
    if (typeof error === 'string') {
      return error;
    }

    if (typeof error === 'object' && error !== null) {
      const record = error as Record<string, unknown>;
      if (typeof record.message === 'string') {
        return record.message;
      }
      if (typeof record.Message === 'string') {
        return record.Message;
      }
    }

    if (error === undefined) {
      return 'Unknown error';
    }

    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }

  /**
   * Normalizes a numeric option to a non-negative integer, falling back when invalid.
   *
   * @param value - User-supplied option value.
   * @param fallback - Default used when `value` is `undefined`, non-finite, `NaN`, or negative.
   * @returns A floored integer `>= 0`.
   */
  private normalizeNonNegativeInteger(value: number | undefined, fallback: number): number {
    if (value === undefined) {
      return fallback;
    }

    if (!Number.isFinite(value) || Number.isNaN(value) || value < 0) {
      return fallback;
    }

    return Math.floor(value);
  }

  /**
   * Throws an `AbortError` when {@link CollectorOptions.signal} has aborted.
   *
   * @throws {Error} With name `AbortError` when the signal is aborted.
   */
  private throwIfAborted(): void {
    if (!this.signal) {
      return;
    }

    if (!this.signal.aborted) {
      return;
    }

    throw this.createAbortError();
  }

  /**
   * Builds an `AbortError` from {@link CollectorOptions.signal} reason when present.
   *
   * Uses `DOMException` on runtimes that provide it; otherwise sets `Error.name` to `AbortError`.
   *
   * @returns An error suitable for rejection when collection is cancelled.
   */
  private createAbortError(): Error {
    const message = this.signal?.reason instanceof Error
      ? this.signal.reason.message
      : typeof this.signal?.reason === 'string'
        ? this.signal.reason
        : 'Aborted';

    if (typeof DOMException !== 'undefined') {
      return new DOMException(message, 'AbortError');
    }

    const error = new Error(message);
    (error as unknown as { name: string }).name = 'AbortError';
    return error;
  }

  /**
   * Rejects when {@link CollectorOptions.signal} aborts while `promise` is still pending.
   *
   * Does not cancel the underlying operation (for example an in-flight HTTP request)
   * unless the callee honors the same signal.
   *
   * @typeParam T - Resolved value type of `promise`.
   * @param promise - Operation to await (for example `fetchPageWith`).
   * @returns `promise` result, or rejects with `AbortError` if aborted first.
   * @throws {Error} With name `AbortError` when the signal aborts before `promise` settles.
   */
  private raceWithAbort<T>(promise: Promise<T>): Promise<T> {
    if (!this.signal) {
      return promise;
    }

    const signal = this.signal;
    if (signal.aborted) {
      return Promise.reject(this.createAbortError());
    }

    return new Promise<T>((resolve, reject) => {
      const onAbort = () => {
        cleanup();
        reject(this.createAbortError());
      };

      const cleanup = () => {
        signal.removeEventListener('abort', onAbort);
      };

      signal.addEventListener('abort', onAbort, { once: true });

      promise.then(
        (value) => {
          cleanup();
          resolve(value);
        },
        (error: unknown) => {
          cleanup();
          reject(this.normalizeError(error));
        },
      );
    });
  }

  /**
   * Waits for `ms` milliseconds, rejecting early when {@link CollectorOptions.signal} aborts.
   *
   * @param ms - Backoff duration between retry attempts.
   * @throws {Error} With name `AbortError` when the signal aborts during the wait.
   */
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
   * Returns the internal {@link AthenaQueryResultPager} for advanced pagination use.
   *
   * @returns The pager instance constructed with the same client and {@link PagerOptions} fields from the collector options.
   */
  getPager(): AthenaQueryResultPager {
    return this.pager;
  }
}

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