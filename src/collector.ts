import { AthenaClient } from '@aws-sdk/client-athena';
import { AthenaQueryResultPager, type ParsedRow, type RowParser, type PageResult, type PagerOptions } from 'athena-query-result-pager';
import {
  AthenaQueryResultCollectorError,
  AthenaQueryResultCollectorAbortError,
  AthenaQueryResultCollectorConcurrentUseError,
} from './core/errors';
import { isMaxRowsReached, remainingRowBudget } from './core/max-rows-predicates';
import { hasMorePages } from './core/page-predicates';

/**
 * Options for {@link AthenaQueryResultCollector}.
 *
 * Extends {@link PagerOptions} from `athena-query-result-pager` (for example `maxResults`,
 * `queryResultType`, and `parseResultSetOptions`), plus collection-specific limits,
 * progress callbacks, and cancellation.
 *
 * Transient API failures (throttling, 5xx, network errors) are retried by the injected
 * {@link AthenaClient} (for example `maxAttempts`). This collector does not add an
 * application-level retry loop on top of `GetQueryResults`.
 */
export interface CollectorOptions extends PagerOptions {
  /**
   * Maximum number of rows to collect or process across **all pages**.
   *
   * This is a collector-level cumulative cap for {@link AthenaQueryResultCollector.collect},
   * {@link AthenaQueryResultCollector.stream}, and {@link AthenaQueryResultCollector.processBatches}.
   * It is **not** the pager/parser `maxRows` option, which limits rows inside a single Athena
   * `ResultSet` (one page).
   *
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
   * Optional `AbortSignal` to cancel long-running collection, streaming, or batch processing.
   *
   * When aborted, the collector stops pagination loops and rejects pending page-fetch waits,
   * then throws {@link AthenaQueryResultCollectorAbortError}
   * (`name === 'AbortError'` for AbortSignal ecosystem compatibility).
   * In-flight HTTP requests are not cancelled unless the underlying pager or AWS SDK client
   * honors the same signal.
   */
  signal?: AbortSignal;
}

/**
 * Result of {@link AthenaQueryResultCollector.collect} or {@link AthenaQueryResultCollector.collectWith}.
 *
 * `rows` holds the full collected set in memory. For large result sets prefer
 * {@link AthenaQueryResultCollector.stream}, {@link AthenaQueryResultCollector.processBatches},
 * or the pager iterators from {@link AthenaQueryResultCollector.getPager}.
 *
 * @typeParam T - Row type produced by the collector or custom {@link RowParser}.
 */
export interface CollectResult<T> {
  /**
   * All collected rows (possibly truncated when `maxRows` is set).
   * This array retains every collected row in memory.
   */
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
 * with optional row limits and cancellation via {@link AbortSignal}.
 *
 * Transient `GetQueryResults` failures are retried by the injected {@link AthenaClient}
 * (for example `maxAttempts`). This class does not wrap page fetches in an extra retry loop.
 *
 * {@link AthenaQueryResultCollector.stream} and {@link AthenaQueryResultCollector.processBatches}
 * paginate like the pager iterators (`iterateRows`, `iteratePagesWith`) by fetching pages through
 * collector abort checks and error normalization.
 *
 * Memory guidance: {@link AthenaQueryResultCollector.collect} and
 * {@link AthenaQueryResultCollector.collectWith} accumulate every row into an array and may
 * exhaust process memory on large results. Prefer {@link AthenaQueryResultCollector.stream}
 * or {@link AthenaQueryResultCollector.processBatches} for large sets, or use
 * {@link AthenaQueryResultCollector.getPager} and call `iterateRows` / `iteratePages`
 * (or `iteratePagesWith`) for lower-level streaming.
 *
 * Concurrency: One instance is **not** safe for overlapping {@link AthenaQueryResultCollector.collect},
 * {@link AthenaQueryResultCollector.collectWith}, {@link AthenaQueryResultCollector.stream}, or
 * {@link AthenaQueryResultCollector.processBatches} calls. The internal pager keeps parser state
 * (for example header-row bookkeeping) that {@link AthenaQueryResultPager.reset} clears between
 * executions. A second in-flight operation throws. For parallel work, create separate collector
 * instances (sharing the same `AthenaClient` is fine). Direct pager use via
 * {@link AthenaQueryResultCollector.getPager} must also be serialized with collector operations.
 */
export class AthenaQueryResultCollector {

  private readonly pager: AthenaQueryResultPager;
  private readonly options: CollectorOptions;
  private readonly signal?: AbortSignal;
  private activeOperation?: 'collect' | 'stream' | 'processBatches';

  /**
   * @param client - AWS SDK v3 `AthenaClient` used to fetch query results.
   * @param options - Collection limits, pager settings, and optional abort signal.
   * @throws {AthenaQueryResultPagerInvalidMaxResultsError} When `maxResults` is not an integer in `1..1000`.
   */
  constructor(client: AthenaClient, options: CollectorOptions = {}) {
    this.pager = new AthenaQueryResultPager(client, this.toPagerOptions(options));
    this.options = options;
    this.signal = options.signal;
  }

  /**
   * Picks pager-only fields from {@link CollectorOptions} for {@link AthenaQueryResultPager}.
   *
   * Collector-specific options (`maxRows`, `onPage`, `signal`) are not forwarded.
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
   * Marks the start of a collector operation and rejects overlapping use of the same instance.
   *
   * @param operation - The operation being started.
   * @throws {AthenaQueryResultCollectorConcurrentUseError} When another operation is already in flight on this collector.
   */
  private beginOperation(operation: 'collect' | 'stream' | 'processBatches'): void {
    if (this.activeOperation !== undefined) {
      throw new AthenaQueryResultCollectorConcurrentUseError(this.activeOperation);
    }

    this.activeOperation = operation;
  }

  /**
   * Clears the in-flight operation marker after `collect`, `stream`, or `processBatches` completes.
   */
  private endOperation(): void {
    this.activeOperation = undefined;
  }

  /**
   * Yields result pages until Athena returns no continuation token.
   *
   * Same pagination shape as {@link AthenaQueryResultPager.iteratePagesWith}: fetch a page,
   * yield it, then continue while {@link hasMorePages} is true. Each fetch goes through
   * {@link AthenaQueryResultCollector.fetchPage} so abort and error normalization stay on
   * the collector. Transient API retries are left to {@link AthenaClient}.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @yields Successive pages in execution order.
   */
  private async *iteratePages<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): AsyncGenerator<PageResult<T>> {
    let nextToken: string | undefined;

    do {
      const page = await this.fetchPage(queryExecutionId, rowParser, nextToken);
      yield page;
      nextToken = page.nextToken;
    } while (hasMorePages(nextToken));
  }

  /**
   * Yields rows from {@link AthenaQueryResultCollector.iteratePages} without buffering
   * the full result set.
   *
   * Same shape as {@link AthenaQueryResultPager.iterateRows}.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @yields Successive `T` values in execution order.
   */
  private async *iterateRows<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): AsyncGenerator<T> {
    for await (const page of this.iteratePages(queryExecutionId, rowParser)) {
      for (const row of page.rows) {
        yield row;
      }
    }
  }

  /**
   * Collects all rows as dictionary-shaped {@link ParsedRow} objects into a single in-memory array.
   *
   * Suitable for small-to-moderate results. Large result sets can exhaust process memory (OOM);
   * use {@link AthenaQueryResultCollector.stream}, {@link AthenaQueryResultCollector.processBatches},
   * or pager iterators via {@link AthenaQueryResultCollector.getPager} instead.
   *
   * @param queryExecutionId - Athena query execution identifier.
   * @returns Aggregated rows and collection metadata.
   * @throws {AthenaQueryResultCollectorAbortError} When `AbortSignal` aborts (`name === 'AbortError'`).
   * @throws {AthenaQueryResultCollectorConcurrentUseError} When another operation is already in flight.
   * @throws {Error} When page fetch fails. Intentional `Error` subclasses from the pager or parser (for example {@link AthenaQueryResultPagerError}) are rethrown unchanged.
   */
  async collect(queryExecutionId: string): Promise<CollectResult<ParsedRow>> {
    return this.collectWith(queryExecutionId, (row) => row);
  }

  /**
   * Collects all rows and maps each {@link ParsedRow} through `rowParser` into a single in-memory array.
   *
   * Paginates like {@link AthenaQueryResultPager.iteratePagesWith} via
   * {@link AthenaQueryResultCollector.iteratePages}, applying collector options
   * (`maxRows`, `onPage`, `signal`) on each fetched page.
   *
   * Accumulates every transformed row before returning. Suitable for small-to-moderate results.
   * Large result sets can exhaust process memory (OOM); prefer
   * {@link AthenaQueryResultCollector.stream} or {@link AthenaQueryResultCollector.processBatches},
   * or call `iterateRows` / `iteratePages` / `iteratePagesWith` on
   * {@link AthenaQueryResultCollector.getPager}.
   *
   * Respects {@link CollectorOptions.maxRows}, invokes {@link CollectorOptions.onPage} after each page,
   * and honors {@link CollectorOptions.signal} between pages.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @returns Aggregated transformed rows and collection metadata.
   * @throws {AthenaQueryResultCollectorAbortError} When `AbortSignal` aborts (`name === 'AbortError'`).
   * @throws {AthenaQueryResultCollectorConcurrentUseError} When another operation is already in flight.
   * @throws {Error} When page fetch fails. Intentional `Error` subclasses from the pager or parser (for example {@link AthenaQueryResultPagerError}) are rethrown unchanged.
   */
  async collectWith<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): Promise<CollectResult<T>> {
    this.beginOperation('collect');

    const rows: T[] = [];
    let pageCount = 0;
    let truncated = false;

    try {
      this.pager.reset();

      this.throwIfAborted();

      for await (const page of this.iteratePages(queryExecutionId, rowParser)) {
        this.throwIfAborted();

        pageCount++;

        const remaining = remainingRowBudget(rows.length, this.options.maxRows);
        if (remaining !== undefined && page.rows.length >= remaining) {
          rows.push(...page.rows.slice(0, remaining));
          truncated = true;
          break;
        }

        rows.push(...page.rows);

        if (this.options.onPage) {
          this.throwIfAborted();
          await this.options.onPage(page, rows.length);
        }
      }

      return {
        rows,
        totalRows: rows.length,
        pageCount,
        truncated,
      };
    } finally {
      this.endOperation();
    }
  }

  /**
   * Lazily yields rows one at a time without buffering the full result set in memory.
   *
   * Paginates like {@link AthenaQueryResultPager.iterateRows} via
   * {@link AthenaQueryResultCollector.iterateRows}, adding
   * {@link CollectorOptions.maxRows} and {@link CollectorOptions.signal} handling.
   *
   * Prefer this over {@link AthenaQueryResultCollector.collect} /
   * {@link AthenaQueryResultCollector.collectWith} when the result set may be large.
   * Only the current page is retained while iterating.
   *
   * Stops after {@link CollectorOptions.maxRows} rows when set. Checks
   * {@link CollectorOptions.signal} before each page fetch and before each yielded row.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @yields Successive `T` values in execution order.
   * @throws {AthenaQueryResultCollectorAbortError} When `AbortSignal` aborts (`name === 'AbortError'`).
   * @throws {AthenaQueryResultCollectorConcurrentUseError} When another operation is already in flight.
   * @throws {Error} When page fetch fails. Intentional `Error` subclasses from the pager or parser (for example {@link AthenaQueryResultPagerError}) are rethrown unchanged.
   */
  async *stream<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
  ): AsyncGenerator<T> {
    this.beginOperation('stream');

    let count = 0;

    try {
      this.pager.reset();

      this.throwIfAborted();

      for await (const row of this.iterateRows(queryExecutionId, rowParser)) {
        this.throwIfAborted();
        if (isMaxRowsReached(count, this.options.maxRows)) {
          return;
        }
        yield row;
        count++;
      }
    } finally {
      this.endOperation();
    }
  }

  /**
   * Processes each fetched page through `batchProcessor` without accumulating all rows in memory.
   *
   * Paginates like {@link AthenaQueryResultPager.iteratePagesWith} via
   * {@link AthenaQueryResultCollector.iteratePages}, adding
   * {@link CollectorOptions.maxRows} and {@link CollectorOptions.signal} handling.
   *
   * Prefer this over {@link AthenaQueryResultCollector.collect} /
   * {@link AthenaQueryResultCollector.collectWith} when writing or forwarding page-sized chunks
   * from a large result set. Only the current page is passed to `batchProcessor`.
   *
   * Respects {@link CollectorOptions.maxRows} across pages and checks
   * {@link CollectorOptions.signal} before each fetch and before invoking `batchProcessor`.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @param batchProcessor - Receives the rows for one page and its zero-based index.
   * @returns Total rows processed and number of pages handled.
   * @throws {AthenaQueryResultCollectorAbortError} When `AbortSignal` aborts (`name === 'AbortError'`).
   * @throws {AthenaQueryResultCollectorConcurrentUseError} When another operation is already in flight.
   * @throws {Error} When page fetch fails. Intentional `Error` subclasses from the pager or parser (for example {@link AthenaQueryResultPagerError}) are rethrown unchanged.
   */
  async processBatches<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
    batchProcessor: (rows: T[], pageIndex: number) => void | Promise<void>,
  ): Promise<{ totalRows: number; pageCount: number }> {
    this.beginOperation('processBatches');

    let pageCount = 0;
    let totalRows = 0;

    try {
      this.pager.reset();

      if (this.options.maxRows === 0) {
        return { totalRows: 0, pageCount: 0 };
      }

      this.throwIfAborted();

      for await (const page of this.iteratePages(queryExecutionId, rowParser)) {
        this.throwIfAborted();
        if (isMaxRowsReached(totalRows, this.options.maxRows)) {
          break;
        }

        let rowsToProcess = page.rows;
        const remaining = remainingRowBudget(totalRows, this.options.maxRows);
        if (remaining !== undefined) {
          rowsToProcess = page.rows.slice(0, remaining);
        }

        this.throwIfAborted();
        await batchProcessor(rowsToProcess, pageCount);

        pageCount++;
        totalRows += rowsToProcess.length;

        if (isMaxRowsReached(totalRows, this.options.maxRows)) {
          break;
        }
      }

      return { totalRows, pageCount };
    } finally {
      this.endOperation();
    }
  }

  /**
   * Fetches a single results page with abort awareness and error normalization.
   *
   * Checks {@link CollectorOptions.signal} before the fetch and races the in-flight fetch
   * against abort. Transient API retries are performed by {@link AthenaClient}, not here.
   *
   * @typeParam T - Output type produced by `rowParser`.
   * @param queryExecutionId - Athena query execution identifier.
   * @param rowParser - Converts each parsed row into `T`.
   * @param nextToken - Continuation token; omit on the first page.
   * @returns One page of transformed rows and pagination metadata.
   * @throws {AthenaQueryResultCollectorAbortError} When `AbortSignal` aborts (`name === 'AbortError'`).
   * @throws {Error} When the page fetch fails.
   *   Intentional `Error` subclasses (for example {@link AthenaQueryResultPagerError}, parser errors, `RangeError`)
   *   are rethrown unchanged; other rejections are normalized to `Error` with `cause`.
   */
  private async fetchPage<T>(
    queryExecutionId: string,
    rowParser: RowParser<T>,
    nextToken?: string,
  ): Promise<PageResult<T>> {
    this.throwIfAborted();
    try {
      return await this.raceWithAbort(
        this.pager.fetchPageWith(queryExecutionId, rowParser, nextToken),
      );
    } catch (error: unknown) {
      this.throwIfAborted();
      throw this.normalizeError(error);
    }
  }

  /**
   * Returns whether `error` represents an abort/cancellation (not a page-fetch failure).
   *
   * Uses the AbortSignal ecosystem convention `error.name === 'AbortError'` rather than
   * `instanceof Error`, because host objects such as `DOMException` may fail `instanceof`
   * checks across Jest VM realms.
   *
   * @param error - Rejected or thrown value.
   */
  private isAbortError(error: unknown): boolean {
    if (typeof error !== 'object' || error === null) {
      return false;
    }

    return (error as { name?: unknown }).name === 'AbortError';
  }

  /**
   * Converts an unknown rejection into an `Error` without losing intentional subclasses.
   *
   * - {@link AthenaQueryResultCollectorError} instances are returned as-is.
   * - AbortSignal-style errors (`error.name === 'AbortError'`, including `DOMException`) are
   *   wrapped as {@link AthenaQueryResultCollectorAbortError} with the original value in `cause`, so callers can
   *   use both `instanceof AthenaQueryResultCollectorError` and `error.name === 'AbortError'`.
   * - Other existing `Error` instances (pager {@link AthenaQueryResultPagerError}, parser errors, AWS SDK errors, and similar)
   *   are returned as-is so original `instanceof` checks keep working.
   * - Non-`Error` values are wrapped in `Error` with a derived message and `Error.cause`.
   *
   * @param error - Rejected or thrown value from the pager or AWS SDK.
   * @returns An `Error` suitable for rethrowing to callers.
   */
  private normalizeError(error: unknown): Error {
    if (error instanceof AthenaQueryResultCollectorError) {
      return error;
    }

    if (this.isAbortError(error)) {
      return new AthenaQueryResultCollectorAbortError(this.extractErrorMessage(error), { cause: error });
    }

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
   * Throws a {@link AthenaQueryResultCollectorAbortError} when {@link CollectorOptions.signal} has aborted.
   *
   * @throws {AthenaQueryResultCollectorAbortError} With `name === 'AbortError'` when the signal is aborted.
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
   * Builds a {@link AthenaQueryResultCollectorAbortError} from {@link CollectorOptions.signal} reason when present.
   *
   * `name` is `AbortError` for AbortSignal ecosystem compatibility. The signal reason is
   * attached as `cause` when it is not already represented by the message alone.
   *
   * @returns An error suitable for rejection when collection is cancelled.
   */
  private createAbortError(): AthenaQueryResultCollectorAbortError {
    const reason = this.signal?.reason;

    if (reason instanceof Error) {
      return new AthenaQueryResultCollectorAbortError(reason.message, { cause: reason });
    }

    if (typeof reason === 'string') {
      return new AthenaQueryResultCollectorAbortError(reason);
    }

    if (reason !== undefined) {
      return new AthenaQueryResultCollectorAbortError('Aborted', { cause: reason });
    }

    return new AthenaQueryResultCollectorAbortError('Aborted');
  }

  /**
   * Rejects when {@link CollectorOptions.signal} aborts while `promise` is still pending.
   *
   * Does not cancel the underlying operation (for example an in-flight HTTP request)
   * unless the callee honors the same signal.
   *
   * @typeParam T - Resolved value type of `promise`.
   * @param promise - Operation to await (for example `fetchPageWith`).
   * @returns `promise` result, or rejects with {@link AthenaQueryResultCollectorAbortError} if aborted first.
   * @throws {AthenaQueryResultCollectorAbortError} With `name === 'AbortError'` when the signal aborts before `promise` settles.
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
   * Returns the internal {@link AthenaQueryResultPager} for advanced pagination use.
   *
   * For large result sets, call the pager's memory-efficient iterators directly
   * (`iterateRows`, `iteratePages`, `iteratePagesWith`) instead of
   * {@link AthenaQueryResultCollector.collect} / {@link AthenaQueryResultCollector.collectWith}.
   * Note that collector options such as `maxRows`, `onPage`, and `signal` are not
   * applied when you drive the pager yourself.
   *
   * Do not use the pager while a collector operation is in flight on the same instance.
   * The pager shares parser state with collector methods. The pager auto-resets when
   * `queryExecutionId` changes; {@link AthenaQueryResultPager.reset} is still available to
   * clear state explicitly (for example before reusing the same execution id).
   *
   * @returns The pager instance constructed with the same client and {@link PagerOptions} fields from the collector options.
   */
  getPager(): AthenaQueryResultPager {
    return this.pager;
  }
}
