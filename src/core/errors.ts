/**
 * Optional constructor options for {@link AthenaQueryResultCollectorError}.
 *
 * Typed locally because this project compiles against ES2020, which does not include `ErrorOptions`.
 */
export interface CollectorErrorOptions {
  cause?: unknown;
}

/**
 * Base error thrown by {@link AthenaQueryResultCollector}.
 *
 * Collector-originated failures share this type so callers can use
 * `instanceof AthenaQueryResultCollectorError` and branch on {@link AthenaQueryResultCollectorError.code}.
 *
 * Pager, parser, and AWS SDK errors are **not** wrapped in this class when they are already
 * `Error` instances; they are rethrown unchanged so original `instanceof` checks keep working.
 */
export abstract class AthenaQueryResultCollectorError extends Error {
  override readonly name: string = 'AthenaQueryResultCollectorError';
  abstract readonly code: string;
  readonly cause?: unknown;

  protected constructor(message: string, options?: CollectorErrorOptions) {
    super(message);
    Object.setPrototypeOf(this, AthenaQueryResultCollectorError.prototype);
    if (options?.cause !== undefined) {
      this.cause = options.cause;
    }
  }
}

/**
 * Thrown when a second collector operation starts while another is still in flight
 * on the same {@link AthenaQueryResultCollector} instance.
 */
export class AthenaQueryResultCollectorConcurrentUseError extends AthenaQueryResultCollectorError {
  override readonly name: string = 'AthenaQueryResultCollectorConcurrentUseError';
  readonly code = 'CONCURRENT_USE' as const;

  /**
   * @param activeOperation - The operation already running (`collect`, `stream`, or `processBatches`).
   */
  constructor(activeOperation: string) {
    super(
      `AthenaQueryResultCollector is already running ${activeOperation}; `
      + 'use one operation at a time per instance or create a separate collector.',
    );
    Object.setPrototypeOf(this, AthenaQueryResultCollectorConcurrentUseError.prototype);
  }
}

/**
 * Thrown when collection is cancelled via {@link AbortSignal}.
 *
 * `name` is always `AbortError` (not the class name) so this remains compatible with
 * AbortSignal ecosystem checks (`error.name === 'AbortError'`), including `DOMException`
 * AbortErrors from other layers that are normalized with the original value in
 * {@link Error.cause}.
 */
export class AthenaQueryResultCollectorAbortError extends AthenaQueryResultCollectorError {
  override readonly name: string = 'AbortError';
  readonly code = 'ABORT' as const;

  /**
   * @param message - Abort reason text.
   * @param options - Optional options (for example `{ cause }` for the original abort value).
   */
  constructor(message: string = 'Aborted', options?: CollectorErrorOptions) {
    super(message, options);
    Object.setPrototypeOf(this, AthenaQueryResultCollectorAbortError.prototype);
  }
}
