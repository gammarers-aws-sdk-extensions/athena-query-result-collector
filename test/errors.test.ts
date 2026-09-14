import {
  AthenaQueryResultCollectorError,
  AthenaQueryResultCollectorAbortError,
  AthenaQueryResultCollectorConcurrentUseError,
} from '../src';

describe('AthenaQueryResultCollectorError hierarchy', () => {
  it('makes AthenaQueryResultCollectorConcurrentUseError a typed collector error', () => {
    const error = new AthenaQueryResultCollectorConcurrentUseError('collect');

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(AthenaQueryResultCollectorError);
    expect(error).toBeInstanceOf(AthenaQueryResultCollectorConcurrentUseError);
    expect(error.name).toBe('AthenaQueryResultCollectorConcurrentUseError');
    expect(error.code).toBe('CONCURRENT_USE');
    expect(error.message).toContain('already running collect');
  });

  it('uses AbortError name on AthenaQueryResultCollectorAbortError for AbortSignal compatibility', () => {
    const cause = new Error('timeout exceeded');
    const error = new AthenaQueryResultCollectorAbortError('timeout exceeded', { cause });

    expect(error).toBeInstanceOf(Error);
    expect(error).toBeInstanceOf(AthenaQueryResultCollectorError);
    expect(error).toBeInstanceOf(AthenaQueryResultCollectorAbortError);
    expect(error.name).toBe('AbortError');
    expect(error.code).toBe('ABORT');
    expect(error.message).toBe('timeout exceeded');
    expect(error.cause).toBe(cause);
  });

  it('defaults AthenaQueryResultCollectorAbortError message to Aborted', () => {
    const error = new AthenaQueryResultCollectorAbortError();

    expect(error.message).toBe('Aborted');
    expect(error.name).toBe('AbortError');
  });
});
