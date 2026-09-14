import { hasMorePages } from '../src/page-predicates';

describe('hasMorePages', () => {
  it.each([
    { nextToken: undefined, expected: false },
    { nextToken: '', expected: false },
    { nextToken: 'token-1', expected: true },
  ] as const)('returns $expected when nextToken is $nextToken', ({ nextToken, expected }) => {
    expect(hasMorePages(nextToken)).toBe(expected);
  });
});
