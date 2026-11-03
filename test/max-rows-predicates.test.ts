import {
  isCollectionTruncated,
  isMaxRowsReached,
  pageFillsRowBudget,
  remainingRowBudget,
} from '../src/core/max-rows-predicates';

describe('isCollectionTruncated', () => {
  it.each([
    { pageRowCount: 2, remaining: 2, nextToken: undefined, expected: false },
    { pageRowCount: 2, remaining: 2, nextToken: '', expected: false },
    { pageRowCount: 2, remaining: 2, nextToken: 'token-1', expected: true },
    { pageRowCount: 3, remaining: 2, nextToken: undefined, expected: true },
    { pageRowCount: 3, remaining: 2, nextToken: '', expected: true },
    { pageRowCount: 3, remaining: 2, nextToken: 'token-1', expected: true },
    { pageRowCount: 0, remaining: 0, nextToken: undefined, expected: false },
    { pageRowCount: 1, remaining: 0, nextToken: undefined, expected: true },
  ] as const)(
    'returns $expected when pageRowCount=$pageRowCount remaining=$remaining nextToken=$nextToken',
    ({ pageRowCount, remaining, nextToken, expected }) => {
      expect(isCollectionTruncated(pageRowCount, remaining, nextToken)).toBe(expected);
    },
  );
});

describe('pageFillsRowBudget', () => {
  it.each([
    { pageRowCount: 0, remaining: undefined, expected: false },
    { pageRowCount: 3, remaining: undefined, expected: false },
    { pageRowCount: 2, remaining: 3, expected: false },
    { pageRowCount: 3, remaining: 3, expected: true },
    { pageRowCount: 4, remaining: 3, expected: true },
    { pageRowCount: 0, remaining: 0, expected: true },
  ] as const)(
    'returns $expected when pageRowCount=$pageRowCount remaining=$remaining',
    ({ pageRowCount, remaining, expected }) => {
      expect(pageFillsRowBudget(pageRowCount, remaining)).toBe(expected);
    },
  );
});

describe('isMaxRowsReached', () => {
  it.each([
    { collected: 0, maxRows: undefined, expected: false },
    { collected: 10, maxRows: undefined, expected: false },
    { collected: 0, maxRows: 0, expected: true },
    { collected: 2, maxRows: 5, expected: false },
    { collected: 5, maxRows: 5, expected: true },
    { collected: 6, maxRows: 5, expected: true },
  ] as const)(
    'returns $expected when collected=$collected maxRows=$maxRows',
    ({ collected, maxRows, expected }) => {
      expect(isMaxRowsReached(collected, maxRows)).toBe(expected);
    },
  );
});

describe('remainingRowBudget', () => {
  it.each([
    { collected: 0, maxRows: undefined, expected: undefined },
    { collected: 3, maxRows: 10, expected: 7 },
    { collected: 10, maxRows: 10, expected: 0 },
    { collected: 12, maxRows: 10, expected: 0 },
  ] as const)(
    'returns $expected when collected=$collected maxRows=$maxRows',
    ({ collected, maxRows, expected }) => {
      expect(remainingRowBudget(collected, maxRows)).toBe(expected);
    },
  );
});
