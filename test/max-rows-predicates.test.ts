import { isMaxRowsReached, remainingRowBudget } from '../src/core/max-rows-predicates';

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
