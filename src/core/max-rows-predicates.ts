/**
 * Returns whether a cumulative row count has reached an optional collector `maxRows` cap.
 *
 * @param collected - Rows already kept (or about to be kept) across pages.
 * @param maxRows - Collector-level cap, or `undefined` when unlimited.
 * @returns `true` when `maxRows` is set and `collected >= maxRows`.
 */
export const isMaxRowsReached = (
  collected: number,
  maxRows: number | undefined,
): boolean => {
  if (maxRows === undefined) {
    return false;
  }

  return collected >= maxRows;
};

/**
 * Returns how many more rows may be kept under `maxRows`, or `undefined` when unlimited.
 *
 * @param collected - Rows already kept across pages.
 * @param maxRows - Collector-level cap, or `undefined` when unlimited.
 * @returns Remaining budget (`>= 0`), or `undefined` when there is no cap.
 */
export const remainingRowBudget = (
  collected: number,
  maxRows: number | undefined,
): number | undefined => {
  if (maxRows === undefined) {
    return undefined;
  }

  return Math.max(maxRows - collected, 0);
};
