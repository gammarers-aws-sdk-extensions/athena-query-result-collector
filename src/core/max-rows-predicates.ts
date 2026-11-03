import { hasMorePages } from './page-predicates';

/**
 * Returns whether stopping on a page that filled the remaining row budget leaves rows unread.
 *
 * Call only when `pageRowCount >= remaining`. Dropping rows on that page, or a non-empty
 * continuation token, means the collected result is truncated. An exact fill of the last
 * page is not truncation.
 *
 * @param pageRowCount - Rows on the page that filled the budget.
 * @param remaining - Rows still allowed under `maxRows` before this page.
 * @param nextToken - Continuation token from this page, if any.
 * @returns `true` when rows were discarded or another page remains.
 */
export const isCollectionTruncated = (
  pageRowCount: number,
  remaining: number,
  nextToken: string | undefined,
): boolean => {
  if (pageRowCount > remaining) {
    return true;
  }

  return hasMorePages(nextToken);
};

/**
 * Returns whether a fetched page fills the remaining collector row budget.
 *
 * Unlimited collection (`remaining === undefined`) does not fill the budget.
 *
 * @param pageRowCount - Rows on the page just fetched.
 * @param remaining - Rows still allowed under `maxRows`, or `undefined` when unlimited.
 * @returns `true` when `remaining` is set and `pageRowCount` is at least that budget.
 */
export const pageFillsRowBudget = (
  pageRowCount: number,
  remaining: number | undefined,
): remaining is number => {
  if (remaining === undefined) {
    return false;
  }

  return pageRowCount >= remaining;
};

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
