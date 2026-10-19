/**
 * Returns whether another Athena results page should be fetched.
 *
 * A continuation token is present only when it is a non-empty string.
 * `undefined` and `''` both mean pagination is finished.
 *
 * @param nextToken - Continuation token from the last fetched page, if any.
 * @returns `true` when `nextToken` is a non-empty string.
 */
export const hasMorePages = (nextToken: string | undefined): boolean => {
  if (typeof nextToken !== 'string') {
    return false;
  }

  return nextToken.length > 0;
};
