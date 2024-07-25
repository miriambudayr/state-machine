export function assert(
  result: boolean,
  message: string,
  tags?: Record<string, unknown>
) {
  if (!result) {
    console.error(message, JSON.stringify(tags));
    throw new Error(`AssertionFailed: ${message}`);
  }
}
