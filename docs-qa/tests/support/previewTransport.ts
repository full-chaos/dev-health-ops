import { previewHeaders } from "./accessHeaders";
import type { PreviewEnvironment, PreviewHeaders } from "./accessHeaders";
import { traceModeForPreview } from "./tracePolicy";
import type { TraceMode } from "./tracePolicy";

export type PreviewTransport = Readonly<{
  headers: PreviewHeaders;
  trace: TraceMode;
}>;

export const previewTransport = (environment: PreviewEnvironment = process.env): PreviewTransport => {
  const headers = previewHeaders(environment);
  return {
    headers,
    trace: traceModeForPreview(Object.keys(headers).length > 0),
  };
};
