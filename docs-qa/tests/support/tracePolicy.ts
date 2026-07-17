export type TraceMode = "off" | "retain-on-failure";

export const traceModeForPreview = (usesProtectedPreview: boolean): TraceMode =>
  usesProtectedPreview ? "off" : "retain-on-failure";
