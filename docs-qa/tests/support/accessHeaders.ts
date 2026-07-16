export type PreviewEnvironment = Readonly<Record<string, string | undefined>>;
export type PreviewHeaders = Readonly<Record<string, string>>;

const isUndeclaredAccessCredential = (name: string, value: string | undefined): boolean =>
  name.startsWith("CF_ACCESS_") &&
  value !== undefined &&
  name !== "CF_ACCESS_CLIENT_ID" &&
  name !== "CF_ACCESS_CLIENT_SECRET";

export const previewHeaders = (environment: PreviewEnvironment = process.env): PreviewHeaders => {
  for (const [name, value] of Object.entries(environment)) {
    if (isUndeclaredAccessCredential(name, value)) {
      throw new Error(`Undeclared Cloudflare Access credential: ${name}`);
    }
  }

  const clientId = environment["CF_ACCESS_CLIENT_ID"];
  const clientSecret = environment["CF_ACCESS_CLIENT_SECRET"];

  if (!clientId || !clientSecret) {
    return {};
  }

  return {
    "CF-Access-Client-Id": clientId,
    "CF-Access-Client-Secret": clientSecret,
  };
};
