export const previewHeaders = (): Readonly<Record<string, string>> => {
  const clientId = process.env["CF_ACCESS_CLIENT_ID"];
  const clientSecret = process.env["CF_ACCESS_CLIENT_SECRET"];

  if (!clientId || !clientSecret) {
    return {};
  }

  return {
    "CF-Access-Client-Id": clientId,
    "CF-Access-Client-Secret": clientSecret,
  };
};
