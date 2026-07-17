import { expect, test } from "@playwright/test";

import { previewHeaders } from "./support/accessHeaders";
import { previewTransport } from "./support/previewTransport";

test.describe("Cloudflare Access preview credentials", () => {
  test("rejects_undeclared_token", () => {
    const environment = {
      CF_ACCESS_CLIENT_ID: "test-client-id",
      CF_ACCESS_CLIENT_SECRET: "trace-sentinel-secret",
      CF_ACCESS_TOKEN: "trace-sentinel-token",
    };

    expect(() => previewHeaders(environment)).toThrow(/undeclared Cloudflare Access credential/i);
  });

  test("does not retain traces when Cloudflare Access credentials are attached", () => {
    const transport = previewTransport({
      CF_ACCESS_CLIENT_ID: "test-client-id",
      CF_ACCESS_CLIENT_SECRET: "trace-sentinel-secret",
    });

    expect(transport.headers).toEqual({
      "CF-Access-Client-Id": "test-client-id",
      "CF-Access-Client-Secret": "trace-sentinel-secret",
    });
    expect(transport.trace).toBe("off");
    expect(previewTransport({}).trace).toBe("retain-on-failure");
  });
});
