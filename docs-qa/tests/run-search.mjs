import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const args = process.argv.slice(2);
const configFlag = args.indexOf("--config");
const configValue =
  configFlag === -1
    ? "../.github/documentation-program/phase-10/search-acceptance.json"
    : args[configFlag + 1];

if (configValue === undefined) {
  throw new Error("--config requires a search acceptance file");
}

const acceptancePath = resolve(process.cwd(), configValue);
if (!existsSync(acceptancePath)) {
  throw new Error(`search acceptance file not found: ${acceptancePath}`);
}

const result = spawnSync("pnpm", ["exec", "playwright", "test", "--project=chrome-search"], {
  env: { ...process.env, DOCS_SEARCH_ACCEPTANCE_PATH: acceptancePath },
  stdio: "inherit",
});

process.exitCode = result.status ?? 1;
