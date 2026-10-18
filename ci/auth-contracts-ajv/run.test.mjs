// Every guard in run.mjs must FAIL CLOSED: each case corrupts a copy of the
// corpus in one way a false-green could hide behind, and the runner must exit
// non-zero. The clean copy must exit 0, or the cases prove nothing.
import { test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, "..", "..");
const runner = path.join(here, "run.mjs");

function withCopy(mutate) {
  const root = mkdtempSync(path.join(os.tmpdir(), "ajv-runner-"));
  try {
    cpSync(path.join(repo, "contracts"), path.join(root, "contracts"), { recursive: true });
    const examples = path.join(root, "contracts", "auth", "v1", "examples");
    mutate(examples);
    return spawnSync("node", [runner, root], { encoding: "utf8" });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}
const manifestOf = (examples, surface) => path.join(examples, surface, "manifest.json");
const editManifest = (examples, surface, edit) => {
  const file = manifestOf(examples, surface);
  const manifest = JSON.parse(readFileSync(file, "utf8"));
  edit(manifest, path.join(examples, surface));
  writeFileSync(file, JSON.stringify(manifest));
};

test("the clean corpus passes", () => {
  const result = withCopy(() => {});
  assert.equal(result.status, 0, result.stdout + result.stderr);
});

test("an emptied accept group fails", () => {
  const result = withCopy((e) => editManifest(e, "principal", (m) => (m.accept = [])));
  assert.equal(result.status, 1, result.stdout);
});

test("a deleted narrower_than_consumer group fails", () => {
  const result = withCopy((e) => editManifest(e, "jwks", (m) => delete m.narrower_than_consumer));
  assert.equal(result.status, 1, result.stdout);
});

test("an emptied narrower_than_consumer group fails", () => {
  const result = withCopy((e) => editManifest(e, "jwks", (m) => (m.narrower_than_consumer = [])));
  assert.equal(result.status, 1, result.stdout);
});

test("a reject entry without expectations fails", () => {
  const result = withCopy((e) =>
    editManifest(e, "principal", (m) => {
      delete m.reject[0].expect_keyword;
      delete m.reject[0].expect_instance_location;
    }),
  );
  assert.equal(result.status, 1, result.stdout);
});

test("a manifest entry that escapes the surface directory fails", () => {
  const result = withCopy((e) =>
    editManifest(e, "discovery", (m, dir) => {
      copyFileSync(path.join(dir, m.accept[0].file), path.join(e, "shared.json"));
      m.accept[0].file = "../shared.json";
    }),
  );
  assert.equal(result.status, 1, result.stdout);
});

test("an unclaimed fixture in a NESTED directory fails", () => {
  const result = withCopy((e) => {
    const nested = path.join(e, "principal", "nested");
    mkdirSync(nested);
    writeFileSync(path.join(nested, "broken.json"), "{ not json");
  });
  assert.equal(result.status, 1, result.stdout);
});

test("an unclaimed fixture at the top level fails", () => {
  const result = withCopy((e) => writeFileSync(path.join(e, "principal", "extra.json"), "{}"));
  assert.equal(result.status, 1, result.stdout);
});

test("a strictRequired schema edit exits 2 (compile failure, not a fixture failure)", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "ajv-runner-"));
  try {
    cpSync(path.join(repo, "contracts"), path.join(root, "contracts"), { recursive: true });
    const file = path.join(root, "contracts", "auth", "v1", "jsonschema", "discovery.v1.schema.json");
    const schema = JSON.parse(readFileSync(file, "utf8"));
    schema.properties.issuer.required = ["nope"];
    writeFileSync(file, JSON.stringify(schema));
    const result = spawnSync("node", [runner, root], { encoding: "utf8" });
    assert.equal(result.status, 2, result.stdout + result.stderr);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
