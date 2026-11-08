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

test("a fixture with a duplicate member fails", () => {
  const result = withCopy((e) => {
    const file = path.join(e, "discovery", "valid-minimal.json");
    const text = readFileSync(file, "utf8");
    writeFileSync(file, text.replace('"issuer"', '"issuer": "https://hidden.example.test", "issuer"'));
  });
  assert.equal(result.status, 1, result.stdout + result.stderr);
});

test("a manifest with a duplicate member fails", () => {
  const result = withCopy((e) => {
    const file = manifestOf(e, "principal");
    writeFileSync(file, readFileSync(file, "utf8").replace('"accept"', '"accept": [], "accept"'));
  });
  assert.notEqual(result.status, 0, result.stdout + result.stderr);
});

test("an UNDECLARED duplicate member in a reject_by_client fixture fails", () => {
  const result = withCopy((e) => {
    const file = path.join(e, "credential-entry", "client-rejected-revoked-at-changed.json");
    const text = readFileSync(file, "utf8");
    writeFileSync(file, text.replace('"status"', '"status": "active", "status"'));
  });
  assert.equal(result.status, 1, result.stdout + result.stderr);
});

test("a duplicate_member declaration on a fixture with no duplicate fails", () => {
  const result = withCopy((e) =>
    editManifest(e, "credential-entry", (m) => {
      m.reject_by_client[0].duplicate_member = true;
    }),
  );
  assert.equal(result.status, 1, result.stdout + result.stderr);
});

test("the deliberate duplicate-member fixture is declared and passes", () => {
  const manifest = JSON.parse(
    readFileSync(path.join(repo, "contracts", "auth", "v1", "examples", "error", "manifest.json"), "utf8"),
  );
  const entry = manifest.reject_by_client.find((e) => e.file === "client-rejected-duplicate-reason-code.json");
  assert.equal(entry.duplicate_member, true);
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

test("the row schema (strict ajv) requires class_disambiguator on a row that accepts two classes", async () => {
  const { default: Ajv2020 } = await import("ajv/dist/2020.js");
  const { default: addFormats } = await import("ajv-formats");
  const v1 = path.join(repo, "contracts", "auth", "v1");
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(JSON.parse(readFileSync(path.join(v1, "endpoint-profile.schema.json"), "utf8")));
  const document = JSON.parse(readFileSync(path.join(v1, "endpoint-profiles.query-api.json"), "utf8"));
  assert.equal(validate(document), true, JSON.stringify(validate.errors));
  const row = document.rows.find((r) => r.class_disambiguator);
  delete row.class_disambiguator;
  assert.equal(validate(document), false);
});

test("the row schema (strict ajv) holds the HS256 opt-in to the class list on every inventory", async () => {
  const { default: Ajv2020 } = await import("ajv/dist/2020.js");
  const { default: addFormats } = await import("ajv-formats");
  const v1 = path.join(repo, "contracts", "auth", "v1");
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(JSON.parse(readFileSync(path.join(v1, "endpoint-profile.schema.json"), "utf8")));
  const accessClasses = ["ops_access_token_hs256", "user_access_token"];
  for (const file of ["endpoint-profiles.auth-service.json", "endpoint-profiles.query-api.json", "endpoint-profiles.go-api.json"]) {
    const read = () => JSON.parse(readFileSync(path.join(v1, file), "utf8"));
    assert.equal(validate(read()), true, `${file}: ${JSON.stringify(validate.errors)}`);
    assert.ok(!read().rows.some((r) => r.accepted_credential_classes.includes("ops_access_token_hs256")), `${file}: a row accepts the retired HS256 class`);
    const user = read().rows.findIndex((r) => r.accepted_credential_classes.length === 1 && r.accepted_credential_classes[0] === "user_access_token");
    const plain = read().rows.findIndex((r) => !r.accepted_credential_classes.some((c) => accessClasses.includes(c)));
    assert.ok(user >= 0 && plain >= 0, `${file}: no user-token-only row or no row without an access-token class`);
    // No row accepts the retired HS256 class: the HS256 probes plant it on a user-token-only row, and the planted
    // row itself must validate, so each probe fails for its own rule.
    const plant = (row) => {
      row.accepted_credential_classes = ["ops_access_token_hs256", "user_access_token"];
      row.class_disambiguator = { kind: "jwt_header_alg", name: "EdDSA" };
      row.access_token_validation = { legacy_hs256: true };
    };
    const planted = read();
    plant(planted.rows[user]);
    assert.equal(validate(planted), true, `${file}: planted HS256 row: ${JSON.stringify(validate.errors)}`);
    const probes = [
      [user, (row) => { plant(row); delete row.access_token_validation; }],
      [user, (row) => { plant(row); row.access_token_validation = { legacy_hs256: false }; }],
      [user, (row) => { plant(row); row.access_token_validation = { legacy_hs256: true, classes: ["ops_access_token_hs256"] }; }],
      [user, (row) => (row.access_token_validation = { legacy_hs256: true })],
      [plain, (row) => (row.access_token_validation = { legacy_hs256: false })],
      [plain, (row) => (row.access_token_validation = { legacy_hs256: true })],
    ];
    for (const [index, mutate] of probes) {
      const document = read();
      mutate(document.rows[index]);
      assert.equal(validate(document), false, `${file} row ${index}: ${mutate}`);
    }
  }
});

test("the row schema (strict ajv) rejects a row that omits accepted_credential_classes", async () => {
  const { default: Ajv2020 } = await import("ajv/dist/2020.js");
  const { default: addFormats } = await import("ajv-formats");
  const v1 = path.join(repo, "contracts", "auth", "v1");
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(JSON.parse(readFileSync(path.join(v1, "endpoint-profile.schema.json"), "utf8")));
  const read = () => JSON.parse(readFileSync(path.join(v1, "endpoint-profiles.query-api.json"), "utf8"));
  assert.equal(validate(read()), true, JSON.stringify(validate.errors));
  for (const withSelector of [true, false]) {
    const document = read();
    const row = document.rows.find((r) => Boolean(r.class_disambiguator) === withSelector);
    delete row.accepted_credential_classes;
    delete row.class_disambiguator;
    assert.equal(validate(document), false, `selector row: ${withSelector}`);
  }
});

test("an audit attribute member dropped from the schema fails the approved key list", () => {
  const result = withCopy((examples) => {
    const file = path.join(examples, "..", "jsonschema", "audit-event.v1.schema.json");
    const schema = JSON.parse(readFileSync(file, "utf8"));
    delete schema.properties.attributes.properties.target_kind;
    writeFileSync(file, JSON.stringify(schema));
  });
  assert.notEqual(result.status, 0, result.stdout);
  assert.match(result.stdout + result.stderr, /approved attribute key list/);
});

test("an audit attribute member added to the schema fails the approved key list", () => {
  const result = withCopy((examples) => {
    const file = path.join(examples, "..", "jsonschema", "audit-event.v1.schema.json");
    const schema = JSON.parse(readFileSync(file, "utf8"));
    schema.properties.attributes.properties.zz_new_key = { type: "string", pattern: "^[a-z]+$" };
    writeFileSync(file, JSON.stringify(schema));
  });
  assert.notEqual(result.status, 0, result.stdout);
  assert.match(result.stdout + result.stderr, /approved attribute key list/);
});
