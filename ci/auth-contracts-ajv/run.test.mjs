// Every guard in run.mjs must FAIL CLOSED: each case corrupts a copy of the
// corpus in one way a false-green could hide behind, and the runner must exit
// non-zero. The clean copy must exit 0, or the cases prove nothing.
import { test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
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
  // The go-api inventory: the query-api one holds no row that accepts two classes since its envelope class was retired.
  const document = JSON.parse(readFileSync(path.join(v1, "endpoint-profiles.go-api.json"), "utf8"));
  assert.equal(validate(document), true, JSON.stringify(validate.errors));
  const row = document.rows.find((r) => r.class_disambiguator && r.accepted_credential_classes.length > 1);
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
  // The go-api inventory holds rows with and without a selector; the query-api one has none with a selector.
  const read = () => JSON.parse(readFileSync(path.join(v1, "endpoint-profiles.go-api.json"), "utf8"));
  assert.equal(validate(read()), true, JSON.stringify(validate.errors));
  for (const withSelector of [true, false]) {
    const document = read();
    const row = document.rows.find((r) => Boolean(r.class_disambiguator) === withSelector);
    delete row.accepted_credential_classes;
    delete row.class_disambiguator;
    assert.equal(validate(document), false, `selector row: ${withSelector}`);
  }
});

test("the row schema (strict ajv) holds the two reachable-validator kinds apart (CHAOS-4742)", async () => {
  const { default: Ajv2020 } = await import("ajv/dist/2020.js");
  const { default: addFormats } = await import("ajv-formats");
  const v1 = path.join(repo, "contracts", "auth", "v1");
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  const validate = ajv.compile(JSON.parse(readFileSync(path.join(v1, "endpoint-profile.schema.json"), "utf8")));
  const read = () => JSON.parse(readFileSync(path.join(v1, "endpoint-profiles.go-api.json"), "utf8"));
  assert.equal(validate(read()), true, JSON.stringify(validate.errors));
  const index = read().rows.findIndex((r) => (r.reachable_validators ?? []).length > 0);
  assert.ok(index >= 0, "no go-api row has a reachable validator");
  const plant = (mutate) => {
    const document = read();
    mutate(document.rows[index].reachable_validators[0]);
    return validate(document);
  };
  assert.equal(plant((v) => { v.is_intended_validator = true; v.earlier_auth_gate = false; v.foreign_class_validator = false; }), true, "the intended validator may set both false");
  assert.equal(plant((v) => { v.earlier_auth_gate = false; v.foreign_class_validator = true; }), true, "a foreign-class entry is valid");
  const refused = {
    // On the intended validator with the other field false, only `required` refuses a missing field.
    "earlier_auth_gate missing": (v) => { v.is_intended_validator = true; v.foreign_class_validator = false; delete v.earlier_auth_gate; },
    "foreign_class_validator missing": (v) => { v.is_intended_validator = true; v.earlier_auth_gate = false; delete v.foreign_class_validator; },
    "the retired name": (v) => (v.reachable_but_not_owner = true),
    "both true": (v) => (v.foreign_class_validator = true),
    "a non-owner with both false": (v) => { v.is_intended_validator = false; v.earlier_auth_gate = false; v.foreign_class_validator = false; },
  };
  for (const [name, mutate] of Object.entries(refused)) assert.equal(plant(mutate), false, name);
});

test("the credential-class schema holds the two validator kinds and the retired member out (CHAOS-4742)", async () => {
  const { default: Ajv2020 } = await import("ajv/dist/2020.js");
  const { default: addFormats } = await import("ajv-formats");
  const v1 = path.join(repo, "contracts", "auth", "v1");
  // Not strict: this schema is outside the strict corpus (its markers rule states maxItems without a type and a
  // required member its then-branch does not define); the cell checks only the validator members.
  const ajv = new Ajv2020({ strict: false, allErrors: true });
  addFormats(ajv);
  ajv.addSchema(JSON.parse(readFileSync(path.join(v1, "endpoint-profile.schema.json"), "utf8")));
  for (const name of readdirSync(path.join(v1, "jsonschema")).filter((n) => n.endsWith(".schema.json"))) {
    ajv.addSchema(JSON.parse(readFileSync(path.join(v1, "jsonschema", name), "utf8")));
  }
  const validate = ajv.compile(JSON.parse(readFileSync(path.join(v1, "credential-classes.schema.json"), "utf8")));
  const read = () => JSON.parse(readFileSync(path.join(v1, "credential-classes.json"), "utf8"));
  assert.equal(validate(read()), true, JSON.stringify(validate.errors));
  const plant = (anchored, mutate) => {
    const document = read();
    const validator = document.classes.flatMap((c) => c.validators ?? []).find((v) => (v.anchor !== null) === anchored);
    assert.ok(validator, `no ${anchored ? "anchored" : "unanchored"} validator`);
    mutate(validator);
    return validate(document);
  };
  // An anchored validator states both fields; an unanchored one states neither (an unverified false is no measurement).
  for (const v of read().classes.flatMap((c) => c.validators ?? [])) {
    const has = "earlier_auth_gate" in v && "foreign_class_validator" in v;
    const none = !("earlier_auth_gate" in v) && !("foreign_class_validator" in v);
    assert.ok(v.anchor !== null ? has : none, `${v.description}: anchor ${JSON.stringify(v.anchor)} with fields ${has}`);
  }
  assert.equal(plant(false, () => {}), true, "an unanchored entry leaves both out");
  assert.equal(plant(false, (v) => { v.earlier_auth_gate = false; v.foreign_class_validator = false; }), true, "the schema still allows the fields on an unanchored entry");
  const refused = {
    "anchored, earlier_auth_gate missing": [true, (v) => delete v.earlier_auth_gate],
    "anchored, foreign_class_validator missing": [true, (v) => delete v.foreign_class_validator],
    "anchored, both missing": [true, (v) => { delete v.earlier_auth_gate; delete v.foreign_class_validator; }],
    "both true": [true, (v) => { v.earlier_auth_gate = true; v.foreign_class_validator = true; }],
    "both true, unanchored": [false, (v) => { v.earlier_auth_gate = true; v.foreign_class_validator = true; }],
    "the retired name": [true, (v) => (v.reachable_but_not_owner = true)],
  };
  for (const [name, [anchored, mutate]] of Object.entries(refused)) assert.equal(plant(anchored, mutate), false, name);
});

// The documented count command, run as written on the docs page: 0 here with the four retired entries named as
// not counted, and 1 with one planted foreign-class entry.
test("the foreign-class count command of the docs answers 0 here, names every unanchored validator, and 1 on a planted fixture", () => {
  const page = readFileSync(path.join(repo, "docs", "reference", "auth", "endpoint-profiles.md"), "utf8");
  const commands = page.split("\n").filter((line) => line.startsWith("jq -n '[inputs] as $d | {foreign_class_validator:"));
  assert.equal(commands.length, 1, "the page holds the count command once");
  const run = (cwd) => {
    const result = spawnSync("sh", ["-c", commands[0]], { cwd, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return JSON.parse(result.stdout);
  };
  // Not counted: exactly the unanchored validators, read from the registry itself (the 4 retired ones among them).
  const registry = JSON.parse(readFileSync(path.join(repo, "contracts", "auth", "v1", "credential-classes.json"), "utf8"));
  const retired = registry.classes.flatMap((c) => (c.validators ?? []).flatMap((v, i) => (v.anchor === null ? [`${c.class_id} validators[${i}]`] : [])));
  for (const id of ["ops_refresh_token", "internal_svc_acr_token", "internal_svc_worker_token", "external_push_ingest_token"]) {
    assert.ok(retired.includes(`${id} validators[0]`), `${id} validators[0] is not among the unanchored`);
  }
  assert.deepEqual(run(repo), { foreign_class_validator: 0, not_counted_unanchored: retired });
  const root = mkdtempSync(path.join(os.tmpdir(), "foreign-count-"));
  try {
    const v1 = path.join(root, "contracts", "auth", "v1");
    mkdirSync(v1, { recursive: true });
    copyFileSync(path.join(repo, "contracts", "auth", "v1", "credential-classes.json"), path.join(v1, "credential-classes.json"));
    const document = JSON.parse(readFileSync(path.join(repo, "contracts", "auth", "v1", "endpoint-profiles.go-api.json"), "utf8"));
    const row = document.rows.find((r) => (r.reachable_validators ?? []).length > 0);
    row.reachable_validators[0].earlier_auth_gate = false;
    row.reachable_validators[0].foreign_class_validator = true;
    writeFileSync(path.join(v1, "endpoint-profiles.go-api.json"), JSON.stringify(document));
    assert.deepEqual(run(root), { foreign_class_validator: 1, not_counted_unanchored: retired });
  } finally {
    rmSync(root, { recursive: true, force: true });
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
