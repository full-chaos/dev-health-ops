// ajv 2020-12 STRICT runner for contracts/auth/v1: the ECMA-262 leg of the
// cross-language proof (Go and Python legs are internal/auth/contracts and
// tests/authclient).
//
// Reads the SAME manifest.json every other runner reads and enumerates
// nothing itself (G-70). Strict mode is load-bearing: strictRequired (ajv
// refusing `required` in a subschema that does not define the property) is the
// divergence this runner exists to catch, and a runner without it enforces
// strictly less while looking equivalent.
//
// Exit 0 = corpus agrees; 1 = a fixture disagrees; 2 = a schema failed to
// COMPILE (an ajv-only error is not a corpus disagreement and must not hide
// behind a generic red). `--self-test` proves strict mode is on.
import { readFileSync, readdirSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const here = path.dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
const selfTest = args.includes("--self-test");
const rootArg = args.find((a) => !a.startsWith("--"));
const root = path.resolve(rootArg ?? path.join(here, "..", ".."));
const v1 = path.join(root, "contracts", "auth", "v1");

const MUST_VALIDATE = { accept: true, reject_by_client: true };
// go_schema_gap: refused here by `format` while Go's Validate accepts it (a calendar-impossible day; CHAOS-10135).
const MUST_REJECT = { reject: true, narrower_than_consumer: true, go_schema_gap: true };

// Categories every surface's manifest must declare and populate. A surface may
// add more (jwks.v1 has narrower_than_consumer); a surface listed here may not
// lose one, and any list group a manifest does carry must be non-empty.
const REQUIRED_GROUPS = {
  jwks: ["accept", "reject", "reject_by_client", "narrower_than_consumer"],
  // reject_by_client here holds the profile gate's join of token pins to the credential-class markers: schema-valid, gate-refused.
  "endpoint-auth-profile": ["accept", "reject", "reject_by_client"],
  // Request surfaces carry no client-only rule; the decision and batch response corpora do.
  authorization_request: ["accept", "reject"],
  authorization_batch_request: ["accept", "reject"],
  // The transport envelope has no field a schema cannot check, so no client-only rule.
  "transport-error": ["accept", "reject"],
  // The catalogs are pure schema documents: no client-only rule.
  "action-catalog": ["accept", "reject"],
  "role-catalog": ["accept", "reject"],
  "organization": ["accept", "reject"],
  "organization-create-request": ["accept", "reject"],
  "membership": ["accept", "reject"],
  "membership-update-request": ["accept", "reject"],
  "invite": ["accept", "reject"],
  "invite-create-request": ["accept", "reject"],
  "invite-accept-request": ["accept", "reject"],
};
const DEFAULT_REQUIRED_GROUPS = ["accept", "reject", "reject_by_client"];

// Closed vocabularies (CHAOS-10194): contracts/auth/v1/vocabulary/reason-codes.json
// is the single source for these three fields. Each entry builds a document
// from a fixture that carries the field, then sets the field to a value.
// Approved lists, written by hand on purpose: widening the vocabulary file and
// the schema enums together must also need a deliberate edit here (CHAOS-10194).
const APPROVED_REVOKED = [
  "compromised",
  "owner_request",
];
const APPROVED_AUDIT = [
  "allowed_by_repository_grant",
  "grant_absent",
  "repository_not_granted",
  "platform_role_absent",
  "credential_missing",
  "rate_limited",
  "current_state_unavailable",
  "authority_unavailable",
  "upstream_unavailable",
  "not_found",
  "revision_conflict",
  "internal_error",
  "compromised",
  "owner_request",
  "role_granted",
  "role_revoked",
  "membership_added",
  "membership_removed",
  "membership_status_changed",
  "grant_created",
  "grant_revoked",
  "credential_issued",
  "credential_rotated",
  "credential_revoked",
  "session_revoked",
  "organization_created",
  "organization_updated",
  "principal_disabled",
  "principal_enabled",
  "principal_projected",
  "principal_provisioned",
  "email_verified",
  "password_changed",
  "identity_linked",
  "identity_unlinked",
  "identity_projected",
  "password_reset",
  "policy_revision_activated",
  "entitlement_snapshot_applied",
  "denied_no_membership",
  "denied_no_grant",
  "denied_policy",
  "accepted",
  "invalid_credentials",
  "account_locked",
  "principal_deactivated",
  "email_unverified",
  "email_collision",
  "signup_unavailable",
  "identity_conflict",
  "provider_already_linked",
  "expired",
  "principal_not_found",
  "malformed",
];
const NOT_IN_THE_VOCABULARY = [
  "secret_0123456789abcdef0123",
  "key_compromised",
  "Compromised",
  "compromised ",
  "compromise",
  "owner_request_",
  "",
  "revoked because bob@example.test leaked it",
];
const VOCABULARY_SURFACES = {
  "credential-entry": {
    list: "revoked_reason",
    schemaPath: ["properties", "revoked_reason"],
    base: "valid-revoked.json",
    set: (doc, v) => { doc.revoked_reason = v; },
  },
  "credential-list": {
    list: "revoked_reason",
    schemaPath: ["properties", "items", "items", "properties", "revoked_reason"],
    base: "valid-two-items.json",
    set: (doc, v) => { doc.items[1].revoked_reason = v; },
  },
  "audit-event": {
    list: "audit_reason_code",
    schemaPath: ["properties", "attributes", "properties", "reason_code"],
    base: "invalid-attributes-reason-code-prose.json",
    set: (doc, v) => { doc.attributes.reason_code = v; },
  },
};

const REQUIRED_SWEEPS = ["jwks"];

// The Unicode general categories, written out here and not read from a sweep
// file, so a deleted sweep entry cannot also lower its own count. Cs and LC are
// the two the sweep excludes; every other category must have an entry.
const JS_GENERAL_CATEGORIES = [
  "Cc", "Cf", "Cn", "Co", "Ll", "Lm", "Lo", "Lt", "Lu", "Mc", "Me", "Mn", "Nd", "Nl", "No",
  "Pc", "Pd", "Pe", "Pf", "Pi", "Po", "Ps", "Sc", "Sk", "Sm", "So", "Zl", "Zp", "Zs",
];

const sweepExpand = (v) =>
  v && typeof v === "object" && !Array.isArray(v) && "$repeat" in v
    ? (v.prefix ?? "") + v.$repeat[0].repeat(v.$repeat[1]) + (v.suffix ?? "")
    : v;
// A sweep path or rename may never name a prototype member, and a step into the
// document must name a member the document owns.
const PROTOTYPE_KEYS = new Set(["__proto__", "constructor", "prototype"]);
function sweepAssertPlainKey(node, key, mayCreate = false) {
  if (PROTOTYPE_KEYS.has(key)) throw new Error(`sweep path names a prototype member: ${key}`);
  if (!mayCreate && !Object.prototype.hasOwnProperty.call(node, key)) throw new Error(`sweep path step ${key} is not a member of the document`);
}
// Applies a sweep row's ops to a copy of the base document.
function sweepBuild(base, ops) {
  let doc = structuredClone(base);
  for (const op of ops) {
    if (op.path.length === 0) {
      doc = sweepExpand(op.set);
      continue;
    }
    const parent = op.path.slice(0, -1).reduce((node, step) => {
      sweepAssertPlainKey(node, step);
      return node[step];
    }, doc);
    const last = op.path[op.path.length - 1];
    if (!op.rename) sweepAssertPlainKey(parent, last, true);
    if (op.rename) sweepAssertPlainKey(parent, op.rename, true);
    if ("rename" in op) {
      parent[op.rename] = parent[last];
      delete parent[last];
    } else if (op.delete) {
      delete parent[last];
    } else {
      parent[last] = sweepExpand(op.set);
    }
  }
  return doc;
}

const newAjv = () => {
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  return ajv;
};
// JSON.parse keeps the LAST of two equal member names, so a fixture whose bytes
// carry a hidden first value would be judged on the survivor. Go and Python
// refuse duplicates before validation; this runner must judge the same bytes.
function assertNoDuplicateMembers(text, label) {
  let i = 0;
  const fail = (message) => {
    throw new Error(`${label}: ${message} at offset ${i}`);
  };
  const skipSpace = () => {
    while (i < text.length && " \t\n\r".includes(text[i])) i += 1;
  };
  const readString = () => {
    const start = i;
    i += 1; // opening quote
    while (i < text.length && text[i] !== '"') i += text[i] === "\\" ? 2 : 1;
    if (i >= text.length) fail("unterminated string");
    i += 1;
    return JSON.parse(text.slice(start, i));
  };
  const readValue = () => {
    skipSpace();
    const ch = text[i];
    if (ch === "{") {
      i += 1;
      const seen = new Set();
      skipSpace();
      if (text[i] === "}") {
        i += 1;
        return;
      }
      for (;;) {
        skipSpace();
        if (text[i] !== '"') fail("expected a member name");
        const name = readString();
        if (seen.has(name)) fail(`duplicate object member ${JSON.stringify(name)}`);
        seen.add(name);
        skipSpace();
        if (text[i] !== ":") fail("expected ':'");
        i += 1;
        readValue();
        skipSpace();
        if (text[i] === ",") {
          i += 1;
          continue;
        }
        if (text[i] === "}") {
          i += 1;
          return;
        }
        fail("expected ',' or '}'");
      }
    } else if (ch === "[") {
      i += 1;
      skipSpace();
      if (text[i] === "]") {
        i += 1;
        return;
      }
      for (;;) {
        readValue();
        skipSpace();
        if (text[i] === ",") {
          i += 1;
          continue;
        }
        if (text[i] === "]") {
          i += 1;
          return;
        }
        fail("expected ',' or ']'");
      }
    } else if (ch === '"') {
      readString();
    } else {
      while (i < text.length && !" \t\n\r,]}".includes(text[i])) i += 1;
    }
  };
  readValue();
}
function readJSON(p) {
  const text = readFileSync(p, "utf8");
  assertNoDuplicateMembers(text, p);
  return JSON.parse(text);
}
// A fixture may carry a duplicate member only when the manifest files it under
// reject_by_client: the schema legitimately sees the survivor, and the clients
// are the ones that must refuse the bytes. Anywhere else a duplicate is a
// corpus defect (the verdict would be about a value the bytes do not show).
function readFixture(p) {
  const text = readFileSync(p, "utf8");
  let duplicate = null;
  try {
    assertNoDuplicateMembers(text, p);
  } catch (error) {
    duplicate = error.message;
  }
  return { document: JSON.parse(text), duplicate };
}

if (selfTest) {
  // A schema that ordinary validators accept and strict ajv refuses at compile
  // time: `required` names a property the subschema does not define.
  const bad = {
    $schema: "https://json-schema.org/draft/2020-12/schema",
    type: "object",
    properties: { a: { type: "object", required: ["b"] } },
  };
  try {
    newAjv().compile(bad);
  } catch (error) {
    console.log(`self-test ok: strict mode refused a strictRequired schema (${error.message})`);
    process.exit(0);
  }
  console.error("self-test FAILED: strict mode accepted a strictRequired schema");
  process.exit(1);
}

let failures = 0;
let checked = 0;
const fail = (message) => {
  failures += 1;
  console.log(`FAIL ${message}`);
};

const surfaces = readdirSync(path.join(v1, "examples"), { withFileTypes: true })
  .filter((e) => e.isDirectory())
  .map((e) => e.name)
  .sort();
if (surfaces.length === 0) {
  console.error("no example directories found: refusing to pass on an empty corpus");
  process.exit(2);
}

// A schema with no corpus would be invisible to this runner.
const schemaDir = path.join(v1, "jsonschema");
for (const file of readdirSync(schemaDir)) {
  const surface = file.replace(/\.v1\.schema\.json$/, "");
  if (file.endsWith(".v1.schema.json") && !surfaces.includes(surface)) {
    fail(`schema ${file} has no examples/${surface}/ corpus`);
  }
}

for (const surface of surfaces) {
  const dir = path.join(v1, "examples", surface);
  const schemaPath = path.join(schemaDir, `${surface}.v1.schema.json`);
  if (!existsSync(schemaPath)) {
    fail(`examples/${surface}/ has no ${path.basename(schemaPath)}`);
    continue;
  }
  let validate;
  try {
    // A schema may $ref another contract schema (the REFERENCE ruling): every
    // sibling and the Wave-0 inventory schema are registered by $id from files in
    // this tree, so a $ref resolves locally and never over the network.
    const ajv = newAjv();
    const siblings = readdirSync(schemaDir)
      .filter((f) => f.endsWith(".schema.json") && path.join(schemaDir, f) !== schemaPath)
      .map((f) => path.join(schemaDir, f));
    siblings.push(path.join(v1, "endpoint-profile.schema.json"));
    for (const sibling of siblings) ajv.addSchema(readJSON(sibling));
    validate = ajv.compile(readJSON(schemaPath));
  } catch (error) {
    console.error(`SCHEMA FAILED TO COMPILE: ${surface}.v1: ${error.message}`);
    process.exit(2);
  }

  const manifest = readJSON(path.join(dir, "manifest.json"));
  const claimed = new Set();
  const counts = {};
  // An emptied category would make every loop below vacuous and exit 0.
  for (const required of REQUIRED_GROUPS[surface] ?? DEFAULT_REQUIRED_GROUPS) {
    if (!Array.isArray(manifest[required]) || manifest[required].length === 0) {
      fail(`${surface}: manifest group "${required}" is missing or empty`);
    }
  }
  for (const [group, entries] of Object.entries(manifest)) {
    if (!Array.isArray(entries)) continue; // prose notes
    const mustValidate = MUST_VALIDATE[group] ?? (MUST_REJECT[group] ? false : undefined);
    if (mustValidate === undefined) {
      fail(`${surface}: manifest group "${group}" has no declared expectation here; teach run.mjs`);
      continue;
    }
    counts[group] = entries.length;
    if (entries.length === 0) fail(`${surface}: manifest group "${group}" is empty`);
    for (const entry of entries) {
      const name = typeof entry === "string" ? entry : entry.file;
      // A fixture is a plain file name inside its own surface directory: a path
      // into another directory would validate a file that is not this
      // surface's corpus while the real fixture goes missing unnoticed.
      if (typeof name !== "string" || name !== path.basename(name) || name === "manifest.json") {
        fail(`${surface}: manifest entry ${JSON.stringify(name)} is not a plain fixture file name`);
        continue;
      }
      if (claimed.has(name)) {
        fail(`${surface}/${name}: claimed by the manifest more than once`);
        continue;
      }
      claimed.add(name);
      checked += 1;
      const fixture = readFixture(path.join(dir, name));
      // A duplicate member is legitimate only in a reject_by_client fixture that
      // DECLARES it (`"duplicate_member": true`), and a declaration must be true.
      // Undeclared, a clients-refuse fixture could be refused for an accidental
      // duplicate instead of the rule it exists to pin, and the ajv leg (which
      // runs no client parser) would never see the difference.
      const declared = group === "reject_by_client" && entry.duplicate_member === true;
      if (fixture.duplicate && !declared) {
        fail(`${surface}/${name} [${group}]: ${fixture.duplicate}; only a reject_by_client entry declaring "duplicate_member": true may carry one`);
        continue;
      }
      if (declared && !fixture.duplicate) {
        fail(`${surface}/${name}: declares "duplicate_member": true but carries no duplicate member`);
        continue;
      }
      const ok = validate(fixture.document);
      if (ok !== mustValidate) {
        const why = validate.errors
          ? validate.errors.map((e) => `${e.instancePath || "/"} ${e.keyword}`).join("; ")
          : "";
        fail(`${surface}/${name} [${group}]: validated=${ok}, want ${mustValidate} ${why}`);
        continue;
      }
      if ((group === "reject" || group === "go_schema_gap") && (!entry.expect_instance_location || !entry.expect_keyword)) {
        // Without both, a reject fixture can drift into failing for an
        // unrelated reason and keep passing.
        fail(`${surface}/${name}: reject entry lacks expect_instance_location or expect_keyword`);
        continue;
      }
      if (group === "reject" || group === "go_schema_gap") {
        // ajv adds an `if` error at the instance root next to the real
        // violation inside a failing `then`; Go and Python report only the
        // violation. It is reporting, not a second rule, so it is not counted.
        const real = validate.errors.filter((e) => e.keyword !== "if");
        const locations = new Set(real.map((e) => e.instancePath || "/"));
        const keywords = new Set(real.map((e) => e.keyword));
        if (locations.size !== 1 || !locations.has(entry.expect_instance_location)) {
          fail(`${surface}/${name}: locations [${[...locations]}], want ${entry.expect_instance_location}`);
        }
        if (!keywords.has(entry.expect_keyword)) {
          fail(`${surface}/${name}: keywords [${[...keywords]}], want ${entry.expect_keyword}`);
        }
      }
    }
  }
  // The whole surface tree, not just its top level: a fixture dropped into a
  // subdirectory would otherwise sit unclaimed and unchecked.
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      fail(`${surface}/${entry.name}/ is a subdirectory; fixtures live directly in the surface directory`);
    } else if (entry.name.endsWith(".json") && entry.name !== "manifest.json" && !claimed.has(entry.name)) {
      fail(`${surface}/${entry.name} is on disk but not claimed by the manifest`);
    }
  }
  for (const name of claimed) {
    if (!existsSync(path.join(dir, name))) fail(`${surface}/${name} is claimed but absent`);
  }
  const vocabulary = VOCABULARY_SURFACES[surface];
  if (vocabulary) {
    const vocabPath = path.join(v1, "vocabulary", "reason-codes.json");
    if (!existsSync(vocabPath)) {
      fail(`${surface}: vocabulary file ${path.relative(root, vocabPath)} is missing`);
    } else {
      const values = readJSON(vocabPath)[vocabulary.list]?.values ?? [];
      if (values.length === 0) fail(`${surface}: the ${vocabulary.list} vocabulary is empty`);
      const approved = vocabulary.list === "revoked_reason" ? APPROVED_REVOKED : APPROVED_AUDIT;
      if (JSON.stringify([...values].sort()) !== JSON.stringify([...approved].sort())) {
        fail(`${surface}: the ${vocabulary.list} vocabulary file differs from the approved list in run.mjs`);
      }
      const build = (v) => {
        const doc = structuredClone(readJSON(path.join(dir, vocabulary.base)));
        vocabulary.set(doc, v);
        return doc;
      };
      for (const v of values) {
        checked += 1;
        if (!validate(build(v))) fail(`${surface}: listed ${vocabulary.list} value ${JSON.stringify(v)} is refused`);
      }
      for (const v of NOT_IN_THE_VOCABULARY) {
        checked += 1;
        if (validate(build(v))) fail(`${surface}: ${JSON.stringify(v)} is not in the ${vocabulary.list} vocabulary but validates`);
      }
      const node = vocabulary.schemaPath.reduce((at, step) => at?.[step], readJSON(schemaPath));
      if (JSON.stringify([...(node?.enum ?? [])].sort()) !== JSON.stringify([...values].sort())) {
        fail(`${surface}: the schema enum differs from the ${vocabulary.list} vocabulary`);
      }
    }
  }
  const sweepPath = path.join(v1, "sweeps", `${surface}.property-sweep.json`);
  if (REQUIRED_SWEEPS.includes(surface) && !existsSync(sweepPath)) {
    fail(`${surface}: property sweep ${path.relative(root, sweepPath)} is missing`);
  }
  if (existsSync(sweepPath)) {
    const sweep = readJSON(sweepPath);
    if (!Array.isArray(sweep.rows) || sweep.rows.length < 100) fail(`${surface}: property sweep has too few rows`);
    const classIds = new Map((sweep.declared_classes ?? []).map((c) => [c.id, 0]));
    if (classIds.size === 0) fail(`${surface}: the sweep declares no narrowing classes`);
    for (const row of [...(sweep.rows ?? []), ...(sweep.byte_layer_rows ?? [])]) {
      if (row.class === "agreement") {
        if (row.declared_as !== undefined) fail(`${surface}/${row.id}: an agreement row names a class`);
      } else if (!classIds.has(row.declared_as)) {
        fail(`${surface}/${row.id}: ${row.class} row declared_as ${JSON.stringify(row.declared_as)} is not a declared class`);
      } else {
        classIds.set(row.declared_as, classIds.get(row.declared_as) + 1);
      }
    }
    const schemaDescription = readJSON(schemaPath).description ?? "";
    for (const [id, rows] of classIds) {
      if (rows === 0) fail(`${surface}: class ${id} has no row`);
      if (!schemaDescription.includes(`[${id}]`)) fail(`${surface}: class ${id} is not named as [${id}] in the schema description`);
    }
    const verdict = (doc) => (validate(doc) ? "accept" : "refuse");
    const ids = new Set();
    for (const row of sweep.rows ?? []) {
      if (ids.has(row.id)) fail(`${surface}: sweep row ${row.id} appears twice`);
      ids.add(row.id);
      checked += 1;
      const want = row.consumer === row.schema ? "agreement" : row.consumer === "accept" ? "narrowing" : "client_enforced";
      if (row.class !== want) fail(`${surface}/${row.id}: class ${row.class} does not follow from the pair`);
      const got = verdict(sweepBuild(sweep.base_document, row.ops));
      if (got !== row.schema) fail(`${surface}/${row.id}: ajv ${got}s, the row says ${row.schema}`);
    }
    for (const entry of sweep.unicode_categories ?? []) {
      checked += 1;
      const doc = structuredClone(sweep.base_document);
      doc.keys[0].kid = `a${String.fromCodePoint(parseInt(entry.rune.slice(2), 16))}b`;
      const got = verdict(doc);
      if (got !== entry.schema) fail(`${surface}/category ${entry.category}: ajv ${got}s, the entry says ${entry.schema}`);
    }
    if (surface === "jwks") {
      const inv = sweep.inventory ?? {};
      if (sweep.rows.length !== inv.rows) fail(`${surface}: ${sweep.rows.length} sweep rows, inventory says ${inv.rows}`);
      const perProperty = {};
      for (const row of sweep.rows) perProperty[row.property] = (perProperty[row.property] ?? 0) + 1;
      if (JSON.stringify(Object.entries(perProperty).sort()) !== JSON.stringify(Object.entries(inv.by_property ?? {}).sort())) {
        fail(`${surface}: rows per property ${JSON.stringify(perProperty)} differ from the inventory`);
      }
      if ((sweep.unicode_categories ?? []).length !== inv.unicode_categories) fail(`${surface}: unicode category count differs from the inventory`);
      if ((sweep.byte_layer_rows ?? []).length !== inv.byte_layer_rows) fail(`${surface}: byte-layer row count differs from the inventory`);
      if (JSON.stringify(Object.keys(sweep.unicode_categories_excluded ?? {}).sort()) !== '["Cs","LC"]') {
        fail(`${surface}: excluded Unicode categories must be exactly Cs and LC`);
      }
      const listedCategories = (sweep.unicode_categories ?? []).map((e) => e.category).sort();
      if (JSON.stringify(listedCategories) !== JSON.stringify([...JS_GENERAL_CATEGORIES].sort())) {
        fail(`${surface}: sweep categories ${listedCategories.join(",")} differ from the Unicode general categories`);
      }
      const anyCategory = new RegExp(`^(?:${JS_GENERAL_CATEGORIES.map((c) => `\\p{${c}}`).join("|")})$`, "u");
      for (let code = 0; code <= 0x10ffff; code += 1) {
        if (code >= 0xd800 && code <= 0xdfff) continue;
        if (!anyCategory.test(String.fromCodePoint(code))) {
          fail(`${surface}: U+${code.toString(16)} is in no listed Unicode general category`);
          break;
        }
      }
      for (const entry of sweep.unicode_categories ?? []) {
        const ch = String.fromCodePoint(parseInt(entry.rune.slice(2), 16));
        if (!new RegExp(`^\\p{${entry.category}}$`, "u").test(ch)) {
          fail(`${surface}/category ${entry.category}: ${entry.rune} is not in that category`);
        }
      }
      const alphabet = new Set(sweep.alphabets.x_characters);
      if (alphabet.size !== 64) fail(`${surface}: the x alphabet must hold 64 characters`);
      const [lo, hi] = sweep.alphabets.kid_range.map((v) => parseInt(v.slice(2), 16));
      const xPrefix = sweep.base_document.keys[0].x.slice(0, 42);
      for (let code = 0; code <= 0x10ffff; code += 1) {
        if (code >= 0xd800 && code <= 0xdfff) continue;
        if (code > 0xffff) continue;
        const ch = String.fromCodePoint(code);
        const xDoc = structuredClone(sweep.base_document);
        xDoc.keys[0].x = xPrefix + ch;
        checked += 1;
        if (verdict(xDoc) !== (alphabet.has(ch) ? "accept" : "refuse")) {
          fail(`${surface}: x ending in U+${code.toString(16)} has the wrong verdict`);
          break;
        }
        const kDoc = structuredClone(sweep.base_document);
        kDoc.keys[0].kid = ch;
        if (verdict(kDoc) !== (code >= lo && code <= hi ? "accept" : "refuse")) {
          fail(`${surface}: kid U+${code.toString(16)} has the wrong verdict`);
          break;
        }
      }
      if (Math.max(...[...alphabet].map((c) => c.codePointAt(0)), hi) > 0xffff) {
        fail(`${surface}: the literal alphabets must be BMP-only for the astral sweep's refuse expectation`);
      }
      // Every astral code point through the compiled validator, mutating one
      // document in place; no stride, so one admitted code point is found.
      const astralX = structuredClone(sweep.base_document);
      const astralKid = structuredClone(sweep.base_document);
      for (let code = 0x10000; code <= 0x10ffff; code += 1) {
        const ch = String.fromCodePoint(code);
        astralX.keys[0].x = xPrefix + ch;
        astralKid.keys[0].kid = ch;
        checked += 2;
        if (verdict(astralX) !== "refuse" || verdict(astralKid) !== "refuse") {
          fail(`${surface}: astral U+${code.toString(16)} is accepted as an x ending or a kid`);
          break;
        }
      }
      const strict = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
      const byteIds = new Set();
      for (const row of sweep.byte_layer_rows ?? []) {
        if (byteIds.has(row.id)) fail(`${surface}: byte-layer row ${row.id} appears twice`);
        byteIds.add(row.id);
        checked += 1;
        const bytes = row.raw !== undefined ? Buffer.from(row.raw, "utf8") : Buffer.from(row.raw_hex, "hex");
        let got;
        try {
          got = verdict(JSON.parse(strict.decode(bytes)));
        } catch {
          got = "refuse";
        }
        if (got !== row.schema) fail(`${surface}/${row.id}: ajv path ${got}s, the row says ${row.schema}`);
        const want = row.consumer === row.schema ? "agreement" : row.consumer === "accept" ? "narrowing" : "client_enforced";
        if (row.class !== want) fail(`${surface}/${row.id}: class ${row.class} does not follow from the pair`);
      }
    }
    if (surface === "jwks") {
      for (let code = 0x21; code < 0x7f; code += 1) {
        const doc = structuredClone(sweep.base_document);
        doc.keys[0].kid = String.fromCharCode(code);
        checked += 1;
        if (verdict(doc) !== "accept") fail(`${surface}: printable ASCII ${JSON.stringify(doc.keys[0].kid)} refused as a kid`);
      }
    }
  }
  console.log(`${surface}.v1: ${JSON.stringify(counts)}`);
}

console.log(`\najv 2020-12 strict (v${JSON.parse(readFileSync(path.join(here, "node_modules", "ajv", "package.json"), "utf8")).version}): ${checked} fixtures, ${failures} failures`);
process.exit(failures ? 1 : 0);
