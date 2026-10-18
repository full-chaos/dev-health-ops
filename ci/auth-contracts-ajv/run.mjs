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
const MUST_REJECT = { reject: true, narrower_than_consumer: true };

// Categories every surface's manifest must declare and populate. A surface may
// add more (jwks.v1 has narrower_than_consumer); a surface listed here may not
// lose one, and any list group a manifest does carry must be non-empty.
const REQUIRED_GROUPS = {
  jwks: ["accept", "reject", "reject_by_client", "narrower_than_consumer"],
  // No client-only rule on this surface: it has no integer fields and no cross-document check, so there is nothing for reject_by_client to hold.
  "endpoint-auth-profile": ["accept", "reject"],
};
const DEFAULT_REQUIRED_GROUPS = ["accept", "reject", "reject_by_client"];

const REQUIRED_SWEEPS = ["jwks"];
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
    let parent = doc;
    for (const step of op.path.slice(0, -1)) {
      sweepAssertPlainKey(parent, step);
      parent = parent[step];
    }
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
      if (fixture.duplicate && group !== "reject_by_client") {
        fail(`${surface}/${name} [${group}]: ${fixture.duplicate}; only reject_by_client may carry a duplicate member`);
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
      if (group === "reject" && (!entry.expect_instance_location || !entry.expect_keyword)) {
        // Without both, a reject fixture can drift into failing for an
        // unrelated reason and keep passing.
        fail(`${surface}/${name}: reject entry lacks expect_instance_location or expect_keyword`);
        continue;
      }
      if (group === "reject") {
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
      const alphabet = new Set(sweep.alphabets.x_characters);
      if (alphabet.size !== 64) fail(`${surface}: the x alphabet must hold 64 characters`);
      const [lo, hi] = sweep.alphabets.kid_range.map((v) => parseInt(v.slice(2), 16));
      const xPrefix = sweep.base_document.keys[0].x.slice(0, 42);
      for (let code = 0; code <= 0x10ffff; code += 1) {
        if (code >= 0xd800 && code <= 0xdfff) continue;
        if (code > 0xffff && code % 61 !== 0) continue;
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
