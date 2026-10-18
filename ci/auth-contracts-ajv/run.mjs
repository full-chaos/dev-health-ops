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
};
const DEFAULT_REQUIRED_GROUPS = ["accept", "reject", "reject_by_client"];

const newAjv = () => {
  const ajv = new Ajv2020({ strict: true, allErrors: true });
  addFormats(ajv);
  return ajv;
};
const readJSON = (p) => JSON.parse(readFileSync(p, "utf8"));

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
    validate = newAjv().compile(readJSON(schemaPath));
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
      const ok = validate(readJSON(path.join(dir, name)));
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
  console.log(`${surface}.v1: ${JSON.stringify(counts)}`);
}

console.log(`\najv 2020-12 strict (v${JSON.parse(readFileSync(path.join(here, "node_modules", "ajv", "package.json"), "utf8")).version}): ${checked} fixtures, ${failures} failures`);
process.exit(failures ? 1 : 0);
