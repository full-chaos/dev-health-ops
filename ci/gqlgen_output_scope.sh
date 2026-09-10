#!/usr/bin/env bash
# Shared output-scope validation for the gqlgen wrapper and the drift guard
# (CHAOS-5489).
#
# WHY THIS IS SHARED. The wrapper grew this check first, and the guard did not
# have it -- so review round r8 pointed `exec.filename` at an ABSOLUTE path and
# the guard, which believes it is safe because it generates inside a temporary
# copy, wrote straight through the copy into a real file. Copying a tree does
# not relocate an absolute path. Two scripts driving the same generator need
# one answer to "where is this allowed to write", so it lives here and both
# source it.
#
# WHAT r8 GOT PAST THE WRAPPER'S OWN VERSION, and what this therefore covers:
#   - absolute output paths (the copy does not contain them)
#   - resolver.filename_template, which can carry ../ segments
#   - OMITTED keys, where gqlgen's DEFAULT is used and validated nowhere
# The last one is the reason this validates defaults explicitly rather than
# only what the config happens to name: a missing key is still an output path.

# gqlgen_output_scope_check <config-dir> <allowed-root-abs>
# Prints offending paths to stderr; returns 0 when every output is in scope.
gqlgen_output_scope_check() {
  local config_dir="$1" allowed_root="$2"
  local reader="${TMPDIR:-/tmp}/gqlgen_outputs.$$.py"

  # Built with the printf BUILTIN, never a heredoc: bash writes a heredoc into
  # a pipe whose read end it also holds, so a payload over the host's pipe
  # budget hangs forever (CHAOS-3362, and this repo has a test for it).
  {
    printf '%s\n' 'import os, sys'
    printf '%s\n' 'try:'
    printf '%s\n' '    import yaml'
    printf '%s\n' 'except ImportError:'
    printf '%s\n' '    print("ERROR: PyYAML unavailable; cannot verify output paths")'
    printf '%s\n' '    sys.exit(3)'
    printf '%s\n' 'with open("gqlgen.yml", encoding="utf-8") as fh:'
    printf '%s\n' '    cfg = yaml.safe_load(fh) or {}'
    printf '%s\n' 'out = []'
    printf '%s\n' 'def add(v):'
    printf '%s\n' '    if isinstance(v, str) and v.strip():'
    printf '%s\n' '        out.append(v)'
    # Defaults matter as much as declared values: an omitted key still writes.
    printf '%s\n' 'defaults = {"exec": "generated.go", "model": "models_gen.go"}'
    printf '%s\n' 'for name, fallback in defaults.items():'
    printf '%s\n' '    sec = cfg.get(name)'
    printf '%s\n' '    if isinstance(sec, dict) and sec.get("filename"):'
    printf '%s\n' '        add(sec["filename"])'
    printf '%s\n' '    else:'
    printf '%s\n' '        add(fallback)'
    printf '%s\n' 'fed = cfg.get("federation")'
    printf '%s\n' 'if isinstance(fed, dict):'
    printf '%s\n' '    add(fed.get("filename"))'
    printf '%s\n' 'res = cfg.get("resolver")'
    printf '%s\n' 'if isinstance(res, dict):'
    printf '%s\n' '    add(res.get("filename"))'
    printf '%s\n' '    d = res.get("dir") or "."'
    printf '%s\n' '    add(d.rstrip("/") + "/")'
    # A template is a path fragment and can escape with ../ just like a filename.
    printf '%s\n' '    tpl = res.get("filename_template")'
    printf '%s\n' '    if isinstance(tpl, str) and tpl.strip():'
    printf '%s\n' '        add(os.path.join(d, tpl.replace("{name}", "probe")))'
    printf '%s\n' 'else:'
    printf '%s\n' '    add("./")'
    printf '%s\n' 'for p in out:'
    printf '%s\n' '    print(p)'
  } >"${reader}"

  local raw rc
  raw="$(cd "${config_dir}" && python3 "${reader}")"
  rc=$?
  rm -f "${reader}"
  if [ "${rc}" -ne 0 ]; then
    echo "gqlgen_output_scope: could not read output paths from gqlgen.yml (exit ${rc})." >&2
    printf '%s\n' "${raw}" >&2
    return 1
  fi
  if printf '%s\n' "${raw}" | grep -q '^ERROR:'; then
    printf '%s\n' "${raw}" >&2
    return 1
  fi

  local bad=0 out abs
  while IFS= read -r out; do
    [ -n "${out}" ] || continue
    abs="$(cd "${config_dir}" && readlink -m "${out}")"
    case "${abs}/" in
      "${allowed_root}/"*) ;;
      *)
        echo "    ${out}  ->  ${abs}" >&2
        bad=1
        ;;
    esac
  done <<EOF
${raw}
EOF
  return "${bad}"
}
