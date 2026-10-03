#!/usr/bin/env bash
# Probe whether any available workflow linter catches an UNDER-SCOPED job
# `permissions:` block -- the release.yml defect of 2026-10-01, where
# `permissions: actions: read` beside an `actions/checkout` step silenced
# `contents: read` and would have failed every release.
#
# Reproduces the matrix recorded in docs/workflow-permissions.md. Run from the
# repo root. Writes a timestamped log and prints its path.
set -uo pipefail

ACTIONLINT=actionlint@1.7.12
ZIZMOR=zizmor@1.30.1

repo_root=$(git rev-parse --show-toplevel)
log_dir="$repo_root/claude_stuff"
mkdir -p "$log_dir"
log="$log_dir/permissions-gate-probe-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$log") 2>&1

probe=$(mktemp -d)
trap 'rm -rf "$probe"' EXIT
mkdir -p "$probe/.github/workflows"
# Both tools require a git repo: a bare directory makes actionlint error with
# "no project was found".
git -C "$probe" init -q

emit() { # emit <name> <permissions-yaml-lines-or-empty>
	local name=$1 perms=$2
	{
		printf 'name: %s\non:\n  push:\n    branches: [main]\njobs:\n  build:\n    runs-on: ubuntu-latest\n' "$name"
		[ -n "$perms" ] && printf '%s\n' "$perms"
		printf '    steps:\n      - uses: actions/checkout@v7\n        with:\n          persist-credentials: false\n      - run: echo hi\n'
	} >"$probe/.github/workflows/$name.yml"
}

# POSITIVE CONTROLS. Without these every actionlint run below exits 0 and you
# cannot tell the tool from a no-op. These two are the permission mistakes
# actionlint DOES catch -- a bad scope VALUE and a bad scope NAME -- and they
# must both come back exit 1, or this probe is proving nothing.
emit y_bad_perm_value '    permissions:
      actions: readonly'
emit z_bad_perm_name '    permissions:
      content: read'

emit a_under_scoped '    permissions:
      actions: read'
emit b_empty_permissions '    permissions: {}'
emit c_no_block ''
emit d_correct_explicit '    permissions:
      actions: read
      contents: read'

echo "=== probe repo: $probe"
echo "=== controls: y=bad value, z=bad name (both MUST be caught, exit 1)"
echo "=== shapes:   a=under-scoped (the defect)  b=permissions:{}  c=no block (MUST stay clean)  d=correct"

run() { # run <label> <tool> <args...>
	local label=$1 tool=$2
	shift 2
	echo
	echo "--- $label : mise x $tool -- $*"
	(cd "$probe" && mise x "$tool" -- "$@")
	echo "    exit=$?"
}

for shape in y_bad_perm_value z_bad_perm_name a_under_scoped b_empty_permissions c_no_block d_correct_explicit; do
	f=".github/workflows/$shape.yml"
	run "$shape" "$ACTIONLINT" actionlint -no-color -oneline "$f"
	run "$shape" "$ZIZMOR" zizmor --offline --no-progress -q "$f"
	run "$shape" "$ZIZMOR" zizmor --offline --no-progress -q --persona=pedantic "$f"
done

echo
echo "=== cost of adopting zizmor against this repo's REAL workflows (default persona)"
(cd "$repo_root" && mise x "$ZIZMOR" -- zizmor --offline --no-progress -q .)
echo "    exit=$?"

echo
echo "log: $log"
