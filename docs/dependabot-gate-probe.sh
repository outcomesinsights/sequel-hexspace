#!/usr/bin/env bash
# Probe the update-type gate in .github/workflows/dependabot-auto-merge.yml.
#
# The question this exists to answer (bead 9zp): can a MAJOR version bump reach
# `main` through auto-merge? The gate admits patch and minor by allowlist and
# also carries `|| steps.metadata.outputs.update-type == ''`, an escape hatch
# added in d4e29b8 for bumps that report no semver update type. If a grouped PR
# -- which is how every github-actions update arrives here -- reported no single
# update-type, that hatch would admit a grouped major with no human and, because
# GITHUB_TOKEN merges trigger no workflows, no CI either.
#
# It does not. The answer is in dependabot/fetch-metadata at the pinned
# 25dd0e34f4fe68f24cc83900b1fe3fe149efef98 (v3.1.0): src/dependabot/output.ts
# sets `update-type` from maxSemver(), which reduces over EVERY updated
# dependency and returns the first hit in the priority order [major, minor,
# patch]. A group containing a major therefore reports semver-major. The full
# evidence, including the eight real major PRs this repo refused, is in
# docs/dependabot-auto-merge-gate.md.
#
# WHAT THIS PROVES AND WHAT IT DOES NOT. Part 1 extracts both `if:` expressions
# from the workflow WITH A YAML PARSER -- never retyped, so it cannot drift from
# the file it claims to test -- and evaluates them over every value update-type
# can take. Part 2 exercises a PORT of maxSemver over grouped shapes. Part 3 is
# the only part that touches reality: it replays this repo's real Dependabot PRs
# through the gate and checks who merged each one.
#
# So Parts 1 and 2 prove the LOGIC and nothing else. They do not prove that
# GitHub's expression evaluator agrees with the evaluator here, and the maxSemver
# port is a port -- if upstream changes it, this probe keeps testing the old
# behaviour while claiming to test the gate. Part 3 is the load-bearing evidence
# and it needs `gh` and network; without those it SKIPS, loudly, and the probe
# still exits 0 on Parts 1 and 2 alone. Read the skip notice before believing a
# green run covered the real PRs.
#
# Run from the repo root. `--live` is the default when `gh` is available; pass
# --offline to skip Part 3 deliberately. Writes a timestamped log, prints its path.
set -uo pipefail

repo_root=$(git rev-parse --show-toplevel)
log_dir="$repo_root/claude_stuff"
mkdir -p "$log_dir"
log="$log_dir/dependabot-gate-probe-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$log") 2>&1

mode=auto
case "${1:-}" in
--offline) mode=offline ;;
--live) mode=live ;;
"") ;;
*)
	echo "usage: $0 [--offline|--live]"
	exit 2
	;;
esac

workflow="$repo_root/.github/workflows/dependabot-auto-merge.yml"
[ -f "$workflow" ] || {
	echo "FATAL: $workflow is missing"
	exit 1
}

# Both gated steps are located by NAME, not by index, so inserting a step above
# them does not silently make this probe test something else. The expressions are
# read from the file and evaluated by a parser of the expression TEXT -- the
# allowlist is never hardcoded here, so editing the workflow's allowlist changes
# what this probe asserts rather than making it lie.
python3 - "$workflow" "$mode" <<'PY'
import json
import re
import subprocess
import sys

import yaml

workflow, mode = sys.argv[1:3]
doc = yaml.safe_load(open(workflow))
job = doc["jobs"]["dependabot"]

GATED = ["Approve PR", "Enable auto-merge"]
exprs = {}
for want in GATED:
    steps = [s for s in job["steps"] if s.get("name") == want]
    assert len(steps) == 1, f"expected exactly one {want!r} step, found {len(steps)}"
    assert "if" in steps[0], f"{want!r} has NO `if:` -- the gate is gone"
    exprs[want] = " ".join(steps[0]["if"].split())

print(f"workflow: {workflow}")
for name, e in exprs.items():
    print(f"  gate on {name!r}: {e}")

passed = failed = 0


def check(ok, desc):
    global passed, failed
    if ok:
        passed += 1
        print(f"    PASS  {desc}")
    else:
        failed += 1
        print(f"    FAIL  {desc}")


# A gate that approves on terms the merge step does not share, or the reverse, is
# a defect in either direction: approve-only leaves an approved PR sitting, and
# merge-only merges something nobody approved.
print("\n=== The two gated steps must carry the IDENTICAL condition")
check(
    exprs["Approve PR"] == exprs["Enable auto-merge"],
    "Approve PR and Enable auto-merge gate on the same expression",
)


# A deliberately small evaluator for the ONE expression shape this gate uses:
# a `||` disjunction of `contains(fromJSON('<json array>'), <ctx>)` terms and
# `<ctx> == '<literal>'` terms. Anything else raises rather than quietly
# returning False, so a rewritten gate fails this probe loudly instead of being
# scored against a shape it no longer has.
CONTAINS = re.compile(r"^contains\(\s*fromJSON\('(?P<arr>.+?)'\)\s*,\s*(?P<ctx>[\w.\-]+)\s*\)$")
EQUALS = re.compile(r"^(?P<ctx>[\w.\-]+)\s*==\s*'(?P<lit>[^']*)'$")
CTX = "steps.metadata.outputs.update-type"


def evaluate(expr, update_type):
    for term in [t.strip() for t in expr.split("||")]:
        m = CONTAINS.match(term)
        if m:
            assert m.group("ctx") == CTX, f"unexpected context {m.group('ctx')!r}"
            if update_type in json.loads(m.group("arr")):
                return True
            continue
        m = EQUALS.match(term)
        if m:
            assert m.group("ctx") == CTX, f"unexpected context {m.group('ctx')!r}"
            if update_type == m.group("lit"):
                return True
            continue
        raise AssertionError(f"evaluator does not understand the term {term!r}")
    return False


gate = exprs["Approve PR"]

print("\n=== Every value steps.metadata.outputs.update-type can take")
# POSITIVE CONTROL first. Without a value the gate ADMITS, this probe cannot tell
# the real gate from `return False`.
for value, want, note in [
    ("version-update:semver-patch", True, "patch -- admitted (positive control)"),
    ("version-update:semver-minor", True, "minor -- admitted"),
    ("version-update:semver-major", False, "major -- REFUSED, the whole point"),
    ("", True, "empty -- admitted by the d4e29b8 escape hatch"),
    ("version-update:semver-unknown", False, "an unenumerated value -- refused"),
    ("VERSION-UPDATE:SEMVER-MAJOR", False, "uppercased major -- see the caveat below"),
]:
    check(evaluate(gate, value) is want, f"{value!r:34} -> {evaluate(gate, value)} ({note})")

# GitHub's expression evaluator compares strings CASE-INSENSITIVELY, both in
# `==` and in `contains()`. The evaluator above is case-SENSITIVE, so the two
# disagree on the last row: on GitHub, 'VERSION-UPDATE:SEMVER-MAJOR' would still
# be refused (it is absent from the allowlist either way) but an uppercased
# 'VERSION-UPDATE:SEMVER-PATCH' would be ADMITTED where this probe refuses it.
# That divergence is harmless because fetch-metadata emits these three strings
# from a literal table (output.ts UPDATE_TYPES_PRIORITY) and cannot emit another
# casing -- but it is a real difference between this probe and production, and it
# is the kind of thing that makes a probe lie if the gate is ever rewritten to
# depend on case.
print("    NOTE: this evaluator is case-sensitive; GitHub's `==`/contains() are not.")
print("          Harmless here (fetch-metadata emits a fixed literal table), but")
print("          it is a divergence between this probe and the real evaluator.")


# A PORT of maxSemver from src/dependabot/output.ts at
# 25dd0e34f4fe68f24cc83900b1fe3fe149efef98 (v3.1.0), lines 10-14 and 79-86:
#
#   const UPDATE_TYPES_PRIORITY = [major, minor, patch]
#   maxSemver = UPDATE_TYPES_PRIORITY.find(l => semverLevels.has(l)) || null
#
# and output.ts line 56 sets the output to that, where @actions/core's
# toCommandValue turns null into the empty string. Being a port, it is evidence
# about the algorithm as read, not about the action as run.
PRIORITY = [
    "version-update:semver-major",
    "version-update:semver-minor",
    "version-update:semver-patch",
]


def max_semver(update_types):
    levels = set(update_types)
    for level in PRIORITY:
        if level in levels:
            return level
    return ""  # maxSemver returns null; toCommandValue renders null as ''


print("\n=== maxSemver over a GROUP: what one PR reports for several dependencies")
for types, want_type, want_admit, note in [
    (["version-update:semver-major"], "version-update:semver-major", False, "PR #13 shape: lone grouped major"),
    (
        ["version-update:semver-major", "version-update:semver-major"],
        "version-update:semver-major",
        False,
        "PR #26 shape: two grouped majors",
    ),
    (
        ["version-update:semver-minor", "version-update:semver-patch"],
        "version-update:semver-minor",
        True,
        "PR #10 shape: minor+patch group",
    ),
    (
        ["version-update:semver-patch", "version-update:semver-major"],
        "version-update:semver-major",
        False,
        "a major hiding behind a patch is still reported",
    ),
    ([], "", True, "no dependencies at all -- the empty group",),
]:
    got = max_semver(types)
    check(
        got == want_type and evaluate(gate, got) is want_admit,
        f"{len(types)} dep(s) -> {got!r}, admitted={evaluate(gate, got)} ({note})",
    )

print("\n=== THE SHARP EDGE: maxSemver IGNORES an unclassifiable entry")
# SYNTHETIC, and it has to be: no PR in this repo's history has ever produced an
# empty update-type, so the real corpus cannot exercise this. maxSemver takes the
# max of the three KNOWN values; an entry whose updateType is '' is simply not in
# the priority table, so it is dropped from the set rather than failing closed.
# A group pairing an UNCLASSIFIABLE dependency with a classified patch therefore
# reports `patch` and is admitted -- by the ALLOWLIST clause, note, not by the
# `== ''` escape hatch, so removing that hatch would not close this. Nothing
# observed exhibits it; it is recorded so the next person does not have to
# rediscover it from the TypeScript.
masked = max_semver(["", "version-update:semver-patch"])
check(
    masked == "version-update:semver-patch" and evaluate(gate, masked) is True,
    f"['', patch] -> {masked!r}, admitted=True -- an unclassifiable dep is INVISIBLE here",
)
check(
    max_semver([""]) == "" and evaluate(gate, "") is True,
    "[''] alone -> '' -- admitted by the escape hatch, the only path that uses it",
)

if mode == "offline":
    print("\n=== Part 3 (real PRs): SKIPPED -- --offline was passed")
    print("    Parts 1 and 2 prove the LOGIC ONLY. Nothing above touched a real PR.")
else:
    print("\n=== Part 3: this repo's REAL Dependabot PRs")
    # The only part of this probe that is evidence about production. For every
    # Dependabot PR ever opened here it reads the commit message fetch-metadata
    # would parse, derives the update-type, and checks the gate's verdict against
    # who actually merged the PR: `app/github-actions` means auto-merge fired, a
    # human login means it did not.
    # `gh` being absent raises FileNotFoundError rather than returning non-zero,
    # so it needs catching explicitly or a machine without gh gets a traceback
    # where it should get the SKIP notice.
    try:
        probe = subprocess.run(
            [
                "gh", "pr", "list", "--state", "all", "--limit", "200", "--json",
                "number,author,mergedBy,state,title",
            ],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        probe = None

    if probe is None or probe.returncode != 0:
        why = "`gh` is not installed" if probe is None else f"`gh pr list` failed: {probe.stderr.strip()[:200]}"
        print(f"    SKIPPED -- {why}")
        print("    Parts 1 and 2 prove the LOGIC ONLY. Nothing above touched a real PR.")
    else:
        prs = [p for p in json.loads(probe.stdout) if p["author"]["login"] == "app/dependabot"]
        print(f"    {len(prs)} Dependabot PRs")
        empty = []
        for pr in sorted(prs, key=lambda p: p["number"]):
            n = pr["number"]
            msg = subprocess.run(
                ["gh", "api", f"repos/{{owner}}/{{repo}}/pulls/{n}/commits", "--jq", ".[].commit.message"],
                capture_output=True,
                text=True,
            )
            if msg.returncode != 0:
                check(False, f"PR #{n}: could not read commits")
                continue
            types = re.findall(r"^  update-type: (\S+)$", msg.stdout, re.M)
            deps = len(re.findall(r"^- dependency-name:", msg.stdout, re.M))
            got = max_semver(types)
            admitted = evaluate(gate, got)
            merged_by = (pr["mergedBy"] or {}).get("login") or "-"
            bot_merged = merged_by == "app/github-actions"
            if got == "":
                empty.append(n)
            shown = got if got else "(empty)"
            # The gate's verdict must match reality: refused => no bot merge.
            check(
                admitted or not bot_merged,
                f"PR #{n:<3} deps={deps} types={len(types)} -> {shown:28} "
                f"admitted={admitted!s:5} mergedBy={merged_by}",
            )
        check(
            not empty,
            f"every PR reported a NON-EMPTY update-type (empty on: {empty or 'none'})",
        )
        print("    An `admitted=False` row merged by a human is the gate WORKING.")
        print("    What this still does not prove: that fetch-metadata, running on")
        print("    GitHub, emitted the update-type derived here from the commit message.")

print(f"\n=== {passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
PY
rc=$?

echo "log: $log"
exit $rc
