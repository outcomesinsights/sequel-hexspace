#!/usr/bin/env bash
# Probe the `ci` aggregate job's gate in .github/workflows/ci.yml against every
# value GitHub documents for a needed job's `result` -- success, failure,
# cancelled, skipped -- plus the degenerate shapes a positive assertion can be
# fooled by. The defect this exists for (bead mwd): the gate used to be a
# blocklist testing only for 'failure' and 'cancelled', so a SKIPPED needed job
# left it printing "All CI jobs passed" and exiting 0, which satisfied both
# `main`'s only required status check and release.yml's "was this commit tested"
# gate.
#
# WHAT THIS PROVES AND WHAT IT DOES NOT. It extracts the step's `run:` block from
# the workflow WITH A YAML PARSER -- never retyped, so it cannot drift from the
# file it claims to test -- and executes it under bash with NEEDS_JSON set to
# each shape by hand. So it proves the SHELL LOGIC. It does NOT prove that
# GitHub's expression evaluator yields these values for `toJSON(needs)`, nor
# that a skipped job makes its result 'skipped', nor that a run with a skipped
# job records conclusion success. Those three are documented GitHub behaviour and
# are not measured anywhere in this repo; the workflow has never run on GitHub
# from this tree.
#
# Run from the repo root. Writes a timestamped log and prints its path.
set -uo pipefail

repo_root=$(git rev-parse --show-toplevel)
log_dir="$repo_root/claude_stuff"
mkdir -p "$log_dir"
log="$log_dir/ci-gate-probe-$(date +%Y%m%d-%H%M%S).log"
exec > >(tee -a "$log") 2>&1

workflow="$repo_root/.github/workflows/ci.yml"
script=$(mktemp)
expected_jobs=$(mktemp)
trap 'rm -f "$script" "$expected_jobs"' EXIT

# The step is located by NAME, not by index, so inserting a step above it does
# not silently make this probe test something else. EXPECTED_JOBS is read from
# the file too: hardcoding it here would let the probe pass while the workflow
# guarded a different set of jobs.
python3 - "$workflow" "$script" "$expected_jobs" <<'PY'
import sys

import yaml

workflow, script_out, expected_out = sys.argv[1:4]
job = yaml.safe_load(open(workflow))["jobs"]["ci"]
steps = [s for s in job["steps"] if s.get("name") == "Check CI status"]
assert len(steps) == 1, f"expected exactly one 'Check CI status' step, found {len(steps)}"
step = steps[0]
assert step["env"]["NEEDS_JSON"] == "${{ toJSON(needs) }}", step["env"]["NEEDS_JSON"]
open(script_out, "w").write(step["run"])
open(expected_out, "w").write(step["env"]["EXPECTED_JOBS"])
print(f"needs: {job['needs']}")
print(f"EXPECTED_JOBS: {step['env']['EXPECTED_JOBS']}")
PY
[ -s "$script" ] || {
	echo "FATAL: could not extract the gate's run: block"
	exit 1
}

rc=0
pass=0
fail=0

needs_json() { # needs_json lint=success test=skipped ...
	local out="{" first=1 pair name result
	for pair in "$@"; do
		name=${pair%%=*}
		result=${pair#*=}
		[ $first -eq 1 ] || out+=","
		first=0
		out+="\"$name\":{\"result\":\"$result\",\"outputs\":{}}"
	done
	printf '%s}' "$out"
}

probe() { # probe <want-rc> <description> <job=result>...
	local want=$1 desc=$2
	shift 2
	local json got
	if [ "$1" = "--raw" ]; then
		json=$2
	else
		json=$(needs_json "$@")
	fi
	printf '\n--- %s\n    NEEDS_JSON=%s\n' "$desc" "$json"
	NEEDS_JSON="$json" EXPECTED_JOBS="$(cat "$expected_jobs")" bash "$script" 2>&1 | sed 's/^/    /'
	got=${PIPESTATUS[0]}
	if [ "$got" = "$want" ]; then
		printf '    => exit %s (want %s) PASS\n' "$got" "$want"
		pass=$((pass + 1))
	else
		printf '    => exit %s (want %s) FAIL\n' "$got" "$want"
		fail=$((fail + 1))
		rc=1
	fi
}

echo "=== The four documented values of needs.*.result"
# POSITIVE CONTROL. Without this the probe cannot tell the gate from `exit 1`.
probe 0 "success + success -- the only shape that may pass" lint=success test=success
probe 1 "success + skipped -- THE DEFECT: this used to exit 0" lint=success test=skipped
probe 1 "skipped + skipped -- both needed jobs skipped" lint=skipped test=skipped
probe 1 "success + failure" lint=success test=failure
probe 1 "success + cancelled" lint=success test=cancelled
probe 1 "failure + skipped -- a failure is still reported alongside a skip" lint=failure test=skipped

echo
echo "=== Shapes a POSITIVE assertion has to be defended against"
# "every needed job succeeded" is vacuously true over an empty set, and a job
# quietly dropped from `needs:` is the realistic way that happens.
probe 1 "needs: {} -- the empty set must not pass vacuously" --raw '{}'
probe 1 "test dropped from needs: -- only lint guarded" lint=success
probe 1 "an extra job in needs: that EXPECTED_JOBS does not name" lint=success test=success extra=success
# A blocklist cannot fail closed on a value nobody enumerated. This is the whole
# argument for the positive form, so it is measured rather than asserted.
probe 1 "an undocumented fifth value" lint=success test=neutral
probe 1 "an empty result string" lint=success test=

echo
echo "=== DEFECT CONTROL: the blocklist this replaced, on the same inputs"
# A SIMULATION, labelled as one. The old step interpolated
# "${{ contains(needs.*.result, 'failure') }}" straight into its script body, so
# there is nothing in today's file to extract; the two booleans are computed here
# the way GitHub's `contains` would. It is here to show that the probe
# discriminates a fixed gate from the broken one, not to re-test the old code.
old_gate() { # old_gate "<space-separated results>"
	local results=$1 has_failure=false has_cancelled=false
	[[ $results == *failure* ]] && has_failure=true
	[[ $results == *cancelled* ]] && has_cancelled=true
	if [[ $has_failure == "true" ]]; then
		echo "One or more CI jobs failed"
		return 1
	fi
	if [[ $has_cancelled == "true" ]]; then
		echo "One or more CI jobs were cancelled"
		return 1
	fi
	echo "All CI jobs passed"
	return 0
}
for results in "success success" "success skipped" "success failure" "success cancelled"; do
	out=$(old_gate "$results")
	printf '    [%s] -> exit %s: %s\n' "$results" "$?" "$out"
done
echo "    'success skipped' exiting 0 above IS the defect; the fixed gate exits 1 on it."

echo
printf '=== %s passed, %s failed\n' "$pass" "$fail"
echo "log: $log"
exit $rc
