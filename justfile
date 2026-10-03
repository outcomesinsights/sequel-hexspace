# Run the full CI suite (lint + tests)
test: lint _test

lint:
    bundle exec rubocop

# The suite runs under TZ=UTC to match CI's environment, and it matters: Spark's
# session timezone is Etc/UTC, so on a host west of UTC the server is already on
# the next date for part of the evening and the CURRENT_DATE test compares it
# against a local Date.today that disagrees. That failed a docs-only push on
# 2026-09-12 and would fail every day between 17:00 and midnight Pacific, while
# CI — whose runners are UTC — stayed green. A gate that rejects what CI accepts
# is one people learn to bypass.
#
# This makes the LOCAL gate blind to genuine client/server timezone divergence.
# What this adapter should do when the two differ is a real question and is not
# answered here.
_test:
    TZ=UTC bundle exec rake test

ci: fmt-check test hygiene

bundle-update *ARGS:
    bundle update {{ ARGS }}

# Rewrite files to canonical format. Run deliberately; never from a hook.
fmt:
    bundle exec rubocop -a
    just --fmt --unstable
    git ls-files "*.md" | xargs -r mdformat

# A formatter that rewrites files mid-commit changes what you already reviewed,
# so the hooks run this instead of `fmt`.
#
# Report format drift without changing anything.
fmt-check:
    bundle exec rubocop
    just --fmt --check --unstable
    git ls-files "*.md" | xargs -r mdformat --check

# Defaults to the complete `ci`; point it at something smaller ONLY where
# running complete CI locally is impractical.
#
# What actually runs before a push.
pre-push: ci

# Must stay FAST — a sub-minute budget, since it runs on every commit. Tests
# belong here when they fit; lint alone when they do not.
#
# What runs before every commit.
pre-commit: fmt-check lint hygiene

# Inherited from overcommit when it was removed on 2026-09-12: MergeConflicts,
# YamlSyntax, JsonSyntax. Its RuboCop and test targets were already covered by
# fmt-check/lint/test. HardTabs and TrailingWhitespace were deliberately dropped
# because they fight shfmt, .tsv files, and generated output.
#
# Check for conflict markers, malformed YAML/JSON, and workflow defects.
hygiene:
    #!/usr/bin/env bash
    set -uo pipefail
    rc=0
    bad=$(git ls-files | xargs -r grep -IlE '^(<{7}|={7}|>{7})( |$)' 2>/dev/null || true)
    [ -n "$bad" ] && { echo "merge conflict markers:"; printf '%s\n' "$bad" | sed 's/^/  /'; rc=1; }
    for f in $(git ls-files '*.yml' '*.yaml'); do
      python3 -c 'import yaml,sys; yaml.safe_load(open(sys.argv[1]))' "$f" 2>/dev/null \
        || { echo "invalid YAML: $f"; rc=1; }
    done
    for f in $(git ls-files '*.json'); do
      jq empty "$f" 2>/dev/null || { echo "invalid JSON: $f"; rc=1; }
    done

    # The YAML loop above proves only that a file PARSES. A workflow can parse
    # perfectly and still be semantically broken, and release.yml -- the gate on
    # publishing -- came one commit from exactly that. An explicit permissions
    # block sets every scope it does not name to none, so the `verify` job's
    # `permissions: actions: read` would have revoked the `contents: read` that
    # the checkout step added in f20fda8 needs, and every release would have
    # failed for a reason that looks nothing like its cause. Valid YAML
    # throughout; hygiene green throughout.
    #
    # CORRECTION (verified against git history): that defect was never
    # committed. f20fda8 added `contents: read`, the checkout, and a comment
    # about this hazard in one diff, and 2.0.0 released through the job the same
    # day. An earlier version of this comment said it "shipped on 2026-10-01";
    # it did not. The defect CLASS is real and ungated, which is the point here,
    # but do not repeat the incident claim -- see docs/workflow-permissions.md.
    #
    # actionlint type-checks workflow syntax, validates expressions and
    # `github`/`needs`/`steps` contexts, checks action input names and
    # `runs-on` labels, and runs shellcheck over every `run:` block. Run from
    # the repo root with no arguments it finds .github/workflows itself, so a
    # new workflow file is covered the day it lands.
    #
    # Called BARE, as of 2026-10-02. It used to need `mise x actionlint@1.7.12
    # -- actionlint`, because the only actionlint on PATH was a GLOBAL mise shim
    # with no version set: `command -v actionlint` succeeded and running it died
    # with "No version is set for shim: actionlint". The repo-local mise.toml
    # fixes that at the root -- it pins actionlint for this tree, so the shim
    # resolves. Demonstrated here before this line was changed, including from
    # an explicitly UNTRUSTED checkout, since a `[tools]`-only mise.toml loads
    # without `mise trust`.
    #
    # The pin is MAJOR-only now (`actionlint = "1"`), not the exact 1.7.12 this
    # line used to carry. A new minor can therefore turn this gate red on
    # workflows nobody touched, which is the standard's deliberate trade: a new
    # rule arrives with the bump and goes red where it lands, rather than being
    # invisible until someone bumps a pin by hand. `mise outdated` shows what
    # moved.
    #
    # actionlint's own shellcheck pass over every `run:` block is only real
    # while mise.toml pins shellcheck: actionlint SKIPS it silently when the
    # binary is absent. That line is in mise.toml with this comment on it.
    #
    # If mise or actionlint is missing this fails loudly; it must never skip
    # quietly, which is how a gate goes toothless without anyone noticing.
    #
    # Cost is 0.07s against this recipe's 0.54s, which is why it sits in
    # hygiene -- running at commit stage as well as pre-push -- instead of
    # pre-push only.
    #
    # WHAT THIS DOES NOT CATCH, measured against 1.7.12: actionlint's own
    # `permissions` check validates scope NAMES and VALUES only. It has no model
    # of which permissions an action requires, so `permissions: {}` next to an
    # `actions/checkout` step lints clean -- and so does the release.yml defect
    # described above. That class of defect is still ungated here, deliberately.
    #
    # DO NOT REACH FOR zizmor TO CLOSE IT -- it was probed, 1.30.1, and it
    # cannot. Its output on the defect shape is IDENTICAL to its output on the
    # fixed shape at every persona, so no exit code derived from it discriminates
    # the two; and the only shape it flags for permissions at its default persona
    # is a job with NO block, which is correct code. It models permissions as a
    # security surface (too BROAD is a finding) and has no model of "too narrow
    # to function" either. Config can suppress its rules, not add one.
    #
    # The accepted gap, the full probe matrix, and the fail-closed argument for
    # accepting it are in docs/workflow-permissions.md, which is also where
    # someone editing a permissions block is pointed. A hand-rolled
    # checkout-only rule was rejected on purpose: right about one action, silent
    # about every other, and it would make this gate LOOK like it covered the
    # class. Re-probe with ./docs/permissions-gate-probe.sh.
    actionlint -no-color -oneline || rc=1

    exit $rc
