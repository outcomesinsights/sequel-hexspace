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

# Report format drift without changing anything. This is what the hooks run —
# a formatter that rewrites files mid-commit changes what you already reviewed.
fmt-check:
    bundle exec rubocop
    just --fmt --check --unstable
    git ls-files "*.md" | xargs -r mdformat --check

# What actually runs before a push. Defaults to the complete `ci`; point it at
# something smaller ONLY where running complete CI locally is impractical.
pre-push: ci

# Runs on every commit, so it must stay FAST — a sub-minute budget. Tests belong
# here when they fit; lint alone when they do not. fmt-check never rewrites.
pre-commit: fmt-check lint hygiene

# Content checks inherited from overcommit when it was removed (2026-09-12):
# MergeConflicts, YamlSyntax, JsonSyntax. RuboCop and the test target were already
# covered by fmt-check/lint/test; HardTabs and TrailingWhitespace were dropped because
# they fight shfmt, .tsv, and generated files. See habituate/standards.md.
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
    exit $rc
