# Run the full CI suite (lint + tests)
test: lint _test

# Four tools, fail-fast in this order. What each one is here for, and what it is
# NOT here for:
#
#   rubocop  -- the cop set is omakase plus the whole Lint department; see
#               .rubocop.yml. `fmt` already autocorrected everything correctable,
#               so what reaches here is what a human has to decide about.
#   shellcheck
#            -- every TRACKED standalone shell script. treefmt's shfmt only
#               FORMATS them; this is their lint. Shell inside a workflow `run:`
#               block is the other path -- actionlint shellchecks that, in
#               `hygiene` -- and until this line the same shell was linted inside
#               a workflow and not in a script. Threshold is `-S warning`: errors
#               and warnings fail, info and style do not. The docs/*.sh probes
#               were measured clean at that threshold on 2026-10-02.
#               The list comes from `git ls-files`, never a glob or a hand-list:
#               a hand-list goes stale the day a script is added, and an
#               unmatched glob is fatal in zsh. `xargs -r` makes a tree with no
#               tracked scripts a pass instead of a bare-`shellcheck` usage error.
#   zizmor   -- GitHub Actions security audits, run on the repo ROOT and not on
#               .github/workflows/, because the root is what also reaches
#               .github/dependabot.yml (confirmed in its own output: it names
#               dependabot.yml among the files it completed).
#   cog      -- conventional-commit subjects for everything since the last v* tag.
#
# zizmor IS NOT A PERMISSIONS GATE and must not be read as one. It was probed at
# 1.30.1 against the exact under-scoped-`permissions` defect this repo cares about
# and its output is IDENTICAL on the broken and the fixed shape, so no exit code
# derived from it tells them apart -- the full matrix is in
# docs/workflow-permissions.md and `hygiene` says the same thing at more length.
# It is adopted here for its OTHER audits (artipacked, cache-poisoning,
# adhoc-packages, template-injection and the rest), which are real and which
# nothing else here covers. The permissions gap stays open and stays declared.
#
# `--offline` so a lint run makes no network call, and `--config` passed
# EXPLICITLY rather than left to discovery: zizmor resolves a discovered config
# against the git COMMON dir, so from one of this repo's worktrees discovery reads
# the MAIN checkout's file and silently ignores the one on the branch being linted.
#
# cog needs cog.toml's `tag_prefix = "v"` to find a baseline at all. Measured here
# 2026-10-02: with the file, "No errored commits" over a real 31-commit range and
# rc 0; with the file moved aside, `Error: unable to get any tag` and rc 1 -- which
# is the dangerous direction, because piped through tee or tail that error reads as
# a pass. Do not narrow the range to make cog quiet; it is green on this history.
#
# Report what a formatter cannot fix: cops, workflow audits, commit subjects.
lint:
    bundle exec rubocop
    git ls-files -z '*.sh' '*.bash' | xargs -0 -r shellcheck -S warning
    zizmor --offline --config .github/zizmor.yml .
    cog check --from-latest-tag --ignore-merge-commits

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
    TZ=UTC bundle exec ruby test/all.rb

# .github/workflows/ci.yml splits this closure across its two jobs and runs
# every piece of it: `lint` runs `just fmt-check lint hygiene`, `test` runs
# `just _test` per matrix Ruby. Change one and change the other.
ci: fmt-check test hygiene

# The last step of gator's CI tool-setup prelude, and what a fresh clone runs
# first: mise installs every tool mise.toml pins, then bundler the gems. A
# clone needs git and mise and nothing else.
#
# Install the pinned tools and the bundle.
setup:
    mise install
    bundle install

bundle-update *ARGS:
    bundle update {{ ARGS }}

# One treefmt.toml now drives every formatter (gator-1sz), replacing the three
# hand-written lines that used to live here -- rubocop -a, `just --fmt`, and
# mdformat over `git ls-files "*.md"`. treefmt.toml records which formatter blocks
# are present, which are deliberately absent, and the one place coverage narrows
# on purpose.
#
# Rewrite files to canonical format. Run deliberately; never from a hook.
fmt:
    treefmt

# SEMANTICS CHANGED ON 2026-10-02, and the change is deliberate. This recipe used
# to be strictly read-only, on the argument that a formatter rewriting files
# mid-commit changes what you already reviewed. `treefmt --fail-on-change` does
# not work that way: it FORMATS THE TREE AND THEN EXITS 1 (measured, treefmt
# 2.6.0). That is formatter-hook semantics -- the fix is applied, the gate fails,
# the author re-stages and commits again -- and it is what gator-1sz ruled, having
# considered exactly this objection. The thing the standard forbids is a hook that
# rewrites and SUCCEEDS silently, which is a different and worse shape: nobody
# reviews what they cannot see failed.
#
# The practical consequence to know: a failing `pre-commit` has already modified
# your working tree. `git diff` after a red commit shows what it did.
#
# Apply canonical format and fail if anything was out of shape.
fmt-check:
    treefmt --fail-on-change

# Defaults to the complete `ci`; point it at something smaller ONLY where
# running complete CI locally is impractical.
#
# What actually runs before a push.
pre-push: ci

# Must stay FAST — a sub-minute budget, since it runs on every commit. Tests
# belong here when they fit; lint alone when they do not.
#
# `seeds-check` is here and NOT in `ci`/`pre-push`: a commit is the only way a
# seed file reaches history, so gating the commit path covers every route in, and
# `ci` is the local stand-in for a CI job on a runner that has no seeds installed.
#
# What runs before every commit.
pre-commit: fmt-check lint hygiene seeds-check

# Not format validity -- content plausibility, plus the `--against-git` tier that
# compares the committed corpus against the working tree. Two things it catches
# that nothing else here does: a bulk sweep rewriting most of the corpus in one
# field, and the single seed whose body moved while its `updated_at` did not,
# which is the signature of a formatter or linter reaching into the store. That
# second one is why treefmt.toml excludes `.seeds/**` -- this recipe and that
# exclusion are two halves of one decision.
#
# Skips when seeds is absent, which is a deliberate hole: this is a deliberation
# store, not shipped code, and a clone without the tool must still be able to
# commit. Nothing else on the commit path is allowed to skip this way.
#
# Verify the seeds store is plausible and has not been rewritten underneath us.
seeds-check:
    @command -v seeds >/dev/null 2>&1 || exit 0; seeds check --against-git

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
    # This covers workflow `run:` blocks ONLY; tracked standalone .sh/.bash
    # scripts are shellchecked directly by `lint`, which fails loudly if the
    # binary is missing rather than skipping.
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
    # DO NOT REACH FOR zizmor TO CLOSE IT. zizmor IS now part of `lint`, as of
    # 2026-10-02, and that does not change this gap by one inch -- it was
    # adopted for its other audits. It was probed at 1.30.1 against this exact
    # defect: its output on the defect shape is IDENTICAL to its output on the
    # fixed shape at every persona, so no exit code derived from it
    # discriminates the two; and the only shape it flags for permissions at its
    # default persona is a job with NO block, which is correct code. It models
    # permissions as a security surface (too BROAD is a finding) and has no
    # model of "too narrow to function" either. Config can suppress its rules,
    # not add one.
    #
    # The accepted gap, the full probe matrix, and the fail-closed argument for
    # accepting it are in docs/workflow-permissions.md, which is also where
    # someone editing a permissions block is pointed. A hand-rolled
    # checkout-only rule was rejected on purpose: right about one action, silent
    # about every other, and it would make this gate LOOK like it covered the
    # class. Re-probe with ./docs/permissions-gate-probe.sh.
    actionlint -no-color -oneline || rc=1

    exit $rc
