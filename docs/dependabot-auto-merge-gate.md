# The Dependabot auto-merge gate — what it admits, measured

**Short answer: a major version bump cannot reach `main` through auto-merge, and never has. The
gate in `.github/workflows/dependabot-auto-merge.yml` already refuses majors, including majors
that arrive inside a group PR.** This page is the evidence, because the claim had been asserted
and contradicted twice from the expression text alone and neither reading was measured.

Re-run the evidence with `./docs/dependabot-gate-probe.sh`.

## What the gate says

Both gated steps — `Approve PR` and `Enable auto-merge` — carry the identical condition:

```yaml
if: >-
  contains(fromJSON('["version-update:semver-patch", "version-update:semver-minor"]'),
  steps.metadata.outputs.update-type)
  || steps.metadata.outputs.update-type == ''
```

An allowlist of patch and minor, plus an escape hatch for the empty string.

## The question that mattered: what does `update-type` report for a GROUP?

Every github-actions update here arrives as a group PR (`.github/dependabot.yml` groups them, and
the group's `update-types` includes `major`). If a group PR reported no single update-type, the
empty-string hatch would admit a grouped major with no human — and because `GITHUB_TOKEN`-driven
merges trigger no workflows, with no CI either. That was the worry.

It is not what happens. `dependabot/fetch-metadata` at the pinned
`25dd0e34f4fe68f24cc83900b1fe3fe149efef98` (v3.1.0) sets the output from `maxSemver()` in
`src/dependabot/output.ts`:

```ts
const UPDATE_TYPES_PRIORITY = [
  'version-update:semver-major',
  'version-update:semver-minor',
  'version-update:semver-patch'
]
// ...
return UPDATE_TYPES_PRIORITY.find(semverLevel => semverLevels.has(semverLevel)) || null
```

It reduces over **every** updated dependency in the PR and returns the first hit in priority
order. Major is first. So a group containing a major reports `version-update:semver-major`, the
allowlist misses, the hatch does not fire because the string is not empty, and both steps are
skipped. The README at that SHA says the same thing in prose — "the highest semver change being
made by this PR" — and `maxSemver` is what implements it.

`maxSemver` returns `null` when none of the three values is present, and `@actions/core`'s
`toCommandValue` renders `null` as `''`. So the escape hatch means exactly: *no dependency in this
PR had a classifiable semver update type.*

## The measurement: 38 real Dependabot PRs

Every Dependabot PR ever opened on this repo, replayed through the gate by
`docs/dependabot-gate-probe.sh --live`, with the verdict checked against who actually merged it.
`app/github-actions` as the merger means auto-merge fired; a human login means it did not.

|                                         |                                                        |
| --------------------------------------- | ------------------------------------------------------ |
| Dependabot PRs opened                   | 38 (60 updated dependencies in total)                  |
| PRs reporting an EMPTY update-type      | **0**                                                  |
| Majors (`#2 #3 #5 #11 #13 #19 #26 #29`) | 8, all `admitted=False`, all merged by `aguynamedryan` |
| Merged by `app/github-actions`          | 20, every one of them patch or minor                   |
| Merged by a human                       | 11 — the 8 majors, plus `#4` `#7` `#20` merged by hand |
| Closed without merging                  | 7 — superseded by a later group PR                     |

20 + 11 + 7 = 38.

Four of the eight majors were **grouped** (`#11`, `#13`, `#19`, `#26`), and `#26` was a group of
two majors at once. All four reported `version-update:semver-major` and all four waited for a
human. That is the grouped-major case, in production, across every Dependabot PR from 2026-02-17
to 2026-10-01.

The correlation is total: no PR the gate refused was ever merged by the bot.

## Correction: `d4e29b8`'s stated reason is false

`d4e29b8` (2026-03-06) added the `|| ... == ''` clause with the message *"The update-type is empty
for git ref (SHA) bumps, causing the approve and auto-merge steps to be skipped."* That premise
does not hold, on two independent counts:

1. **No action in this repo was SHA-pinned at `d4e29b8`.** At `d4e29b8^` every `uses:` was a tag —
   `actions/checkout@v6`, `ruby/setup-ruby@v1`, `actions/cache@v5`, `actions/upload-artifact@v7`,
   `dependabot/fetch-metadata@v2`. There was no SHA-ref bump to observe. (SHA pinning arrived
   today, 2026-10-02, with bead `ejy`.)
2. **A SHA-ref bump does not produce an empty update-type anyway.** Dependabot resolves a pinned
   SHA to its release tag and still emits the update type. `sigstore/cosign` pins
   `actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0`; its PR #5106 bumped that
   pin from SHA `924ae3a1…` to SHA `b7ad1dad…` and its commit message carries
   `update-type: version-update:semver-major`.

What the step skipping on 2026-03-06 almost certainly was: PR #13, a grouped **major**
(`actions/upload-artifact` 6 → 7), which the gate refused **correctly**. It was merged by hand at
`01:44:20Z` and `d4e29b8` was authored 16 minutes later at `02:00:38Z`. A correctly-refused major
was diagnosed as an empty update-type from a bump shape the repo did not contain.

So the escape hatch is dead code resting on a false rationale. It has never fired in 38 PRs.

## Decisions

**The gate stays as written.** Majors already require a human and that is demonstrated, not
inferred. There is nothing to fix.

**`major` stays in the github-actions group's `update-types`.** A grouped major does not defeat
the per-PR gate — `maxSemver` surfaces it — so keeping `major` means Dependabot proposes major
action bumps and a human rules on each one, which is the behaviour we want. Removing `major` would
mean they are never proposed at all and the pins quietly rot at whatever major they are on. That
is strictly worse: an unexamined-but-visible PR beats an invisible non-update.

**The `|| ... == ''` clause is left in place, and that is a deferred call, not an endorsement.**
The case for deleting it is good — it is dead, its justification is false, and it is the one path
by which an update nobody can classify would auto-merge without CI. The case for leaving it is
narrower: removing it is a behaviour change to this repo's most privileged workflow, justified by
"no shape producing an empty update-type was found" rather than by "no such shape exists", and
only bundler and github-actions are in play today. If a future ecosystem (a Docker digest, a git
submodule, an action pinned to a SHA with no semver tags) ever does report empty, the clause
auto-merges it unexamined, whereas deleting the clause makes it wait for a human — the same safe
failure every major already gets. **Recommend deleting it; it needs a ruling, not an agent.**

## The sharp edge nobody had noticed

`maxSemver` takes the max of the three *known* values. An entry whose update type is `''` is not
in the priority table, so it is dropped from the set rather than failing closed.

A group pairing an **unclassifiable** dependency with a classified **patch** therefore reports
`patch`, and is admitted — by the **allowlist**, not by the escape hatch. Deleting the escape
hatch would not close this. No observed PR exhibits it, and it cannot be exercised from the real
corpus, so the probe covers it with a synthetic input. It is recorded here so the next person does
not have to rediscover it from the TypeScript.

## What is still not proven

- That fetch-metadata, running on GitHub, emitted the update-type that the probe derives from each
  PR's commit message. The probe parses the YAML metadata block the action parses and ports
  `maxSemver`; it does not observe the action's actual output. The 20 bot merges and 8 human-merged
  majors are consistent with nothing else, but they are circumstantial.
- That GitHub's expression evaluator agrees with the probe's. The probe's is case-sensitive and
  GitHub's is not; harmless here because fetch-metadata emits these strings from a literal table,
  but a real divergence. See the note the probe prints.
- Anything about the `== ''` branch in production. It has never fired, so its behaviour on GitHub
  is untested by construction.

## Load-bearing, do not undo

These are independent of the gate and landed separately; they are listed because this file is what
someone reads before editing the workflow.

- `github.event.pull_request.user.login == 'dependabot[bot]'` — the PR's author, not
  `github.actor`, which is the actor of *this* event and changes on a re-run (bead `ejy`).
- The trigger is `pull_request`, never `pull_request_target`.
- Workflow-level `permissions: {}`, with `contents: write` + `pull-requests: write` on the job.
  Read `docs/workflow-permissions.md` before touching either; nothing in this repo's gate catches
  a block that is too narrow to function.
- The SHA pin on `dependabot/fetch-metadata`, and the 7-day `cooldown` in `dependabot.yml`
  (bead `xdg`).
- This workflow auto-merges **without push CI**, which is an accepted risk on the record — a
  release tags its own version-bump commit, and that commit does get push CI. Do not "fix" it.
