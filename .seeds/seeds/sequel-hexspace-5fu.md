---
id: sequel-hexspace-5fu
title: 'Release step: make sure all gems are up to date'
status: captured
type: question
created_at: 2026-10-26T13:08:48.908836+00:00
updated_at: 2026-10-26T13:08:48.908836+00:00
---

## Proposal (Ryan, 2026-10-26)

"I think as part of a release going forward, we should make sure all gems are up to date"

## What prompted it: 2.0.1 shipped behind its own dependencies

2.0.1 was released 2026-10-25 (tag v2.0.1 on f3af9cb, release.yml run 40245965781).
The next morning `bundle outdated` showed 15 gems behind
(log: claude_stuff/bundle-outdated-20261026-060805.log). Two are runtime
dependencies whose newest release the 2.0.1 gemspec refuses:

```
gem       gemspec bound        newest   released
thrift    >= 0.23, < 0.25      0.25.0   2026-09-30
hexspace  >= 0.2.1, < 0.4      0.4.0    2026-10-15
sequel    ~> 5.0               5.109.0  2026-10-01  (admitted; lock is on 5.108.0)
```

- thrift 0.25.0 already existed when the 2.0.1 changelog was written (2026-10-07).
- The 2.0.1 changelog says of hexspace "0.3.0 is its newest release, so the bound
  excludes nothing that exists". That was true on 2026-10-07 and false on release day.
- A consumer whose bundle needs thrift 0.25 or hexspace 0.4 cannot install 2.0.1.

Dependabot (monthly, 7-day cooldown, minor-and-patch grouped) did not catch either.
It updates Gemfile.lock inside the gemspec bounds, so an upper bound hides a new
release from it. No Dependabot PRs were open on 2026-10-26.

## What "up to date" can mean (not ruled)

(a) Lockfile current: `bundle update` before the release, so the local gate and CI
test the newest versions the gemspec allows. Covers dev and lint gems too.
(b) Bounds current: every runtime `s.add_dependency` upper bound admits the newest
released version. If one does not, either widen it after the suite passes, or the
changelog says why the newer version is excluded.

## Where to enforce it (not ruled)

1. A step in docs/release-setup.md only. Nothing checks it; this is how the thrift
   gap shipped.
2. A `just` recipe (for example `release-check`) that fails when (a) or (b) fails.
   The person cutting the release runs it at step 1. Recommended.
3. A check in release.yml's verify job. This is the hard gate, but it makes a release
   fail because of an upstream release that has nothing to do with the tagged commit,
   and it could block an urgent fix release.

## Risks of widening the two bounds

- thrift_compat.rb holds verbatim thrift 0.23.0 methods. thrift 0.25 may change the
  methods the shim replaces, so the shim needs a diff against 0.25 as well as a test run.
- hexspace is a 0.x gem, so 0.4.0 may break its API. That is why the bound exists.

## Scope

The sibling gems have the same release shape. If this becomes a fleet rule, gator owns
the standard; this seed covers sequel-hexspace only.
