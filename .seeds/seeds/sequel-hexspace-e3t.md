---
id: sequel-hexspace-e3t
title: 'Unruled from the release review: ci-green lookup strictness and v* tag rulesets'
status: resolved
type: question
created_at: 2026-10-01T22:16:31.535884+00:00
updated_at: 2026-10-16T13:30:49.205036+00:00
resolved_at: 2026-10-16T13:30:49.205027+00:00
resolution: Both questions ruled 2026-10-01; CI lookup built in 703484f, v* tag ruleset verified by gator-doctor --remote
tags:
  - release
  - ci
  - unruled
---

## Status: NOT RULED. Do not build.

Two items came out of the release review (sequel-duckdb-4zy.8, 2026-10-01) explicitly
marked as awaiting Ryan's ruling, relayed by the jigsaw_builder session with "Do NOT
build until Ryan says so". Captured here so they are not lost and not mistaken for
approved work. Four beads were filed from the same review; these two deliberately were
not.

### 1. Pin the ci-green lookup to workflow path / event / branch

release.yml's verify job asks the Actions API for successful runs of ci.yml at the tagged
SHA:

```
repos/$REPO/actions/workflows/ci.yml/runs?head_sha=$SHA&status=success
```

It counts any successful run of that workflow for that commit. It does not constrain the
event (push vs pull_request vs workflow_dispatch) or the branch. So a success recorded
against the same SHA from a pull_request run, or from a manual dispatch, satisfies the
gate as readily as a push to main.

Whether that is too loose is the unruled question. The argument for tightening: a green
run on a PR is not the same assurance as a green run on main, and workflow_dispatch is
trivially re-runnable. The argument against: it is the same commit and the same workflow,
so the tests that ran are the same tests, and extra constraints add ways for a legitimate
release to be blocked for reasons nobody can see from the error.

Measured when the gate was written (2026-10-01): main's head returned 1, its merge parent
returned 0, a bogus SHA returned 0 -- so the lookup discriminates correctly on the simple
cases. The looseness is about which KIND of success counts, not whether it works.

### 2. v\* tag rulesets

Whether to add a GitHub ruleset governing v\* tags -- who may create them, whether they
can be deleted or moved once pushed. Today any account with write access can push,
delete and re-point a v\* tag, and re-pointing one after a release means the tag no longer
describes what was published.

Related and relevant, though separately ruled: main carries classic branch protection
requiring the ci check with enforce_admins=false, so the required check is advisory for
an admin. Both pushes on 2026-10-01 reported "Bypassed rule violations". A tag ruleset
would face the same question about whether admins are exempt, and an exempt admin is the
only person likely to be pushing release tags -- so a ruleset that exempts admins may
protect nobody.

### Why these are here and not in beads

Ryan has not ruled. Filing them as beads would present unsettled design questions as
approved work, and both change release behaviour: one can block a legitimate release,
the other can block a tag push outright.

## RULED 2026-10-01, found already built on 2026-10-16

Both questions were ruled on 2026-10-01 in the release review. This seed was never updated.

1. CI lookup: only a successful run of ci.yml from a push to main, at exactly the tagged SHA,
   counts. Built here in 703484f (release.yml queries `event=push&branch=main` and filters on
   path and head_sha). sequel-duckdb built the same rule in cf71a2d. gator-107 records the ruling.
2. v\* tags: an active tag ruleset on refs/tags/v\* with deletion and update rules and NO bypass
   actors. Tag creation stays unrestricted. `gator-doctor --remote` checks this
   (tag_ruleset_finding) and reports no finding for this repo on 2026-10-16, so the ruleset is
   in place on origin.
