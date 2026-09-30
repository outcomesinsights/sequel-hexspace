---
id: sequel-hexspace-5fh
title: 'Single-owner risk across the OI gems: co-owner, role account, or Trusted Publishing?'
status: captured
type: idea
created_at: 2026-09-30T14:00:06.298689+00:00
updated_at: 2026-09-30T14:00:06.298689+00:00
tags:
  - rubygems
  - ownership
  - release
---

## Context

Surfaced 2026-09-30 while finishing the sequel-hexspace ownership handoff from
Jeremy Evans. Both transfers were already complete (GitHub repo transferred,
rubygems owner is aguynamedryan alone); the remaining gap is not hexspace-specific.

Measured on rubygems.org that day, every OI-maintained gem has exactly ONE owner,
`aguynamedryan`:

| gem             | owners        |
| --------------- | ------------- |
| sequel-hexspace | aguynamedryan |
| sequel_impala   | aguynamedryan |
| sequelizer      | aguynamedryan |
| conceptql       | aguynamedryan |
| sequel-duckdb   | aguynamedryan |

No `outcomesinsights` rubygems account exists (profiles/outcomesinsights returns 404).
sequel-hexspace has no release workflow (.github/workflows holds ci.yml and
dependabot-auto-merge.yml only), so a release is a manual `gem push` with a
personal API key from a personal machine.

## Why org ownership is not simply available

RubyGems Organizations -- the feature that would let the company be the owner of
record -- is in LIMITED PRIVATE BETA per guides.rubygems.org/organizations/
(checked 2026-09-30); joining means emailing support@rubygems.org. Outside the
beta a gem is owned by user accounts only.

## The options

1. Co-owner: a colleague's personal rubygems account. `gem owner <gem> --add <handle-or-email>`; the invitee must confirm by email within 48h or it lapses.
   Roles are Owner (can manage owners and configure trusted publishing) or
   Maintainer (push and yank only). Fixes administration; needs a willing human.
2. Company role account on a shared/distribution address -- the pre-organizations
   pattern, becomes the de facto org identity and could fold into a real org later.
   Downside: a shared credential with MFA that nobody personally owns.
3. Trusted Publishing (generally available, not beta): register repo plus workflow
   name against the gem; releases become "push a git tag" with a short-lived OIDC
   token scoped to that one gem, so no long-lived API key exists. Does NOT create
   an owner, so it does not cover the "account unreachable" case on its own.

## Open questions

- Who is the human second owner, and at which role?
- Do we want the org identity enough to request the beta, or is Trusted
  Publishing sufficient for the next year?
- Decide once for all five gems, or per gem? A sweep across five repos is a
  multi-repo change and wants an adversarial review first.

## Recommendation on the table

Trusted Publishing for the release path plus one human co-owner for
administration. Not acted on -- the choice of person and of account shape is
Ryan's.
