# Release setup — RubyGems Trusted Publishing

How `sequel-hexspace` publishes to rubygems.org. **The setup is done.** This is a record of
what is configured and why; nothing here is a step to perform. The one browser-only part,
registering the trusted publisher, was completed on 2026-10-01, and 2.0.0 shipped through it
the same day.

## How publishing is configured (verified against the live APIs, 2026-10-02)

| Piece                           | State                                                                                                                     |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `.github/workflows/release.yml` | Present on GitHub's `main`, workflow state `active`. Has run once — tag `v2.0.0`, 2026-10-01, both jobs successful        |
| RubyGems trusted publisher      | Registered: owner `outcomesinsights`, repo `sequel-hexspace`, workflow `release.yml`, environment **not set** (see below) |
| GitHub `release` environment    | Exists, with one protection rule: deployments restricted to tags matching `v*`. No required reviewers and no wait timer   |
| Tag ruleset                     | "Release tags are immutable" — `refs/tags/v*`, blocks tag deletion and update, enforcement active, zero bypass actors     |
| Actions secrets                 | Zero, in all three scopes — repository, the `release` environment, and organization-visible. No `RUBYGEMS_API_KEY` exists |
| Published versions              | 1.0.0 (2024-04-03) and 2.0.0 (2026-10-01). Neither yanked. 1.0.1 was bumped in the gemspec (`ce8b23e`) but never tagged   |

2.0.0 was pushed by `rubygems/release-gem@v1` with a short-lived token minted through GitHub's
OIDC provider at run time, and the timing leaves no room for doubt that it came from that run:
the `push` job ran 19:28:22–19:29:26 on 2026-10-01, and rubygems.org records 2.0.0 as created at
19:28:45. With no Actions secret in any scope, then or now, the trusted publisher is the only
thing that could have authorised it. That is the observable proof the registration is real —
the API that would show it directly cannot be read from here (see the standing rules below).

## The registered values, and why each one

They are matched against the OIDC token GitHub presents at release time. Any mismatch means the
token exchange is refused and the publish fails.

| RubyGems field    | Registered value   | What it has to correspond to                                              |
| ----------------- | ------------------ | ------------------------------------------------------------------------- |
| RubyGem           | `sequel-hexspace`  | the gem being published                                                   |
| Repository owner  | `outcomesinsights` | the owner of the repo the workflow runs in                                |
| Repository name   | `sequel-hexspace`  | that repo's name                                                          |
| Workflow filename | `release.yml`      | `.github/workflows/release.yml` — the filename, not the `name:` inside it |
| Environment       | *not set*          | would be `environment: release` on the `push` job, if it were pinned      |

### Why the environment column is empty

It was left unset at registration, and pinning it is **deferred** — not overlooked, and not a
value that was set and then lost. On rubygems.org's side an empty environment is a wildcard, not
a requirement that the token carry no environment, so such a registration still accepts a token
from a job that *does* declare one. The 2.0.0 publish bears that out: GitHub records a
deployment to the `release` environment for ref `v2.0.0`, so the token it minted named that
environment, and the exchange was accepted against the empty column anyway.

`environment: release` on the `push` job therefore takes effect in full on the GitHub side
regardless: the deployment restriction, and anything else hung on that environment, apply
exactly as they would if the column were filled.

What pinning would add is a refusal, on RubyGems' side, of a token minted by some *other* job in
`release.yml` that did not route through the `release` environment. When this was first weighed
the environment carried no protection rules, so that bought nothing. It now restricts
deployments to `v*` tags, so pinning would close a narrow but real gap: a second job added to
this workflow could skip the environment, skip that restriction, and still push a gem. The
condition to revisit under is therefore concrete — if `release.yml` ever gains a job besides
`verify` and `push`, pin the column.

## Cutting a release

In order, because each step gates the next:

1. Bump `s.version` in `sequel-hexspace.gemspec` and land it on `main`.
2. Wait for `ci.yml` to pass on that exact commit. `release.yml`'s `verify` job requires a
   successful CI run for the tagged SHA and fails closed, so a tag pushed in the same breath as
   the commit will fail while CI is still running. That is intended; re-run the workflow once CI
   finishes.
3. `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag must name the version the gemspec reads —
   `verify` asserts it, so a mismatch fails the release instead of publishing the wrong number.
4. Confirm <https://rubygems.org/gems/sequel-hexspace> lists the new version.

Being a gem owner on rubygems.org is not required for any of this. Anyone with write access to
the repo can cut a release, which is the main thing Trusted Publishing bought.

### GitHub runs its own copy of the workflow, not yours

A tag push runs `release.yml` as it exists on GitHub, which is not necessarily the file in your
checkout — this repo is routinely well ahead of `origin/main` (20 unpushed commits on
2026-10-02, among them `f20fda8`, which is what added the tag/gemspec assertion to `verify`; the
2.0.0 release ran without it). Step 1 above repairs this incidentally, because landing the
version bump on `main` pushes everything else with it. The trap is only for a tag pushed from an
un-synced branch: do not read the workflow in your tree and assume that is what will run.

## Standing rules

- **Do not create an API key for publishing**, and do not add a `RUBYGEMS_API_KEY` secret to the
  repo. The entire point is that no long-lived credential exists to leak, rotate, or tie the gem
  to one person's machine. A consequence worth knowing: no gem API key is stored on titan, and
  the trusted-publisher API requires one (it answers 401 unauthenticated), so the registration
  can only be read in the web UI while signed in as a gem owner. Everything else in the table
  above is readable from the GitHub and RubyGems public APIs.
- **Do not yank or edit a published version.** 1.0.0 and 2.0.0 stay as they are.
- **Do not change gem ownership.** `aguynamedryan` is the sole owner of record. Whether to add a
  second owner, use a role account, or wait for RubyGems Organizations is an open decision —
  seed `sequel-hexspace-5fh`. Making `outcomesinsights` the owner of record needs an
  Organizations invite (email support@rubygems.org); nothing breaks without it.

## Open work that would change this page

- `sequel-hexspace-wer` may restructure `release.yml`'s jobs — a tighter CI-green lookup, and an
  install-and-load smoke test that would replace `rubygems/release-gem` with a hand-rolled build
  and push. It is **open**, so nothing above assumes its shape; if it lands, the workflow rows
  here need rechecking.
