# Release setup — RubyGems Trusted Publishing

How `sequel-hexspace` is configured to publish, and the one part of it that can only be
done in a browser on rubygems.org.

## Current state (2026-10-01)

| Piece                           | State                                                                   |
| ------------------------------- | ----------------------------------------------------------------------- |
| `.github/workflows/release.yml` | Committed locally, **not pushed** — GitHub has never seen it            |
| GitHub `release` environment    | Created, no protection rules                                            |
| RubyGems trusted publisher      | **Unknown — this is the task below**                                    |
| Published version               | 1.0.0 (April 2024). 1.0.1 was never released; 2.0.0 is prepared locally |

The stored API key on titan cannot read the trusted-publisher list (403, "This API key cannot
perform the specified action on this gem"), so whether one exists can only be seen in the web
UI while signed in.

## Task 1 — look at what is already there

1. Sign in to rubygems.org as `aguynamedryan`.
2. Go to <https://rubygems.org/gems/sequel-hexspace>.
3. In the **Links** section on the right, find **Trusted publishers**. That link only appears
   to gem owners; if it is missing, the sign-in did not take.
4. The page may ask for the account password on entry. That is expected — it is a
   re-confirmation with a 10-minute grace period, not a sign-in failure.
5. Report what is listed. Either there are no publishers, or there is one — and if there is,
   report its four values so they can be compared against the table below.

## Task 2 — create the publisher, if none exists

Click **Create** and fill in exactly these values:

| Field             | Value                                    |
| ----------------- | ---------------------------------------- |
| RubyGem           | `sequel-hexspace` (should be pre-filled) |
| Repository owner  | `outcomesinsights`                       |
| Repository name   | `sequel-hexspace`                        |
| Workflow filename | `release.yml`                            |
| Environment       | `release`                                |

Then click **Create Rubygem trusted publisher**.

Afterwards the gem's trusted-publishers page should list exactly one entry naming
`outcomesinsights/sequel-hexspace`, workflow `release.yml`, environment `release`.

### If one already exists but differs

Report the difference rather than editing it. A stale entry pointing at `jeremyevans` or at a
workflow filename we do not have would explain a refused publish, but which way to fix it —
change the registration or change the workflow — is a decision, not a correction.

## Why those exact five values

They are matched against the OIDC token GitHub presents at release time. Any mismatch means
the token exchange is refused and the publish fails. They correspond to:

| RubyGems field          | Where it comes from                                                       |
| ----------------------- | ------------------------------------------------------------------------- |
| Repository owner / name | The repo the workflow runs in, `outcomesinsights/sequel-hexspace`         |
| Workflow filename       | `.github/workflows/release.yml` — the filename, not the `name:` inside it |
| Environment             | `environment: release` on the `push` job in that workflow                 |

## Do not do these

- **Do not create an API key for publishing**, and do not add a `RUBYGEMS_API_KEY` secret to
  the repo. The entire point is that no long-lived credential exists in the release path.
- **Do not yank or edit any published version.** 1.0.0 stays as it is.
- **Do not change gem ownership.** Whether to add a second owner or wait for Organizations is
  an open decision (seed `sequel-hexspace-ui4`'s sibling, `sequel-hexspace-5fh`).

## Optional, same site, separate decisions

- **Organizations private beta.** Making `outcomesinsights` the owner of record instead of a
  personal account needs an invite: email support@rubygems.org. Nothing breaks without it.
- **API key scopes.** If CLI verification of trusted publishers is wanted in future, the
  stored key needs the scope that covers them. Worth weighing against giving a long-lived key
  broader reach than it has now.

## Not RubyGems tasks

So a browser session does not go looking for them: pushing the ten local commits, tagging
`v2.0.0`, and the GitHub `release` environment are all elsewhere. The environment is already
created.

## After the publisher is registered

In order, because each step gates the next:

1. Push the local commits, so `release.yml` exists on `main` and GitHub can run it.
2. Wait for CI to pass on the commit to be tagged. The workflow's `verify` job requires a
   successful `ci.yml` run for that exact SHA and fails closed.
3. `git tag v2.0.0 && git push origin v2.0.0`.
4. Confirm <https://rubygems.org/gems/sequel-hexspace> shows 2.0.0, and that the release ran
   without any API key.
