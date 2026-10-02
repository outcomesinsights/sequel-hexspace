# Workflow `permissions:` blocks — the rule, and the gap no gate closes

**If you are adding or narrowing a `permissions:` block in `.github/workflows/`, read this first.
No linter in this repo will catch the mistake this page is about.** You have to reason it out
step by step, and this page tells you how.

## The rule

An explicit `permissions:` block sets every scope it does **not** name to `none`. It is a
replacement, not an addition. So the moment you write one, you have silently revoked everything
you did not list — including `contents: read`, which the default grant gives you for free and
which `actions/checkout` cannot work without.

Therefore, every time you add a `permissions:` block to a job, or add a step to a job that already
has one:

1. Enumerate what every step in that job needs, including the implicit needs of each action.
2. Name all of them. `contents: read` is not implied by anything — if the job checks out the repo,
   it must be spelled out.
3. Re-check the job's *other* steps. Narrowing for one step revokes for all of them.

## The near-miss this is written from

`release.yml`'s `verify` job came one commit away from this, and the history is worth getting right
because it has been retold inaccurately.

`2ac5584` (2026-09-30) created the job with `permissions: actions: read` and no checkout step. That
was *correct*: the job only called `gh api` to look for a green CI run, and `actions: read` is
exactly what that needs.

`f20fda8` (2026-10-01) then added a checkout step, so the gemspec would be on disk for a new
tag-matches-version assertion. The shape that would have broken every release is this:

```yaml
permissions:
  actions: read          # correct for the `gh api` call, and all that was there before
steps:
  - uses: actions/checkout@v7   # needs contents: read -- which the block above revokes
```

**That was never committed.** `f20fda8` added `contents: read`, the checkout step, and a comment
explaining the hazard, all in the same diff. The release of 2.0.0 went through the job successfully
the same day.

So this page does not document an outage. It documents a defect class that **no gate in this repo
can see**, which was avoided the one time it came up by someone reasoning it out at the point of
edit and leaving a comment behind. That is the entire mechanism protecting it, which is why this
page exists and why those comments must not be tidied away.

Treat any claim that this defect "shipped on 2026-10-01" as false — including one that was briefly
written into the `hygiene` recipe's own comment. No commit in this repo has ever had a job with an
under-scoped explicit block beside a checkout.

## Why there is no gate (measured, not assumed)

Two linters were probed against the exact defect shape in a throwaway git repo. Both are silent on
it, for the same structural reason: **they model permissions as a security surface — too *broad* is
a finding — and neither has any model of which permissions a given action requires.** "Too narrow
to function" is not a question either tool asks.

Shapes, each a job with an `actions/checkout` step, differing only in the `permissions:` block:

| Shape                                                | actionlint 1.7.12  | zizmor 1.30.1 (default)           | zizmor (pedantic/auditor)  |
| ---------------------------------------------------- | ------------------ | --------------------------------- | -------------------------- |
| **A** `permissions: actions: read` — *the defect*    | clean, exit 0      | silent on permissions             | `excessive-permissions` ×1 |
| **B** `permissions: {}`                              | clean, exit 0      | silent on permissions             | `excessive-permissions` ×1 |
| **C** no block at all — *must stay clean*            | clean, exit 0      | **fires** `excessive-permissions` | `excessive-permissions` ×2 |
| **D** `actions: read` + `contents: read` — *the fix* | clean, exit 0      | silent on permissions             | `excessive-permissions` ×1 |
| **Y** `actions: readonly` — bad *value*, a control   | **caught, exit 1** | —                                 | —                          |
| **Z** `content: read` — bad *name*, a control        | **caught, exit 1** | —                                 | —                          |

Read rows **A** and **D** together: zizmor's output on the defect is *identical* to its output on
the fix, finding-for-finding, at every persona. It cannot distinguish a broken permissions block
from a correct one, so no exit code derived from it is a gate on this class. And the one shape it
does flag for permissions at its default persona is **C** — a job with no block, which is correct
code, because the default grant includes `contents: read`. The tool's polarity is the opposite of
the defect's.

Rows **Y** and **Z** are positive controls, and they are the reason rows A–D mean anything: they
prove actionlint's `permissions` check is live and does fail on this file shape, so its exit 0 on
the defect is a real negative rather than a tool that was never running. What it validates is scope
*names* and *values* only — it stays in the hygiene gate for that and much else. It does not and
will not catch an under-scoped block.

No configuration changes this. zizmor's config can suppress findings; it cannot add a rule, and
`--persona` only lowers the reporting threshold on rules that already exist.

Reproduce any of it: `./docs/permissions-gate-probe.sh` (builds the shapes in a temp git
repo — both tools need one, a bare directory makes actionlint error "no project was found" — runs
both tools over each, and prints the cost against this repo's real workflows).

## Why the gap is accepted rather than papered over

**This class fails closed.** An under-scoped permission breaks the job that holds it. For
`release.yml` that means a release that *does not happen*; it cannot publish a bad or unsigned gem,
because the failure lands on the checkout, long before `rubygems/release-gem` runs. The cost is a
blocked release and a confusing error, paid by whoever cut the tag, recoverable by fixing the block
and re-running. Nothing escapes into a published artifact and nothing needs a yank.

A hand-rolled rule — "warn if a job has a `permissions:` block without `contents: read` and a
`uses: actions/checkout`" — was considered and **deliberately rejected**. It would be right about
one action and silent about every other, so the gate would *look* like it covered the class while
covering a single instance of it. That is the convenient-proxy detector failure, and it is worse
than an honest gap: an honest gap is written down here, where you are reading it, whereas a
proxy gate quietly teaches everyone that green means safe.

Adopting zizmor anyway, for its other audits, is a separate decision with its own cost — against
this repo's three workflows plus `dependabot.yml` it reports 21 findings at the default persona (13
of them `high`), led by `unpinned-uses` on every `uses:` in the repo, since nothing here is pinned
to a SHA. None of those 21 is this defect class. That trade is not made here, and this page is not
an argument against it.

## The jobs that hold write scopes

`dependabot-auto-merge.yml` holds `contents: write` and `pull-requests: write`, and `release.yml`'s
`push` job holds `contents: write` and `id-token: write`. Narrowing those is where this defect is
most expensive — `id-token: write` is what mints the publishing token, and a block that drops it
fails the token exchange with a message about OIDC, not about permissions. Change them one job at a
time, and re-read the rule above before each one.
