---
id: sequel-hexspace-47x
title: Offer ankane/hexspace an upstream fix for thrift 0.24 support
status: captured
type: exploration
created_at: 2026-09-05T18:58:14.330453+00:00
updated_at: 2026-09-05T18:58:14.330453+00:00
tags:
  - cutting
---

**What we were discussing:** Whether to contribute our thrift 0.24 fix back to ankane/hexspace, and in what form. We carry a local shim (commit cd5a949, `lib/sequel/adapters/hexspace.rb`) that restores `Thrift::Client#handle_exception` and `#reply_seqid`, which thrift 0.24.0 removed and hexspace's generated client still calls 21 times each. Without it every Spark connection dies in `recv_OpenSession` with NoMethodError, surfacing as `Sequel::DatabaseConnectionError`.

**Why it came up:** The bug reached conceptql and t_shank `main` during a Dependabot sweep on 2026-09-04. Root cause was ours to begin with -- commit e3a7e93 had pinned `thrift < 0.24` deliberately, and Dependabot PR #30 ("bump the minor-and-patch group with 3 updates", 2026-07-21) widened it to `< 0.25` while leaving e3a7e93's warning comment standing above the line it now contradicted. Our shim unblocks us, but hexspace itself stays broken on thrift 0.24 for everyone. Ryan asked: "Should we submit an upstream PR?" then "is there a discussion or forum for hexspace we should hit up first? or should we just open an issue and offer to make a PR if arkane would like?"

**Established:**

- Upstream is not going to fix this on its own. `lib/hexspace/tcli_service.rb` is untouched since 2023-05-08; latest release 0.3.0 (Apr 2025) predates thrift 0.24; last commits (Dec 2025) are CI-only; zero issues or PRs mention thrift. Repo is not archived, Apache-2.0.
- **There is no forum, Discussions board, or wiki** -- all disabled. Issues are the only channel (1 open at time of writing). No CONTRIBUTING.md, no issue templates.
- The README explicitly invites contribution: "Everyone is encouraged to help improve this project. Report bugs / Fix bugs and submit pull requests."
- A PR is mechanically easy: the repo ships the IDL at `thrift/TCLIService.thrift` plus a `rake generate` task that runs the `thrift` compiler and rewrites `lib/hexspace/*`.
- **The catch that shapes the whole decision:** the generated Ruby's shape follows the *compiler* version. Regenerating with thrift 0.24 emits `validate_message_begin` calls, which requires thrift >= 0.24 at runtime. hexspace currently declares `thrift >= 0.18`, so the clean fix drops support for six thrift versions. That is a maintainer's call, not ours.
- Therefore we agreed the etiquette is **issue first, offering the PR** -- not dropping a breaking PR unannounced. Ryan's framing: "just open an issue and offer to make a PR if arkane would like?" (note: the handle is `ankane`, Andrew Kane).
- Two PR shapes to offer him: (1) regenerate + raise the thrift floor to `>= 0.24` -- correct, uses his own tooling, mildly breaking; (2) carry the same ~12-line compat shim we wrote inside hexspace -- keeps `>= 0.18` working across all versions, at the cost of a monkey patch on thrift internals.
- Our shim is verified: identical behavior under 0.23 and 0.24 on both the reply and exception paths, `unless method_defined?` so it declines to override on <= 0.23, deleting it reproduces the exact production error, and the full suite passes against a live Spark server (284 runs, 1155 assertions, 0 failures). `test/thrift_client_compat_test.rb` covers it.
- Nothing was posted. Opening the issue would go out under Ryan's GitHub account, so it needs his explicit go.

**Open:** Do we open the issue at all, and if so does the offer lead with shape (1) or (2)? Secondary: if ankane doesn't respond (plausible given the repo's cadence), do we just keep the shim indefinitely, or vendor/fork hexspace? The shim keeps thrift 0.23 semantics -- it does not add 0.24's stricter checks for invalid message type and wrong method name -- so staying on it forever means permanently declining those checks.
