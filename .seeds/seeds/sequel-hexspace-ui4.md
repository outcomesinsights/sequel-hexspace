---
id: sequel-hexspace-ui4
title: Declare required_ruby_version on sequel-hexspace, and does that force a 2.0.0?
status: resolved
type: question
created_at: 2026-10-01T17:06:47.556389+00:00
updated_at: 2026-10-01T17:13:49.323279+00:00
resolved_at: 2026-10-01T17:13:49.323269+00:00
resolution: "Ryan ruled 2026-10-01: adhere to SemVer, go 2.0.0, declare >= 3.3. Both questions answered -- declare it, and accept the major bump rather than treating an EOL-Ruby floor as outside SemVer's promise. Shipped in 462fca9 via bead sequel-hexspace-prq: gemspec declares required_ruby_version >= 3.3 and version 2.0.0, lock regenerated, CHANGELOG.md added and shipped. Side finding while writing the changelog: 1.0.1 was never published, so 2.0.0 is the first release to carry ~18 months of work including the thrift 0.24 fix."
tags:
  - ruby
  - semver
  - release
---

## The question

sequel-hexspace declares no `required_ruby_version`. Should it, and at what
cost? Raised by the jigsaw_builder session on 2026-10-01 while relaying Ryan's
ruling that the jigsaw repos drop Ruby 3.2 (EOL March 2026) with 4.0.x primary.
That session explicitly declined to choose, which is right -- this is a release
decision, not a config edit.

## What is true now

- The gemspec has no `required_ruby_version` at all, so `gem install` succeeds on
  any Ruby and a user on 3.2 gets an unexplained runtime failure rather than
  "this gem needs >= 3.3".
- CI tests 3.3, 3.4, 4.0 (as of commit dropping 3.2). `.rubocop.yml`
  TargetRubyVersion is now 3.3, matching the matrix floor.
- Published version is 1.0.0; the repo is at 1.0.1 unreleased.
- `gem build` already warns about the missing attribute: "make sure you specify
  the oldest ruby version constraint ... that you want your gem to support".

## Why it is not a free edit

Declaring `>= 3.3` is **breaking for users**: anyone resolving this gem on Ruby
3.2 stops being able to install the new version. Under ordinary SemVer on a 1.x
gem that argues for 2.0.0, not 1.0.2 or 1.1.0.

Counter-argument worth weighing: 3.2 is EOL, nobody should be on it, and the gem
is at 740 downloads total with the dependency chain inside our own stack
(conceptql, t_shank, the Rails app). The practical blast radius may be zero. The
Ruby ecosystem convention is split -- plenty of gems raise the floor in a minor
release and treat EOL Ruby as outside SemVer's promise.

A third option: declare `>= 3.3` AND cut it as 2.0.0, getting the honest signal
without arguing about whether EOL counts. Cheapest if nothing external depends
on the 1.x line.

## Coupling to watch

rubocop's `Gemspec/RequiredRubyVersion` cop requires the gemspec floor and
`.rubocop.yml`'s TargetRubyVersion to agree. They are consistent today only
because neither is declared-and-mismatched; declaring `>= 3.3` while
TargetRubyVersion is 3.3 keeps the cop happy. Moving one without the other reds
the build.

## Needs a ruling from Ryan

1. Declare `required_ruby_version >= 3.3` at all, or leave it absent?
2. If declared: 2.0.0, or treat an EOL-Ruby floor as outside SemVer and ship it
   as 1.1.0?

Not acted on. Deliberately not bundled into the Ruby 3.2 commit
(sequel-hexspace-265), which was scoped to CI.
