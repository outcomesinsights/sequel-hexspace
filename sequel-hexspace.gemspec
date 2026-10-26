Gem::Specification.new do |s|
  s.name = 'sequel-hexspace'
  s.version = '2.0.1'
  s.platform = Gem::Platform::RUBY
  # Must stay in step with .rubocop.yml's TargetRubyVersion and the floor of
  # ci.yml's matrix: rubocop's Gemspec/RequiredRubyVersion cop fails the build if
  # this and TargetRubyVersion disagree. Raised to 3.3 on 2026-10-01 when Ruby
  # 3.2 went out of support, which is what makes this release 2.0.0 rather than
  # 1.0.2 -- a floor is breaking for anyone resolving the gem on an older Ruby.
  s.required_ruby_version = '>= 3.3'
  s.extra_rdoc_files = [ "LICENSE" ]
  s.rdoc_options += [ "--quiet", "--line-numbers", "--inline-source", '--title', 'sequel-hexspace: Sequel adapter for hexspace driver and Apache Spark database', '--main', 'README' ]
  s.license = "MIT"
  s.summary = "Sequel adapter for hexspace driver and Apache Spark database"
  s.authors = [ "Jeremy Evans", "Ryan Duryea" ]
  s.email = "aguynamedryan@gmail.com"
  s.homepage = "https://github.com/outcomesinsights/sequel-hexspace"
  # Makes rubygems.org refuse gem-level privileged actions (push, yank, owner
  # changes) from an account without MFA enabled. Applies to versions published
  # after it ships; it cannot be applied retroactively to one already out.
  s.metadata = { 'rubygems_mfa_required' => 'true' }
  s.files = %w[CHANGELOG.md LICENSE README] + Dir["lib/**/*.rb"]
  s.description = <<END
This is a hexspace adapter for Sequel, designed to be used with Spark (not
Hive). You can use the hexspace:// protocol in the Sequel connection URL
to use this adapter.
END
  s.add_dependency('sequel', '~> 5.0')
  # The floor is 0.4.0 (2026-10-15). It regenerated the TCLIService client
  # against thrift >= 0.25, so the Thrift::Client shim that hexspace 0.3 needed
  # under thrift 0.24+ is gone. This repo's gems have no outside users that need
  # an older hexspace (Ryan, 2026-10-26).
  #
  # Ceiling is the next MINOR, not the next major, because hexspace is a 0.x gem
  # -- semver permits 0.5.0 to break the API outright, and `~> 0.4` (rubygems'
  # own suggestion here) would admit every 0.x release sight unseen. This adapter
  # also leans on more of hexspace than its documented surface: it introspects
  # Hexspace::Client#initialize's keyword list to filter connection options, and
  # it reads `columns`/`rows`/`column_types` off the result object. Either can
  # move in a 0.x minor without hexspace calling it breaking. Raising the ceiling
  # is a deliberate step: diff the new minor's source against that surface, then
  # run the integration suite against it.
  s.add_dependency('hexspace', '>= 0.4', '< 0.5')
  # hexspace 0.4 requires thrift >= 0.25. The floor states that here so the
  # gemspec shows what actually resolves. The ceiling is the next minor, for the
  # same 0.x reason as hexspace's.
  s.add_dependency('thrift', '>= 0.25', '< 0.26')
  # The bounds on minitest, minitest-hooks and minitest-global_expectations below are
  # development-only: nothing that resolves this gem as a dependency installs
  # them, so each is a checkpoint on this repo's own dev/CI environment rather
  # than a promise to consumers. Each sits at the major CI actually exercises, so
  # a new major cannot arrive unannounced.
  s.add_development_dependency("minitest", '~> 6.0')
  # This one is NOT just hygiene -- the floor is a real requirement.
  # minitest-hooks declares only `minitest > 5.3`, loose enough for bundler to
  # pair an old minitest-hooks with the minitest ~> 6.0 required above. Per its
  # CHANGELOG, 1.5.3 is the first release that works on minitest 6 at all and
  # 1.5.4 the first that reports assertion counts correctly on it, so 1.5.4 is
  # the floor. The blast radius is the whole suite, not a corner of it:
  # test/spec_helper.rb loads minitest/hooks/default, which does
  # `register_spec_type(//, Minitest::HooksSpec)` -- so every `describe` here is
  # a HooksSpec, and the suite uses minitest-hooks' `before(:all)` throughout.
  s.add_development_dependency("minitest-hooks", '~> 1.5', '>= 1.5.4')
  # Single-purpose (global expectation methods, loaded via
  # minitest/global_expectations/autorun). 1.0.0, 1.0.1 and 1.0.2 are its only
  # releases ever, so a major ceiling constrains nothing in practice.
  s.add_development_dependency("minitest-global_expectations", '~> 1.0')
  # rubocop/rubocop-minitest are NOT development dependencies here: they pull in
  # gems (parallel) whose required_ruby_version is >= 3.3, which is exactly the
  # floor of the CI matrix since Ruby 3.2 was dropped on 2026-10-01 -- no
  # headroom, so a bump there would break `bundle install` on the oldest
  # supported Ruby. They live in the Gemfile's :lint group instead, which the
  # test jobs exclude.
  s.add_development_dependency('simplecov', '~> 1.0')
end
