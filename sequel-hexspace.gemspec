Gem::Specification.new do |s|
  s.name = 'sequel-hexspace'
  s.version = '1.0.1'
  s.platform = Gem::Platform::RUBY
  s.extra_rdoc_files = ["LICENSE"]
  s.rdoc_options += ["--quiet", "--line-numbers", "--inline-source", '--title', 'sequel-hexspace: Sequel adapter for hexspace driver and Apache Spark database', '--main', 'README']
  s.license = "MIT"
  s.summary = "Sequel adapter for hexspace driver and Apache Spark database"
  s.authors = ["Jeremy Evans", "Ryan Duryea"]
  s.email = "aguynamedryan@gmail.com"
  s.homepage = "https://github.com/outcomesinsights/sequel-hexspace"
  s.files = %w(LICENSE README) + Dir["lib/**/*.rb"]
  s.description = <<END
This is a hexspace adapter for Sequel, designed to be used with Spark (not
Hive). You can use the hexspace:// protocol in the Sequel connection URL
to use this adapter.
END
  s.add_dependency('sequel', '~> 5.0')
  s.add_dependency('hexspace', '>= 0.2.1')
  # thrift 0.24 removed Thrift::Client#handle_exception and #reply_seqid, which
  # hexspace's generated client still calls -> NoMethodError on every Spark
  # connection (Sequel::DatabaseConnectionError). Both are restored by the shim
  # in lib/sequel/adapters/hexspace.rb, covered by
  # test/thrift_client_compat_test.rb, so 0.24 is supported rather than excluded.
  s.add_dependency('thrift', '>= 0.18', '< 0.25')
  s.add_development_dependency('rake')
  s.add_development_dependency("minitest", '~> 6.0')
  s.add_development_dependency("minitest-hooks")
  s.add_development_dependency("minitest-global_expectations")
  # rubocop/rubocop-minitest are NOT development dependencies here: they pull in
  # gems (parallel) whose required_ruby_version is >= 3.3, which is exactly the
  # floor of the CI matrix since Ruby 3.2 was dropped on 2026-10-01 -- no
  # headroom, so a bump there would break `bundle install` on the oldest
  # supported Ruby. They live in the Gemfile's :lint group instead, which the
  # test jobs exclude.
  s.add_development_dependency('simplecov', '~> 1.0')
end
