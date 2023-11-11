Gem::Specification.new do |s|
  s.name = 'sequel-hexspace'
  s.version = '1.0.0'
  s.platform = Gem::Platform::RUBY
  s.extra_rdoc_files = ["LICENSE"]
  s.rdoc_options += ["--quiet", "--line-numbers", "--inline-source", '--title', 'sequel-hexspace: Sequel adapter for hexspace driver and Apache Spark database', '--main', 'README']
  s.license = "MIT"
  s.summary = "Sequel adapter for hexspace driver and Apache Spark database"
  #s.author = ""
  #s.email = ""
  #s.homepage = ""
  s.files = %w(LICENSE README) + Dir["lib/**/*.rb"]
  s.description = <<END
This is a hexspace adapter for Sequel, designed to be used with Spark (not
Hive). You can use the hexspace:// protocol in the Sequel connection URL
to use this adapter.
END
  s.add_dependency('sequel', '~> 5.0')
  s.add_dependency('hexspace')
  s.add_development_dependency('rake')
  s.add_development_dependency("minitest", '~> 5.7')
  s.add_development_dependency("minitest-hooks")
  s.add_development_dependency("minitest-global_expectations")
end
