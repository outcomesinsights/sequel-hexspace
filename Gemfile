# frozen_string_literal: true

source "https://rubygems.org"

gemspec

# thrift 0.22.0 requires base64 but doesn't declare it; removed from default gems in Ruby 3.4
gem 'base64'
gem 'overcommit', '~> 0.73'

# Linters live here, not in the gemspec's development dependencies: rubocop
# pulls in parallel, whose required_ruby_version floor is above the lowest Ruby
# in the CI matrix. CI's test jobs set BUNDLE_WITHOUT=lint, and bundler does not
# apply the ruby-version check to an excluded group, so Ruby 3.2 installs
# cleanly from this one lockfile. The lint job installs everything.
group :lint do
  gem 'rubocop', '~> 1.0'
  gem 'rubocop-minitest', '~> 0.25'
end
