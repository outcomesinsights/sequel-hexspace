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
#
# To exclude them locally, use the env var — `BUNDLE_WITHOUT=lint bundle install`
# — not `bundle install --without lint`. The flag is persisted into .bundle/config
# by bundler < 4, so every later command in this tree silently inherits it and
# `bundle exec rubocop` starts failing with "rubocop not found". (Bundler 4.0
# removed the flag outright.)
group :lint do
  gem 'rubocop', '~> 1.0'
  gem 'rubocop-minitest', '~> 0.25'
end
