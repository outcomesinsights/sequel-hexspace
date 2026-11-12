require "simplecov"
SimpleCov.start do
  skip "/test/"
  enable_coverage :branch
end

require "logger"
require "sequel"

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../lib/"))

ENV["MT_NO_PLUGINS"] = "1" # Work around stupid autoloading of plugins
require "minitest/global_expectations/autorun"
require "minitest/hooks/default"

DB = Sequel.connect(ENV["SEQUEL_INTEGRATION_URL"] || "hexspace:///sequel_hexspace_test")

# Sequel 5.110 deprecated Sequel.datetime_class= (it goes in Sequel 6), but the
# adapter must still round-trip DateTime while Sequel supports it. This is Sequel's
# own spec helper (spec/deprecation_helper.rb): it switches to DateTime for the
# block only, silences that one deprecation, and always restores Time.
class Minitest::HooksSpec
  def with_datetime_class_datetime
    output = Sequel::Deprecation.output
    Sequel::Deprecation.output = nil
    Sequel.datetime_class = DateTime
    yield
  ensure
    Sequel.datetime_class = Time
    Sequel::Deprecation.output = output
  end
end
