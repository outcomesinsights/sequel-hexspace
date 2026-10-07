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
