require 'logger'
require 'sequel'

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../lib/"))

ENV['MT_NO_PLUGINS'] = '1' # Work around stupid autoloading of plugins
require 'minitest/global_expectations/autorun'
require 'minitest/hooks/default'

class Minitest::HooksSpec
  def log
    begin
      DB.loggers << Logger.new(STDOUT)
      yield
    ensure
     DB.loggers.pop
    end
  end
end

DB = Sequel.connect(ENV['SEQUEL_INTEGRATION_URL'] || 'hexspace:///')
