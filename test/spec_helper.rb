require 'logger'
require 'sequel'

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../lib/"))

require_relative 'guards_helper'
require_relative 'async_spec_helper'

IDENTIFIER_MANGLING = false

DB = Sequel.connect(ENV['SEQUEL_INTEGRATION_URL'] || 'hexspace:///')
