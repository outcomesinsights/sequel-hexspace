require_relative 'spec_helper'
require 'open3'
require 'rbconfig'

describe 'mock hexspace without driver' do
  let(:ruby){ RbConfig.ruby }
  let(:repo_root){ File.expand_path('..', __dir__) }

  def run_ruby(code)
    gem_path = Gem.path.join(File::PATH_SEPARATOR)
    Bundler.with_unbundled_env do
      Open3.capture3(
        { 'GEM_PATH' => gem_path },
        ruby,
        '-Ilib',
        '-e',
        code,
        chdir: repo_root,
      )
    end
  end

  it 'loads shared hexspace support without requiring the hexspace gem' do
    code = <<~RUBY
      module Kernel
        alias __orig_require__ require

        def require(path)
          raise LoadError, "blocked hexspace" if path == "hexspace"
          __orig_require__(path)
        end
      end

      require "sequel"
      require_relative "lib/sequel/adapters/shared/hexspace"

      db = Sequel.connect("mock://hexspace")
      abort "wrong db type" unless db.database_type == :spark
      puts db[:items].with(:x, db[:items]).sql
    RUBY

    stdout, stderr, status = run_ruby(code)

    _(status.success?).must_equal true, "stdout=#{stdout}\nstderr=#{stderr}"
    _(stdout).must_match(/WITH `x` AS/)
  end
end
