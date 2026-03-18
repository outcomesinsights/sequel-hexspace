require_relative "spec_helper"
require "open3"
require "rbconfig"

describe "mock hexspace without driver" do
  let(:ruby) { RbConfig.ruby }
  let(:repo_root) { File.expand_path("..", __dir__) }

  def run_ruby(code)
    Open3.capture3(
      {"RUBYOPT"=>nil.to_s, "BUNDLE_GEMFILE"=>nil.to_s},
      ruby,
      "-Ilib",
      "-e",
      code,
      :chdir=>repo_root,
    )
  end

  it "loads shared hexspace support without requiring the hexspace gem" do
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
      abort "wrong db type" unless db.database_type == :hexspace
      puts db[:items].with(:x, db[:items]).sql
    RUBY

    stdout, stderr, status = run_ruby(code)
    status.success?.must_equal true, "stdout=#{stdout}\nstderr=#{stderr}"
    stdout.must_match(/WITH `x` AS/)
  end
end
