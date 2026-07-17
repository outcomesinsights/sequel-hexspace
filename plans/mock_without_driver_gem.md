# Hexspace Mock-Without-Driver Refactor Plan

## Goal

Make Hexspace follow the same adapter split as Sequel PostgreSQL:

- shared SQL/mock behavior must be loadable without the `hexspace` gem
- real `hexspace://...` connections must still require the `hexspace` gem
- mock SQL generation should be possible for the Hexspace adapter identity itself, not only through `mock://spark`

This plan is written for a less capable agent and includes code-shape recommendations.

## Why This Refactor Exists

Today [`lib/sequel/adapters/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/hexspace.rb) begins with:

```ruby
require 'hexspace'
require_relative 'shared/spark'
```

That means the Hexspace adapter cannot be loaded at all unless the `hexspace` gem is installed, even if we only want SQL generation or mock behavior.

At the same time, the shared Spark layer is already separated enough that:

```ruby
Sequel.connect('mock://spark')
```

works without loading `hexspace`.

So the missing piece is not a full rewrite. The missing piece is giving Hexspace its own shared adapter entrypoint that can be used in mock mode without pulling in the driver.

## Current Files Involved

- [`lib/sequel/adapters/shared/spark.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/shared/spark.rb)
- [`lib/sequel/adapters/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/hexspace.rb)
- [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/sql_test.rb)
- [`test/database_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/database_test.rb)
- [`test/spec_helper.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/spec_helper.rb)

## Design Choice

There are two valid designs. Use Design A unless a specific obstacle appears.

### Design A: Add a shared Hexspace wrapper around Spark

Add a new shared adapter file that aliases Hexspace mock/shared behavior onto the Spark SQL modules.

This gives:

- `Sequel.mock(host: :hexspace)` or `Sequel.connect('mock://hexspace')`
- same SQL generation as Spark/Hexspace today
- no `require 'hexspace'` for mock mode

### Design B: Rename Spark shared modules to generic names

This would be a larger cleanup:

- rename `Sequel::Spark` shared layer to a more generic shared engine layer
- let both `spark` and `hexspace` map to it

Do **not** do this first. It is broader than needed.

Use Design A for the initial implementation.

## Required Outcome

After the refactor, these should be true:

1. `Sequel.connect('mock://hexspace')` works without the `hexspace` gem.
2. `Sequel.mock(host: :hexspace)` works without the `hexspace` gem.
3. Generated SQL for mock Hexspace matches the shared Spark/Hexspace SQL behavior already covered by existing tests.
4. Real `hexspace://...` connections still require the `hexspace` gem and still work when installed.

## Implementation Steps

### Step 1: Add a shared Hexspace entrypoint

Create a new file:

- [`lib/sequel/adapters/shared/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/shared/hexspace.rb)

Recommended initial implementation:

```ruby
# frozen-string-literal: true

require_relative 'spark'

module Sequel
  module Hexspace
    DatabaseMethods = Sequel::Spark::DatabaseMethods
    DatasetMethods = Sequel::Spark::DatasetMethods

    module MockAdapterDatabaseMethods
      # Add here only if Hexspace needs mock-specific overrides later.
    end

    def self.mock_adapter_setup(db)
      # Reuse Spark behavior first.
      if Sequel::Spark.respond_to?(:mock_adapter_setup)
        Sequel::Spark.mock_adapter_setup(db)
      end

      db.extend(MockAdapterDatabaseMethods)
    end
  end

  Sequel::Database.set_shared_adapter_scheme(:hexspace, Sequel::Hexspace)
end
```

If constant aliasing causes warnings or lookup issues, replace it with wrapper modules:

```ruby
module Sequel
  module Hexspace
    module DatabaseMethods
      include Sequel::Spark::DatabaseMethods
    end

    module DatasetMethods
      include Sequel::Spark::DatasetMethods
    end
  end
end
```

The wrapper-module version is safer if there is any doubt.

### Step 2: Stop requiring the real driver at top-level shared load points

Edit [`lib/sequel/adapters/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/hexspace.rb).

Change the top to:

```ruby
require_relative 'shared/hexspace'
require 'hexspace'
```

or, if load order requires the namespace first:

```ruby
require_relative 'shared/hexspace'
require 'hexspace'
```

The important point is:

- shared Hexspace behavior must load without the gem
- real connection setup remains in the real adapter file and still requires the gem

### Step 3: Keep real connection code in the real adapter file

Do not move `connect` logic into the shared layer.

This is real-driver-only code and should stay in [`lib/sequel/adapters/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/hexspace.rb):

```ruby
ALLOWED_CLIENT_KEYWORDS = ::Hexspace::Client.instance_method(:initialize).parameters.map(&:last).freeze

def connect(server)
  opts = server_opts(server)
  opts[:username] = opts[:user]
  opts.select! { |k, v| v.to_s != '' && ALLOWED_CLIENT_KEYWORDS.include?(k) }
  ::Hexspace::Client.new(**opts)
end
```

Likewise, anything that directly references `::Hexspace` classes must remain out of the shared file.

### Step 4: Support mock Hexspace identity in tests

Right now [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/sql_test.rb) uses:

```ruby
@db = Sequel.connect('mock://spark')
```

Add a second path that exercises `hexspace` identity directly.

The least invasive approach is:

- keep all existing `mock://spark` tests
- add new tests for `mock://hexspace`
- assert SQL is identical for representative statements

Example new test:

```ruby
it "supports mock://hexspace with the shared SQL behavior" do
  db = Sequel.connect('mock://hexspace')
  db.sqls
  db.create_table(:items, using: :parquet, location: '/tmp/items.parquet') {}
  db.sqls.must_equal ["CREATE TABLE `items` USING parquet LOCATION '/tmp/items.parquet'"]
end
```

Also add:

```ruby
it "supports Sequel.mock(host: :hexspace)" do
  db = Sequel.mock(host: :hexspace)
  db.dataset.send(:supports_cte?).must_equal true
end
```

### Step 5: Add an explicit no-driver mock test

Add a new file:

- [`test/mock_without_driver_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/mock_without_driver_test.rb)

Use a subprocess that blocks `require 'hexspace'`.

Recommended shape:

```ruby
require_relative "spec_helper"
require "open3"
require "rbconfig"

describe "mock hexspace without driver" do
  let(:ruby) { RbConfig.ruby }
  let(:repo_root) { File.expand_path("..", __dir__) }

  def run_ruby(code)
    Open3.capture3(
      { "RUBYOPT" => nil.to_s, "BUNDLE_GEMFILE" => nil.to_s },
      ruby,
      "-Ilib",
      "-e",
      code,
      chdir: repo_root,
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
```

The exact harness can vary. The required behavior cannot.

### Step 6: Separate mock tests from integration tests more clearly

Right now [`test/spec_helper.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/spec_helper.rb) eagerly creates:

```ruby
DB = Sequel.connect(ENV['SEQUEL_INTEGRATION_URL'] || 'hexspace:///sequel_hexspace_test')
```

That is fine for integration tests, but it is not good for new no-driver mock tests.

Recommended change:

- leave `spec_helper.rb` alone if possible for now
- in the new mock-without-driver test, do **not** require `spec_helper.rb`
- instead require only `sequel` and the new shared adapter file inside the subprocess

If the test suite becomes awkward, split bootstrap files:

- `test/support/integration_helper.rb`
- `test/support/mock_helper.rb`

Do this only if needed.

### Step 7: Optional but recommended namespace file

Consider adding:

- [`lib/sequel/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/hexspace.rb)

This can be a lightweight namespace file similar to `lib/sequel/duckdb.rb`.

Suggested contents:

```ruby
# frozen-string-literal: true

module Sequel
  module Hexspace
  end
end
```

This is optional, but useful if load order gets messy.

## Tests To Add Or Adjust

### New tests

1. `test/mock_without_driver_test.rb`
   - blocks `require 'hexspace'`
   - verifies `mock://hexspace` still works

2. Add new examples to [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/sql_test.rb)
   - `Sequel.connect('mock://hexspace')`
   - `Sequel.mock(host: :hexspace)`

### Existing tests that should remain integration tests

- [`test/database_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/database_test.rb)
- [`test/dataset_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/dataset_test.rb)
- [`test/prepared_statement_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/prepared_statement_test.rb)
- [`test/schema_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/schema_test.rb)
- [`test/type_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/type_test.rb)

### Existing tests that already represent the shared SQL behavior

- [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/sql_test.rb)
- [`test/date_arithmetic_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/date_arithmetic_test.rb)
- [`test/timezone_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/timezone_test.rb)

Those should be expanded to confirm the same SQL behavior under Hexspace mock identity.

## Suggested Minimal Patch Shape

### Files likely added

- [`lib/sequel/adapters/shared/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/shared/hexspace.rb)
- [`test/mock_without_driver_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/mock_without_driver_test.rb)

### Files likely edited

- [`lib/sequel/adapters/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/adapters/hexspace.rb)
- [`test/sql_test.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/sql_test.rb)

### Files only if needed

- [`lib/sequel/hexspace.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/lib/sequel/hexspace.rb)
- [`test/spec_helper.rb`](/home/ryan/projects/outins/jigsaw/main/gems/sequel-hexspace/test/spec_helper.rb)

## Verification Plan

Run after implementation:

1. Shared/mock behavior

```bash
bundle exec ruby -Itest test/sql_test.rb
bundle exec ruby -Itest test/date_arithmetic_test.rb
bundle exec ruby -Itest test/timezone_test.rb
bundle exec ruby -Itest test/mock_without_driver_test.rb
```

2. Real connection behavior

```bash
bundle exec ruby -Itest test/database_test.rb
bundle exec ruby -Itest test/dataset_test.rb
bundle exec ruby -Itest test/schema_test.rb
```

3. Full suite

```bash
bundle exec rake test
```

## Definition Of Done

- `hexspace` is no longer required just to load Hexspace shared SQL/mock behavior.
- `mock://hexspace` and/or `Sequel.mock(host: :hexspace)` work without the `hexspace` gem.
- Existing shared SQL behavior remains unchanged.
- Real `hexspace://...` connections still require and use the actual `hexspace` gem.
