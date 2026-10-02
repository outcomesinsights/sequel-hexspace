require_relative "spec_helper"
require "stringio"

# Spark has no DELETE and no UPDATE, so the adapter emulates both by building
# `<table>__sequel_delete_emulate` holding the rows the operation should leave
# behind and then swapping those rows in for the original table's contents.
#
# The sequence is not atomic, so debris outlives a run that is killed part way
# through -- and until 2026-10-01 that debris poisoned every later run: one
# aborted suite left `a__sequel_delete_emulate` on the shared thriftserver and
# the next run reported 51 errors in tests that never touch table `a`, because
# Spark's CREATE TABLE is not idempotent.
#
# Since 2026-10-02 (sequel-hexspace-rho) the swap is an INSERT OVERWRITE rather
# than a drop-and-rename, so the original table object survives the operation
# and no window exists in which a reader sees table-or-view-not-found. The rows
# to keep are also never the only copy until the overwrite has been verified.
# What that does NOT buy is atomicity -- see _with_temp_table and the README.
#
# These tests plant debris, and stand in for the overwrite, rather than aborting
# a run, so they cover the same ground without needing a second process. The
# real-process-death case is not expressible here and is demonstrated out of
# band, the way sequel-hexspace-c5q's was.
#
# This group owns its fixtures outright instead of sharing the `items` fixture
# in dataset_test.rb: what is sitting on the server before an operation starts
# IS the thing under test, so a shared `create_table!` macro would be deciding
# the measurement.
describe "emulated delete/update temp table" do
  before do
    DB.create_table!(:items) { Integer :i }
    DB[:items].import([ :i ], [ [ 1 ], [ 2 ] ])
  end

  after do
    DB.drop_table?(:items, :items__sequel_delete_emulate,
                   :other, :other__sequel_delete_emulate)
  end

  # Capture the SQL the adapter issues, so a test can assert on statements
  # rather than only on the state they leave behind. The `log` helper in
  # spec_helper writes to STDOUT, which a test cannot read back.
  def capture_sql
    io = StringIO.new
    DB.loggers << Logger.new(io)
    begin
      yield
    ensure
      DB.loggers.pop
    end
    io.string
  end

  it "should tolerate a stranded temp table when deleting" do
    DB.create_table(:items__sequel_delete_emulate) { Integer :i }

    _(DB[:items].where(i: 1).delete).must_equal 1
    _(DB[:items].select_order_map(:i)).must_equal [ 2 ]
    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
  end

  it "should tolerate a stranded temp table when updating" do
    DB.create_table(:items__sequel_delete_emulate) { Integer :i }

    _(DB[:items].where(i: 1).update(i: 9)).must_equal 1
    _(DB[:items].select_order_map(:i)).must_equal [ 2, 9 ]
    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
  end

  # The drop that makes the above work must name only the one table this
  # operation is about to create. The server is shared, and anything reaching
  # for a pattern or a table listing could take out a table belonging to nobody
  # here -- so the planted neighbour is itself debris-shaped, which is the case
  # a pattern match gets wrong.
  it "should leave tables it does not own alone" do
    DB.create_table(:other) { Integer :i }
    DB.create_table(:other__sequel_delete_emulate) { Integer :i }

    _(DB[:items].where(i: 1).delete).must_equal 1

    _(DB.table_exists?(:other)).must_equal true
    _(DB.table_exists?(:other__sequel_delete_emulate)).must_equal true
  end

  # A raise or a ^C still unwinds, so the temp table must not survive one.
  it "should not strand the temp table when the operation raises before the swap" do
    _ { DB[:items].send(:_with_temp_table) { raise Sequel::Error, "simulated abort" } }
      .must_raise Sequel::Error

    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
    _(DB[:items].select_order_map(:i)).must_equal [ 1, 2 ]
  end

  # The structural half of sequel-hexspace-rho, and the test that fails if the
  # drop-and-rename sequence is ever restored. Asserting on the statements is
  # the only way to pin "the original is never absent" from inside the process
  # doing the operation: a single-process test cannot observe the window it is
  # itself occupying.
  #
  # `DROP TABLE IF EXISTS` is the temp table's own cleanup and is expected; a
  # bare `DROP TABLE `items`` can only come from dropping the original.
  it "should swap by overwriting the original rather than dropping it" do
    sql = capture_sql { _(DB[:items].where(i: 1).delete).must_equal 1 }

    _(sql).must_include "INSERT OVERWRITE TABLE `items`"
    _(sql).wont_include "DROP TABLE `items`"
    _(sql).wont_match(/RENAME TO/)
    _(DB[:items].select_order_map(:i)).must_equal [ 2 ]
  end

  # Deleting every row is the case where a drop-and-rename and an overwrite are
  # easiest to confuse, because the correct end state has no rows in it. The
  # table must still be there.
  it "should leave the table present and empty when every row is deleted" do
    _(DB[:items].delete).must_equal 2

    _(DB.table_exists?(:items)).must_equal true
    _(DB[:items].select_order_map(:i)).must_equal []
    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
  end

  # Replaces c5q's 'should keep the temp table when the original has already
  # been dropped', which is obsolete: nothing drops the original any more, so
  # its stub on #rename_table would never fire. The rule it pinned is kept
  # intact and is the one asserted here -- while the original is not known to be
  # intact, the temp table holds the only copy of the rows to keep and must
  # survive. What has changed is the state that counts as "not intact": an
  # unfinished overwrite rather than a missing table.
  it "should keep the temp table when the overwrite does not finish" do
    ds = DB[:items].where(i: 1).with_extend do
      private def _overwrite_from_temp_table(*)
        raise Sequel::Error, "simulated abort inside the overwrite"
      end
    end

    _ { ds.delete }.must_raise Sequel::Error

    # What rho bought: the original is still present and readable, where the
    # drop/rename sequence left it absent.
    _(DB.table_exists?(:items)).must_equal true
    _(DB[:items].select_order_map(:i)).must_equal [ 1, 2 ]
    _(DB[:items__sequel_delete_emulate].select_order_map(:i)).must_equal [ 2 ]
  end

  # The temp table must not be dropped on the strength of the overwrite
  # statement merely returning. Stands in for an overwrite that reported success
  # without landing -- a partial write, a truncating write -- which on a
  # non-ACID table a plain SELECT reports silently rather than loudly. The row
  # count against the temp table is what turns that back into an error.
  it "should keep the temp table when the overwrite cannot be verified" do
    ds = DB[:items].where(i: 1).with_extend do
      private def _overwrite_from_temp_table(*)
        # Deliberately does nothing.
      end
    end

    err = _ { ds.delete }.must_raise Sequel::Error
    _(err.message).must_include "items__sequel_delete_emulate"

    _(DB.table_exists?(:items)).must_equal true
    _(DB[:items].select_order_map(:i)).must_equal [ 1, 2 ]
    _(DB[:items__sequel_delete_emulate].select_order_map(:i)).must_equal [ 2 ]
  end
end

# The temp table's NAME, which is the whole of sequel-hexspace-5hh. Until
# 2026-10-02 it was built by stripping the backticks out of the literalized
# target, which collapsed a qualified name into one identifier containing a
# dot. These assert on generated SQL rather than on the Ruby object, because
# where the server puts the table is the thing under test, and they use the
# mock database so that the catalog-qualified case is covered whether or not
# the live server has a second catalog.
describe "emulated delete/update temp table name" do
  before do
    @db = Sequel.connect("mock://spark")
    @db.sqls
  end

  def tmp_sql(table_name)
    @db[table_name].send(:_temp_table_name, table_name).then { |t| @db.from(t).sql }
  end

  # The documented convention, and the case that has always worked. This must
  # stay byte-for-byte what it was: the README tells consumers to look for
  # exactly this name, and `<table>__sequel_delete_emulate` is the only reason
  # a recovery copy is findable at all.
  it "should append the suffix for an unqualified target" do
    _(tmp_sql(:items)).must_equal "SELECT * FROM `items__sequel_delete_emulate`"
    _(tmp_sql(Sequel.identifier(:items))).must_equal "SELECT * FROM `items__sequel_delete_emulate`"
  end

  # The defect. The old derivation produced the single identifier
  # `sch.items__sequel_delete_emulate` -- a table in the DEFAULT database whose
  # name contains a dot, which Spark rejects outright as a name.
  it "should keep a schema qualifier intact and suffix only the table" do
    _(tmp_sql(Sequel[:sch][:items]))
      .must_equal "SELECT * FROM `sch`.`items__sequel_delete_emulate`"
  end

  # Spark names can be catalog.schema.table, which Sequel nests as a
  # QualifiedIdentifier inside a QualifiedIdentifier. Suffixing the last
  # component rather than reassembling the name handles that for free.
  it "should keep a catalog and schema qualifier intact" do
    _(tmp_sql(Sequel[:cat][:sch][:items]))
      .must_equal "SELECT * FROM `cat`.`sch`.`items__sequel_delete_emulate`"
  end
end

# The same rules c5q and rho pinned for an unqualified target, against a
# schema-qualified one. Nothing here replaces one of their guards -- the
# unqualified derivation is unchanged, so all of them still pass; this group is
# additional, because a qualified target was a shape the suite never exercised
# and the operation raised on it at CREATE TABLE.
#
# This group owns a schema of its own rather than reusing schema_test.rb's
# `sequel_test1`, which asserts its own exact contents. The leading drop_schema
# is the same defensive statement schema_test.rb uses and for the same reason:
# CREATE SCHEMA is not idempotent, so a run killed before `after` otherwise
# poisons every later run. Both statements name this one schema; nothing
# enumerates the server.
describe "emulated delete/update against a schema-qualified table" do
  def schema_name = :sequel_test_delete_emulate
  def qualified = Sequel[schema_name][:items]
  def temp_table = Sequel[schema_name][:items__sequel_delete_emulate]

  # The single dotted identifier the old derivation produced, in the DEFAULT
  # database. Named so the tests below can assert it is never created.
  def dotted = "#{schema_name}.items__sequel_delete_emulate"

  before do
    DB.drop_schema(schema_name, if_exists: true, cascade: true)
    DB.create_schema(schema_name)
    DB.create_table(qualified) { Integer :i }
    DB[qualified].import([ :i ], [ [ 1 ], [ 2 ] ])
  end

  after do
    DB.drop_schema(schema_name, if_exists: true, cascade: true)
    DB.drop_table?(dotted)
  end

  it "should delete from a qualified table" do
    _(DB[qualified].where(i: 1).delete).must_equal 1

    _(DB[qualified].select_order_map(:i)).must_equal [ 2 ]
    _(DB.tables(schema: schema_name)).must_equal [ :items ]
    _(DB.table_exists?(dotted)).must_equal false
  end

  it "should update a qualified table" do
    _(DB[qualified].where(i: 1).update(i: 9)).must_equal 1

    _(DB[qualified].select_order_map(:i)).must_equal [ 2, 9 ]
    _(DB.tables(schema: schema_name)).must_equal [ :items ]
    _(DB.table_exists?(dotted)).must_equal false
  end

  # The acceptance criterion: the recovery copy must be somewhere a human can
  # find it. "Somewhere" is the target's own schema, under the name the README
  # names -- not a dotted name in `default`, where the old derivation sent it
  # and where Spark refused to create it at all.
  it "should leave the recovery copy in the target's own schema when the overwrite does not finish" do
    ds = DB[qualified].where(i: 1).with_extend do
      private def _overwrite_from_temp_table(*)
        raise Sequel::Error, "simulated abort inside the overwrite"
      end
    end

    _ { ds.delete }.must_raise Sequel::Error

    _(DB.table_exists?(temp_table)).must_equal true
    _(DB[temp_table].select_order_map(:i)).must_equal [ 2 ]
    _(DB.table_exists?(dotted)).must_equal false

    # rho's rule, unchanged by qualification: the original survives intact.
    _(DB[qualified].select_order_map(:i)).must_equal [ 1, 2 ]
  end

  # rho's verification error names the temp table so that a consumer who never
  # reads this source can still find the rows. For a qualified target that name
  # has to be the qualified one -- and it has to be literalized, because
  # interpolating a Sequel::SQL::QualifiedIdentifier yields
  # `#<Sequel::SQL::QualifiedIdentifier:0x...>`, which names nothing.
  it "should name the qualified temp table in the verification error" do
    ds = DB[qualified].where(i: 1).with_extend do
      private def _overwrite_from_temp_table(*)
        # Deliberately does nothing.
      end
    end

    err = _ { ds.delete }.must_raise Sequel::Error
    _(err.message).must_include DB.literal(temp_table)
    _(err.message).wont_match(/QualifiedIdentifier/)

    _(DB[temp_table].select_order_map(:i)).must_equal [ 2 ]
  end

  # The leading drop_table? has to name the qualified temp table too, or a
  # stranded one makes every later qualified delete fail with
  # TABLE_OR_VIEW_ALREADY_EXISTS -- the poisoning c5q fixed for the
  # unqualified case.
  it "should tolerate a stranded qualified temp table" do
    DB.create_table(temp_table) { Integer :i }

    _(DB[qualified].where(i: 1).delete).must_equal 1

    _(DB[qualified].select_order_map(:i)).must_equal [ 2 ]
    _(DB.tables(schema: schema_name)).must_equal [ :items ]
  end
end
