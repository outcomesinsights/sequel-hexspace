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
