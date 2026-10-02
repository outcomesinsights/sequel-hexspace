require_relative 'spec_helper'

# Spark has no DELETE and no UPDATE, so the adapter emulates both by building
# `<table>__sequel_delete_emulate` and swapping it in for the original. The
# sequence is not atomic, so debris outlives a run that is killed part way
# through -- and until 2026-10-01 that debris poisoned every later run: one
# aborted suite left `a__sequel_delete_emulate` on the shared thriftserver and
# the next run reported 51 errors in tests that never touch table `a`, because
# Spark's CREATE TABLE is not idempotent.
#
# These tests plant that debris deliberately rather than aborting a run, so
# they cover the same ground without needing a second process.
#
# This group owns its fixtures outright instead of sharing the `items` fixture
# in dataset_test.rb: what is sitting on the server before an operation starts
# IS the thing under test, so a shared `create_table!` macro would be deciding
# the measurement.
describe 'emulated delete/update temp table' do
  before do
    DB.create_table!(:items){ Integer :i }
    DB[:items].import([:i], [[1], [2]])
  end

  after do
    DB.drop_table?(:items, :items__sequel_delete_emulate,
                   :other, :other__sequel_delete_emulate)
  end

  it 'should tolerate a stranded temp table when deleting' do
    DB.create_table(:items__sequel_delete_emulate){ Integer :i }

    _(DB[:items].where(i: 1).delete).must_equal 1
    _(DB[:items].select_order_map(:i)).must_equal [2]
    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
  end

  it 'should tolerate a stranded temp table when updating' do
    DB.create_table(:items__sequel_delete_emulate){ Integer :i }

    _(DB[:items].where(i: 1).update(i: 9)).must_equal 1
    _(DB[:items].select_order_map(:i)).must_equal [2, 9]
    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
  end

  # The drop that makes the above work must name only the one table this
  # operation is about to create. The server is shared, and anything reaching
  # for a pattern or a table listing could take out a table belonging to nobody
  # here -- so the planted neighbour is itself debris-shaped, which is the case
  # a pattern match gets wrong.
  it 'should leave tables it does not own alone' do
    DB.create_table(:other){ Integer :i }
    DB.create_table(:other__sequel_delete_emulate){ Integer :i }

    _(DB[:items].where(i: 1).delete).must_equal 1

    _(DB.table_exists?(:other)).must_equal true
    _(DB.table_exists?(:other__sequel_delete_emulate)).must_equal true
  end

  # A raise or a ^C still unwinds, so the temp table must not survive one.
  it 'should not strand the temp table when the operation raises before the swap' do
    _{ DB[:items].send(:_with_temp_table){ raise Sequel::Error, 'simulated abort' } }
      .must_raise Sequel::Error

    _(DB.table_exists?(:items__sequel_delete_emulate)).must_equal false
    _(DB[:items].select_order_map(:i)).must_equal [1, 2]
  end

  # Deliberately asserts the limit of the fix rather than a fix. Once the
  # original is gone the temp table holds the only copy of the rows, so the
  # cleanup must not touch it; dropping it there would turn an interrupted
  # operation into unrecoverable data loss. See sequel-hexspace-rho for the
  # window itself, which no ensure can close.
  it 'should keep the temp table when the original has already been dropped' do
    DB.define_singleton_method(:rename_table){ |*| raise Sequel::Error, 'simulated abort' }
    begin
      _{ DB[:items].where(i: 1).delete }.must_raise Sequel::Error
    ensure
      DB.singleton_class.send(:remove_method, :rename_table)
    end

    _(DB.table_exists?(:items)).must_equal false
    _(DB[:items__sequel_delete_emulate].select_order_map(:i)).must_equal [2]
  end
end
