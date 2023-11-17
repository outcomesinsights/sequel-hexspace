require_relative "spec_helper"

# These are tests for expected SQL generation, that use a mock database

describe "Database" do
  before do
    @db = Sequel.connect('mock://spark')
    @db.sqls
  end

  it "#create_table should support :using and :path options" do
    @db.create_table(:parquetTable, :using=>'org.apache.spark.sql.parquet', :options=>{:path=>"/path/to/view.parquet"}) do
      Integer :x
    end
    @db.sqls.must_equal ["CREATE TABLE `parquetTable` (`x` integer) USING org.apache.spark.sql.parquet OPTIONS ('path'='/path/to/view.parquet')"]
  end

  it "#create_view should support :using and :path options" do
    @db.create_view(:parquetTable, :temp=>true, :using=>'org.apache.spark.sql.parquet', :options=>{:path=>"/path/to/view.parquet"})
    @db.sqls.must_equal ["CREATE TEMPORARY VIEW `parquetTable` USING org.apache.spark.sql.parquet OPTIONS ('path'='/path/to/view.parquet')"]
  end
end
