require_relative "spec_helper"

# These are tests for expected SQL generation, that use a mock database

describe "Database#create_view" do
  before do
    @db = Sequel.connect('mock://spark')
    @db.sqls
  end

  it "should support :using and :path options" do
    @db.create_view(:parquetTable, :temp=>true, :using=>'org.apache.spark.sql.parquet', :options=>{:path=>"/path/to/view.parquet"})
    @db.sqls.must_equal ["CREATE TEMPORARY VIEW `parquetTable` USING org.apache.spark.sql.parquet OPTIONS ('path'='/path/to/view.parquet')"]
  end
end
