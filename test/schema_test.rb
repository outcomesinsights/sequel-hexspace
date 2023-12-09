require_relative "spec_helper"

describe "Database schema parser" do
  after do
    DB.drop_table?(:items)
  end

  it "should not issue an sql query if the schema has been loaded unless :reload is true" do
    DB.create_table!(:items){Integer :number}
    DB.schema(:items, :reload=>true)
    DB.schema(:items)
    DB.schema(:items, :reload=>true)
  end

  it "Model schema should include columns in the table, even if they aren't selected" do
    DB.create_table!(:items){String :a; Integer :number}
    m = Sequel::Model(DB[:items].select(:a))
    m.columns.must_equal [:a]
    m.db_schema[:number][:type].must_equal :integer
  end

  it "should raise an error when the table doesn't exist" do
    proc{DB.schema(:no_table)}.must_raise(Sequel::Error, Sequel::DatabaseError)
  end

  it "should return the schema correctly" do
    DB.create_table!(:items){Integer :number}
    schema = DB.schema(:items, :reload=>true)
    schema.must_be_kind_of(Array)
    schema.length.must_equal 1
    col = schema.first
    col.must_be_kind_of(Array)
    col.length.must_equal 2
    col.first.must_equal :number
    col_info = col.last
    col_info.must_be_kind_of(Hash)
    col_info[:type].must_equal :integer
    DB.schema(:items)
  end

  it "should parse types from the schema properly" do
    DB.create_table!(:items){Integer :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Fixnum :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Bignum :number}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){Float :number}
    DB.schema(:items).first.last[:type].must_equal :float
    DB.create_table!(:items){BigDecimal :number, :size=>[11, 2]}
    DB.schema(:items).first.last[:type].must_equal :decimal
    DB.create_table!(:items){Numeric :number, :size=>[12, 0]}
    DB.schema(:items).first.last[:type].must_equal :integer
    DB.create_table!(:items){String :number}
    DB.schema(:items).first.last[:type].must_equal :string
    DB.create_table!(:items){Date :number}
    DB.schema(:items).first.last[:type].must_equal :date
    DB.create_table!(:items){Time :number}
    DB.schema(:items).first.last[:type].must_equal :datetime
    DB.create_table!(:items){DateTime :number}
    DB.schema(:items).first.last[:type].must_equal :datetime
    DB.create_table!(:items){File :number}
    DB.schema(:items).first.last[:type].must_equal :blob
    DB.create_table!(:items){TrueClass :number}
    DB.schema(:items).first.last[:type].must_equal :boolean
    DB.create_table!(:items){FalseClass :number}
    DB.schema(:items).first.last[:type].must_equal :boolean
  end

  it "should round trip database types from the schema properly" do
    DB.create_table!(:items){String :number, :size=>50}
    db_type = DB.schema(:items).first.last[:db_type]
    DB.create_table!(:items){column :number, db_type}
    DB.schema(:items).first.last[:db_type].must_equal db_type

    DB.create_table!(:items){Numeric :number, :size=>[11,3]}
    db_type = DB.schema(:items).first.last[:db_type]
    DB.create_table!(:items){column :number, db_type}
    DB.schema(:items).first.last[:db_type].must_equal db_type
  end

  int_types = [Integer, :Bignum, [Numeric, {:size=>7}], :tinyint, :byte, :short, :smallint]
  decimal_types = [[Numeric, {:size=>[10, 2]}], [BigDecimal, {:size=>[8, 3]}]]

  int_types.each do |type|
    it "should correctly parse maximum and minimum values for #{type} columns" do
      DB.create_table!(:items){column :a, *type}
      sch = DB.schema(:items).first.last
      max = sch[:max_value]
      min = sch[:min_value]
      max.must_be_kind_of Integer
      min.must_be_kind_of Integer
      ds = DB[:items]
      proc{ds.insert(max+1)}.must_raise(Sequel::DatabaseError, Sequel::InvalidValue)
      proc{ds.insert(min-1)}.must_raise(Sequel::DatabaseError, Sequel::InvalidValue)
      ds.insert(max)
      ds.insert(min)
      ds.select_order_map(:a).must_equal [min, max]
    end
  end

  decimal_types.each do |type|
    it "should correctly parse maximum and minimum values for #{type} columns" do
      DB.create_table!(:items){column :a, *type}
      sch = DB.schema(:items).first.last
      max = sch[:max_value]
      min = sch[:min_value]
      max.must_be_kind_of BigDecimal
      min.must_be_kind_of BigDecimal
      ds = DB[:items]

      inc = case max.to_s('F')
      when /\A9+\.0\z/
        1
      when /\A(?:0|9+)\.(0*9+)\z/
        BigDecimal(1)/(10**$1.length)
      when /\A(?:9+)(0+)\.0+\z/
        BigDecimal(1) * (10**$1.length)
      else
        raise "spec error, cannot parse maximum value"
      end

      proc{ds.insert(max+inc)}.must_raise(Sequel::DatabaseError, Sequel::InvalidValue)
      proc{ds.insert(min-inc)}.must_raise(Sequel::DatabaseError, Sequel::InvalidValue)
      ds.insert(max)
      ds.insert(min)
      ds.select_order_map(:a).must_equal [min, max]
    end
  end
end

describe "Database schema modifiers" do
  before do
    @db = DB
    @ds = @db[:items]
  end
  after do
    # Use instead of drop_table? to work around issues on jdbc/db2
    @db.drop_table(:items) rescue nil
    @db.drop_table(:items2) rescue nil
  end

  it "should create tables correctly" do
    @db.create_table!(:items){Integer :number}
    @db.table_exists?(:items).must_equal true
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number]
    @ds.insert([10])
    @ds.columns!.must_equal [:number]
  end
  
  it "should create tables from select statements correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert([10])
    @db.create_table(:items2, :as=>@db[:items])
    @db.schema(:items2, :reload=>true).map{|x| x.first}.must_equal [:number]
    @db[:items2].columns.must_equal [:number]
    @db[:items2].all.must_equal [{:number=>10}]
  end
  
  it "should not raise an error if table doesn't exist when using drop_table :if_exists" do
    @db.drop_table(:items, :if_exists=>true)
  end

  it "should create tables with :using and :options options" do
    @db.create_table(:items, :using=>'org.apache.spark.sql.parquet', :options=>{:path=>"sequel_hexspace_test_items.parquet"}){Integer :x}
    @db[:items].delete # in case parquet file was already created
    @db[:items].insert 1
    @db[:items].all.must_equal [{:x=>1}]
  end

  describe "views" do
    before do
      @db.drop_view(:items_view2) rescue nil
      @db.drop_view(:items_view) rescue nil
      @db.create_table!(:items){Integer :number}
      @ds.insert(:number=>1)
      @ds.insert(:number=>2)
    end
    after do
      @db.drop_view(:items_view2) rescue nil
      @db.drop_view(:items_view) rescue nil
    end

    it "should create views correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).must_equal [1]
    end

    it "should create views with explicit columns correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1), :columns=>[:n])
      @db[:items_view].map(:n).must_equal [1]
    end

    it "should create views with just options and no dataset with :temp, :using, and :options options" do
      if ENV['RUNNING_IN_CI']
        skip 'Does not work in CI as CI uses path relative to sequel-hexspace instead of relative to Spark installation'
      end

      @db.create_view(:items_view, :temp=>true, :using=>'org.apache.spark.sql.parquet', :options=>{:path=>"examples/src/main/resources/users.parquet"})
      @db[:items_view].count.must_equal 2
    end

    it "should drop views correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db.drop_view(:items_view)
      proc{@db[:items_view].map(:number)}.must_raise(Sequel::DatabaseError)
    end

    it "should not raise an error if view doesn't exist when using drop_view :if_exists" do
      @db.drop_view(:items_view, :if_exists=>true)
    end

    it "should create or replace views correctly" do
      @db.create_or_replace_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).must_equal [1]
      @db.create_or_replace_view(:items_view, @ds.where(:number=>2))
      @db[:items_view].map(:number).must_equal [2]
    end

    it "should create views only if they don't exist correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).must_equal [1]
      @db.create_view(:items_view, @ds.where(:number=>2), :if_not_exists=>true)
      @db[:items_view].map(:number).must_equal [1]
    end
  end
  
  it "should have create_table? only create the table if it doesn't already exist" do
    @db.create_table!(:items){String :a}
    @db.create_table?(:items){String :b}
    @db[:items].columns.must_equal [:a]
    @db.drop_table?(:items)
    @db.create_table?(:items){String :b}
    @db[:items].columns.must_equal [:b]
  end

  it "should rename tables correctly" do
    @db.drop_table?(:items)
    @db.create_table!(:items2){Integer :number}
    @db.rename_table(:items2, :items)
    @db.table_exists?(:items).must_equal true
    @db.table_exists?(:items2).must_equal false
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number]
    @ds.insert([10])
    @ds.columns!.must_equal [:number]
  end
  
  it "should add columns to tables correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert(:number=>10)
    @db.alter_table(:items){add_column :name, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number, :name]
    @ds.columns!.must_equal [:number, :name]
    @ds.all.must_equal [{:number=>10, :name=>nil}]
  end

  it "should add primary key columns to tables correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert(:number=>10)
    @db.alter_table(:items){add_primary_key :id}
    @db.schema(:items, :reload=>true).map{|x| x.first}.must_equal [:number, :id]
    @ds.columns!.must_equal [:number, :id]
    @ds.map(:number).must_equal [10]
  end
end

describe "Database#tables and #views" do
  before do
    @db = DB
    @db.drop_view(:sequel_test_view) rescue nil
    @db.drop_table?(:sequel_test_table)
    @db.create_table(:sequel_test_table){Integer :a}
    @db.create_view :sequel_test_view, @db[:sequel_test_table]
  end
  after do
    @db.drop_view :sequel_test_view
    @db.drop_table :sequel_test_table
  end

  it "#tables should return an array of symbols" do
    ts = @db.tables
    ts.must_be_kind_of(Array)
    ts.each{|t| t.must_be_kind_of(Symbol)}
    ts.must_include(:sequel_test_table)
    ts.wont_include(:sequel_test_view)
  end

  it "#views should return an array of symbols" do
    ts = @db.views
    ts.must_be_kind_of(Array)
    ts.each{|t| t.must_be_kind_of(Symbol)}
    ts.wont_include(:sequel_test_table)
    ts.must_include(:sequel_test_view)
  end

  it "#views should support temporary views without a namespace when using :qualify option" do
    @db.create_view :sequel_test_view2, @db[:sequel_test_table], :temp=>true
    @db.views(:qualify=>true, :like=>'sequel_test_view2').must_equal [:sequel_test_view2]
  end
end

describe "Database" do
  before do
    @db = DB
    @db.create_schema(:sequel_test1)
    @db.create_table(Sequel[:sequel_test1][:t1]){Integer :id}
  end
  after do
    @db.drop_schema(:sequel_test1, :if_exists=>true, :cascade=>true)
  end

  it "#create_schema creates schemas and drop_schema drops them" do
    ds = @db.from{sequel_test1[:t1]}
    ds.insert(1)
    ds.select_map(Sequel[:sequel_test1][:t1][:id]).must_equal [1]
    ds.where{{sequel_test1[:t1][:id]=>1}}.map(:id).must_equal [1]

    @db.table_exists?(Sequel[:sequel_test1][:t1]).must_equal true
    @db.drop_schema(:sequel_test1, :if_exists=>true, :cascade=>true)
    @db.table_exists?(Sequel[:sequel_test1][:t1]).must_equal false
  end

  it "#schema can get column information for table in non-default schema" do
    @db.schema(Sequel[:sequel_test1][:t1]).must_equal [[:id, {:db_type=>"int", :type=>:integer, :ruby_default=>nil, :min_value=>-2147483648, :max_value=>2147483647}]]
  end

  it "#tables supports :schema, :qualify, and :like options to only return tables in a given schema" do
    @db.create_view Sequel[:sequel_test1][:v1], @db[Sequel[:sequel_test1][:t1]]
    @db.tables(:schema=>:sequel_test1).must_equal [:t1]
    @db.tables(:schema=>:sequel_test1, :qualify=>true).must_equal [Sequel::SQL::QualifiedIdentifier.new("sequel_test1", "t1")]
    @db.tables(:schema=>:sequel_test1, :like=>".1").must_equal [:t1]
    @db.tables(:schema=>:sequel_test1, :like=>".2").must_equal []
  end

  it "#views supports :schema, :qualify, and :like options to only return views in a given schema" do
    @db.create_view Sequel[:sequel_test1][:v1], @db[Sequel[:sequel_test1][:t1]]
    @db.views(:schema=>:sequel_test1).must_equal [:v1]
    @db.views(:schema=>:sequel_test1, :qualify=>true).must_equal [Sequel::SQL::QualifiedIdentifier.new("sequel_test1", "v1")]
    @db.views(:schema=>:sequel_test1, :like=>".1").must_equal [:v1]
    @db.views(:schema=>:sequel_test1, :like=>".2").must_equal []
  end
end
