require_relative "spec_helper"

describe "Supported types" do
  def create_items_table_with_column(name, type, opts={})
    DB.create_table!(:items){column name, type, opts}
    DB[:items]
  end

  after(:all) do
    DB.drop_table?(:items)
  end

  it "should support casting correctly" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 1)
    ds.select(Sequel.cast(:number, String).as(:n)).map(:n).must_equal %w'1'
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name=> '1')
    ds.select(Sequel.cast(:name, Integer).as(:n)).map(:n).must_equal [1]
  end

  it "should support NULL correctly" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => nil)
    ds.all.must_equal [{:number=>nil}]
  end

  it "should support generic integer type" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 2)
    ds.all.must_equal [{:number=>2}]
  end
  
  it "should support generic bignum type" do
    ds = create_items_table_with_column(:number, :Bignum)
    ds.insert(:number => 2**34)
    ds.all.must_equal [{:number=>2**34}]
  end
  
  it "should support generic float type" do
    ds = create_items_table_with_column(:number, Float)
    ds.insert(:number => 2.1)
    ds.all.must_equal [{:number=>2.1}]
  end
  
  it "should support generic numeric type" do
    ds = create_items_table_with_column(:number, Numeric, :size=>[15, 10])
    ds.insert(:number => BigDecimal('2.123456789'))
    ds.all.must_equal [{:number=>BigDecimal('2.123456789')}]
    ds = create_items_table_with_column(:number, BigDecimal, :size=>[15, 10])
    ds.insert(:number => BigDecimal('2.123456789'))
    ds.all.must_equal [{:number=>BigDecimal('2.123456789')}]
  end

  it "should support generic string type" do
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name => 'Test User')
    ds.all.must_equal [{:name=>'Test User'}]
  end
  
  it "should support generic text type" do
    ds = create_items_table_with_column(:name, String, :text=>true)
    ds.insert(:name => 'Test User'*100)
    ds.all.must_equal [{:name=>'Test User'*100}]

    name = ds.get(:name)
    ds = create_items_table_with_column(:name, String, :text=>true)
    ds.insert(:name=>name)
    ds.all.must_equal [{:name=>'Test User'*100}]
  end
  
  it "should support generic date type" do
    ds = create_items_table_with_column(:dat, Date)
    d = Date.today
    ds.insert(:dat => d)
    ds.first[:dat].must_be_kind_of(Date)
    ds.first[:dat].to_s.must_equal d.to_s
  end
  
  it "should support generic datetime type" do
    ds = create_items_table_with_column(:tim, DateTime)
    t = DateTime.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').must_equal t.strftime('%Y%m%d%H%M%S')
    ds = create_items_table_with_column(:tim, Time)
    t = Time.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').must_equal t.strftime('%Y%m%d%H%M%S')
  end
  
  it "should support generic file type" do
    ds = create_items_table_with_column(:name, File)
    ds.insert(:name =>Sequel.blob("A\0"*300))
    ds.all.must_equal [{:name=>Sequel.blob("A\0"*300)}]
    ds.first[:name].must_be_kind_of(::Sequel::SQL::Blob)
  end
  
  it "should support generic boolean type" do
    ds = create_items_table_with_column(:number, TrueClass)
    ds.insert(:number => true)
    ds.all.must_equal [{:number=>true}]
    ds = create_items_table_with_column(:number, FalseClass)
    ds.insert(:number => true)
    ds.all.must_equal [{:number=>true}]
  end
  
  it "should support generic boolean type with defaults" do
    ds = create_items_table_with_column(:number, TrueClass, :default=>true)
    DB.create_table!(:items){TrueClass :t, :default=>true; TrueClass :f, :default=>false}
    ds.insert(:t=>false)
    ds.all.must_equal [{:t=>false, :f=>false}]
    DB.create_table!(:items){TrueClass :t, :default=>true; TrueClass :f, :default=>false}
    ds.insert(:f=>true)
    ds.all.must_equal [{:t=>true, :f=>true}]
  end
end
