require_relative "spec_helper"

describe Sequel::Database do
  before do
    @db = DB
  end

  it "should provide disconnect functionality" do
    @db.disconnect
    @db.pool.size.must_equal 0
    @db.test_connection
    @db.pool.size.must_equal 1
  end

  it "should provide disconnect functionality after preparing a statement" do
    @db.create_table!(:items){Integer :i}
    @db[:items].prepare(:first, :a).call
    @db.disconnect
    @db.pool.size.must_equal 0
    @db.drop_table?(:items)
  end

  it "should raise Sequel::DatabaseError on invalid SQL" do
    proc{@db << "S"}.must_raise(Sequel::DatabaseError)
  end

  it "should have Sequel::DatabaseError#sql give the SQL causing the error" do
    (@db << "SELECT") rescue (e = $!)
    e.sql.must_equal "SELECT"
  end if ENV['SEQUEL_ERROR_SQL']

  it "should store underlying wrapped exception in Sequel::DatabaseError" do
    begin
      @db << "SELECT"
    rescue Sequel::DatabaseError=>e
      if defined?(Java::JavaLang::Exception)
        (e.wrapped_exception.is_a?(Exception) || e.wrapped_exception.is_a?(Java::JavaLang::Exception)).must_equal true
      else
        e.wrapped_exception.must_be_kind_of(Exception)
      end
    end
  end

  it "should not have the connection pool swallow non-StandardError based exceptions" do
    proc{@db.pool.hold{raise Interrupt, "test"}}.must_raise(Interrupt)
  end

  it "should be able to disconnect connections more than once without exceptions" do
    conn = @db.synchronize{|c| c}
    @db.disconnect
    @db.disconnect_connection(conn)
    @db.disconnect_connection(conn)
  end

  it "should provide ability to check connections for validity" do
    conn = @db.synchronize{|c| c}
    @db.valid_connection?(conn).must_equal true
    @db.disconnect
    @db.valid_connection?(conn).must_equal false
  end
end
