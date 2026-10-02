require_relative "spec_helper"

describe "date_arithmetic extension" do
  before(:all) do
    @db = DB
    @db.extension(:date_arithmetic)
    @date = Date.civil(2010, 7, 12)
    @dt = Time.local(2010, 7, 12)
    @h0 = { days: 0 }
    @h1 = { days: 1, years: nil, hours: 0 }
    @h2 = { years: 1, months: 1, days: 1, hours: 1, minutes: 1, seconds: 1 }
    @a1 = Time.local(2010, 7, 13)
    @a2 = Time.local(2011, 8, 13, 1, 1, 1)
    @s1 = Time.local(2010, 7, 11)
    @s2 = Time.local(2009, 6, 10, 22, 58, 59)
    @sql = lambda { |expr| @db.dataset.literal(expr) }
    @check = lambda do |meth, in_date, in_interval, should|
      output = @db.get(Sequel.send(meth, in_date, in_interval))
      output = Time.parse(output.to_s) unless output.is_a?(Time) || output.is_a?(DateTime)
      output.year.must_equal should.year
      output.month.must_equal should.month
      output.day.must_equal should.day
      output.hour.must_equal should.hour
      output.min.must_equal should.min
      output.sec.must_equal should.sec
    end
  end

  it "be able to use Sequel.date_add to add interval hashes to dates and datetimes" do
    @check.call(:date_add, @date, @h0, @dt)
    @check.call(:date_add, @date, @h1, @a1)
    @check.call(:date_add, @date, @h2, @a2)

    @check.call(:date_add, @dt, @h0, @dt)
    @check.call(:date_add, @dt, @h1, @a1)
    @check.call(:date_add, @dt, @h2, @a2)
  end

  it "be able to use Sequel.date_sub to subtract interval hashes from dates and datetimes" do
    @check.call(:date_sub, @date, @h0, @dt)
    @check.call(:date_sub, @date, @h1, @s1)
    @check.call(:date_sub, @date, @h2, @s2)

    @check.call(:date_sub, @dt, @h0, @dt)
    @check.call(:date_sub, @dt, @h1, @s1)
    @check.call(:date_sub, @dt, @h2, @s2)
  end

  it "should cast to the generic timestamp type when no :cast is given" do
    @sql.call(Sequel.date_add(@date, @h0)).must_equal "CAST(DATE '2010-07-12' AS timestamp)"
    @sql.call(Sequel.date_add(@date, days: 1))
      .must_equal "CAST((DATE '2010-07-12' + make_dt_interval(1, 0, 0, 0)) AS timestamp)"

    @db.get(Sequel.date_add(@date, @h0)).must_be_kind_of Time
    @db.get(Sequel.date_add(@date, days: 1)).must_be_kind_of Time
  end

  # Spark returns DATE from `DATE + make_ym_interval`, unlike every other
  # interval/input combination, so a year/month-only addition to a date came
  # back as a Date before the cast was applied.
  it "should cast year/month-only arithmetic, which Spark returns as a DATE" do
    expr = Sequel.date_add(@date, years: 1, months: 2)
    @sql.call(expr).must_equal "CAST((DATE '2010-07-12' + make_ym_interval(1, 2)) AS timestamp)"

    value = @db.get(expr)
    value.must_be_kind_of Time
    value.year.must_equal 2011
    value.month.must_equal 9
    value.day.must_equal 12
  end

  it "should apply the type requested with the :cast option" do
    date_expr = Sequel.date_add(@date, { days: 1 }, cast: :date)
    @sql.call(date_expr).must_equal "CAST((DATE '2010-07-12' + make_dt_interval(1, 0, 0, 0)) AS date)"
    value = @db.get(date_expr)
    value.must_be_kind_of Date
    value.must_equal Date.civil(2010, 7, 13)

    string_expr = Sequel.date_add(@date, { days: 1 }, cast: String)
    @sql.call(string_expr).must_equal "CAST((DATE '2010-07-12' + make_dt_interval(1, 0, 0, 0)) AS string)"
    @db.get(string_expr).must_equal "2010-07-13 00:00:00"

    # The requested type must win over what the arithmetic would produce, in
    # both directions: timestamp-producing arithmetic cast back down to a date
    # above, and date-producing arithmetic cast to a date here.
    ym_expr = Sequel.date_add(@date, { years: 1 }, cast: :date)
    @sql.call(ym_expr).must_equal "CAST((DATE '2010-07-12' + make_ym_interval(1, 0)) AS date)"
    @db.get(ym_expr).must_equal Date.civil(2011, 7, 12)
  end

  it "should apply the :cast option to date_sub as well" do
    expr = Sequel.date_sub(@date, { days: 1 }, cast: :date)
    @sql.call(expr).must_equal "CAST((DATE '2010-07-12' + make_dt_interval(-1, 0, 0, 0)) AS date)"
    @db.get(expr).must_equal Date.civil(2010, 7, 11)
  end

  # Not pre-screened in the adapter -- the server rejects the type and names
  # it, which is better than an allowlist that goes stale.
  it "should raise for a :cast type Spark cannot express" do
    expr = Sequel.date_add(@date, { days: 1 }, cast: :timestamptz)
    @sql.call(expr).must_equal "CAST((DATE '2010-07-12' + make_dt_interval(1, 0, 0, 0)) AS timestamptz)"
    error = assert_raises(Sequel::DatabaseError) { @db.get(expr) }
    error.message.must_include "UNSUPPORTED_DATATYPE"
  end
end
