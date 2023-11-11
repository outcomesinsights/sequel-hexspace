# frozen-string-literal: true

require 'sequel/adapters/utils/unmodified_identifiers'

module Sequel
  module Spark
    Sequel::Database.set_shared_adapter_scheme(:spark, self)

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      def database_type
        :spark
      end

      # Spark does not support primary keys, so do not
      # add any options
      def serial_primary_key_options
        # We could raise an exception here instead of just
        # ignoring the primary key setting.
        {:type=>Integer}
      end
 
      def supports_create_table_if_not_exists?
        true
      end

      def tables(opts=OPTS)
        dataset.with_sql("SHOW TABLES").map(:tableName).map(&:to_sym) - views
      end

      # Spark does not support transactions.
      def transaction(opts=nil)
        yield
      end

      def views(opts=OPTS)
        dataset.with_sql("SHOW VIEWS").map(:viewName).map(&:to_sym)
      end

      private

      def schema_parse_table(table, opts)
        m = output_identifier_meth(opts[:dataset])
        im = input_identifier_meth(opts[:dataset])
        metadata_dataset.with_sql("DESCRIBE #{im.call(table)}").map do |row|
          [m.call(row[:col_name]), {:db_type=>row[:data_type], :type=>schema_column_type(row[:data_type])}]
        end
      end

      def type_literal_generic_file(column)
        'binary'
      end

      def type_literal_generic_float(column)
        'float'
      end

      def type_literal_generic_string(column)
        'string'
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      def date_add_sql_append(sql, da)
        expr = da.expr
        cast_type = da.cast_type || Time

        h = Hash.new(0)
        da.interval.each do |k, v|
          h[k] = v || 0
        end

        if h[:weeks]
          h[:days] += h[:weeks] * 7
        end

        if h[:years] != 0 || h[:months] != 0
          expr = Sequel.+(expr, Sequel.function(:make_ym_interval, h[:years], h[:months]))
        end

        if h[:days] != 0 || h[:hours] != 0 || h[:minutes] != 0 || h[:seconds] != 0
          expr = Sequel.+(expr, Sequel.function(:make_dt_interval, h[:days], h[:hours], h[:minutes], h[:seconds]))
        end

        literal_append(sql, expr)
      end

      # Emulate delete by selecting all rows except the ones being deleted
      # into a new table, drop the current table, and rename the new
      # table to the current table name.
      #
      # This is designed to minimize the changes to the tests, and is
      # not recommended for production use.
      def delete
        _with_temp_table
      end

      def update(columns)
        updated_cols = columns.keys
        other_cols = db.from(first_source_table).columns - updated_cols
        updated_vals = columns.values

        _with_temp_table do |tmp_name|
          db.from(tmp_name).insert([*updated_cols, *other_cols], select(*updated_vals, *other_cols))
        end
      end

      private def _with_temp_table
        n = count
        table_name = first_source_table
        tmp_name = literal(table_name).gsub('`', '') + "__sequel_delete_emulate"
        db.create_table(tmp_name, :as=>select_all.invert)
        yield tmp_name if defined?(yield)
        db.drop_table(table_name)
        db.rename_table(tmp_name, table_name)
        n
      end

      def complex_expression_sql_append(sql, op, args)
        case op
        when :<<
          literal_append(sql, Sequel.function(:shiftleft, *args))
        when :>>
          literal_append(sql, Sequel.function(:shiftright, *args))
        when :~
          literal_append(sql, Sequel.function(:regexp, *args))
        when :'!~'
          literal_append(sql, ~Sequel.function(:regexp, *args))
        when :'~*'
          literal_append(sql, Sequel.function(:regexp, Sequel.function(:lower, args[0]), Sequel.function(:lower, args[1])))
        when :'!~*'
          literal_append(sql, ~Sequel.function(:regexp, Sequel.function(:lower, args[0]), Sequel.function(:lower, args[1])))
        else
          super
        end
      end

      def multi_insert_sql_strategy
        :values
      end

      def quoted_identifier_append(sql, name)
        sql << '`' << name.to_s.gsub('`', '``') << '`'
      end

      def requires_sql_standard_datetimes?
        true
      end

      def insert_supports_empty_values? 
        false
      end

      def literal_blob_append(sql, v)
        sql << "to_binary('" << [v].pack("m*").gsub("\n", "") << "', 'base64')"
      end

      def literal_false
        "false"
      end

      def literal_string_append(sql, v)
        sql << "'" << v.gsub(/(['\\])/, '\\\\\1') << "'"
      end

      def literal_true
        "true"
      end

      def supports_cte?(type=:select)
        type == :select
      end

      def supports_group_cube?
        true
      end

      def supports_group_rollup?
        true
      end

      def supports_grouping_sets?
        true
      end

      def supports_regexp?
        true
      end

      def supports_window_functions?
        true
      end
    end
  end
end
