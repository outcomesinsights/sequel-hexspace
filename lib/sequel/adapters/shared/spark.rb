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

      # Spark does not support transactions.
      def transaction(opts=nil)
        yield
      end

      private

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
        sql << "to_binary('" << [v].pack("m*") << "', 'base64')"
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

      def supports_window_functions?
        true
      end
    end
  end
end
