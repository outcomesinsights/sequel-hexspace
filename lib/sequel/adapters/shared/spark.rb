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
        table_name = first_source_table
        tmp_name = literal(table_name) + "__sequel_delete_emulate"
        db.create_table(tmp_name, :as=>invert)
        db.drop_table(table_name)
        db.rename_table(tmp_name, table_name)
      end

      def quote_identifiers?
        false
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

      def literal_true
        "true"
      end

    end
  end
end
