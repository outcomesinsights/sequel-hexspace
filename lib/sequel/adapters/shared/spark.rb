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
        {}
      end
 
      def supports_create_table_if_not_exists?
        true
      end

      # Spark does not support transactions.
      def transaction(opts=nil)
        yield
      end

      private

      def type_literal_generic_float(column)
        'float'
      end

      def type_literal_generic_string(column)
        'string'
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      def quote_identifiers?
        false
      end

      def requires_sql_standard_datetimes?
        true
      end
    end
  end
end
