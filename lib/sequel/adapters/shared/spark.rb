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

      # Spark does not support transactions.
      def transaction(opts=nil)
        yield
      end
    end

    module DatasetMethods
      include UnmodifiedIdentifiers::DatasetMethods

      def quote_identifiers?
        false
      end
    end
  end
end
