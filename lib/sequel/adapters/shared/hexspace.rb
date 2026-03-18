# frozen-string-literal: true

require_relative 'spark'

module Sequel
  module Hexspace
    module DatabaseMethods
      include Sequel::Spark::DatabaseMethods

      def database_type
        :hexspace
      end
    end

    module DatasetMethods
      include Sequel::Spark::DatasetMethods
    end
  end

  Sequel::Database.set_shared_adapter_scheme(:hexspace, Sequel::Hexspace)
end
