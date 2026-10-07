# frozen_string_literal: true

require_relative "spark"

module Sequel
  module Hexspace
    DatabaseMethods = Sequel::Spark::DatabaseMethods
    DatasetMethods = Sequel::Spark::DatasetMethods
  end

  Sequel::Database.set_shared_adapter_scheme(:hexspace, Sequel::Hexspace)
end
