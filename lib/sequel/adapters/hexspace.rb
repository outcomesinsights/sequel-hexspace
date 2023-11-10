require 'hexspace'
require_relative 'shared/spark'

module Sequel
  module Hexspace
    class Client < ::Hexspace::Client
      attr_accessor :sequel_db

      private

      def column_names(metadata)
        super.map!(&:to_sym)
      end

      def type_converter(type)
        case type
        when 'binary'
          Sequel.method(:blob)
        when 'timestamp'
          sequel_db.method(:to_application_timestamp)
        else
          super
        end
      end
    end

    class Database < Sequel::Database
      include Spark::DatabaseMethods

      set_adapter_scheme :hexspace

      ALLOWED_CLIENT_KEYWORDS = Client.instance_method(:initialize).parameters.map(&:last).freeze

      def connect(server)
        opts = server_opts(server)
        opts[:username] = opts[:user]
        opts.select!{|k,v| v.to_s != '' && ALLOWED_CLIENT_KEYWORDS.include?(k)}
        client = Client.new(**opts, include_columns: true)
        client.sequel_db = self
        client
      end

      def dataset_class_default
        Dataset
      end

      def disconnect_connection(conn)
        # Hexspace does not appear to support a disconnection method
        # To keep tests happy, mark the connection as invalid
        conn.instance_variable_set(:@sequel_invalid, true)
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          res = log_connection_yield(sql, conn){conn.execute(sql)}
        rescue => e
          raise_error(e)
        else
          yield res if defined?(yield)
        end
      end

      def execute_insert(sql, opts=OPTS)
        execute(sql, opts)

        # Return nil instead of empty array.
        # Spark does not support primary keys nor autoincrementing values
        nil
      end

      def valid_connection?(conn)
        !conn.instance_variable_get(:@sequel_invalid)
      end
    end

    class Dataset < Sequel::Dataset
      include Spark::DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |rows, columns|
          self.columns = columns

          rows.each do |row|
            yield row
          end
        end
      end
    end
  end
end
