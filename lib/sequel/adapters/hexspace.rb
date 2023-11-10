require 'hexspace'
require_relative 'shared/spark'

module Sequel
  module Hexspace
    class Database < Sequel::Database
      include Spark::DatabaseMethods

      set_adapter_scheme :hexspace

      ALLOWED_CLIENT_KEYWORDS = ::Hexspace::Client.instance_method(:initialize).parameters.map(&:last)

      def connect(server)
        opts = server_opts(server)
        opts[:username] = opts[:user]
        opts.select!{|k,v| !v.to_s == '' && ALLOWED_CLIENT_KEYWORDS.include?(k)}
        ::Hexspace::Client.new(**opts)
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
        execute(sql) do |rows|
          next unless row = rows.shift
          columns = {}
          row.transform_keys! do |k|
            columns[k] = k.to_sym
          end
          self.columns = columns.values
          yield row

          rows.each do |row|
            row.transform_keys!(columns)
            yield row
          end
        end
      end
    end
  end
end
