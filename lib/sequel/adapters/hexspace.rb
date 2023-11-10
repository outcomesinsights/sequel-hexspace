require 'hexspace'
require_relative 'shared/spark'

module Sequel
  module Hexspace
    class Client < ::Hexspace::Client
      attr_accessor :sequel_db

      private

      def process_result(stmt)
        req = ::Hexspace::TGetResultSetMetadataReq.new
        req.operationHandle = stmt.operationHandle
        metadata = client.GetResultSetMetadata(req)
        check_status metadata

        rows = []
        columns = metadata.schema.columns.map{|c| c.columnName.to_sym}
        types = metadata.schema.columns.map { |c| ::Hexspace::TYPE_NAMES[c.typeDesc.types.first.primitiveEntry.type].downcase }

        loop do
          req = ::Hexspace::TFetchResultsReq.new
          req.operationHandle = stmt.operationHandle
          req.maxRows = 10_000
          result = client.FetchResults(req)
          check_status result

          new_rows = 0
          start_offset = result.results.startRowOffset

          # columns can be nil with Spark 3.4+
          result.results.columns&.each_with_index do |col, j|
            name = columns[j]
            value = col.get_value

            if j == 0
              new_rows = value.values.size
              new_rows.times do
                rows << {}
              end
            end

            offset = start_offset
            nulls = value.nulls.unpack1("b*")
            values = value.values

            case types[j]
            # timestamp type was commented out in hexspace
            when "timestamp"
               values.each do |v|
                 rows[offset][name] = nulls[offset] == "1" ? nil : sequel_db.to_application_timestamp(v)
                 offset += 1
               end
            when "date"
              values.each do |v|
                rows[offset][name] = nulls[offset] == "1" ? nil : Date.parse(v)
                offset += 1
              end
            when "decimal"
              values.each do |v|
                rows[offset][name] = nulls[offset] == "1" ? nil : BigDecimal(v)
                offset += 1
              end
            # binary type not treated specially in hexspace
            when "binary"
              values.each do |v|
                rows[offset][name] = nulls[offset] == "1" ? nil : Sequel.blob(v)
                offset += 1
              end
            else
              values.each do |v|
                rows[offset][name] = nulls[offset] == "1" ? nil : v
                offset += 1
              end
            end
          end

          break if new_rows < req.maxRows && !result.hasMoreRows
        end

        req = ::Hexspace::TCloseOperationReq.new
        req.operationHandle = stmt.operationHandle
        check_status client.CloseOperation(req)

        [rows, columns]
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
        client = Client.new(**opts)
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
