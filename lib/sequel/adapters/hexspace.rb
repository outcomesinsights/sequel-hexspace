require_relative 'shared/hexspace'
require 'hexspace'

# Backport upstream fix (THRIFT-5909, PR #3270) for thrift 0.22.0 on Ruby 3.4+.
# Thrift::Bytes.empty_byte_buffer calls force_encoding on string literals,
# which triggers "literal string will be frozen in the future" warnings.
# Remove this once thrift > 0.22.0 is released with the fix.
if defined?(Thrift::Bytes) && (spec = Gem.loaded_specs['thrift']) && spec.version < Gem::Version.new('0.23')
  module Thrift
    module Bytes
      def self.empty_byte_buffer(size=nil)
        if size&.positive?
          "\0".b * size
        else
          ''.b
        end
      end
    end
  end
end

module Sequel
  module Hexspace
    class Database < Sequel::Database
      include DatabaseMethods

      set_adapter_scheme :hexspace

      ALLOWED_CLIENT_KEYWORDS = ::Hexspace::Client.instance_method(:initialize).parameters.map(&:last).freeze

      def connect(server)
        opts = server_opts(server)
        opts[:username] = opts[:user]
        opts.select!{|k,v| v.to_s != '' && ALLOWED_CLIENT_KEYWORDS.include?(k)}
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

      # Hexspace returns timestamp strings in UTC without an explicit offset.
      # When no database timezone is configured, Sequel treats naive strings as
      # local time, which shifts CURRENT_TIMESTAMP by the local UTC offset.
      def to_application_timestamp(value)
        if value.is_a?(String) && timezone.nil? && value !~ /(?:Z|[+-]\d{2}(?::?\d{2})?)\z/
          Sequel.convert_timestamp(value, :utc)
        else
          super
        end
      end

      def execute(sql, opts=OPTS)
        synchronize(opts[:server]) do |conn|
          res = log_connection_yield(sql, conn){conn.execute(sql, result_object: true)}
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
      include DatasetMethods

      def fetch_rows(sql)
        execute(sql) do |result|
          columns = result.columns.map(&:to_sym)
          self.columns = columns
          next if result.rows.empty?

          types = result.column_types
          column_info = columns.map.with_index do |name, i|
            conversion_proc = case types[i]
            when 'binary'
              Sequel.method(:blob)
            when 'timestamp'
              db.method(:to_application_timestamp)
            end

            [i, name, conversion_proc]
          end

          result.rows.each do |row|
            h = {}
            column_info.each do |i, name, conversion_proc|
              value = row[i]
              h[name] = if value.nil?
                nil
              elsif conversion_proc
                conversion_proc.call(value)
              else
                value
              end
            end
            yield h
          end
        end
      end
    end
  end
end
