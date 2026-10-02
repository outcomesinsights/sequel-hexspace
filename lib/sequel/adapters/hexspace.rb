require_relative "shared/hexspace"
require "hexspace"

# Backport upstream fix (THRIFT-5909, PR #3270) for thrift 0.22.0 on Ruby 3.4+.
# Thrift::Bytes.empty_byte_buffer calls force_encoding on string literals,
# which triggers "literal string will be frozen in the future" warnings.
# Remove this once thrift > 0.22.0 is released with the fix.
if defined?(Thrift::Bytes) && (spec = Gem.loaded_specs["thrift"]) && spec.version < Gem::Version.new("0.23")
  module Thrift
    module Bytes
      def self.empty_byte_buffer(size = nil)
        if size&.positive?
          "\0".b * size
        else
          "".b
        end
      end
    end
  end
end

# Restore Thrift::Client#handle_exception and #reply_seqid for thrift >= 0.24.
#
# hexspace's generated Thrift client (lib/hexspace/tcli_service.rb) calls both,
# 21 times each. thrift 0.24.0 removed them, folding their work into the new
# #validate_message_begin -- so on thrift 0.24 every Spark connection dies in
# recv_OpenSession with NoMethodError, surfacing as
# Sequel::DatabaseConnectionError.
#
# Upstream will not fix this for us: hexspace's generated client is untouched
# since 2023-05-08, the latest release (0.3.0, Apr 2025) predates thrift 0.24,
# and no issue or PR there mentions thrift. Regenerating that client against
# 0.24 would drop support for thrift < 0.24, which is a maintainer's decision
# rather than ours to force.
#
# Both methods are restored verbatim from thrift 0.23.0 and are implemented
# purely in terms of primitives thrift 0.24 still provides and still uses in
# validate_message_begin. The dequeue accounting is unchanged: exactly one
# dequeue_pending_seqid per call on both the reply and the exception path.
#
# Note this keeps 0.23 semantics -- it does NOT add 0.24's stricter checks for
# invalid message type and wrong method name. Remove once hexspace regenerates
# its client against thrift >= 0.24.
if defined?(Thrift::Client)
  # Both method bodies below are copied verbatim from thrift 0.23.0 and
  # deliberately not restyled: keeping them byte-comparable with upstream is
  # what makes them auditable.
  #
  # That is now ENFORCED rather than merely asked for. Each def is wrapped in
  # `rubocop:disable all`, so `just fmt` -- which is `rubocop -a` -- leaves
  # those bytes alone. Until 2026-10-02 nothing backed the claim: the methods
  # came through that day's omakase sweep intact only because they happen to
  # contain no single-quoted strings and no `{|x|` brace blocks, which is most
  # of what the sweep rewrote.
  #
  # The paired Lint/RedundantCopDisableDirective disable is load-bearing, not
  # decoration. That cop is, in its own words, "not disabled when disabling all
  # cops", and nothing in these regions currently offends -- so without it
  # rubocop reports each guard as an unnecessary disable of all cops, and that
  # offense is autocorrectable, meaning `rubocop -a` DELETES the guard using the
  # very command the guard exists to survive. Order is load-bearing too: the
  # named disable must precede `disable all` and the named enable must precede
  # `enable all`, or Lint/MissingCopEnableDirective and
  # Lint/RedundantCopEnableDirective fire in its place. Verified against rubocop
  # 1.91.0 by perturbing each region and running the autocorrect.
  #
  # The wrapping is deliberately tight -- only the two upstream defs sit inside
  # it. The `unless` guards around them, and the rest of this adapter, are ours
  # and stay linted; a file-level AllCops/Exclude was rejected for that reason.
  #
  # reply_seqid also cannot be renamed to a predicate -- hexspace's generated
  # client calls it by that exact name.
  module Thrift
    module Client
      unless method_defined?(:handle_exception) || private_method_defined?(:handle_exception)
        # rubocop:disable Lint/RedundantCopDisableDirective
        # rubocop:disable all
        def handle_exception(mtype)
          if mtype == MessageTypes::EXCEPTION
            dequeue_pending_seqid
            raise_application_exception
          end
        end
        # rubocop:enable Lint/RedundantCopDisableDirective
        # rubocop:enable all
      end

      unless method_defined?(:reply_seqid) || private_method_defined?(:reply_seqid)
        # rubocop:disable Lint/RedundantCopDisableDirective
        # rubocop:disable all
        def reply_seqid(rseqid)
          expected_seqid = dequeue_pending_seqid
          !expected_seqid.nil? && rseqid == expected_seqid
        end
        # rubocop:enable Lint/RedundantCopDisableDirective
        # rubocop:enable all
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
        opts.select! { |k, v| v.to_s != "" && ALLOWED_CLIENT_KEYWORDS.include?(k) }
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

      def execute(sql, opts = OPTS)
        synchronize(opts[:server]) do |conn|
          res = log_connection_yield(sql, conn) { conn.execute(sql, result_object: true) }
        rescue => e
          raise_error(e)
        else
          yield res if defined?(yield)
        end
      end

      def execute_insert(sql, opts = OPTS)
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
            when "binary"
              Sequel.method(:blob)
            when "timestamp"
              db.method(:to_application_timestamp)
            end

            [ i, name, conversion_proc ]
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
