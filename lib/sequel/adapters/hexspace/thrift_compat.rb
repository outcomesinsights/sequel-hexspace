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
  # what makes them auditable. That is enforced by where this file lives, not by
  # inline directives: .rubocop.yml excludes this one path, and treefmt runs
  # rubocop with --force-exclusion so `just fmt` honours that exclusion even
  # though it names files explicitly. This file holds the shim and nothing else
  # so the rest of the adapter stays linted.
  #
  # reply_seqid also cannot be renamed to a predicate -- hexspace's generated
  # client calls it by that exact name.
  module Thrift
    module Client
      unless method_defined?(:handle_exception) || private_method_defined?(:handle_exception)
        def handle_exception(mtype)
          if mtype == MessageTypes::EXCEPTION
            dequeue_pending_seqid
            raise_application_exception
          end
        end
      end

      unless method_defined?(:reply_seqid) || private_method_defined?(:reply_seqid)
        def reply_seqid(rseqid)
          expected_seqid = dequeue_pending_seqid
          !expected_seqid.nil? && rseqid == expected_seqid
        end
      end
    end
  end
end
