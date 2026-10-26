# rubocop:disable all
# Restore Thrift::Client#handle_exception and #reply_seqid for thrift >= 0.24.
#
# hexspace's generated Thrift client (lib/hexspace/tcli_service.rb) calls both,
# 21 times each. thrift 0.24.0 removed them, folding their work into the new
# #validate_message_begin -- so on thrift 0.24 every Spark connection dies in
# recv_OpenSession with NoMethodError, surfacing as
# Sequel::DatabaseConnectionError.
#
# hexspace 0.3.0 and older ship a client generated in 2023, before thrift 0.24.
#
# Both methods are restored verbatim from thrift 0.23.0 and are implemented
# purely in terms of primitives thrift 0.24 still provides and still uses in
# validate_message_begin. The dequeue accounting is unchanged: exactly one
# dequeue_pending_seqid per call on both the reply and the exception path.
#
# Note this keeps 0.23 semantics -- it does NOT add 0.24's stricter checks for
# invalid message type and wrong method name.
#
# hexspace 0.4.0 (2026-10-15) regenerated its client against thrift >= 0.25 and
# no longer calls either method, so this shim is dead code under hexspace 0.4.
# It stays while the gemspec admits hexspace < 0.4. Delete it when that floor
# rises to 0.4.
if defined?(Thrift::Client)
  # Both method bodies below are copied verbatim from thrift 0.23.0 and
  # deliberately not restyled: keeping them byte-comparable with upstream is
  # what makes them auditable. That is enforced by the `rubocop:disable all` on
  # this file's first line, which rubocop honours however it is invoked -- a
  # .rubocop.yml Exclude is not, because rubocop ignores Exclude for a path named
  # on the command line and treefmt names them. This file holds the shim and
  # nothing else so the rest of the adapter stays linted.
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
