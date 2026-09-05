require_relative 'spec_helper'
require 'sequel/adapters/hexspace'

# Protocol double complete enough for Thrift::ApplicationException#read.
class FakeThriftProtocol

  def initialize(mtype)
    @mtype = mtype
  end

  def read_message_begin = ['OpenSession', @mtype, 1]
  def read_message_end; end
  def read_struct_begin; end
  def read_struct_end; end
  def read_field_begin = [nil, Thrift::Types::STOP, 0]
  def read_field_end; end
  def skip(type); end

end

# Guards the shim in lib/sequel/adapters/hexspace.rb that restores
# Thrift::Client#handle_exception and #reply_seqid, which thrift 0.24.0 removed
# and hexspace's generated client still calls 21 times each.
#
# Without it every Spark connection dies in recv_OpenSession with NoMethodError,
# surfacing as Sequel::DatabaseConnectionError. These tests exercise the two
# methods directly, so a future thrift release removing more of this API fails
# here rather than downstream.
describe 'Thrift::Client compatibility shim' do
  def client_for(mtype, pending:)
    client = Hexspace::TCLIService::Client.new(FakeThriftProtocol.new(mtype))
    client.instance_variable_set(:@pending_seqids, pending)
    client
  end

  def pending_of(client) = client.instance_variable_get(:@pending_seqids)

  it 'defines both methods the generated client calls' do
    %i[handle_exception reply_seqid].each do |meth|
      defined = Thrift::Client.method_defined?(meth) ||
                Thrift::Client.private_method_defined?(meth)

      assert defined, "Thrift::Client##{meth} missing; hexspace's generated client calls it"
    end
  end

  it 'passes a normal reply through and drains exactly one pending seqid' do
    client = client_for(Thrift::MessageTypes::REPLY, pending: [1])
    _fname, mtype, rseqid = client.receive_message_begin
    client.send(:handle_exception, mtype) # must not raise on a REPLY

    assert client.send(:reply_seqid, rseqid)
    assert_empty pending_of(client)
  end

  it 'reports a mismatched seqid rather than raising' do
    client = client_for(Thrift::MessageTypes::REPLY, pending: [99])

    refute client.send(:reply_seqid, 1)
  end

  it 'raises ApplicationException on an EXCEPTION message and drains the seqid' do
    client = client_for(Thrift::MessageTypes::EXCEPTION, pending: [7])

    assert_raises(Thrift::ApplicationException) do
      client.send(:handle_exception, Thrift::MessageTypes::EXCEPTION)
    end
    assert_empty pending_of(client)
  end
end
