require 'spec_helper'

RSpec.describe SyncProducer do
  describe "creation" do

    it "sets correct defaults" do
      sp = SyncProducer.new(nil,nil)
      expect(sp.ack_timeout_ms).to eq(1500)
      expect(sp.retry_backoff_ms).to eq(100)
      expect(sp.metadata_refresh_interval_ms).to eq(600_000)
      expect(sp.required_acks).to eq(0)
      expect(sp.max_send_retries).to eq(3)
      expect(sp.socket_timeout_ms).to eq(10_000)
    end

    it "raises ArgumentError on unknown options" do
      expect { SyncProducer.new(nil,nil,:unknown => true) }.to raise_error(ArgumentError)
    end
  end

  # Fetches metadata

  describe "sending" do
    before(:each) do
      allow(Kernel).to receive(:sleep)

      @broker_pool = double('broker_pool').as_null_object
      allow(BrokerPool).to receive(:new).and_return(@broker_pool)

      @cluster_metadata = double('cluster_metadata', :last_refreshed_at => Time.now).as_null_object
      allow(ClusterMetadata).to receive(:new).and_return(@cluster_metadata)

      @mbts = double('messages_to_send', :needs_metadata? => false).as_null_object
      allow(MessagesToSend).to receive(:new).and_return(@mbts)
    end

    context "needs metadata" do
      before(:each) do
        allow(@mbts).to receive(:needs_metadata?).and_return(true)
      end

      it "fetches metadata" do
        @broker_pool.should_recieve(:fetch_metadata)

        @sp = SyncProducer.new("test_client", [])
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue Errors::UnableToFetchMetadata
      end
    end

    context "there are messages to send" do
      before(:each) do
        allow(@mbts).to receive(:messages_for_brokers).and_return([double('mfb').as_null_object])
      end

      it "sends messages" do
        @broker_pool.should_recieve(:execute_api_call, :producer, anything, anything, anything)

        @sp = SyncProducer.new("test_client", [])
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue StandardError
      end
    end

    context "always fails" do
      before(:each) do
        allow(@mbts).to receive(:pending_messages?).and_return(true)
        @sp = SyncProducer.new("test_client", [])
      end

      it "retries the correct number of times" do
        expect(@mbts).to receive(:messages_for_brokers).exactly(4).times
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue StandardError
      end

      it "sleeps the correct amount between retries" do
        expect(Kernel).to receive(:sleep).with(0.1).exactly(4).times
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue StandardError
      end

      it "refreshes metadata between retries" do
        expect(@cluster_metadata).to receive(:update).exactly(4).times
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue StandardError
      end

      it "raises an exception" do
        expect {
          @sp.send_messages([Message.new(:topic => "topic", :value => "value")])
        }.to raise_error
      end
    end

    context "no retries" do
      before(:each) do
        allow(@mbts).to receive(:pending_messages?).and_return(true)
        @sp = SyncProducer.new("test_client", [], max_send_retries: 0)
      end

      it "does not call sleep" do
        expect(Kernel).to receive(:sleep).exactly(0).times
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")]) rescue Errors::UnableToFetchMetadata
      end
    end

    context "succeeds on first attempt" do
      before(:each) do
        allow(@mbts).to receive(:pending_messages?).and_return(false)
        @sp = SyncProducer.new("test_client", [])
      end

      it "returns true" do
        expect(@sp.send_messages([Message.new(:topic => "topic", :value => "value")])).to eq(true)
      end

      it "does not sleep" do
        expect(Kernel).not_to receive(:sleep)
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")])
      end

      it "only attempts to send once" do
        expect(@mbts).to receive(:messages_for_brokers).once
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")])
      end
    end

    context "succeeds on second attempt" do
      before(:each) do
        allow(@mbts).to receive(:pending_messages?).and_return(true, false)
        @sp = SyncProducer.new("test_client", [])
      end

      it "returns true" do
        expect(@sp.send_messages([Message.new(:topic => "topic", :value => "value")])).to eq(true)
      end

      it "sleeps once" do
        expect(Kernel).to receive(:sleep).once
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")])
      end

      it "attempts to send twice" do
        expect(@mbts).to receive(:messages_for_brokers).twice
        @sp.send_messages([Message.new(:topic => "topic", :value => "value")])
      end
    end
  end
end
