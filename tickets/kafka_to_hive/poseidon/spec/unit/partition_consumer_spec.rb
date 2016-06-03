require 'spec_helper'

RSpec.describe PartitionConsumer do
  before(:each) do
    @connection = double('connection')
    allow(Connection).to receive(:new).and_return(@connection)

    offset = Protocol::Offset.new(100)
    partition_offsets = [Protocol::PartitionOffset.new(0, 0, [offset])]
    @offset_response = [Protocol::TopicOffsetResponse.new("test_topic", partition_offsets)]
    allow(@connection).to receive(:offset).and_return(@offset_response)
  end

  describe "creation" do
    context "when passed unknown options" do
      it "raises an ArgumentError" do
        expect { PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, :earliest_offset, :unknown => true) }.to raise_error(ArgumentError)
      end
    end

    context "when passed an unknown offset" do
      it "raises an ArgumentError" do
        expect { PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, :coolest_offset) }.to raise_error(ArgumentError)
      end
    end
  end

  describe "next offset" do
    context "when offset is not set" do
      it "resolves offset if it's not set" do
        expect(@connection).to receive(:offset).and_return(@offset_response)
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, :earliest_offset)

        pc.next_offset
      end

      it "returns resolved offset" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, :earliest_offset)
        expect(pc.next_offset).to eq(100)
      end
    end

    context "when offset is set" do
      it "does not resolve it" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, 200)
        pc.next_offset
      end
    end

    context "when call returns an error" do
      it "is raised" do
        allow(@offset_response.first.partition_offsets.first).to receive(:error).and_return(2)
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, :earliest_offset)

        expect { pc.next_offset }.to raise_error(Errors::InvalidMessage)
      end
    end

    context "when no offset exists" do
      it "sets offset to 0" do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, :earliest_offset)

        allow(@offset_response.first.partition_offsets.first).to receive(:offsets).and_return([])
        expect(pc.next_offset).to eq(0)
      end
    end

    context "when offset negative" do
      it "resolves offset to one " do
        pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic",
                                   0, -10)
        expect(pc.next_offset).to eq(90)
      end
    end
  end

  describe "fetching messages" do
    before(:each) do
      message_set = MessageSet.new
      message_set << Message.new(:value => "value", :key => "key", :offset => 90)
      partition_fetch_response = Protocol::PartitionFetchResponse.new(0, 0, 100, message_set)
      topic_fetch_response = Protocol::TopicFetchResponse.new('test_topic',
                                                    [partition_fetch_response])
      @response = Protocol::FetchResponse.new(double('common'), [topic_fetch_response])

      allow(@connection).to receive(:fetch).and_return(@response)
      @pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, :earliest_offset)
    end

    it "returns FetchedMessage objects" do
      expect(@pc.fetch.first.class).to eq(FetchedMessage)
    end

    it "uses object defaults" do
      expect(@connection).to receive(:fetch).with(10_000, 1, anything)
      @pc.fetch
    end

    context "when options are passed" do
      it "overrides object defaults" do
        expect(@connection).to receive(:fetch).with(20_000, 1, anything)
        @pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, :earliest_offset, :max_wait_ms => 20_000)

        @pc.fetch
      end
    end

    context "when negative offset beyond beginning of partition is passed" do
      it "starts from the earliest offset" do
        @pc = PartitionConsumer.new("test_client", "localhost", 9092, "test_topic", 0, -10000)
        pfr = @response.topic_fetch_responses.first.partition_fetch_responses.first
        allow(pfr).to receive(:error).and_return(1, 1, 0)

        @pc.fetch
      end
    end

    context "when call returns an error" do
      it "is raised" do
        pfr = @response.topic_fetch_responses.first.partition_fetch_responses.first
        allow(pfr).to receive(:error).and_return(2)

        expect { @pc.fetch }.to raise_error(Errors::InvalidMessage)
      end
    end

    it "sets the highwater mark" do
      @pc.fetch
      expect(@pc.highwater_mark).to eq(100)
    end

    it "sets the latest offset" do
      @pc.fetch
      expect(@pc.next_offset).to eq(91)
    end
  end
end
