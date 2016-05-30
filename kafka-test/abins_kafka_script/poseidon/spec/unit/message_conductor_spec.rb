require 'spec_helper'

include Protocol
RSpec.describe MessageConductor do
  context "two available partitions" do
    before(:each) do
      partitions = [
        # These are intentionally not ordered by partition_id.
        # [:error, :id, :leader, :replicas, :isr]
        PartitionMetadata.new(0, 1, 2, [2,1], [2,1]),
        PartitionMetadata.new(0, 0, 1, [1,2], [1,2])
      ]
      topics = [TopicMetadata.new(TopicMetadataStruct.new(0, "test", partitions))]
      brokers = [Broker.new(1, "host1", 1), Broker.new(2, "host2", 2)]

      @mr = MetadataResponse.new(0, brokers, topics)

      @cm = ClusterMetadata.new
      @cm.update(@mr)
    end

    context "no custom partitioner" do
      before(:each) do
        @mc = MessageConductor.new(@cm, nil)
      end

      context "for unkeyed messages" do
        it "round robins which partition the message should go to" do
          destinations = 4.times.map do
            @mc.destination("test").first
          end

          first = [destinations[0], destinations[2]]
          second = [destinations[1], destinations[3]]
          expect([first.uniq, second.uniq].sort).to eq([[0],[1]])
        end

        context "unknown topic" do
          it "returns -1 for broker and partition" do
            expect(@mc.destination("no_exist")).to eq([-1,-1])
          end
        end
      end

      context "keyed message" do
        it "sends the same keys to the same destinations" do
          keys = 1000.times.map { rand(500).to_s }
          key_destinations = {}

          keys.sort_by { rand }.each do |k|
            partition,broker = @mc.destination("test", k)

            key_destinations[k] ||= []
            key_destinations[k].push([partition,broker])
          end

          expect(key_destinations.values.all? { |destinations| destinations.uniq.size == 1 }).to eq(true)
        end
      end
    end

    context "custom partitioner" do
      before(:each) do
        partitioner = Proc.new { |key, count| key.split("_").first.to_i % count }
        @mc = MessageConductor.new(@cm, partitioner)
      end

      it "obeys custom partitioner" do
        expect(@mc.destination("test", "2_hello").first).to eq(0)
        expect(@mc.destination("test", "3_hello").first).to eq(1)
      end
    end

    context "partitioner always sends to partition 1" do
      before(:each) do
        partitioner = Proc.new { 1 }
        @mc = MessageConductor.new(@cm, partitioner)
      end

      it "sends to partition 1 on broker 2" do
        expect(@mc.destination("test", "2_hello")).to eq([1,2])
      end
    end

    context "broken partitioner" do
      before(:each) do
        partitioner = Proc.new { |key, count| count + 1 }
        @mc = MessageConductor.new(@cm, partitioner)
      end

      it "raises InvalidPartitionError" do
        expect{@mc.destination("test", "2_hello").first}.to raise_error(Errors::InvalidPartitionError)
      end
    end
  end

  context "two partitions, one is unavailable" do
    before(:each) do
      partitions = [
        Protocol::PartitionMetadata.new(0, 0, 1, [1,2], [1,2]),
        Protocol::PartitionMetadata.new(0, 1, -1, [2,1], [2,1])
      ]
      topics = [TopicMetadata.new(TopicMetadataStruct.new(0, "test", partitions))]
      brokers = [Broker.new(1, "host1", 1), Broker.new(2, "host2", 2)]

      @mr = MetadataResponse.new(0, brokers, topics)

      @cm = ClusterMetadata.new
      @cm.update(@mr)

      @mc = MessageConductor.new(@cm, nil)
    end

    context "keyless message" do
      it "is never sent to an unavailable partition" do
        10.times do |destination|
          expect(@mc.destination("test").first).to eq(0)
        end
      end
    end

    context "keyed message" do
      it "is sent to unavailable partition" do
        destinations = Set.new
        100.times do |key|
          destinations << @mc.destination("test",key.to_s).first
        end
        expect(destinations).to eq(Set.new([0,1]))
      end
    end
  end

  context "no available partitions" do
    before(:each) do
      partitions = [
        Protocol::PartitionMetadata.new(0, 0, -1, [1,2], [1,2]),
        Protocol::PartitionMetadata.new(0, 1, -1, [2,1], [2,1])
      ]
      topics = [TopicMetadata.new(TopicMetadataStruct.new(0, "test", partitions))]
      brokers = [Broker.new(1, "host1", 1), Broker.new(2, "host2", 2)]

      @mr = MetadataResponse.new(0, brokers, topics)

      @cm = ClusterMetadata.new
      @cm.update(@mr)

      @mc = MessageConductor.new(@cm, nil)
    end

    context "keyless message" do
      it "return -1 for broker and partition" do
        expect(@mc.destination("test")).to eq([-1,-1])
      end
    end

    context "keyed message" do
      it "returns a valid partition and -1 for broker" do
        partition_id, broker_id = @mc.destination("test", "key")
        expect(partition_id).to_not eq(-1)
        expect(broker_id).to eq(-1)
      end
    end
  end
end
