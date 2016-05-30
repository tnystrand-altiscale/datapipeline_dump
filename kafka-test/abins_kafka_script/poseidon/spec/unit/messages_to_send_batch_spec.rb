require 'spec_helper'

RSpec.describe MessagesToSendBatch do
  context "messages sent to two different brokers" do
    before(:each) do
      message_conductor = double('message_conductor')
      allow(message_conductor).to receive(:destination).and_return([0,0],[1,1])

      @messages = [
        Message.new(:topic => "topic1", :value => "hi"),
        Message.new(:topic => "topic1", :value => "hi")
      ]
      @batch = MessagesToSendBatch.new(@messages, message_conductor)
    end

    it "returns a couple messages brokers" do
      expect(@batch.messages_for_brokers.size).to eq(2)
    end

    it "has all messages in the returned message brokers" do
      messages = @batch.messages_for_brokers.map(&:messages).flatten
      expect(messages.to_set).to eq(@messages.to_set)
    end
  end
end
