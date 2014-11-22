module EM::SQS
  class Queue

    # Exceptions
    class RequestSizeExceeded < StandardError;end

    # Constans
    SEND_LIMIT_SIZE = 256.kilobytes.freeze
    WAIT_TIME_SECONDS = 20 # Long poll timeout 
    POOL_SIZE = 10

    attr_reader :url, :name

    def initialize(name)
      @url ||= SqsWorker::SQS_CLIENT.get_queue_url(queue_name: name)["queue_url"]
    end

    def send_message(attributes)
      message = attributes.is_a?(Message) ? attributes : Message.new(attributes)

      if message.bytesize > SEND_LIMIT_SIZE
        raise RequestSizeExceeded.new("Send request size exceeded! Limit size is 256KB.")
      end

      SqsWorker::SQS_CLIENT.send_message({queue_url: @url}.merge(message.to_hash))
    end

    def send_message_batch(array)

      all_messages = if array.first.is_a?(Message) 
        array 
      else 
        array.map{|el| Message.new(el) }
      end

      all_messages.each_slice(10).with_index do |messages|
        SqsWorker::SQS_CLIENT.send_message_batch queue_url: @url, 
          entries: messages.map{|m| m.to_hash(with_id: true)}
      end

    end

    def receive_messages(*attributes)
      request = {
        queue_url: @url, 
        wait_time_seconds: WAIT_TIME_SECONDS,
        max_number_of_messages: POOL_SIZE
      }
      request[:message_attribute_names] = attributes if attributes.present?
      messages = SqsWorker::SQS_CLIENT.receive_message(request).messages
      if messages.present?
        messages.map do |struct|  
          ReceivedMessage.new struct
        end
      end
    end

    class Message

      attr_reader :attributes, :name, :bytesize

      # Only String messages implemented now
      def initialize(attributes)
        @name, attributes = attributes.to_a.flatten
        @id = SecureRandom.uuid
        @attributes = attributes.map do |key, value|
          [
            key, 
            {
              string_value: value, 
              data_type: "String"
            }
          ]
        end.to_h
        @bytesize = to_hash.to_json.bytesize
      end

      def to_hash(attrs)
        attrs[:with_id] ||= false
        res = {
          message_body: name,
          delay_seconds: 1,
          message_attributes: attributes
        }
        with_id ? res.merge(id: @id) : res
      end

    end

    class ReceivedMessage

      attr_reader :struct

      def initialize(struct)
        @struct = struct
      end

      def body
        @struct.body
      end

      def [](key)
        @struct.message_attributes[key.to_s].try(:string_value)
      end

    end

  end
end