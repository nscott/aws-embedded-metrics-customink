# frozen_string_literal: true

require 'tcp-client'

module Aws
  module Embedded
    module Metrics
      module Sinks
        #
        # Create a sink that will communicate to a CloudWatch Log Agent over a TCP connection.
        # This version pushes messages into a queue which is read by a separate thread and piped to the agent.
        #
        # See https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch_Embedded_Metric_Format_Generation_CloudWatch_Agent.html
        # for configuration information
        class TcpAsync
          DEFAULT_ENV_VAR_NAME = 'AWS_EMF_AGENT_ENDPOINT'
          attr_reader :queue, :sender

          #
          # Create a new TcpAsync sink. This builds on the Tcp sink; see the documentation there for more information.
          #
          # This was built as a performance-critical piece of code since metrics are often a high-volume item.
          # +#accept+, which is what takes in messages to send to the CW Metric Agent, puts messages into a thread-safe
          # queue. A separate thread is then picking up from that queue and sending the messages to a Tcp sink.
          #
          # <b>Creating a new EcsFargate sink will create a new thread and connection to the agent.</b>
          # This sink is intended to be used sparingly.
          #
          # Messages that time out or can't be sent are lost.
          # If a message is enqueued and the queue is full, the message is dropped and a warning is logged.
          #
          # @param conn_str [String] A connection string, formatted like 'tcp://127.0.0.1:25888'
          # @param conn_timeout_secs [Numeric] The number of seconds before timing out the connection to the agent.
          #   10 by default.
          # @param write_timeout_secs [Numeric] The number of seconds to wait before timing out a write.
          #   10 by default.
          # @param logger [Logger] A standard Ruby logger to propagate warnings and errors.
          #   Suggested to use Rails.logger.
          # @param max_queue_size [Numeric] The number of messages to buffer in-memory.
          #   A negative value will buffer everything.
          def initialize(conn_str: ENV.fetch(DEFAULT_ENV_VAR_NAME, nil),
                         conn_timeout_secs: 10,
                         write_timeout_secs: 10,
                         logger: nil,
                         max_queue_size: 1_000)
            @sender = Tcp.new(conn_str: conn_str,
                              conn_timeout_secs: conn_timeout_secs,
                              write_timeout_secs: write_timeout_secs,
                              logger: logger)

            @max_queue_size = max_queue_size
            @queue = Queue.new
            @lock = Mutex.new
            @stop = false
            @logger = logger
            start_sender(@queue)
          end

          def accept(message)
            if @max_queue_size > -1 && @queue.length > @max_queue_size
              @logger&.warn("TcpAsync metrics queue is full (#{@max_queue_size} items)! Dropping metric message.")
              return
            end

            @queue.push(message)
          end

          #
          # Shut down the sink. By default this blocks until all messages are sent to the agent, or
          # the wait time elapses. No more messages will be accepted as soon as this is called.
          #
          # @param wait_time_seconds [Numeric] The seconds to wait for messages to be sent to the agent.
          def shutdown(wait_time_seconds = 30)
            # We push a "stop" message to ensure there's something in the queue,
            # otherwise it will indefinitely block.
            # When a "stop message" comes through it will break the loop.
            @queue.push(StopMessage.new)
            @queue.close

            start = Time.now.utc
            until @queue.empty? || Time.now.utc > (start + wait_time_seconds)
              # Yield this thread until the queue has processed
              sleep(0)
            end

            # If we haven't been able to eat through the queue, this should terminate the loop
            # and allow the thread to rejoin.
            @lock.synchronize do
              @stop = true
            end

            @sender_thread&.join
          end

          def should_stop
            @lock.synchronize do
              @stop
            end
          end

          def start_sender(queue)
            @sender_thread = Thread.new do
              stop_message_class = StopMessage.new.class
              # Infinitely read from the queue and send messages to the agent
              until should_stop
                # We use a special message class to ensure
                message = queue.pop
                break if stop_message_class == message.class

                @sender.accept(message)
              end
            end
          end

          # Special class to signal that the thread should exit and finish.
          class StopMessage; end
          private_constant :StopMessage
        end
      end
    end
  end
end
