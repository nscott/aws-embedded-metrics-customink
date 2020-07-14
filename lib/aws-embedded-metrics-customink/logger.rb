module Aws
  module Embedded
    module Metrics
      class Logger

        def initialize(sink = Sinks::Lambda)
          @sink = sink.new
          @namespace = Config.config.namespace
          @dimensions = []
          @metrics = []
          @properties = {}
        end

        def metrics
          yield(self)
        ensure
          flush
        end

        def flush
          Metrics.sink.accept(message) unless empty?
        end

        def put_dimension(name, value)
          @dimensions << { name => value }
          self
        end

        def put_metric(name, value, unit = nil)
          @metrics << { 'Name' => name }.tap { |m|
            m['Unit'] = unit if unit
          }
          set_property name, value
        end

        def set_property(name, value)
          @properties[name] = value
          self
        end

        def empty?
          [@dimensions, @metrics, @properties].all? { |x| x.empty? }
        end

        def message
          {
            '_aws' => {
              'Timestamp' => timestamp,
              'CloudWatchMetrics' => [
                {
                  'Namespace' => @namespace,
                  'Dimensions' => [@dimensions.map(&:keys).flatten],
                  'Metrics' => @metrics
                }
              ]
            }
          }.tap do |m|
            @dimensions.each { |dim| m.merge!(dim) }
            m.merge!(@properties)
          end
        end

        def timestamp
          Time.now.strftime('%s%3N').to_i
        end

      end
    end
  end
end