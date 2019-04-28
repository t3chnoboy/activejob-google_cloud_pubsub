require 'active_job/base'
require 'active_support/core_ext/numeric/time'
require 'activejob-google_cloud_pubsub/pubsub_extension'
require 'google/cloud/pubsub'
require 'json'
require 'logger'

module ActiveJob
  module GoogleCloudPubsub
    class Worker
      MAX_DEADLINE = 10.minutes

      using PubsubExtension

      def initialize(queue: 'default', min_threads: 0, pubsub: Google::Cloud::Pubsub.new, logger: Logger.new($stdout))
        @queue_name  = queue
        @min_threads = min_threads
        @max_threads = 1
        @use_threads = true
        @pubsub      = pubsub
        @logger      = logger
      end

      def run
        @logger&.warn "Running without threads"

        @pubsub.subscription_for(@queue_name).listen {|message|
          @logger&.info "Message(#{message.message_id}) was received."
          process message
        }.start

        sleep
      end

      def ensure_subscription
        @pubsub.subscription_for @queue_name
        nil
      end

      private

      def process(message)
        begin
          succeeded = false
          failed    = false

          ActiveJob::Base.execute JSON.parse(message.data)

          succeeded = true
        rescue Exception
          failed = true
          raise
        ensure
          if succeeded || failed
            message.acknowledge!
            @logger&.info "Message(#{message.message_id}) was acknowledged."
          end
        end
      end
    end
  end
end
