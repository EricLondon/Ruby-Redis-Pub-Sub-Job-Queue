require 'redis'
require 'securerandom'
require 'json'

module Worker

  # make module methods class methods
  extend self

  # each worker gets a "unique" id
  WORKER_ID = SecureRandom.hex

  # module container for worker methods
  module WorkerMethods
  end

  # method used to define worker tasks
  def add(task_name, &block)
    raise "Block required" unless block_given?
    WorkerMethods.define_singleton_method task_name, block
  end

  def work

    # process existing tasks in list
    while RedisWorker.got_tasks?
      RedisWorker.do_next_task
    end

    # subscribe for new work
    RedisWorker.subscribe
  end

  class RedisWorker

    # connect to redis for non-pub/sub commands
    @redis = Redis.new

    def self.do_next_task(task=nil)

      # get task if not passed as argument
      task = task_counts.delete_if {|k,v| v==0}.keys.sample if task.nil?

      # pop task from redis list
      json = @redis.lpop task

      return if json.nil?

      # parse the task
      data = JSON.parse json

      # debug output
      puts "WORKER: #{WORKER_ID} - #{data}"

      WorkerMethods.send task

    end

    # boolean if any tasks exist
    def self.got_tasks?
      total = task_counts.inject(0) {|sum, n| sum + n[1]}
      return total>0 ? true : false
    end

    # check redis list size for each worker task
    def self.task_counts
      WorkerMethods.singleton_methods.each_with_object({}) {|task, hsh| hsh[task] = @redis.llen task }
    end

    # use redis pub/sub to subscribe to channel for new tasks
    def self.subscribe
      @channel = defined?(CHANNEL) ? CHANNEL : 'job_server'
      @pubsub = Redis.new
      @pubsub.subscribe(@channel) do |on|
        on.message do |channel, msg|

          # message
          data = JSON.parse(msg)

          # process jobs this worker knows how to complete
          if WorkerMethods.respond_to? data['task']
            do_next_task data['task']
          end

        end
      end
    end

  end

end
