#!/usr/bin/env ruby

require 'redis'
require 'json'

CHANNEL = 'job_server'

redis = Redis.new

tasks = ['say_wee','say_oh_yah']

1000.times do
  task = tasks.sample
  job = {task: tasks.sample}.to_json
  redis.rpush task, job
  redis.publish CHANNEL, job
end
