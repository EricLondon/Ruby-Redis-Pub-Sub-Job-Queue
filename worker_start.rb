#!/usr/bin/env ruby

CHANNEL = 'job_server'

$:.unshift File.dirname(__FILE__) + '/lib'

require 'worker'

Worker.add :say_wee do
  puts "wee"
end

Worker.add :say_oh_yah do
  puts "oh yah"
end

Worker.work
