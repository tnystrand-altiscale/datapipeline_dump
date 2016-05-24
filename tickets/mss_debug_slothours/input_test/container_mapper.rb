#!/usr/bin/env ruby
require 'json'
require 'ostruct'
require 'optparse'
begin
  load './Utils.rb'
rescue Exception => e
  require_relative './Utils.rb'
end

#Assign the application_id partitioning key for each kind of message
#Ensure that rollover messages get processed first by assigning them timestamp 0 or 1
#
#The input comes from both:
#1. The result of hive/container_fact_export.q
#2. The result of hive/parser_export.q (run over the RM log)
#

#to enable lexical sorting on offset numbers
def pad_offset(offset) 
  offset = offset.to_s
  len = 30
  len = len - offset.size
  while(len > 0)
    offset = "0#{offset}"
    len = len - 1
  end
  offset
end

def latest_container_timestamp(container_resource)
  result = [container_resource.requestedtime.to_i, container_resource.reservedtime.to_i, 
            container_resource.allocatedtime.to_i, container_resource.acquiredtime.to_i,
            container_resource.expiredtime.to_i, container_resource.runningtime.to_i,
            container_resource.killedtime.to_i, container_resource.releasedtime.to_i,
            container_resource.completedtime.to_i].max
  if result == nil || result <= 0
    raise "Assertion failure: unfinished_container record must have some timestamp populated"
  end
  result
end

begin
  $stdout.set_encoding 'utf-8'
  STDIN.each do |line|
    line.strip!
    msg = Utils::make_struct(line) 
    appid = nil
    case msg.type
      when 'app_summary'
       appid = msg.appId
       msg.offset = pad_offset(msg.offset)
      when 'completed_container'
       containerid = msg.id
       splits = containerid.split('_')
       appid = 'application_' + splits[1] + '_' + splits[2]
       msg.offset = pad_offset(msg.offset)
      when 'bucket'
       #ensure that rolled over request queues are re-constructed in order
       msg.offset = pad_offset(0)
       msg.timestamp = msg.requestedtime
       appid = msg.applicationid
      when 'unfinished_container'
       msg.offset = pad_offset(0)
       msg.timestamp = latest_container_timestamp(msg)
       appid = msg.applicationid
      when 'container_request'
       appid = msg.appid
       msg.offset = pad_offset(msg.offset)
      when 'transitions'
       id = msg.id
       splits = id.split('_')
       appid = 'application_' + splits[1] + '_' + splits[2]
       msg.offset = pad_offset(msg.offset)
      when 'app_activated'
       appid = msg.appid
       msg.offset = pad_offset(msg.offset)
      when 'assigned_container'
       containerid = msg.containerid
       splits = containerid.split('_')
       appid = 'application_' + splits[1] + '_' + splits[2]
       msg.offset = pad_offset(msg.offset)
    end
    if appid != nil
      puts "#{appid}\t#{msg.timestamp}_#{msg.offset}\t#{Utils::struct_to_map_line(msg.type, msg)}"
    end
  end
end
