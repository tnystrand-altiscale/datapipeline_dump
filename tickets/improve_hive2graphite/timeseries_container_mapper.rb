require 'ostruct'
require 'optparse'
require 'time'
require 'json'

begin
  load './Utils.rb'
rescue Exception => e
  require_relative './Utils.rb'
end

def ymd(date)
  date.strftime("%Y-%m-%d")
end

def maybe(value)
  if value == nil
    "\\N"
  else
    value
  end
end

def get_running_metric(metric, state)
  if state != 'REQUESTED' && state != 'EXPIRED'
    metric
  else
    0
  end
end

def get_requested_waiting(waiting_time, state)
  if state == 'REQUESTED'
    waiting_time
  else 
    0
  end
end

def putter(date, sec, memory, vcores, state, waiting_time)
  running_memory = get_running_metric(memory, state)
  waiting_time = get_requested_waiting(waiting_time, state)
  hash = {}
  hash['container_wait_time'] = waiting_time
  hash['memory'] = running_memory
  hash['cluster_memory'] = @cluster_memory
  hash['cluster_vcores'] = @cluster_vcores
  hash['minute_start'] = sec
  hash['queue'] = @queue
  hash['job_id'] = @jobid
  hash['container_id'] = @containerid
  hash['state'] = state
  hash['system'] = @system
  hash['measure_date'] = date
  hash['account'] = @account.to_i
  hash['cluster_uuid'] = @cluster_uuid
  hash['principal_uuid'] = maybe(@principal_uuid)
  hash['user_key'] = maybe(@user_key)
  hash['vcores'] = get_running_metric(maybe(vcores), state)
  hash['number_apps'] = maybe(@number_apps)
  hash['host'] = maybe(@host)
  hash['date'] = @date
  #outputs hash as a Hive MAP text line which is read by hive/container_time_series_import.q
  puts Utils::hash_to_map_line('container_time_series', hash)
end

def first_partial_minute(timestamp_cursor, stop_timestamp, current_state) 
  if((timestamp_cursor % 24*60*60000) != 0)
    first_minute_start = timestamp_cursor - (timestamp_cursor % 24*60*60000)
    next_minute = timestamp_cursor + (24*60*60000 - (timestamp_cursor % 24*60*60000))
    first_stop = [stop_timestamp, next_minute].min
    time_used_this_minute = first_stop - timestamp_cursor
    waiting_time = 0
    if (current_state == 'REQUESTED')
      waiting_time = time_used_this_minute
    end
    putter(ymd(Time.at(first_minute_start.to_i/1000)), first_minute_start.to_i/1000, (@memory*(time_used_this_minute.to_f/(24*60*60000.to_f))), (@vcores*(time_used_this_minute.to_f/(24*60*60000.to_f))), current_state, waiting_time)
    timestamp_cursor = first_stop
  end
  timestamp_cursor
end

def intermediate_minutes(timestamp_cursor, stop_timestamp, current_state)
  if ((timestamp_cursor % 24*60*60000) == 0)
    while timestamp_cursor + 24*60*60000 <= stop_timestamp 
      waiting_time = 0
      if (current_state == 'REQUESTED')
        waiting_time = 24*60*60000
      end
      time = Time.at(timestamp_cursor.to_i/1000)
      putter(ymd(time), timestamp_cursor.to_i/1000, @memory, @vcores, current_state, waiting_time)
      timestamp_cursor = timestamp_cursor + 24*60*60000
    end
  end
  timestamp_cursor
end

def last_partial_minute(timestamp_cursor, stop_timestamp, current_state)
  if timestamp_cursor != stop_timestamp
    time_used = stop_timestamp - timestamp_cursor
    waiting_time = 0 
    if (current_state == 'REQUESTED')
      waiting_time = time_used
    end
    putter(ymd(Time.at(timestamp_cursor.to_i/1000)), timestamp_cursor.to_i/1000, (@memory*(time_used.to_f/(24*60*60000).to_f)), (@vcores*(time_used.to_f/(24*60*60000).to_f)), current_state, waiting_time)
  end
end

def next_state(timestamp_cursor, stop_timestamp, current_state)
  if timestamp_cursor <= stop_timestamp
    timestamp_cursor = first_partial_minute(timestamp_cursor, stop_timestamp, current_state)
    timestamp_cursor = intermediate_minutes(timestamp_cursor, stop_timestamp, current_state)
    last_partial_minute(timestamp_cursor, stop_timestamp, current_state)
  end
end

def populate_timestamps(hash)
  @timestamps = []
  @timestamps << {:label => 'RESERVED', :time => hash['reservedtime'].to_i}
  @timestamps << {:label => 'REQUESTED', :time => hash['requestedtime'].to_i}
  @timestamps << {:label => 'ALLOCATED', :time => hash['allocatedtime'].to_i}
  @timestamps << {:label => 'ACQUIRED', :time => hash['aquiredtime'].to_i}
  @timestamps << {:label => 'EXPIRED', :time => hash['expiredtime'].to_i}
  @timestamps << {:label => 'RUNNING', :time => hash['runningtime'].to_i}
  @timestamps << {:label => 'KILLED', :time => hash['killedtime'].to_i}
  @timestamps << {:label => 'RELEASED', :time => hash['releasedtime'].to_i}
  @timestamps << {:label => 'COMPLETED', :time => hash['completedtime'].to_i}
end

def populate_attributes(hash)
  @memory = hash['memory'].to_i
  @queue = hash['queue'] 
  @containerid = hash['containerid']
  @account = hash['account']
  @cluster_uuid = hash['cluster_uuid']
  @principal_uuid = hash['principal_uuid']
  @user_key = hash['user_key']
  @system = hash['system']
  @cluster_memory = hash['clustermemory']
  @cluster_vcores = hash['cluster_vcores']
  @vcores = hash['vcores'].to_i
  @number_apps = hash['numberapps']
  @host = hash['host']  
  @date = hash['date']
  splits2 = @containerid.split('_')
  @jobid = "job_#{splits2[1]}_#{splits2[2]}"
end

STDIN.each do |line|
  line.strip!
  hash = Utils::make_hash(line)
  populate_timestamps(hash)
  populate_attributes(hash)
  allocated_container = false
  current_timestamp_index = nil
  next_timestamp_index = nil
  timestamp_cursor = nil

  #start from the REQUESTED state or the ALLOCATED state (needed for reserved containers which may not have a request time)
  if (@timestamps[1][:time] != 0 && @timestamps[1][:time] != nil)
    current_timestamp_index = 1
    next_timestamp_index = 2
    timestamp_cursor = @timestamps[1][:time]
    allocated_container = true
  elsif (@timestamps[2][:time] != 0 && @timestamps[2][:time] != nil)
    current_timestamp_index = 2
    next_timestamp_index = 3
    timestamp_cursor = @timestamps[2][:time]
    allocated_container = true
  end

  #loop through the states from the container_fact table in timestamp order.
  #for each state, the function 'next_state', will output a row into container_time_series for each
  #second the container is in that state
  if allocated_container
    while next_timestamp_index < @timestamps.length
      if @timestamps[next_timestamp_index][:time] != 0 && @timestamps[next_timestamp_index][:time] != nil
        next_state(timestamp_cursor, @timestamps[next_timestamp_index][:time], @timestamps[current_timestamp_index][:label])
        timestamp_cursor = @timestamps[next_timestamp_index][:time]
        current_timestamp_index = next_timestamp_index
        next_timestamp_index = next_timestamp_index + 1
      else
        #the next container state is unused, try the next next
        next_timestamp_index = next_timestamp_index + 1
      end
    end
  end
 
  #Handle reserved state
  if (@timestamps[0][:time] != 0 && @timestamps[0][:time] != nil)
    current_timestamp_index = 0
    if (@timestamps[2][:time] != 0 && @timestamps[2][:time] != nil)
      next_timestamp_index = 2
    else
      next_timestamp_index = 8
    end
    timestamp_cursor = @timestamps[0][:time]
    next_state(timestamp_cursor, @timestamps[next_timestamp_index][:time], @timestamps[current_timestamp_index][:label])
  end

end
