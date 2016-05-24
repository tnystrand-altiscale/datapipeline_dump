#!/usr/bin/env ruby

require 'json'
require 'ostruct'
require 'optparse'
require 'date'
begin
  load './Utils.rb'
  load './ApplicationRequestQueues.rb'
  load './EdbSdbData.rb'
rescue Exception => e
  require_relative './Utils.rb'
  require_relative './ApplicationRequestQueues.rb'
  require_relative './EdbSdbData.rb'
end

#Input comes from ruby/container_mapper.rb
#It is in the form:
#<application_id>\t<key_value_pairs>
#where
#key_value_pairs is a set of key/value pairs in Hive MAP text format
#the MAP text format is converted to an OpenStruct for use in the code

class RMContainerReduce

  ContainerResource = Struct.new :jobid, :memory, :vcores, :requested_time, :reserved_time, :allocated_time, :acquired_time, :expired_time, :running_time, :killed_time, :released_time, :completed_time, :container_id, :queue, :host, :priority, :applicationid, :cluster_memory, :cluster_vcores, :number_apps, :completed

  def maybe(value)
    if value == nil
      "\\N" #Hive's external representation for null
    else
      value
    end
  end

  def get_jobid(container_id)
    splits = container_id.split('_')
    "job_#{splits[1]}_#{splits[2]}"
  end

  #Get container struct by id or create a new one
  #Assign its jobid from the container_id
  def get_container(container_id)
    @container_hash[container_id]
  end

  #Each instance is used to process one application (job)
  #The static 'run' method creates instances as new partitioner keys are encountered
  def initialize(appid, cluster, logger, edb_sdb, bootstrap, start_date, end_date)
    @appid = appid
    @cluster = cluster
    @logger = logger
    @app_request_queues = ApplicationRequestQueues.new(appid, cluster, logger, start_date, end_date)
    @user = nil
    @finished = false
    @edb_sdb = edb_sdb
    @interval_start_date = start_date
    @interval_end_date = end_date
    @bootstrap = bootstrap
    @container_hash = Hash.new { |hash, container_id| 
      new_container = ContainerResource.new
      new_container.jobid = get_jobid(container_id)
      new_container.completed = false
      hash[container_id] = new_container
    }

    @lines = Array.new
    @current_date = @interval_start_date
  end

  #Handle the app_summary message
  def process_app_summary(timestamp)
    @logger.info("App complete: #{@appid}")
    @finished = true
    @container_hash.each do |container_id, container_resource|
      if (container_resource.applicationid == @appid && container_resource.completed == false)
        container_resource.completed_time = timestamp
        container_resource.completed = true
        @container_hash[container_id] = container_resource
      end
    end
  end

  #Handle the bucket message
  def process_bucket(msg, timestamp)
    key = msg.key
    if key == 'VOID_KEY'
      @activated = msg.requestedtime
    end
    count = msg.count.to_i
    @user = msg.user 
    @logger.info("Received buffered app #{@appid}")
    if count != 0 
      splits = key.split(',')
      priority = splits[0]
      capability = splits[1]
      @app_request_queues.add_request(priority, capability, timestamp, count)
      @logger.info("Received buffered bucket for app #{@appid}")
    end
  end

  #Handle the unfinished_container message
  def process_unfinished_container(msg)
    priority = msg.priority
    cores = msg.vcores
    memory = msg.memory
    capability = cores + '-' + memory
    container_id = msg.containerid
    container_resource = get_container(container_id)
    container_resource.applicationid = @appid;
    container_resource.requested_time = msg.requestedtime
    container_resource.reserved_time = msg.reservedtime
    container_resource.allocated_time = msg.allocatedtime
    container_resource.acquired_time = msg.acquiredtime
    container_resource.expired_time = msg.expiredtime
    container_resource.running_time = msg.runningtime
    container_resource.killed_time = msg.killedtime
    container_resource.released_time = msg.releasedtime
    container_resource.completed_time = msg.completedtime
    container_resource.cluster_memory = msg.clustermemory
    container_resource.cluster_vcores = msg.cluster_vcores
    container_resource.number_apps = msg.numberapps
    container_resource.completed = false 
    container_resource.memory = memory
    container_resource.vcores = cores
    container_resource.queue = msg.queue
    container_resource.host = msg.host
    container_resource.priority = priority
    container_resource.container_id = container_id
    @container_hash[container_id] = container_resource
    @logger.debug("Received unfinished container: #{container_resource.inspect}")
  end

  #Handle the container_request message
  def process_container_request(msg, timestamp)
    num_containers = msg.num_containers.to_i
    priority = msg.priority
    cores = msg.cores
    memory = msg.memory
    capability = cores + '-' + memory
    @app_request_queues.process_new_request(priority, capability, num_containers, timestamp)
  end

  #Handle the transitions message
  def process_transitions(msg, timestamp)
    container_id = msg.id
    container_resource = @container_hash[container_id]
    if @container_hash[container_id] == nil
      container_resource = get_container(container_id)
      container_resource.completed = false
    end
    container_resource.applicationid = @appid;
    transition_type = msg.to
    container_resource.reserved_time = timestamp if transition_type == 'RESERVED'
    container_resource.allocated_time = timestamp if transition_type == 'ALLOCATED'
    container_resource.acquired_time = timestamp if transition_type == 'ACQUIRED'
    container_resource.expired_time = timestamp if transition_type == 'EXPIRED'
    container_resource.running_time = timestamp if transition_type == 'RUNNING'
    container_resource.killed_time = timestamp if transition_type == 'KILLED'
    container_resource.released_time = timestamp if transition_type == 'RELEASED'
    container_resource.completed_time = timestamp if transition_type == 'COMPLETED'
    if transition_type == 'COMPLETED' || transition_type == 'RELEASED' || transition_type == 'KILLED'
      container_resource.completed = true
    end
    @container_hash[container_id] = container_resource
  end

  #Handle the app_added message
  def process_app_activated(msg)
    @user = msg.user
    @activated = msg.timestamp
    @logger.info("Adding app #{@appid} from user #{msg.user}")
  end

  #Handle the completion time of unused reserved containers. Unlike other containers there is no 'transition' message
  #which reports the change in their state. However, this message does report when the reservation is cancelled.
  #Without processing this message we would over-estimate the amount of reserved memory (and therefore any queue usage stats)
  def process_completed_container(msg, timestamp)
    container_resource = get_container(msg.id) 
    if !container_resource.completed
      container_resource.completed = true
      container_resource.completed_time = timestamp
    end
  end

  #Handle the assigned_container message
  def process_assigned_container(msg, timestamp)
    priority = msg.priority
    cores = msg.cores
    memory = msg.memory
    capability = cores + '-' + memory
    container_id = msg.containerid
    container_resource = get_container(container_id)
    container_resource.applicationid = @appid;
    @app_request_queues.allocate_container(priority, capability, container_resource, @bootstrap)
    container_resource.allocated_time = timestamp
    container_resource.cluster_memory = msg.cluster_memory.to_i
    container_resource.cluster_vcores = msg.cluster_cores.to_i
    container_resource.number_apps = msg.num_apps.to_i
    container_resource.host = msg.node
    container_resource.memory = memory
    container_resource.vcores = cores
    container_resource.priority = priority
    container_resource.queue = msg.queue
    container_resource.container_id = container_id
    @container_hash[container_id] = container_resource
  end

  #After all the messages for a particular app have been buffered, they are sorted, and then this method
  #dispatches the messages to one of the above handler methods
  def process_app
    @logger.info("Processing app #{@appid}")
    previous_timestamp = nil
    @lines.each do |msg|
          @logger.debug(msg.inspect)
          timestamp = msg.timestamp.to_i
          if timestamp == nil || timestamp < 10000
            next
          end
          if previous_timestamp != nil
            date = Time.at(timestamp/1000).to_s[0..9]
            previous_date = Time.at(previous_timestamp/1000).to_s[0..9]
            if date != previous_date 
               # The reason for the following loop:
               # Necessary to maintain proper # unfinished tables and buckets
               # Partitions are overwritten each time the oozie jobs runs
               # The loop after process_app will not save
               # unfinished containers if application finishes its containers
               # This would means containers would be lost
               # the last day of the interval since no containers are rolled over
               #
               # This wont however maintain the 'true' picture
               # for applications without messages>1 day which 
               # 1) terminates
               # 2) runs at same days as other apps with unfinished containers
               if previous_date >= @interval_start_date
                 while previous_date < date
                   @logger.debug("Saving job state for #{previous_date}")
                   #Save the end state of a day for the application after each day's messages are processed
                   save(true, previous_date)
                   previous_date_obj = Date.parse(previous_date) + 1
                   previous_date = previous_date_obj.to_s[0..9]
                 end
               end
              @current_date = date
            end
          end
          previous_timestamp = timestamp

          case msg.type
            when 'container_request'
              process_container_request(msg, timestamp)
            when 'app_summary'
              process_app_summary(timestamp)
            when 'bucket'
              process_bucket(msg, timestamp)
            when 'transitions'
              process_transitions(msg, timestamp)
            when 'app_activated'
              process_app_activated(msg)
            when 'assigned_container'
              process_assigned_container(msg, timestamp)
            when 'unfinished_container'
              process_unfinished_container(msg)
            when 'completed_container'
              process_completed_container(msg, timestamp)
          end
    end
  end

  def sort_lines
    tmp = {}
    @lines.each do |timestamp, offsets|
      tmp[timestamp] = offsets.sort
    end
    @lines = tmp.sort
  end

  def insert_line(msg)
    @lines.push(msg)
  end

  #THIS IS THE 'main' FUNCTION
  #Buffers messages for each app and detects when the partitioner key changes
  def self.run(logger, edb_sdb, bootstrap, start_date, end_date)
    app = nil
    previous_app = nil
    cluster = nil
    previous_cluster = nil
    reduce_app = nil
    STDIN.set_encoding 'utf-8'
    STDIN.each do |line|
      line.strip!
      splits = line.split("\t")
      app = splits[0]
      msg = Utils::make_struct(splits[2])
      cluster = msg.system
      begin
        edb_sdb.get_account(cluster)
      rescue
        next
      end
      if previous_app != app
        if previous_app != nil
          reduce_app.save_interval_end
        end
        reduce_app = RMContainerReduce.new(app, cluster, logger, edb_sdb, bootstrap, start_date, end_date)
        logger.info("Start processing application #{app}")
      end
      reduce_app.insert_line(msg)
      previous_app = app
      previous_cluster = cluster
    end    
    if previous_app != nil
      begin
        edb_sdb.get_account(cluster)
        reduce_app.save_interval_end
      rescue
      end
    end
  end

  #Save the state of the application after all messages have been buffered
  def save_interval_end
    process_app
    #This loop is needed for the case when messages are seen on day X (e.g. the first couple days of the interval) but not on a day > X
    #In that case the day boundary crossing in 'process_app' will not be triggered, 
    #but we still want to save the state of day X for rollover (saw this a lot for H20 jobs)
    #
    # Some notable uglyhacking:
    #   When processing several days worth of data:
    #       Some apps will finish in day1 and have a current_date < interval_date
    #       These will not be saved in the loop though, since
    #       Its containers have 'completed' set to true
    #       AND The methods unfinished flag is called with 'true'
    #       AND The applications finish flag is set to 'true'
    #   The method will just loop through to the bottom of the method
    while @current_date < @interval_end_date
      save(true, @current_date)
      next_date = Date.parse(@current_date) + 1 
      @current_date = next_date.to_s[0..9]
    end
    save(false, @interval_end_date)
  end

  #Save each container for an application and save the ApplicationRequestQueues
  #This is called both by 'process_app' (for rollover state) and by 'save_interval_end' (for final state)
  def save(unfinished, date)
    @logger.info("Saving app state for cluster #{@cluster}, unfinished=#{unfinished}, date=#{date}")
    @container_hash.each do |container_id, container_resource|
      container = {}
      container['containerid'] = container_id
      container['jobid'] = container_resource.jobid
      container['requestedTime'] = container_resource.requested_time.to_i
      container['reservedTime'] = container_resource.reserved_time.to_i
      container['allocatedTime'] = container_resource.allocated_time.to_i
      container['acquiredTime'] = container_resource.acquired_time.to_i
      container['expiredTime'] = container_resource.expired_time.to_i
      container['runningTime'] = container_resource.running_time.to_i
      container['killedTime'] = container_resource.killed_time.to_i
      container['releasedTime'] = container_resource.released_time.to_i
      container['completedTime'] = container_resource.completed_time.to_i
      container['clusterMemory'] = container_resource.cluster_memory.to_i
      if container_resource.cluster_vcores == nil
        container['cluster_vcores'] = maybe(container_resource.cluster_vcores)
      else
        container['cluster_vcores'] = container_resource.cluster_vcores.to_i
      end
      container['numberApps'] = container_resource.number_apps.to_i
      container['memory'] = container_resource.memory.to_i
      container['vcores'] = container_resource.vcores.to_i
      container['queue'] = maybe(container_resource.queue)
      container['host'] = maybe(container_resource.host)
      container['priority'] = container_resource.priority.to_i
      container['account'] = @edb_sdb.get_account(@cluster)
      container['cluster_uuid'] = @edb_sdb.get_uuid(@cluster)
      @logger.debug("Trying to save container #{container.inspect}")
      if @user != nil
        @logger.debug("Looking up user for app: #{container_resource.applicationid}")
        @logger.debug("Username: #{@user}")
        person = @edb_sdb.find_person(@user, @cluster)
        if person == nil
          container['principal_uuid'] = "\\N"
          container['user_key'] = "\\N"
        else
          container['principal_uuid'] = maybe(person['principal_uuid'])
          container['user_key'] = maybe(person['user'])
        end
        if (container_resource.completed == false)
          container['applicationid'] = @appid
          save_partial(container, date)
        end
        if (container_resource.completed == true)
          save_fact(container) if !unfinished
        end
      elsif !@bootstrap
        @logger.error("Container #{container_id} has unknown user. Aborting.")
        raise "Container #{container_id} has unknown user. Aborting."
      end
    end
    #Only rollover request queues for apps that haven't seen the app_summary message
    @app_request_queues.save(date, @user, @activated) if !@finished
  end

  def container_end_date(hash)
    timestamp_ms = nil
    if hash['completedTime'] != 0 && hash['completedTime'] != nil
      timestamp_ms = hash['completedTime']
    elsif hash['releasedTime'] != 0 && hash['releasedTime'] != nil
      timestamp_ms = hash['releasedTime']
    elsif hash['killedTime'] != 0 && hash['killedTime'] != nil
      timestamp_ms = hash['killedTime']
    else
      raise "Invalid container state, trying to save an unfinished container: #{containerid}"
    end
    Time.at(timestamp_ms/1000).to_s[0..9]
  end

  #Save a finished container
  def save_fact(data)
    fields = %w(jobid containerid requestedTime reservedTime allocatedTime acquiredTime expiredTime runningTime killedTime releasedTime completedTime memory vcores queue host priority account cluster_uuid principal_uuid user_key clusterMemory cluster_vcores numberApps )
    hash = data.select{|k,v| fields.include?(k)}
    date = container_end_date(hash)
    hash['system'] = @cluster
    hash['date'] = date
    @logger.debug("Saving a fact: #{hash.inspect}")
    puts Utils::hash_to_map_line('container_fact', hash)
  end

  #Save an unfinished container to rollover for further processing
  def save_partial(data, date)
    fields = %w(applicationid containerid requestedTime reservedTime allocatedTime acquiredTime expiredTime runningTime killedTime releasedTime completedTime memory vcores queue host priority clusterMemory cluster_vcores numberApps )
    hash = data.select{|k,v| fields.include?(k)}
    hash['system'] = @cluster
    hash['date'] = date.to_s[0..9]
    # This check is important for maintina unfinished container table
    # If this does not exists, containers which have not finished for
    # many days will cause unnecessary overwrite of old partitions
    if(hash['date'] >= @interval_start_date && hash['date'] <= @interval_end_date)
      @logger.info("Saving a partial: #{hash.inspect}")
      @app_request_queues.proxy_timestamp(hash['requestedTime']) 
      puts Utils::hash_to_map_line('unfinished_container', hash)
    end
  end

end

@settings = OpenStruct.new
options = OptionParser.new do |opts|

  @settings.log_level = 'info'
  @settings.bootstrap = false

  opts.on('-b STR', '--bootstrap', 'Be lenient for missing apps') do |bootstrap|
    @settings.bootstrap = (bootstrap == 'true')
  end

  opts.on('-s STR', '--start_date', 'Start date') do |start_date|
    @settings.start_date = start_date
  end

  opts.on('-e STR', '--end_date', 'End date') do |end_date|
    @settings.end_date = end_date
  end

  opts.on('-l', '--log-level LEVEL', "Log level: #{Utils::LOG_LEVELS.join(', ')}") do |log_level|
    @settings.log_level = log_level
  end

  opts.on_tail('--help', 'Show this usage message and quit') do
    puts opts.help
    exit
  end

end

options.parse!

begin
  if __FILE__ == $PROGRAM_NAME
    logger = Utils::make_logger(@settings.log_level)
    edb_sdb = EdbSdbData.new(logger, 'clusters', 'user_map')
    RMContainerReduce::run(logger, edb_sdb, @settings.bootstrap, @settings.start_date, @settings.end_date)
  end
end
