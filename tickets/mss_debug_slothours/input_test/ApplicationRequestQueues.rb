
#Encapsulate the state of outstanding container requests
#When a container allocation message is processed, this data is used to determine how long the
#container has been waiting (and the requested_time of the container is determined)
class ApplicationRequestQueues

  RequestResource = Struct.new :timestamp, :count

  def initialize(appid, cluster, logger, start_date, end_date)
    @appid = appid
    @cluster = cluster
    @logger = logger
    @first_request_timestamp = nil
    #There are multiple request queues per application, identified by the triple (priority, memory, vcores)
    #The pair (memory, vcores) is referred to as a capability
    @request_queues = {}
    @start_date = start_date
    @end_date = end_date
  end

  #A container has been allocated, determine when it was requested
  def allocate_container(priority, capability, container_resource, bootstrap)
    queue = get_request_queue(priority, capability)
    oldest_request = queue[0]
    if oldest_request == nil
      msg = 'internal error: there should be a request queue'
      @logger.error(msg)
      if !bootstrap
        raise msg 
      end
    else
      @logger.debug("Oldest request on assign: #{oldest_request.inspect}")
      container_resource.requested_time = oldest_request.timestamp
      oldest_request.count = oldest_request.count - 1
      if oldest_request.count == 0
        queue.shift
      end
    end
  end

  #Used to re-construct the state of the request queues from serialized request "buckets"
  #Needed for rolling state during incremental processing
  def add_request(priority, capability, timestamp, count)
    if @first_request_timestamp == nil
      @first_request_timestamp = timestamp
    end
    queue = get_request_queue(priority, capability)
    request = RequestResource.new
    request.timestamp = timestamp
    request.count = count
    queue.push(request)
  end

  #Process a container request message from the log message stream
  #The logic is needed here because of the AM <=> RM protocol
  #e.g. if the AM makes a request for X containers, and then before any containers are allocated,
  #it makes another request for X + Y containers, then there are X + Y container requests outstanding, 
  #not 2X + Y
  def process_new_request(priority, capability, num_containers, timestamp)
    if @first_request_timestamp == nil
      @first_request_timestamp = timestamp
    end
    queue = get_request_queue(priority, capability)
    current_waiting = 0
    queue.each do |request|
      current_waiting = current_waiting + request['count']
    end
    if num_containers > current_waiting
      request = RequestResource.new
      request.timestamp = timestamp
      request.count = num_containers - current_waiting
      @logger.debug("Pushing new request: #{request.inspect}")
      queue.push(request)
    else
      @logger.debug("Ignoring request: num_containers #{num_containers} current_waiting #{current_waiting}")
    end 
  end

  #Create an empty queue if it does not already exist
  def get_request_queue(priority, capability)
    key = priority + "," + capability
    queue = @request_queues[key]
    if queue == nil
      queue = []
      @request_queues[key] = queue
    end
    queue
  end

  #Saves each request in all of the request queues as a separate output (called a bucket) 
  #If there are no requests outstanding, a special NOOP bucket is saved to indicate that the
  #application is alive (save_app_no_pending_containers)
  def save(date, user, start_time)
    writable_request = {}
    writable_request['applicationid'] = @appid
    request_saved_flag = false #Each unfinished app needs at least one request to be saved 
    save_app_no_pending_containers(date, user, start_time)
    @request_queues.each do |key, request_queue|
      writable_request['key'] = key
      if request_queue[0] != nil
        request_queue.each do |request|
          writable_request['requestedTime'] = request['timestamp'].to_i
          writable_request['count'] = request['count'].to_i
          writable_request['user'] = user
          save_bucket(writable_request, date)
          request_saved_flag = true
        end
      end
    end
  end

  def proxy_timestamp(timestamp)
    if @first_proxy_timestamp == nil || timestamp < @first_proxy_timestamp
      @first_proxy_timestamp = timestamp
    end
  end

  def save_app_no_pending_containers(date, user, start_time)
    if @first_request_timestamp == nil
      @first_request_timestamp = @first_proxy_timestamp 
    end
    data = {}
    data['applicationid'] = @appid
    #Pass dummy 'VOID_KEY' when key is not present.
    #The use of empty string was confusing MR-Streaming (because the mapper uses a "space" separated values format).
    data['key'] = 'VOID_KEY'
    data['requestedTime'] = start_time
    data['count'] = 0
    data['user'] = user
    save_bucket(data, date)
  end
    
  def save_bucket(data, date)
    fields = %w(applicationid key requestedTime count user)
    hash = data.select{|k,v| fields.include?(k)}
    hash['system'] = @cluster
    hash['date'] = date.to_s[0..9]
    if(hash['date'] >= @start_date && hash['date'] <= @end_date)
      @logger.info("Saving a partial: #{hash.inspect}")
      puts Utils::hash_to_map_line('bucket', hash)
    end
  end
end
