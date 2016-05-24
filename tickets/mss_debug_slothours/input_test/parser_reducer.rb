require 'ostruct'
require 'optparse'
require 'json'
require 'logger'
require 'date'
begin
  load './Utils.rb'
rescue Exception => e
  require_relative './Utils.rb'
end

class MessageParser

  def initialize(config, service, loglevel, input = STDIN, output = STDOUT)
    @config = config
    @service = service
    @logger = Utils::make_logger(loglevel)
    @input = input
    @output = output
    @regexes = {}
  end
  
  def get_regexp(regex)
    regexp = @regexes[regex]
    if regexp == nil
      regexp = Regexp.new(regex, Regexp::MULTILINE) 
      @regexes[regex] = regexp
    end
    regexp
  end

  def to_boolean(str)
    str = str.downcase
    str == 'true'
  end

  #Check strings for type conformance
  def type_conversion(value_data, type) 
    case type
    when 'boolean'
      return to_boolean(value_data) 
    when 'int'
      val = Integer(value_data)
      if (val < -2147483648) || (val > 2147483647)
        raise "Integer out of range: #{val}" 
      end
      return val
    when 'long'
      val = Integer(value_data)
      if (val < -9223372036854775808) || (val > 9223372036854775807)
        raise "Long out of range: #{val}" 
      end
      return val
    when 'double'
      return Float(value_data)
    when 'string'
      return value_data
    else
      raise "Unsupported type: #{type}" 
    end  
  end

  def populate_match(matched_hash, match, schema_hash)
    schema_hash.each do |field_name, type|
      value_data = match[field_name]
      if value_data == nil
        matched_hash[field_name] = "\\N"
      else
        matched_hash[field_name] = type_conversion(value_data, type) 
      end
    end 
    matched_hash
  end

  def handle_match(match, parser, timestamp, offset)
    matched_hash = {}
    matched_hash['timestamp'] = timestamp
    matched_hash['offset'] = offset
    schema_hash = parser['schema']
    populate_match(matched_hash, match, schema_hash) 
    matched_hash
  end

  def try_parsers(source, parsers, message, timestamp, offset)
    @logger.debug("Matching msg: #{message}")
    parsers.each do |parser| 
      regex = parser['regex']  
      matcher = get_regexp(regex)
      @logger.debug("Trying regex: #{regex}")
      match = matcher.match(message)
      if match != nil
        @logger.debug("PASS: #{message}")
        return handle_match(match, parser, timestamp, offset), parser['event']
      end
    end
    @logger.debug("FAIL: #{message}")
    return nil, nil
  end

  #create a new buffer for multiline messages but record the 'source', 
  #'timestamp' and 'offset' of the first line
  def init_buffer(buffer, hash)
    buffer['message'] = hash['message'].gsub("\n", ' ')
    buffer['source'] = hash['source']
    buffer['timestamp'] = hash['timestamp']
    buffer['offset'] = hash['offset']
    buffer['system'] = hash['system']
    timestamp = buffer['timestamp'].to_i
    buffer['date'] = Time.at(timestamp/1000).to_s[0..9]
  end

  #parse the buffer as a multiline message and emit the matching fields
  def flush_buffer(buffer)
    source = buffer['source']
    parsers = @config[source]
    if parsers != nil
      parse_match, table = try_parsers(source, parsers, buffer['message'], buffer['timestamp'], buffer['offset'])
      @logger.debug "table #{table} parse match #{parse_match.to_json}"
      unless parse_match == nil
        parse_match['type'] = table
        parse_match['system'] = buffer['system']
        parse_match['date'] = Time.at(buffer['timestamp'].to_i/1000).to_s[0..9]
        @output << "#{Utils::hash_to_map_line(@service, parse_match)}\n"
      end
    end
  end

  #append a message to the 'message' field of the current buffer, 
  #replace newlines with space to shield authored regex expressions from the line/multiline distinction
  def append_buffer(buffer, hash)
    if hash['message'] != nil
      buffer['message'] << " #{hash['message'].gsub("\n", ' ')}"
    end
  end

  def run
    buffer = {}
    buffer['message'] = ''
    buffer['source'] = nil
    buffer['timestamp'] = nil
    buffer['offset'] = nil
    previous_key = nil

    @input.each do |line|
      splits = line.split("\t")
      #Use the partitioner key in the usual style to determine when a new partition has started
      key = splits[0]
      #Skip over the two sorting keys
      value = splits[3 .. -1].join("\t")
      @logger.debug("Received value: #{value}")
      hash = Utils::make_hash(value)
      line_source = hash['source']
      buffer_source = buffer['source']
      @logger.debug("Decoded a line value: #{hash}")

      if previous_key != nil && key != previous_key
        if buffer['source'] != nil
          flush_buffer(buffer)
        end
        buffer = {}
        buffer['message'] = ''
        buffer['source'] = nil
        buffer['timestamp'] = nil
        buffer['offset'] = nil
      end
      previous_key = key

      #The following if/elsif logic is needed to merge multi-line messages
      #This happens due to newlines in log messages

      #line is start of message and buffer is non-empty
      #flush the buffer and init a new buffer with the line
      if line_source != 'OTHER' && buffer_source != nil
        @logger.debug("line_source != 'OTHER' && buffer_source != nil")
        flush_buffer(buffer)
        init_buffer(buffer, hash)
      #line is start of message and buffer is empty
      #init a new buffer with the line
      elsif line_source != 'OTHER' && buffer_source == nil
        @logger.debug("line_source != 'OTHER' && buffer_source == nil")
        init_buffer(buffer, hash)
      #line is continuing a message and buffer is non-empty
      #add the line to the buffer
      elsif line_source == 'OTHER' && buffer_source != nil
        @logger.debug("line_source == 'OTHER' && buffer_source != nil")
        append_buffer(buffer, hash)
      #else noop, this only occurs when a log starts with
      #a stracktrace, we can discard the stacktrace
      end
    end
    #Catch the last line which is not caught by moving to a new partitioner key
    if buffer['source'] != nil
      flush_buffer(buffer) 
    end
  end

end

settings = OpenStruct.new

settings.log_level = 'debug'
options = OptionParser.new do |opts|

  opts.on('-s STR', '--service', 'Service to parse messages for') do |service|
    settings.service = service
  end

  opts.on('-l',
          '--log-level LEVEL',
          "Log level: #{Utils::LOG_LEVELS.join(', ')}") do |log_level|
    settings.log_level = log_level
  end

  opts.on_tail('--help', 'Show this usage message and quit') do
    puts opts.help
    exit
  end

end

options.parse!

#entry point for streaming hadoop job
begin
  if __FILE__ == $PROGRAM_NAME
    service = settings.service
    json_string = File.open("#{service}_parsers.json").read
    config = JSON.parse(json_string)
    MessageParser.new(config, service, settings.log_level).run
  end
end
