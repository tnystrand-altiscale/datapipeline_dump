require 'logger'
require 'ostruct'

class Utils

  def self.utc_s(time)
    time.utc.strftime('%F %T')
  end

  LOG_LEVELS = %w(debug info warn error fatal)
  LOG_MAP = Hash[LOG_LEVELS.map.with_index.to_a]

  def self.make_logger(sev = Logger::INFO)
    logger = Logger.new(STDERR)
    logger.level = sev.is_a?(String) ? LOG_MAP[sev] : logger.level = sev 
    logger.formatter = proc do |severity, datetime, _progname, msg|
    "#{utc_s(datetime)}: #{severity} #{msg}\n"
    end
    logger
  end

  #extract the numeric id from an appid, jobid, or containerid
  def self.numeric_id(id)
    splits = id.split('_')
    "#{splits[1]}_#{splits[2]}"
  end

  #Convert a serialized Hive MAP to a string-keyed Ruby Hash
  def self.make_hash(line)
    hash = {}
    splits = line.split("\u0002")
    splits.each do |split|
      key_value = split.split("\u0003")
      if key_value[1] == "\\N"
        key_value[1] = nil
      end
      hash[key_value[0]] = key_value[1]
    end
    hash
  end

  #Convert a serialized Hive MAP to a symbol-keyed Ruby Hash
  def self.make_symbol_hash(line)
    hash = {}
    splits = line.split("\u0002")
    splits.each do |split|
      key_value = split.split("\u0003")
      if key_value[1] == "\\N"
        key_value[1] = nil
      end
      hash[key_value[0].intern] = key_value[1]
    end
    hash
  end

  #Serialize a Ruby Hash to a Hive MAP without adding a type qualifier
  def self.hash_to_map(hash)
    return "\\N" if hash == nil
    line = ''
    hash.each do |key, value|
      if value == nil
        value = "\\N"
      end
      if line != ''
        line = "#{line}\u0002"
      end
      line = "#{line}#{key.to_s}\u0003#{value}"
    end
    #Prevent subsequent processing from interpreting the two character sequence \n
    #in any value as a control character.
    #Optimistically assuming we don't encounter the substring \\\\nn
    line.gsub("\n", ' ').gsub('\n',' ').gsub("\t", ' ').gsub('\t', ' ')
  end

  #Serialize a Ruby Hash to a Hive MAP
  def self.hash_to_map_line(type, hash)
    return "\\N" if hash == nil
    line = "type\u0003#{type}"
    hash.each do |key, value|
      if value == nil
        value = "\\N"
      end
      line = "#{line}\u0002#{key.to_s}\u0003#{value}"
    end
    #Remove newlines/tabs from values which would otherwise be interpreted 
    #by MR streaming as line separators or key/values separators respectively
    line.gsub("\n", ' ').gsub('\n',' ').gsub("\t", ' ').gsub('\t', ' ')
  end
  
  #Convert a serialized Hive MAP to a Ruby OpenStruct
  def self.make_struct(line)
    struct = OpenStruct.new
    hash = self.make_symbol_hash(line)
    struct.marshal_load(hash)
    struct
  end

  def self.struct_to_map(struct)
    return "\\N" if struct == nil
    hash = struct.marshal_dump
    self.hash_to_map(hash)
  end

  #Convert an OpenStruct to a hash with string valued keys (not symbol valued keys)
  def self.struct_to_hash(struct)
    return nil if struct == nil 
    hash = {}
    struct.marshal_dump.each do |key, value|
      hash[key.to_s] = value
    end
    hash
  end

  #Serialize a Ruby OpenStruct to a Hive MAP
  def self.struct_to_map_line(type, struct)
    return "\\N" if struct == nil
    hash = struct.marshal_dump
    self.hash_to_map_line(type, hash)
  end

  def self.array_to_array_line(array)
    return "\\N" if array == nil
    array.join("\u0002")
  end

  #extract value or nil from nullable Avro union type
  def self.try_value(hash, key, type)
    if hash[key] != nil && hash[key].is_a?(Hash)
      if hash[key].has_key?('null')
        return nil
      else
        return hash[key][type]
      end
    end
    hash[key]
  end

end
