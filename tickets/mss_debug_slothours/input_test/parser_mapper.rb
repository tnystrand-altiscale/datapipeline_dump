begin
  load './Utils.rb'
rescue Exception => e
  require_relative './Utils.rb'
end

STDIN.each do |line|
  line.strip!
  hash = Utils::make_hash(line)
  service = hash['service']
  system = hash['system']
  date = hash['date']
  timestamp = hash['timestamp']
  offset = hash['offset']
  #Partitioner key is the tuple (system, service, date)
  key = "#{system}_#{service}_#{date}"
  #Two additional sorting keys are indicated: timestamp and offset (parser.workflow.xml refers to these)
  puts "#{key}\t#{timestamp}\t#{offset}\t#{line}"
end
