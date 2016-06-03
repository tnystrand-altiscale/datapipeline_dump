# Small script to compute hash conflicts.

# Recipe:
# $ RMs=`ls ~/src/clusters/environments/fantastic_four/ | grep -v secrets | sed -e s/^/rm-/` ruby lib/poseidon/hash_check.rb $RMs -p 5

require 'optparse'

require_relative 'partition_hasher.rb'

def main
  partitions = nil
  OptionParser.new do |opts|
    opts.on('-p', '--partitions PART', 'Current number of partitions') do |p|
      partitions = p.to_i
    end
  end.parse!

  strings = ARGV
  fail 'Give me one or more strings to hash' if strings.empty?

  h_infinite = Poseidon::PartitionHasher.new(2**64)
  h_partitions = Poseidon::PartitionHasher.new(partitions) if partitions

  bucket_size = Hash.new 0
  strings.each do |s|
    bucket = (partitions ? h_partitions : h_infinite).partition_for s
    bucket_size[bucket] += 1
    print "#{s} :  #{h_infinite.partition_for(s)}"
    print " ---> #{bucket}" if partitions
    puts
  end
  hist = Hash.new 0
  bucket_size.each { |_, sz| hist[sz] += 1 }
  puts 'Bucket size histogram:', \
       hist.to_a.sort.reverse.map { |sz, nbuckets| "#{sz}: #{nbuckets}" } \
    .join('    ')
end

main
