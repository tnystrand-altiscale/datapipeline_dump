# Spec'ed by:
#  https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L244   (Java)
#  https://github.com/mumrah/kafka-python/blob/master/kafka/partitioner/hashed.py#L40     (Python client)

module Poseidon
  class PartitionHasher
    def initialize(num_partitions)
      @partitions = num_partitions
    end

    def partition_for(key)
      # Java does 32-bit abs, followed by positive modulus.
      (PartitionHasher.murmur2_hash(key) & 0x7fffffff) % @partitions
    end

    def self.murmur2_hash(str)
      data = str.to_s.bytes

      length = data.length
      # Magic MurMur constants.
      seed = 0x9747b28c
      m = 0x5bd1e995
      r = 24
      h = seed ^ length
      restrict32 = lambda { |x| x & 0xffffffff }

      i4 = 0
      while i4 + 4 <= length
        k = data[i4 + 0] + (data[i4 + 1] << 8) + (data[i4 + 2] << 16) \
            + (data[i4 + 3] << 24)
        k = restrict32[k * m]
        k ^= k >> r # triple ("unsigned") shift not needed in Ruby
        k = restrict32[k * m]
        h = restrict32[h * m]
        h ^= k
        i4 += 4
      end

      # Handle the last few bytes of the input array. Java does this with a Duff
      # device, oh well.
      h ^= data[i4 + 2] << 16 if length > i4 + 2
      h ^= data[i4 + 1] << 8 if length > i4 + 1
      if length > i4
        h ^= data[i4] & 0xff
        h = restrict32[h * m]
      end
      h ^= h >> 13
      h = restrict32[h * m]
      h ^= h >> 15
      h
    end
  end
end
