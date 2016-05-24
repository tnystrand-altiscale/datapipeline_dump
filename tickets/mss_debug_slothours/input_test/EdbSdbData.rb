require 'logger'

#Encapsulate cluster_dim and user_map data to determine the foreign keys needed in fact tables
class EdbSdbData
  def initialize(logger, clusters_file, user_map_file)
    @logger = logger
    @clusters = {}
    @persons = {}
    if File.exist?(clusters_file) && File.exist?(user_map_file)
      File.open(clusters_file).each do |line|
        line.strip!
        read_cluster(line)
      end
      File.open(user_map_file).each do |line|
        line.strip!
        read_person_map(line)
      end
    end
  end

  def read_person_map(line)
    person = Utils::make_hash(line)
    add_person(person)
  end

  def get_account(cluster)
    @clusters[cluster]['account']
  end

  def get_uuid(cluster)
    @clusters[cluster]['uuid']
  end

  def read_cluster(line)
    cluster = Utils::make_hash(line)
    @clusters[cluster['name']] = cluster
  end

  def get_persons(name)
    persons = @persons[name]
    if persons == nil
      persons = []
      @persons[name] = persons
    end
    persons
  end

  def add_person(person)
    name = person['system_user_name']
    persons = get_persons(name)
    persons.push(person)
  end

  def find_person(name, cluster)
    persons = get_persons(name)
    if persons.size == 1
      return persons[0]
    elsif persons.size > 1
      persons.each do |person|
        if cluster == person['cluster']
          return person
        end
      end
    else
      @logger.warn "Couldn't find person(#{name}) on cluster(#{cluster})"
      return {}
    end
    return {}
  end
  
end
