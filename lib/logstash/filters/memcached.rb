require "logstash/filters/base"
require "logstash/namespace"


#you will have to install a GEM into logstash for this to work
#cd /opt/logstash
#env GEM_HOME=vendor/bundle/jruby/1.9 GEM_PATH="" java -jar vendor/jar/jruby-complete-1.7.11.jar -S gem install jruby-memcached


# Search memcached for a previous log event and copy some fields from it
# into the current event.  Below is a complete example of how this filter might
# be used.
# memcached {
#    host => "localmchost:11211"
#    key => "mymc_%{arg_Key}"
#    fields => ["hello", "helloReNamed"]
#}


class LogStash::Filters::Memcached < LogStash::Filters::Base
  config_name "memcached"
  milestone 1

  # host
  config :host, :validate => :array, :required => true

  # key (queue name in case of Kestrel)
  config :key, :validate => :string, :required => true
  
  # Hash of fields to copy from old event into new event (see example)
  config :fields, :validate => :hash, :default => {}

  public
  def register
    require 'memcached'
    require 'json'
    $memcached = Memcached.new(@host)
  end

  public
  def filter(event)
    return unless filter?(event)
    
    begin
      key_str = event.sprintf(@key)
      data = $memcached.get key_str
       
      if data
	my_hash = JSON.parse(data)
	#we found the key in cache
	@fields.each do |old, new|
	  pdata = my_hash[old]
	  #we have to check to make sure it has the data and is not nil
	  if pdata
	    event[new] = pdata
	  end
	end
	filter_matched(event)
      end
      
    rescue => e
      @logger.warn("Failed to query memcached for previous event", :error => e, :key => key_str, :my_hash => my_hash ,:data => data)
    end
  end # def filter
end # class LogStash::Outputs::Memcached
