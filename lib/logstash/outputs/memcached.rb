require "logstash/outputs/base"
require "logstash/namespace"


#you will have to install a GEM into logstash for this to work
#cd /opt/logstash
#env GEM_HOME=vendor/bundle/jruby/1.9 GEM_PATH="" java -jar vendor/jar/jruby-complete-1.7.11.jar -S gem install jruby-memcached

# based on https://raw.githubusercontent.com/exp-snatesan/logstash-contrib/master/lib/logstash/outputs/memcached.rb
# Push events to memcached or any service using memcached protocol ( eg  Kestrel)
#store the entire doc
# memcached {
#    host => "localmchost:11211"
#    key => "mymc_%{arg_Key}"
#}
#we could also store a subset if we like, make sure its json, if you want to use the memcached filter
# memcached {
#    host => "localmchost:11211"
#    key => "mymc_%{arg_Key}"
#    value => "{"hello": "%{somename}"}"
#}

class LogStash::Outputs::Memcached < LogStash::Outputs::Base
  config_name "memcached"
  milestone 1

  # host
  config :host, :validate => :array, :required => true

  # key (queue name in case of Kestrel)
  config :key, :validate => :string, :required => true

  # Value (what data we want to store)
  config :value, :validate => :string, :required => false
  
  # expiration (how long we want to store the key, in seconds )
  config :expiration, :validate => :number, :default => 3600
  
  public
  def register
    require 'memcached'
    $memcached = Memcached.new(@host)
  end

  public
  def receive(event)
    return unless output?(event)
    begin
      key = event.sprintf(@key)
      if not @value
	$memcached.set key, event.to_json, @expiration
      else
	$memcached.set key, @value, @expiration
      end
    rescue Exception => e
      @logger.warn("Unhandled exception", :event => event, :exception => e, :stacktrace => e.backtrace)
    end
  end

end # class LogStash::Outputs::Memcached
