require "chunked_array"
require "thread"
Thread.abort_on_exception = true

module Orchestra
  Event = Struct.new(:name, :time, :thread_id, :payload)
  
  class Stream
    attr_reader :mutex, :signaler
    def initialize
      @stream = ChunkedArray.new
      @mutex, @signaler = Mutex.new, ConditionVariable.new
      @listeners = {}
    end
    
    def push(name, *payload)
      @mutex.synchronize do
        @stream.push Event.new(name, Time.now, Thread.current.object_id, payload)
        @signaler.broadcast
      end
    end
    
    def at(pointer)
      @stream[pointer]
    end
    
    def register_listener(listener)
      @listeners[listener] = 0
    end
    
    def unregister_listener(listener)
      @listeners.delete(listener)
    end
    
    def checkin(listener, pointer)
      @listeners[listener] = pointer
      min = @listeners.min {|a,b| a[1] <=> b[1]}[1]
      @stream.gc(min)
    end
  end
  
  class Listener
    def initialize(stream, event_name)
      @stream, @event_name, @pointer = stream, regex(event_name), 0
      @stream.register_listener(self)
    end
    
    def disconnect!
      @stream.unregister_listener(self)
    end
    
    def regex(name)
      Regexp.new("^" << Regexp.escape(name).gsub(/\\\*/, ".*") << "$")
    end
    
    def wait
      @stream.mutex.synchronize do
        @stream.signaler.wait(@stream.mutex)
      end
    end
    
    def next
      if item = @stream.at(@pointer)
        @pointer += 1
        @stream.checkin(self, @pointer)
        item.name =~ @event_name ? item : self.next
      end
    end
    
    def consume
      Thread.new do
        loop {
          (event = self.next) ? (yield event) : wait
        }
      end
    end
  end
end

# describe "a listener with events on the queue", :shared => true do
#   it "is on the queue when Listener#next is called" do
#     event = @listener.next
#     event.should be_event(:name => (@name || "awesome"), :payload => ["orchestra"])
#   end
# end
# 
# describe "Orchestra::Listener" do
#   class BeEvent
#     def initialize(options)
#       options[:thread_id] ||= Thread.current.object_id
#       @options = options
#     end
#     
#     def matches?(event)
#       if !event
#         @error = "Expected event to exist"
#       elsif event.name != @options[:name]
#         @error = "Expected event's name to be #{@options[:name].inspect}, but was #{event.name.inspect}"
#       elsif !event.time.kind_of?(Time)
#         @error = "Expected event's time to be a Time object, but was #{event.time.inspect}"
#       elsif event.thread_id != @options[:thread_id]
#         @error = "Expected event's thread id to be #{@options[:thread_id]}, but was #{event.thread_id}"
#       elsif event.payload != @options[:payload]
#         @error = "Expected event's payload to be #{@options[:payload].inspect}, but was #{event.payload.inspect}"
#       end
#       !@error
#     end
#     
#     def failure_message() @error end
#   end
#   
#   def be_event(options)
#     BeEvent.new(options)
#   end
#   
#   before(:each) do
#     @stream = Orchestra::Stream.new
#   end
#   
#   it "is instantiated with a Stream and event name" do
#     o = Orchestra::Listener.new(@stream, "eventz")
#   end
#   
#   describe "instantiated listening to the 'awesome' event" do
#     before(:each) do
#       @listener = Orchestra::Listener.new(@stream, "awesome")
#     end
#     
#     it "returns nil from #next before anything has been pushed" do
#       @listener.next.should == nil
#     end
#     
#     describe "when blocking on an event" do
#       before(:each) do
#         @test = []
#         Thread.new { @listener.wait; @test << @listener.next }
#       end
#       
#       it "initially blocks" do
#         @test.should be_empty
#       end
#       
#       it "unblocks when an event is pushed on the queue" do
#         @stream.push("awesome", "orchestra")
#         sleep 0.1
#         @test.first.should be_event(:payload => ["orchestra"], :name => "awesome")
#       end
#     end
#     
#     describe "when consuming events" do
#       before(:each) do
#         @test = []
#         @listener.consume {|event| @test << event }
#       end
#       
#       it "initially blocks" do
#         @test.should be_empty
#       end
#       
#       it "unblocks when a matching event is pushed on the queue" do
#         @stream.push "awesome", "orchestra"
#         sleep 0.1
#         @test.first.should be_event(:payload => ["orchestra"], :name => "awesome")
#       end
#       
#       it "unblocks when non-matching events are pushed, followed by matching ones" do
#         @stream.push "wot", "orchestra"
#         @stream.push "wot", "orchestra"
#         @stream.push "awesome", "orchestra"
#         sleep 0.1
#         @test.size.should == 1
#         @test.first.should be_event(:payload => ["orchestra"], :name => "awesome")
#       end
#     end
#     
#     describe "when several listeners are consuming events" do
#       before(:each) do
#         @test1, @test2 = [], []
#         listener = Orchestra::Listener.new(@stream, "wot")
#         listener.consume {|event| @test1 << event}
#         
#         listener = Orchestra::Listener.new(@stream, "awesome")
#         listener.consume {|event| @test2 << event}
#       end
#       
#       it "initially blocks" do
#         @test1.should be_empty
#         @test2.should be_empty
#       end
#       
#       it "unblocks all queues when matching events are pushed on" do
#         @stream.push "wot", "listener1"
#         @stream.push "awesome", "listener2"
#         sleep 0.1
#         @test1.size.should == 1
#         @test1.first.should be_event(:payload => ["listener1"], :name => "wot")
#         @test2.size.should == 1
#         @test2.first.should be_event(:payload => ["listener2"], :name => "awesome")
#       end      
#     end
#     
#     describe "when an event is added to the stream that matches" do
#       before(:each) do
#         @stream.push("awesome", "orchestra")
#       end
#       
#       it_should_behave_like "a listener with events on the queue"
#     end
#     
#     describe "when an event is added to the stream that doesn't match" do
#       before(:each) do
#         @stream.push("wot", "orchestra")
#       end
#       
#       it "is not on the queue when Listener#next is called" do
#         event = @listener.next
#         event.should == nil
#       end
#     end
#     
#     describe "when events are added that don't match, followed by one that does" do
#       before(:each) do
#         @stream.push("wot", "orchestra")
#         @stream.push("wot", "orchestra")
#         @stream.push("awesome", "orchestra")
#       end
#       
#       it_should_behave_like "a listener with events on the queue" 
#     end
#   end
#   
#   describe "instantiated listening to the 'awesome.*'" do
#     before(:each) do
#       @listener = Orchestra::Listener.new(@stream, "awesome.*")
#     end
#     
#     describe "when an event is added to the stream that matches" do
#       before(:each) do
#         @name = "awesome.rails"
#         @stream.push(@name, "orchestra")
#       end
#       
#       it_should_behave_like "a listener with events on the queue"
#     end
#     
#     describe "when an event is added to the stream that doesn't match" do
#       before(:each) do
#         @stream.push("awesomezrails", "orchestra")
#       end
#       
#       it "is not on the queue when Listener#next is called" do
#         event = @listener.next
#         event.should == nil
#       end      
#     end
#   end
#   
#   describe "multiple listeners and many events" do
#     before(:each) do
#       @listeners = [
#         Orchestra::Listener.new(@stream, "wot"),
#         Orchestra::Listener.new(@stream, "wotwot")
#       ]
#       
#       1000.times do |i|
#         @stream.push("wot", "payload#{i}")
#       end
#     end
#     
#     it "is safe when many events are added" do
#       @stream.at(0).should be_event(:name => "wot", :payload => ["payload#{0}"])
#     end
#     
#     it "is safe when only one listener reads through the queue" do
#       @listeners[0].consume {}
#       sleep 0.1
#       @stream.at(0).should be_event(:name => "wot", :payload => ["payload#{0}"])
#     end
#     
#     it "cleans up buckets once all listeners have read through the queue" do
#       consumers = Hash.new {|h,k| h[k] = []}
#       @listeners.each {|l| l.consume {|e| consumers[l] << e}}
#       sleep 0.1
#       @stream.at(0).should be_nil
#       consumers[@listeners[0]].size.should == 1000
#       consumers[@listeners[1]].size.should == 0
#     end
#   end
# end


stream = Orchestra::Stream.new
listener = Orchestra::Listener.new(stream, "hello")

listener.consume do |event|
end

i = 0

# trap("INT") do
#   puts i
#   p stream.at(100)
#   GC.start
# end

loop {
  stream.push("hello", "payload")
  i += 1
}