class ChunkedArray
  def initialize(bucket_size = 500)
    @array, @bucket_size, @offset = [[]], bucket_size, 0
    @mutex = Mutex.new
  end
  
  def push(item)
    @mutex.synchronize do
      if @array.last.nil? || @array.last.size == @bucket_size
        @array << []
      end
      @array.last << item
    end
  end
  
  def [](index)
    bucket = (index / @bucket_size) - @offset
    idx = index % @bucket_size
    if bucket >= 0 && @array[bucket]
      @array[bucket][idx]
    end
  end
  
  def gc(lowest_valid_item)
    bucket = (lowest_valid_item / @bucket_size)
    if bucket > @offset
      @mutex.synchronize do
        (bucket - @offset).times { @array.shift }
        @offset = bucket
      end
    end
  end
end

describe "a chunked Array" do
  before(:each) do
    @array = ChunkedArray.new
  end
  
  it "works like an array for the first element" do
    @array.push 1
    @array[0].should == 1
  end
  
  it "works like an Array for the 750th element" do
    750.times {|i| @array.push i }
    @array[749].should == 749
  end
  
  it "continues to work after collecting a bucket" do
    750.times {|i| @array.push i }
    @array.gc(501)
    @array[0].should == nil
    @array[749].should == 749
  end
end
