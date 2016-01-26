require 'net/http'
require 'json'
require 'logger'


params = {"stmt" => "desc testcsv"}
json_headers = {"Content-Type" => "application/json",
                "Accept" => "application/json"}

uri = URI.parse('http://192.168.99.100:9090/cmd')
http = Net::HTTP.new(uri.host, uri.port)

response = http.post(uri.path, params.to_json, json_headers)

@rsp = JSON.parse(response.body)

def checktbl
 	
 return @rsp["Status"] == 0 ? "YES" : "NO"
end

puts checktbl

# values = Array.new
# values2 = Array.new
# values2 << 1
# values2 << "2"
# values.push("(#{values2.join(",")})")
# values.push("id1")
# values.push(2)
# values.push("id2")
# str = "INSERT INTO #{values.join(",")}"
# log = Logger.new(STDOUT)
# puts str

#   def init
#     @http = Net::HTTP::Persistent.new 'embulk-plugin-bigobject'
#     @http.headers['Content-Type'] = {"Content-Type" => "application/json", "Accept" => "application/json"}
#     @uri = URI.parse('http://192.168.99.100:9090/cmd')
#     puts "initialized"
#   end

#   def send
#   	#uri = URI.parse('http://192.168.99.100:9090/cmd')
#     post = Net::HTTP::Post.new @uri.request_uri
#     post.body = {"stmt" => "show tables"}.to_json
#     response = @http.request @uri, post
#     puts JSON.parse(response.body)
#   end

#  def shutdown
#     @http.shutdown
#     puts "shutdown"
#   end

# init
# send
# shutdown



