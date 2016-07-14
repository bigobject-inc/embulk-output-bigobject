module Embulk

  module Output

    class Bigobject < OutputPlugin

      require 'rest-client'
      require 'json'
	  require 'socket'

      Plugin.register_output("bigobject", self)

      def self.configure(config, schema, count)
        # configuration code:
        task = {
          "host"           => config.param("host",           :string,  :default => "localhost"), # string, optional
          "restport"       => config.param("restport",       :integer, :default => 9090),  # integer, optional
          "ncport"         => config.param("ncport",         :integer, :default => 9091),  # integer, optional
          "table"          => config.param("table",          :string),        # string, required
		  "column_options" => config.param("column_options", :array,   :default => []),   
        }
        
		task
	  end 

	  def self.column_options_map(column_options) 
		(column_options || {}).map do |column_option| 
		  [column_option['name'], column_option] 
		end.to_h 
	  end

      def self.transaction(config, schema, count, &control)
		task = self.configure(config, schema, count)
		task['co_map'] = self.column_options_map task["column_options"]
        task['rest_uri'] = "http://#{task['host']}:#{task['restport']}/cmd".freeze
		task['ttl_counter'] = 0
        
        Embulk.logger.debug { "Transaction #{count}" }
        # resumable output:
        # resume(task, schema, count, &control)

        # Create-Table if it does not exist
        response = rest_exec(task['rest_uri'], "desc #{task['table']}") # check table
        if response["Status"] == 0 then # the table exists
          Embulk.logger.debug { "#{response}" }
        elsif response["Status"] == -11 then # the table does not exist
          #response = rest_exec(task['rest_uri'], "#{create_botable_stmt("#{task['table']}",schema, task['co_map'])}")
          response = rest_exec(task['rest_uri'], "#{create_botable_stmt("#{task['table']}",schema, task["column_options"])}")
          if response["Status"] != 0 then 
            Embulk.logger.error { "#{response}" }
            raise "Create table #{task['table']} in BigObject Failed"
          end
          Embulk.logger.info { "embulk-output-bigobject: Create table #{task['table']}" }
        else # should not be here
          Embulk.logger.error { "#{response}" }
          raise "Please check table #{task['table']} in BigObject First"  
        end

        # non-resumable output:
        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

	  def safe_io_puts(buff)
		@@io ||= create_shared_io
		@@mutext ||= Mutex.new
        @@mutext.synchronize do 
		  @@io.puts buff
		end
	  end

	  def create_shared_io
		io = TCPSocket.new @task['host'], @task['ncport']
		#io = File.new "out.dump", "w"
		io.write "csv\x01"
		io.puts @task['table']
		io
	  end

      #def self.resume(task, schema, count, &control)
      #  task_reports = yield(task)
      #
      #  next_config_diff = {}
      #  return next_config_diff
      #end

      def initialize(task, schema, index)
		super

        # Embulk.logger.debug { "Initialize #{index}" }
        # initialization code:
        @table = task["table"]
        @counter = 0 
      end

      def close
      end

      def add(page)
        # output code:
        
        #data = Array.new
        values = Array.new
		count = 0
        
        page.each do |records|
          values = []
          records.each do |row| values << "\"#{row.to_s.gsub(/\"/,"\"\"")}\"" end
          #data.push("(#{values.join(",")})")
		  safe_io_puts "#{values.join(",")}"
		  count += 1
        end

        @counter += count
        @task['ttl_counter'] += count

      end

      def finish
      end

      def abort
        raise "Please Check BigObject"
      end

      def commit
        task_report = {
          "records" => @counter
        }
        return task_report
      end

      def self.rest_exec(uri, stmt)
        begin
          response = RestClient.post uri, { "Stmt" => "#{stmt}" }.to_json, :content_type => :json, :accept => :json, :timeout => 2
          JSON.parse(response.body)
        rescue RestClient::Exception => e
          #Embulk.logger.error { "RestClient: #{e.http_code}, #{e.message}, response: #{e.response}" }
          Embulk.logger.warn { "Timeout: statement: #{stmt}" }
          begin
            response = RestClient.post uri, { "Stmt" => "#{stmt}" }.to_json, :content_type => :json, :accept => :json, :timeout => 4
            JSON.parse(response.body)
          rescue RestClient::Exception => e2
            Embulk.logger.error { "RestClient: #{e2.http_code}, #{e2.message}, response: #{e2.response}" }
          end
	    rescue JSON::Exception => e
          Embulk.logger.error { "JSON: #{e.message}" }
        end
      end

      def self.create_botable_stmt(tbl,schema, cos)
		val_array = Array.new
		schema.each do |c|
		  co = cos[c.index] || {}
		  Embulk.logger.debug {"#{c.index}, #{c.name}, #{co}"}
		  val_array.push "#{co["name"] || c.name} #{to_bigobject_column_type(c.type.to_s, c.format.to_s, co)}" 
		end
	    bo_table_schema = val_array.join(',')
		Embulk.logger.debug {"schema (#{schema.class}): #{schema}"}
		Embulk.logger.debug {"schema: #{bo_table_schema}"}
		keys = Array.new
		cos.each do |co|
		  keys.push co["name"] if co["is_key"] 
		end
		if keys.length == 0
          "CREATE TABLE #{tbl} (#{bo_table_schema})"
		else
          "CREATE TABLE #{tbl} (#{bo_table_schema} KEY(#{keys.join(',')}))"
		end
      end

      def self.to_bigobject_column_type(type, format, co)
		co = co || {}
		Embulk.logger.debug {"type: #{type}, format #{format}, option #{co}"}
		return co["type"] if co["type"]

        case type
        when 'long'
          botype = :INT64
        when 'boolean'
          botype = :INT8
        when 'string'
          botype = :STRING
        when 'double'
          botype = :DOUBLE
        when 'timestamp'
          if format.include? "%H" 
            botype = :DATETIME32
          else
            botype = :DATE32
          end
        end
        botype
      end

    end

  end
end
