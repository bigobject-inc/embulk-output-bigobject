module Embulk
  module Output

    class Bigobject < OutputPlugin

      require 'rest-client'
      require 'json'

      Plugin.register_output("bigobject", self)

      def self.transaction(config, schema, count, &control)
        # configuration code:
        task = {
          "host" => config.param("host", :string),                     # string, required
          "port" => config.param("port", :integer, default: 9090),  # integer, optional
          "table" => config.param("table", :string),        # string, required
        }
        
        @@bo_url = "http://#{task['host']}:#{task['port']}/cmd".freeze
        @@ttlcounter = 0
        
        # resumable output:
        # resume(task, schema, count, &control)

        # Create Table if not exists
        response = send_stmt2borest("desc #{task['table']}") # check table
        if response["Status"] == 0 then # table exist
              Embulk.logger.debug { "#{response}" }
        elsif response["Status"] == -11 then # table not exist
          response = send_stmt2borest("#{create_botable_stmt("#{task['table']}",schema)}")
            if response["Status"] != 0 then 
              Embulk.logger.error { "#{response}" }
              raise "Create table #{task['table']} in BigObject Failed"
            end
        else # should not be here
          Embulk.logger.error { "#{response}" }
          raise "Please check table #{task['table']} in BigObject First"  
        end

        # non-resumable output:
        task_reports = yield(task)
        next_config_diff = {}
        return next_config_diff
      end

      #def self.resume(task, schema, count, &control)
      #  task_reports = yield(task)
      #
      #  next_config_diff = {}
      #  return next_config_diff
      #end

      def init
        # initialization code:
        @table = task["table"]
        @counter = 0 
      end

      def close
      end

      def add(page)
        # output code:
        
        data = Array.new
        values = Array.new
        
        page.each do |records|
          values = []
          records.each do |row| values << "#{row}".to_json end
          data.push("(#{values.join(",")})")
        end

        @counter += data.length
        @@ttlcounter += data.length
        
        Embulk.logger.trace { "INSERT INTO #@table VALUES #{data.join(",")}" }
        rsp = self.class.send_stmt2borest("INSERT INTO #@table VALUES #{data.join(",")}")
        if rsp["Status"] == 0 then
           Embulk.logger.debug { "add #{data.length} to BigObject - #@counter | #@@ttlcounter" }
        else
           Embulk.logger.error { "add #{data.length} to BigObject failed!!" }
          abort
        end

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

      def self.send_stmt2borest(stmt)
        begin
          response = RestClient.post @@bo_url, { "Stmt" => "#{stmt}" }.to_json, :content_type => :json, :accept => :json
          JSON.parse(response.body)
        rescue Exception => e
          Embulk.logger.error { e }
        end
      end

      def self.create_botable_stmt(tbl,schema)
        bo_table_schema = schema.map {|column| "#{column.name} #{to_bigobject_column_type(column.type.to_s, column.format.to_s)}" }.join(',')
        "CREATE TABLE #{tbl} (#{bo_table_schema})"
      end

      def self.to_bigobject_column_type(type, format)
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
