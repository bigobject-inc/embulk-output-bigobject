module Embulk
  module Output

    class Bigobject < OutputPlugin
      require 'logger'
      require 'net/http'
      require 'json'

      Plugin.register_output("bigobject", self)

      def self.transaction(config, schema, count, &control)
        # configuration code:
        task = {
          "host" => config.param("host", :string),                     # string, required
          "port" => config.param("port", :integer, default: 9090),  # integer, optional
          "table" => config.param("table", :string),        # string, required
        }
        @@log = Logger.new(STDOUT)
        @@tblexist = false
        @@ttlcounter = 0
        # resumable output:
        # resume(task, schema, count, &control)

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

        @host = task["host"]
        @port = task["port"]
        @table = task["table"]

        @uri = URI.parse("http://#@host:#@port/cmd")
        @http = Net::HTTP.new(@uri.host, @uri.port)
        @counter = 0
        
        if !istbexist(@table)
          if !createtbl(@table)
            abort
          end
        end
      end

      def close
        @http.finish
      end

      def add(page)
        # output code:
        
        data = Array.new
        values = Array.new
        
        page.each do |records|
          values = []
          records.each do |row| values << row.to_json end
          data.push("(#{values.join(",")})")
        end
        @counter += data.length
        @@ttlcounter += data.length

        rsp = send_stmt("INSERT INTO #@table VALUES #{data.join(",")}")
        if rsp["Status"] == 0 then
          @@log.info "add #{data.length} to BigObject - #@counter | #@@ttlcounter"
        else
          @@log.error "add #{data.length} to BigObject failed!!"
          abort
        end

      end

      def finish
      end

      def abort
        raise "Please Check BigObject"
      end

      def commit
        task_report = {}
        return task_report
      end

private
        def send_stmt(stmt)
          begin
            params = {"stmt" => "#{stmt}"}
            json_headers = {"Content-Type" => "application/json", "Accept" => "application/json"}
            response = @http.post(@uri.path, params.to_json, json_headers)
            return JSON.parse(response.body)
          rescue Exception => e
            @@log.error e
            abort
          end
          
        end
  
        def istbexist(tbl)
          if !@@tblexist then
            stmt = "desc #{tbl}"
            rsp = send_stmt(stmt)
            rsp["Status"] == 0 ? @@tblexist = true : false
          end
          return @@tblexist
        end
  
        def createtbl(tbl)
          rsp = send_stmt("CREATE TABLE #{tbl} asdf (#{change2botype(schema).join(",")})")
          rsp["Status"] == 0 ? @@tblexist = true : false
          return @@tblexist
        end
  
        def change2botype(schema)
          colntype = Array.new
          schema.each do |s|
            botype = ""
            case s[2]
              when /long/
                botype = :INT64
              when /boolean/
                botype = :INT8
              when /string/
                botype = :STRING
              when /double/
                botype = :DOUBLE
              when /timestamp/
                if s[3].include? "%H" 
                  botype = :DATETIME32
                else
                  botype = :DATE32
                end
            end
            
            colntype << "'#{s[1]}' #{botype}"
          end
          return colntype
        end 

    end

  end
end
