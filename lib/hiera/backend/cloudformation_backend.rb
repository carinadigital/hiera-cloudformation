=begin
Copyright 2013-2014 FanDuel Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
=end

class Hiera
    module Backend
        class Cloudformation_backend
            TIMEOUT = 60  # 1 minute timeout for AWS API response caching

            def initialize
                begin
                    require 'aws'
                    require 'timedcache'
                    require 'json'
                rescue LoadError
                    require 'rubygems'
                    require 'aws'
                    require 'timedcache'
                    require 'json'
                end

                # Class variables
                # Data shared amongst all instance of this class.
                @@aws_config = {} # AWS access credentials from yaml.
                # Caches are shared, so that multiple AWS instances from seperate threads can share cache data
                # e.g. When a scaling event occurs.
                @@output_cache = TimedCache.new(:default_timeout => 60)    #Default timeout in 60 seconds.
                @@resource_cache = TimedCache.new(:default_timeout => 60)

                # Class instance variables
                # We want don't want two instances trying to reuse the same connection objects, and possible
                # the same connection, so give each class it's own connection objects.
                @cf = Hash.new # Variable for hash of connection options, keyed by region.

                # Check we have the AWS region.
                # This can be a static region name of an interpolated fact.
                # TODO: This should be improved so it can fallback to default environment or 
                # dot file values. 
                if not Config[:cloudformation].include?(:region) then
                    error_message = "[cloudformation_backend]: :region missing in configuration."
                    Hiera.warn(error_message)
                    raise Exception, error_message 
                end


                if Config[:cloudformation].fetch(:parse_metadata, false) then
                    debug("Will convert CloudFormation stringified metadata back to numbers or booleans.")
                    @parse_metadata = true
                else
                    @parse_metadata = false
                end
                
                debug("Using AWS access key #{Config[:cloudformation][:access_key_id]}")
                @@aws_config['access_key_id'] = Config[:cloudformation][:access_key_id]
                @@aws_config['secret_access_key'] = Config[:cloudformation][:secret_access_key]
                debug("Using AWS region #{Config[:cloudformation][:region]}")
                @@aws_config['region'] = Config[:cloudformation][:region]
                debug("Hiera cloudformation backend loaded")
            end


            def lookup(key, scope, order_override, resolution_type)
                answer = nil
                
                # Lookups can potentially come from different agents in different AWS regions.
                # Interpolate the value from hiera.yaml for this agent's region.
                 if @@aws_config.include?('region')
                    agent_region = Backend.parse_answer(@@aws_config['region'], scope)
                end

                # Idempotent connection creation of AWS connections for reuse.
                  create_connection(agent_region)

                Backend.datasources(scope, order_override) do |elem|
                    case elem
                    when /cfstack\/([^\/]+)\/outputs/
                        debug("Looking up #{agent_region} #{$1} #{key} as an output of stack.")
                        raw_answer = stack_output_query($1, key, agent_region)
                    when /cfstack\/([^\/]+)\/resources\/([^\/]+)/
                        debug("Looking up #{agent_region} #{$1} #{$2} #{key} in metadata of stack resource")
                        raw_answer = stack_resource_query($1, $2, key,agent_region)
                    else
                        #debug("#{elem} doesn't seem to be a CloudFormation hierarchy element")
                        next
                    end

                    next if raw_answer.nil?

                    if @parse_metadata then
                        raw_answer = convert_metadata(raw_answer)
                    end

                    new_answer = Backend.parse_answer(raw_answer, scope)

                    case resolution_type
                    when :array
                        raise Exception, "Hiera type mismatch: expected Array and got #{new_answer.class}" unless new_answer.kind_of? Array or new_answer.kind_of? String
                        answer ||= []
                        answer << new_answer
                    when :hash
                        raise Exception, "Hiera type mismatch: expected Hash and got #{new_answer.class}" unless new_answer.kind_of? Hash
                        answer ||= {}
                        answer = Backend.merge_answer(new_answer, answer)
                    else
                        answer = new_answer
                        break
                    end
                end

                return answer
            end


            # Ensures that connetion is created for this region in the class variable for connection.
            def create_connection(region)
              
                # If we already have a connection object then return early.
                if @cf.has_key?(region) then
                    return
                end

                debug("Creating new persistent aws connection for region #{region}.")                       
             

                if Config[:cloudformation].include?(:access_key_id) and Config[:cloudformation].include?(:secret_access_key) and Config[:cloudformation].include?(:region) then
            
                    # Check this is a valid aws region.
                    if not is_aws_region_name?(region)
                        # If we don't have a region specified then the cloudformation endpoint will be malformed
                        # resulting in networking errors.
                        # Fail now with a proper error mesage.
                        error_message = "[cloudformation_backend]: AWS Region #{region} is invalid."
                            Hiera.warn(error_message)
                        raise Exception, error_message
                    end
            
                    # Create an AWS connecton object for this region using given credentials
                    @cf[region] = AWS::CloudFormation.new(
                    :access_key_id => @@aws_config['access_key_id'],
                    :secret_access_key => @@aws_config['secret_access_key'],
                    :region => region
                    )
                elsif Config[:cloudformation].include?(:profile) and Config[:cloudformation].include?(:region) then
                        # Create an AWS connecton object from a profile
                        @cf[region] = AWS::CloudFormation.new(
                        :profile => @@aws_config['profile'],
			 :region => region
                        )
		else Config[:cloudformation].include?(:region) then
                    	# Create an AWS connecton object using Instance Role and a region
                    	@cf[region] = AWS::CloudFormation.new(
		    	:region => region
		    	)
            end


            def stack_output_query(stack_name, key, region)
                outputs = @@output_cache.get(region+stack_name)

                if outputs.nil? then
                    debug("#{stack_name} outputs not cached, fetching...")
                    begin
                         outputs = @cf[region].stacks[stack_name].outputs
                    rescue AWS::CloudFormation::Errors::ValidationError
                        debug("Stack #{stack_name} outputs can't be retrieved")
                        outputs = []  # this is just a non-nil value to serve as marker in cache
                    end
                    @@output_cache.put(region+stack_name, outputs, TIMEOUT)
                end

                output = outputs.select { |item| item.key == key }

                return output.empty? ? nil : output.shift.value
            end


            def stack_resource_query(stack_name, resource_id, key, region)
                metadata = @@resource_cache.get({:stack => region+stack_name, :resource => resource_id})

                if metadata.nil? then
                    debug("#{stack_name} #{resource_id} metadata not cached, fetching")
                    begin
                        metadata = @cf[region].stacks[stack_name].resources[resource_id].metadata
                    rescue AWS::CloudFormation::Errors::ValidationError
                        # Stack or resource doesn't exist
                        debug("Stack #{stack_name} resource #{resource_id} can't be retrieved")
                        metadata = "{}" # This is just a non-nil value to serve as marker in cache
                    end
                    @@resource_cache.put({:stack => region+stack_name, :resource => resource_id}, metadata, TIMEOUT)
                end

                if metadata.respond_to?(:to_str) then
                    data = JSON.parse(metadata)

                    if data.include?('hiera') then
                        return data['hiera'][key] if data['hiera'].include?(key)
                    end
                end

                return nil
            end


            def convert_metadata(json_object)
                if json_object.is_a?(Hash) then
                    # convert each value of a Hash
                    converted_object = {}
                    json_object.each do |key, value|
                        converted_object[key] = convert_metadata(value)
                    end
                    return converted_object
                elsif json_object.is_a?(Array) then
                    # convert each item in an Array
                    return json_object.map { |item| convert_metadata(item) }
                elsif json_object == "true" then
                    # Boolean literals
                    return true
                elsif json_object == "false" then
                    return false
                elsif json_object == "null" then
                    return nil
                elsif /^-?([1-9]\d*|0)(.\d+)?([eE][+-]?\d+)?$/.match(json_object) then
                    # Numeric literals
                    if json_object.include?('.') then
                        return json_object.to_f
                    else
                        return json_object.to_i
                    end
                else
                    return json_object
                end
            end


            # Custom function to wrap our debug messages to make them easier to find in the output.
            def debug(message)
              Hiera.debug("[cloudformation_backend]: #{message}")
            end


            # Check region name is a valid AWS regoion
            def is_aws_region_name?(name)
                   # Make an array of valid AWS regions.
                 aws_region_names = []
                 AWS.regions.each do |aws_region|
                    aws_region_names.push(aws_region.name)
                end

                # Check this is a valid aws region.
                if aws_region_names.include?(name)
                    return true
                else
                    return false
                end
            end

        end
    end
end
