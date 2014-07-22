# Copyright 2014 Cognitect. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Transit
  # Transit::Reader converts incoming transit data into appropriate
  # values/objects in Ruby.
  # @see https://github.com/cognitect/transit-format
  class Reader
    # @api private
    class JsonUnmarshaler
      class ParseHandler
        def initialize(decoder)
          @cache = RollingCache.new
          @decoder = decoder
        end

        def each(&block)  @yield_v = block end
        def hash_start()  {} end
        def array_start() [] end

        def add_value(v)
          if @yield_v
            @yield_v[decode(v, false)]
          end
        end

        def decode(v, as_map_key)
#          puts "********** decode"
#          p v, as_map_key
          case v
          when Array
            case k = v.first
            when MAP_AS_ARRAY
              decode(Hash[*v[1..-1]], as_map_key)
            when Decoder::Tag
              if handler = @decoder.handlers[k]
                handler.from_rep(v[1])
              else
                @decoder.default_handler.from_rep(k,v[1])
              end
            else
              v
            end
          when Hash
            if Decoder::Tag === (tag = v.keys.first)
              val = v.values.first
              if handler = @decoder.handlers[tag]
                handler.from_rep(val)
              else
                @decoder.default_handler.from_rep(tag,val)
              end
            else
              v
            end
          else
            @decoder.decode(v, @cache, as_map_key)
          end
        end

        def hash_set(h,k,v)
#          puts "********** hash_set"
#          p h,k,v
          h.store(decode(k, true), decode(v, false))
        end

        def array_append(a,v)
#          puts "********** array_append"
#          p a,v
          case a.size
          when 0
            a << decode(v, true)
          when 1
            a << decode(v, MAP_AS_ARRAY == a[0])
          else
            a << decode(v, false)
          end
        end

        def error(message, line, column)
          raise Exception.new(message, line, column)
        end
      end

      def initialize(io, opts)
        @io = io
        @decoder = Transit::Decoder.new(opts)
        @parse_handler = ParseHandler.new(@decoder)
      end

      # @see Reader#read
      def read
        if block_given?
          @parse_handler.each {|v| yield @decoder.decode(v)}
        else
          @parse_handler.each {|v| return @decoder.decode(v)}
        end
        Oj.sc_parse(@parse_handler, @io)
      end
    end

    # @api private
    class MessagePackUnmarshaler
      def initialize(io, opts)
        @decoder = Transit::Decoder.new(opts)
        @unpacker = MessagePack::Unpacker.new(io)
      end

      # @see Reader#read
      def read
        if block_given?
          @unpacker.each {|v| yield @decoder.decode(v)}
        else
          @decoder.decode(@unpacker.read)
        end
      end
    end

    extend Forwardable

    # @!method read
    #   Reads transit values from an IO (file, stream, etc), and
    #   converts each one to the appropriate Ruby object.
    #
    #   With a block, yields each object to the block as it is processed.
    #
    #   Without a block, returns a single object.
    #
    #   @example
    #     reader = Transit::Reader.new(:json, io)
    #     reader.read {|obj| do_something_with(obj)}
    #
    #     reader = Transit::Reader.new(:json, io)
    #     obj = reader.read
    def_delegators :@reader, :read

    # @param [Symbol] format required any of :msgpack, :json, :json_verbose
    # @param [IO]     io required
    # @param [Hash]   opts optional
    # Creates a new Reader configured to read from <tt>io</tt>,
    # expecting <tt>format</tt> (<tt>:json</tt>, <tt>:msgpack</tt>).
    #
    # Use opts to register custom read handlers, associating each one
    # with its tag.
    #
    # @example
    #
    #   json_reader                 = Transit::Reader.new(:json, io)
    #   # ^^ reads both :json and :json_verbose formats ^^
    #   msgpack_writer              = Transit::Reader.new(:msgpack, io)
    #   writer_with_custom_handlers = Transit::Reader.new(:json, io,
    #     :handlers => {"point" => PointReadHandler})
    #
    # @see Transit::ReadHandlers
    def initialize(format, io, opts={})
      @reader = case format
                when :json, :json_verbose
                  require 'oj'
                  JsonUnmarshaler.new(io, opts)
                else
                  require 'msgpack'
                  MessagePackUnmarshaler.new(io, opts)
                end
    end
  end
end
