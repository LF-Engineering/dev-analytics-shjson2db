#!/usr/bin/env ruby

require 'pry'
require 'json'
require 'mysql2'

connect = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "root", :database => "merged")
result = connect.query("select name from organizations")
eorgs = []
result.each { |row| eorgs << row['name'] }
eorgs = eorgs.sort.uniq

o = JSON.parse File.read 'orgs.json'
bl = o['blacklist']
orgs = o['organizations'].keys
orgs = orgs.sort.uniq

miss = 0
orgs.each do |org|
  unless eorgs.include?(org)
    puts "Missing #{org}" 
    miss += 1
  end
end
puts "Missing orgs: #{miss}"
