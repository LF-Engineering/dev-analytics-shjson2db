#!/usr/bin/env ruby

require 'pry'
require 'json'
require 'mysql2'

dbg = !ENV["DBG"].nil?

connect = Mysql2::Client.new(:host => "localhost", :username => "root", :password => "root", :database => "merged")
result = connect.query("select name from organizations")
eorgs = []
result.each { |row| eorgs << row['name'] }
eorgs = eorgs.sort.uniq

o = JSON.parse File.read 'orgs.json'
orgs = o['organizations'].keys
orgs = orgs.sort.uniq

miss = 0
all = 0
orgs.each do |org|
  unless eorgs.include?(org)
    puts "Missing #{org}" if dbg
    miss += 1
  end
  all += 1
end
puts "Missing orgs: #{miss}/#{all}" if miss > 0

i = JSON.parse File.read 'identities.json'
bl = i['blacklist'].sort.uniq

result = connect.query("select excluded from matching_blacklist")
ebl = []
result.each { |row| ebl << row['excluded'] }
ebl = ebl.sort.uniq

miss = 0
all = 0
bl.each do |b|
  unless ebl.include?(b)
    puts "Missing #{b}" if dbg
    miss += 1
  end
  all += 1
end
puts "Missing black list entries: #{miss}/#{all}" if miss > 0

uids = i['uidentities'].keys

result = connect.query("select uuid from uidentities")
euids = []
result.each { |row| euids << row['uuid'] }
euids = euids.sort.uniq

miss = 0
all = 0
uids.each do |uid|
  unless euids.include?(uid)
    puts "Missing #{uid}" if dbg
    miss += 1
  end
  all += 1
end
puts "Missing uids: #{miss}/#{all}" if miss > 0

profiles = []
i['uidentities'].each do |uuid, data|
  binding.pry if uuid != data['uuid'] || uuid != data['profile']['uuid']
  profiles << data['profile']
end

binding.pry
