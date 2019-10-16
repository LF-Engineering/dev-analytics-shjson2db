#!/usr/bin/env ruby

require 'pry'
require 'json'
require 'mysql2'

dbg = !ENV["DBG"].nil?
fix = !ENV["FIX"].nil?

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
    if fix
      connect.query("insert into organizations(name) values('#{org}')")
    end
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
    if fix
      connect.query("insert into matching_blacklist(excluded) values('#{b}')")
    end
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
    if fix
      connect.query("insert into uidentities(uuid, last_modified) values('#{uid}', now())")
    end
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

result = connect.query("select uuid, country_code, email, gender, gender_acc, is_bot, name from profiles")
euids = []
eprofiles = {}
result.each do |row|
  uuid = row['uuid']
  euids << uuid
  eprofiles[uuid] = row
end
euids = euids.sort.uniq

miss = 0
all = 0
profiles.each do |p|
  uid = p['uuid']
  diff = false
  includes = euids.include?(uid)
  update = []
  if includes
    ep = eprofiles[uid]
    p['is_bot'] = 1 if p['is_bot'] === true
    p['is_bot'] = 0 if p['is_bot'] === false
    if p['country'] != ep['country_code']
      if ep['country_code'].nil? && !p['country'].nil?
        update << "country_code = '#{p['country'].gsub("'", "\\\\'")}'"
        diff = true
      end
      #puts "uid: #{uid}, country diff: #{ep['country_code']} != #{p['country']}" if dbg && ep['country_code'].nil? && !p['country'].nil?
      puts "uid: #{uid}, country diff: #{ep['country_code']} != #{p['country']}" if !ep['country_code'].nil? && !p['country'].nil? && ep['country_code'].downcase != p['country'].downcase
    end
    if p['email'] != ep['email']
      if ep['email'].nil? && !p['email'].nil?
        update << "email = '#{p['email']}'"
        diff = true
      end
      # puts "uid: #{uid}, email diff: #{ep['email']} != #{p['email']}" if !ep['email'].nil? && !p['email'].nil? && ep['email'].downcase != p['email'].downcase
    end
    if p['gender'] != ep['gender']
      if ep['gender'].nil? && !p['gender'].nil?
        update << "gender = '#{p['gender']}'"
        diff = true
      end
      puts "uid: #{uid}, gender diff: #{ep['gender']} != #{p['gender']}" if !ep['gender'].nil? && !p['gender'].nil? && ep['gender'].downcase != p['gender'].downcase
    end
    if p['gender_acc'] != ep['gender_acc']
      if ep['gender_acc'].nil? && !p['gender_acc'].nil?
        update << "gender_acc = #{p['gender_acc']}"
        diff = true
      end
      puts "uid: #{uid}, gender_acc diff: #{ep['gender_acc']} != #{p['gender_acc']}" if !ep['gender_acc'].nil? && !p['gender_acc'].nil?
    end
    if p['is_bot'] != ep['is_bot']
        if (ep['is_bot'].nil? && !p['is_bot'].nil?) || (ep['is_bot'] == 0 && p['is_bot'] == 1)
        update << "is_bot = #{p['is_bot']}"
        diff = true
      end
      puts "uid: #{uid}, is_bot diff: #{ep['is_bot']} != #{p['is_bot']}" if !ep['is_bot'].nil? && !p['is_bot'].nil?
    end
    if p['name'] != ep['name']
      if ep['name'].nil? && !p['name'].nil?
        update << "name = '#{p['name'].gsub("'", "\\\\'")}'"
        diff = true
      end
      # puts "uid: #{uid}, name diff: #{ep['name']} != #{p['name']}" if !ep['name'].nil? && !p['name'].nil? && ep['name'].downcase != p['name'].downcase
    end
  end
  diff = false
  if !includes || diff
    puts "Missing #{uid}" if dbg
    if fix
      country = p['country'].nil? ? 'null' : "'#{p['country'].gsub("'", "\\\\'")}'"
      email = p['email'].nil? ? 'null' : "'#{p['email']}'"
      gender = p['gender'].nil? ? 'null' : "'#{p['gender']}'"
      gender_acc = p['gender_acc'].nil? ? 'null' : "#{p['gender_acc']}"
      is_bot = p['is_bot'].nil? ? 'null' : "#{p['is_bot']}"
      name = p['name'].nil? ? 'null' : "'#{p['name'].gsub("'", "\\\\'")}'"
      begin
        connect.query "insert into profiles(uuid, country_code, email, gender, gender_acc, is_bot, name) values('#{uid}', #{country}, #{email}, #{gender}, #{gender_acc}, #{is_bot}, #{name})"
      rescue
        puts "insert into profiles(uuid, country_code, email, gender, gender_acc, is_bot, name) values('#{uid}', #{country}, #{email}, #{gender}, #{gender_acc}, #{is_bot}, #{name})"
        binding.pry
      end
    end
    miss += 1
  end
  all += 1
end
puts "Missing profiles: #{miss}/#{all}" if miss > 0

# binding.pry
