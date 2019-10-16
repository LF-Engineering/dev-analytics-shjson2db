#!/usr/bin/env ruby

require 'pry'
require 'json'
require 'mysql2'

dbg = !ENV['DBG'].nil?
fix = !ENV['FIX'].nil?
host = ENV['HOST'] || 'localhost'
user = ENV['USER'] || 'root'
pass = ENV['PASS'] || 'root'
db = ENV['DB'] || 'merged'

connect = Mysql2::Client.new(:host => host, :username => user, :password => pass, :database => db)
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
identities = []
enrollments = []
i['uidentities'].each do |uuid, data|
  binding.pry if uuid != data['uuid'] || uuid != data['profile']['uuid']
  profiles << data['profile']
  data['identities'].each do |row|
    identities << row
  end
  data['enrollments'].each do |row|
    enrollments << row
  end
end

## Profiles
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
upd = 0
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
  if !includes || diff
    if fix
      if diff
        puts "Updating #{uid}" if dbg
        updates = update.join ', '
        begin
          connect.query "update profiles set #{updates} where uuid = '#{uid}'"
        rescue
          puts "update profiles set #{updates} where uuid = '#{uid}'"
          binding.pry
        end
      else
        puts "Missing #{uid}" if dbg
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
    end
    if includes
      upd += 1
    else
      miss += 1
    end
  end
  all += 1
end
puts "Missing profiles: #{miss}/#{all}, profiles requiring updates: #{upd}" if miss > 0 || upd > 0


## Identities
result = connect.query("select id, name, email, username, source, uuid from identities")
eids = []
eidentities = {}
result.each do |row|
  id = row['id']
  eids << id
  eidentities[id] = row
end
eids = eids.sort.uniq

miss = 0
all = 0
upd = 0
identities.each do |i|
  id = i['id']
  diff = false
  includes = eids.include?(id)
  update = []
  if includes
    ei = eidentities[id]
    if i['name'] != ei['name']
      if ei['name'].nil? && !i['name'].nil?
        update << "name = '#{i['name'].gsub("'", "\\\\'")}'"
        diff = true
      end
      # puts "id: #{id}, name diff: #{ei['name']} != #{i['name']}" if !ei['name'].nil? && !i['name'].nil? && ei['name'].downcase != i['name'].downcase
    end
    if i['email'] != ei['email']
      if ei['email'].nil? && !i['email'].nil?
        update << "email = '#{i['email']}'"
        diff = true
      end
      # puts "id: #{id}, email diff: #{ei['email']} != #{i['email']}" if !ei['email'].nil? && !i['email'].nil? && ei['email'].downcase != i['email'].downcase
    end
    if i['username'] != ei['username']
      if ei['username'].nil? && !i['username'].nil?
        update << "username = '#{i['username'].gsub("'", "\\\\'")}'"
        diff = true
      end
      puts "id: #{id}, username diff: #{ei['username']} != #{i['username']}" if !ei['username'].nil? && !i['username'].nil? && ei['username'].downcase != i['username'].downcase
    end
    if i['source'] != ei['source']
      if ei['source'].nil? && !i['source'].nil?
        update << "source = '#{i['source']}'"
        diff = true
      end
      puts "id: #{id}, source diff: #{ei['source']} != #{i['source']}" if !ei['source'].nil? && !i['source'].nil?
    end
    if i['uuid'] != ei['uuid']
      if ei['uuid'].nil? && !i['uuid'].nil?
        update << "uuid = '#{i['uuid']}'"
        diff = true
      end
      # puts "id: #{id}, uuid diff: #{ei['uuid']} != #{i['uuid']}" if !ei['uuid'].nil? && !i['uuid'].nil?
    end
  end
  if !includes || diff
    if fix
      if diff
        puts "Updating #{id}" if dbg
        updates = update.join ', '
        begin
          connect.query "aupdate identities set #{updates}, last_modified = now() where id = '#{id}'"
        rescue
          puts "aupdate identities set #{updates}, last_modified = now() where id = '#{id}'"
          binding.pry
        end
      else
        puts "Missing #{id}" if dbg
        name = i['name'].nil? ? 'null' : "'#{i['name'].gsub("'", "\\\\'")}'"
        email = i['email'].nil? ? 'null' : "'#{i['email']}'"
        username = i['username'].nil? ? 'null' : "'#{i['username'].gsub("'", "\\\\'")}'"
        source = i['source'].nil? ? 'null' : "'#{i['source']}'"
        uuid = i['uuid'].nil? ? 'null' : "'#{i['uuid']}'"
        begin
          connect.query "insert into identities(id, name, email, username, source, uuid, last_modified) values('#{id}', #{name}, #{email}, #{username}, #{source}, #{uuid}, now())"
        rescue
          puts "insert into identities(id, name, email, username, source, uuid, last_modified) values('#{id}', #{name}, #{email}, #{username}, #{source}, #{uuid}, now())"
          binding.pry
        end
      end
    end
    if includes
      upd += 1
    else
      miss += 1
    end
  end
  all += 1
end
puts "Missing identities: #{miss}/#{all}, identities requiring update: #{upd}" if miss > 0 || upd > 0

## Enrollments
result = connect.query("select e.uuid, e.start, e.end, o.name as organization from enrollments e, organizations o where e.organization_id = o.id")
ks = []
eenrollments = {}
result.each do |row|
  uuid = row['uuid']
  from = row['start']
  to = row['end']
  key = [uuid, from, to]
  ks << key
  eenrollments[key] = row
end
ks = ks.sort.uniq

binding.pry

miss = 0
all = 0
upd = 0
identities.each do |i|
  id = i['id']
  diff = false
  includes = eids.include?(id)
  update = []
  if includes
    ei = eidentities[id]
    if i['name'] != ei['name']
      if ei['name'].nil? && !i['name'].nil?
        update << "name = '#{i['name'].gsub("'", "\\\\'")}'"
        diff = true
      end
      # puts "id: #{id}, name diff: #{ei['name']} != #{i['name']}" if !ei['name'].nil? && !i['name'].nil? && ei['name'].downcase != i['name'].downcase
    end
    if i['email'] != ei['email']
      if ei['email'].nil? && !i['email'].nil?
        update << "email = '#{i['email']}'"
        diff = true
      end
      # puts "id: #{id}, email diff: #{ei['email']} != #{i['email']}" if !ei['email'].nil? && !i['email'].nil? && ei['email'].downcase != i['email'].downcase
    end
    if i['username'] != ei['username']
      if ei['username'].nil? && !i['username'].nil?
        update << "username = '#{i['username'].gsub("'", "\\\\'")}'"
        diff = true
      end
      puts "id: #{id}, username diff: #{ei['username']} != #{i['username']}" if !ei['username'].nil? && !i['username'].nil? && ei['username'].downcase != i['username'].downcase
    end
    if i['source'] != ei['source']
      if ei['source'].nil? && !i['source'].nil?
        update << "source = '#{i['source']}'"
        diff = true
      end
      puts "id: #{id}, source diff: #{ei['source']} != #{i['source']}" if !ei['source'].nil? && !i['source'].nil?
    end
    if i['uuid'] != ei['uuid']
      if ei['uuid'].nil? && !i['uuid'].nil?
        update << "uuid = '#{i['uuid']}'"
        diff = true
      end
      # puts "id: #{id}, uuid diff: #{ei['uuid']} != #{i['uuid']}" if !ei['uuid'].nil? && !i['uuid'].nil?
    end
  end
  if !includes || diff
    if fix
      if diff
        puts "Updating #{id}" if dbg
        updates = update.join ', '
        begin
          connect.query "aupdate identities set #{updates}, last_modified = now() where id = '#{id}'"
        rescue
          puts "aupdate identities set #{updates}, last_modified = now() where id = '#{id}'"
          binding.pry
        end
      else
        puts "Missing #{id}" if dbg
        name = i['name'].nil? ? 'null' : "'#{i['name'].gsub("'", "\\\\'")}'"
        email = i['email'].nil? ? 'null' : "'#{i['email']}'"
        username = i['username'].nil? ? 'null' : "'#{i['username'].gsub("'", "\\\\'")}'"
        source = i['source'].nil? ? 'null' : "'#{i['source']}'"
        uuid = i['uuid'].nil? ? 'null' : "'#{i['uuid']}'"
        begin
          connect.query "insert into identities(id, name, email, username, source, uuid, last_modified) values('#{id}', #{name}, #{email}, #{username}, #{source}, #{uuid}, now())"
        rescue
          puts "insert into identities(id, name, email, username, source, uuid, last_modified) values('#{id}', #{name}, #{email}, #{username}, #{source}, #{uuid}, now())"
          binding.pry
        end
      end
    end
    if includes
      upd += 1
    else
      miss += 1
    end
  end
  all += 1
end
puts "Missing identities: #{miss}/#{all}, identities requiring update: #{upd}" if miss > 0 || upd > 0

# binding.pry
