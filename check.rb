#!/usr/bin/env ruby

require 'pry'
require 'json'

o = JSON.parse File.read 'orgs.json'
bl = o['blacklist']
orgs = o['organizations'].keys
binding.pry
