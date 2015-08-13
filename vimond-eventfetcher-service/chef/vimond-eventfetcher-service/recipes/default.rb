#
# Cookbook Name:: vimond_eventfetcher_service
# cookbook:: default
#
# Copyright 2013, Vimond
#
# All rights reserved - Do Not Redistribute
#

user = node[:vimond_eventfetcher_service][:user]
user_home = node[:vimond_eventfetcher_service][:user_home]
group = node[:vimond_eventfetcher_service][:group]
version =  node[:vimond_eventfetcher_service][:version]
#
host = node[:vimond_eventfetcher_service][:host]
repo = node[:vimond_eventfetcher_service][:repo]
service_name = node[:vimond_eventfetcher_service][:name]
artifact_name = node[:vimond_eventfetcher_service][:artifact_name]
groupid = node[:vimond_eventfetcher_service][:groupid]

log_folder = node[:vimond_eventfetcher_service][:log_folder]

service_jar_url = "#{host}/#{repo}/#{groupid}/#{artifact_name}/#{version}/#{artifact_name}-#{version}.jar"

service_folder = node[:vimond_eventfetcher_service][:service_folder]
local_service_file = "#{service_folder}/#{service_jar_url.split('/')[-1]}"

node.set[:vimond_eventfetcher_service][:host_ipaddress] = node[:ipaddress]
template_vars =  node[:vimond_eventfetcher_service]

puts node[:vimond_eventfetcher_service].inspect
puts service_jar_url

# Create group
group group do
  action :create
end

# Create user
user user do
  comment 'vimond-eventfetcher-service user'
  group group
  home user_home
  shell '/bin/bash'
  system true
  action :create
end

# Create base folder
[user_home,service_folder,log_folder].each do |folder|
  directory folder do
    owner user
    group group
    mode '0755'
    recursive true
    action :create
  end
end


# Download file
remote_file local_service_file do
  source service_jar_url
  group group
  owner user
  action :create_if_missing
end

link "#{service_folder}/#{service_name}.jar" do
    to local_service_file
    group group
    owner user
    mode '0440'
end

template "#{service_folder}/config.yml" do
  mode 0440
  owner user
  group group
  variables(template_vars)
end


template "upstart-#{service_name}.conf" do
  source 'upstart.conf.erb'
  path "/etc/init/#{service_name}.conf"
  mode 0644
  owner 'root'
  group 'root'
  variables(template_vars)
end


template "jvm-#{service_name}.conf" do
  source 'jvm.conf.erb'
  path "#{service_folder}/jvm.conf"
  mode 0644
  owner user
  group group
  variables(template_vars)
end

service "#{service_name} start" do
  provider Chef::Provider::Service::Upstart
  service_name service_name
  supports :status => true, :restart => true, :stop => true
  action [ :enable, :start ]
end
