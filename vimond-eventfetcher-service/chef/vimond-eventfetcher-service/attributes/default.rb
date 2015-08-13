# Attributes for vimond-eventfetcher-service

default[:vimond_eventfetcher_service][:user] = 'vimond-eventfetcher'
default[:vimond_eventfetcher_service][:group] = 'vimond-eventfetcher'
default[:vimond_eventfetcher_service][:group] = 'vimond-api'
default[:vimond_eventfetcher_service][:user_home] = '/opt/vimond-eventfetcher'
default[:vimond_eventfetcher_service][:version] = '0.0.1-SNAPSHOT'
default[:vimond_eventfetcher_service][:name]='vimond-eventfetcher'
default[:vimond_eventfetcher_service][:artifact_name]='vimond-eventfetcher-service'
default[:vimond_eventfetcher_service][:service_folder]='/opt/vimond-eventfetcher'
default[:vimond_eventfetcher_service][:log_folder]='/var/log/vimond-eventfetcher'

#Shared between service and nginx
default[:vimond_eventfetcher_service][:service_port]='8132'
default[:vimond_eventfetcher_service][:admin_port]='8132'
default[:vimond_eventfetcher_service][:server_name]='eventfetcher'


# Nginx config settings
default[:vimond_eventfetcher_service][:nginx_context]='/'
default[:vimond_eventfetcher_service][:nginx_port]='9000'
default[:vimond_eventfetcher_service][:set_real_ip_from]='10.0.0.0/24'

# Installation from repo-props

default[:vimond_eventfetcher_service][:repo] = 'libs-release-local'
default[:vimond_eventfetcher_service][:host] = 'http://read-only-download-user:NTfJv7KofZZzhIP@repo.vimond.com/artifactory'
default[:vimond_eventfetcher_service][:groupid] = 'com/vimond/service'