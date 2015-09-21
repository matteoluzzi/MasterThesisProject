#!/bin/bash

scp -r -i ../.aws/keys/matteo_ireland.pem ubuntu@54.72.127.122:/var/files/batch .
scp -r -i ../.aws/keys/matteo_ireland.pem ubuntu@52.19.119.237:/home/ubuntu/logs .
scp -r -i ../.aws/keys/matteo_ireland.pem ubuntu@54.72.127.122:/var/log/storm .
scp -r -i ../.aws/keys/matteo_ireland.pem ubuntu@52.18.81.98:/var/log/storm .
scp -r -i ../.aws/keys/matteo_ireland.pem ubuntu@event-fetcher:/var/log/vimond-eventfetcher-service/vimond-eventfetcher-service.log .