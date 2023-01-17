#!/bin/bash

umount SM_storage_mount
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker network rm jail_network