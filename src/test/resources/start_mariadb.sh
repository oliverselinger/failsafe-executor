#!/bin/bash

docker run -p 3306:3306 --name failsafe_mariadb -e MARIADB_USER=failsafe -e MARIADB_PASSWORD=failsafe -e MARIADB_ROOT_PASSWORD=failsafe -e MARIADB_DATABASE=failsafe -d mariadb:10.4