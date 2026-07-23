#!/bin/bash
if [ "$EUID" -ne 0 ]; then 
    echo "Detected not running as root, elevating via sudo..."
    exec sudo "$0" "$@"
fi

cp ./udp-server.service /etc/systemd/system/udp-server.service
systemctl enable udp-server

# build the process, so that the service works
echo "building process..."
./build.sh