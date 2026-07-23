#!/bin/bash
if [ ! -d "./build" ]; then
	# make build directory
	mkdir build
fi
cd build && cmake .. && make

# restart the service (only works when running as root)
if [ "$(id -u)" -ne 0 ]; then 
    echo "Detected not running as root, elevating via sudo..."
	echo "Ctrl+C to exit if you don't need to restart the server."
	cd ..
    exec sudo "$0" "$@"
fi
systemctl stop udp-server
systemctl start udp-server
echo "Build finished and server started."