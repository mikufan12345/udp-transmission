#!/bin/bash
echo "If any errors occur, please run as root."
cp ./udp-server.service /etc/systemd/system/udp-server.service
systemctl enable udp-server
systemctl start udp-server
echo "Startup success".