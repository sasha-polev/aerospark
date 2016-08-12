#!/bin/bash

function disableFirewall {
	echo "disabling firewall"
	service iptables save
	service iptables stop
	chkconfig iptables off
}

echo "setup centos"

disableFirewall