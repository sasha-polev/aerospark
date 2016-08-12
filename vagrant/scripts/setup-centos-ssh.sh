#!/bin/bash
START=3
TOTAL_NODES=3

while getopts p:s:t: option
do
	case "${option}"
	in
		p) IP_PREFIX=${OPTARG};;
		s) START=${OPTARG};;
		t) TOTAL_NODES=${OPTARG};;
	esac
done
#echo "total nodes = $TOTAL_NODES"

function installSSHPass {
	yum -y install sshpass
}

function overwriteSSHCopyId {
	cp -f $RES_SSH_COPYID_MODIFIED /usr/bin/ssh-copy-id
}

function setupHosts {
	echo "modifying /etc/hosts file"
	for i in $(seq 1 $TOTAL_NODES)
	do 
		entry="${IP_PREFIX}${i} node${i}"
		echo "adding ${entry}"
		echo "${entry}" >> /etc/hosts
	done
}

function createSSHKey {
	echo "generating ssh key"
	ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
	cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
	cp -f $RES_SSH_CONFIG ~/.ssh
}

function sshCopyId {
	echo "executing ssh-copy-id"
	for i in $(seq $START $TOTAL_NODES)
	do 
		node="node${i}"
		echo "copy ssh key to ${node}"
		ssh-copy-id -i ~/.ssh/id_rsa.pub $node
	done
}

echo "setup ssh"
installSSHPass
createSSHKey
overwriteSSHCopyId
sshCopyId
