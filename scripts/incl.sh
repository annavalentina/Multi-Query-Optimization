#!/usr/bin/env bash


password="my_sudoer_password"
workers=(anaconda panther deer unicorn pegasus cerberos)

declare -A adapters=(["anaconda"]="enp8s0" ["panther"]="enp3s1" ["deer"]="enp4s0" ["rabbit"]="enp10s1" ["unicorn"]="enp1s0f1" ["pegasus"]="enp1s0f1" ["cerberos"]="enp1s0f1")
declare -A ips=(["anaconda"]="10.0.0.1" ["panther"]="10.0.0.2" ["deer"]="10.0.0.2" ["rabbit"]="10.0.0.3" ["unicorn"]="10.0.0.4" ["pegasus"]="10.0.0.5" ["cerberos"]="10.0.0.6")