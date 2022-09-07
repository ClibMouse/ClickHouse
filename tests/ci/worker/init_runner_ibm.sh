#!/usr/bin/env bash
set -uo pipefail

####################################
#            IMPORTANT!            #
# VPC  instance should have         #
# `github-runner-type:` tag         #
# set accordingly to a runner role #
####################################

# Set API_KEY and RUNNER_TOKEN prior to running this 

echo "Running init script"
export DEBIAN_FRONTEND=noninteractive
export RUNNER_HOME=/home/ubuntu/actions-runner

export RUNNER_URL="https://github.com/ClibMouse"

# Funny fact, but metadata service has fixed IP
ACCESS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/instance_identity/v1/token?version=2022-03-08" -H "Metadata-Flavor: ibm" -d '{"expires_in": 3600}' | jq -r '(.access_token)')
IAM_TOKEN=$(curl -s -X POST "https://iam.cloud.ibm.com/identity/token" -H "content-type: application/x-www-form-urlencoded" -H "accept: application/json" -d "grant_type=urn%3Aibm%3Aparams%3Aoauth%3Agrant-type%3Aapikey&apikey=$API_KEY" | jq -r '(.access_token)')
CRN=$(curl -s -X GET "http://169.254.169.254/metadata/v1/instance?version=2022-03-08" -H "Accept:application/json" -H "Authorization: Bearer $ACCESS_TOKEN" | jq -r '(.crn)')

INSTANCE_ID=$(curl -s -X GET "http://169.254.169.254/metadata/v1/instance?version=2022-03-08" -H "Accept:application/json" -H "Authorization: Bearer $ACCESS_TOKEN" | jq -r '(.id)')
export INSTANCE_ID

# combine labels
RUNNER_TYPE=$(curl -s -X GET --header "Authorization: Bearer $IAM_TOKEN" --header "Accept: application/json" "https://tags.global-search-tagging.cloud.ibm.com/v3/tags?attached_to=$CRN" | jq -r '.items[] | select( .name | startswith("github-runner-type:")) | .name | split(":") | .[1]' | paste -s -d, - )
LABELS="self-hosted,Linux,$(uname -m),$RUNNER_TYPE"
export LABELS

while true; do
    runner_pid=$(pgrep run.sh)
    echo "Got runner pid $runner_pid"

    cd $RUNNER_HOME || exit 1
    if [ -z "$runner_pid" ]; then
        echo "Will try to remove runner"
        sudo -u ubuntu ./config.sh remove --token "$RUNNER_TOKEN" ||:

        echo "Going to configure runner"
        sudo -u ubuntu ./config.sh --url $RUNNER_URL --token "$RUNNER_TOKEN" --name "$INSTANCE_ID" --runnergroup Default --labels "$LABELS" --work _work

        echo "Run"
        sudo -u ubuntu ./run.sh &
        sleep 15
    else
        echo "Runner is working with pid $runner_pid, nothing to do"
        sleep 10
    fi
done