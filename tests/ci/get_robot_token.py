#!/usr/bin/env python3
import boto3  # type: ignore
import hvac # type: ignore
import os
from github import Github  # type: ignore
from env_helper import VAULT_URL, VAULT_TOKEN, VAULT_PATH, VAULT_MOUNT_POINT

def get_parameter_from_ssm(name, decrypt=True, client=None):
    if VAULT_URL:
        if not client:
            client = hvac.Client(url=VAULT_URL,token=VAULT_TOKEN)
        parameter = client.secrets.kv.v2.read_secret_version(mount_point=VAULT_MOUNT_POINT,path=VAULT_PATH)["data"]["data"][name]
    else:
        if not client:
            client = boto3.client("ssm", region_name="us-east-1")
        parameter = client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
    return parameter

def get_best_robot_token(token_prefix_env_name="github_robot_token_", total_tokens=4):
    client = None
    if VAULT_URL:
        client = hvac.Client(url=VAULT_URL,token=VAULT_TOKEN)
    else:
        client = boto3.client("ssm", region_name="us-east-1")
    tokens = {}
    for i in range(1, total_tokens + 1):
        token_name = token_prefix_env_name + str(i)
        token = get_parameter_from_ssm(token_name, True, client)
        gh = Github(token)
        rest, _ = gh.rate_limiting
        tokens[token] = rest

    return max(tokens.items(), key=lambda x: x[1])[0]
