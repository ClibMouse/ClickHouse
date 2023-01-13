#!/usr/bin/env python3
import logging
from dataclasses import dataclass

import boto3  # type: ignore
import hvac # type: ignore
from env_helper import VAULT_URL, VAULT_TOKEN, VAULT_PATH, VAULT_MOUNT_POINT
from github import Github
from github.AuthenticatedUser import AuthenticatedUser


@dataclass
class Token:
    user: AuthenticatedUser
    value: str
    rest: int

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

def get_best_robot_token(token_prefix_env_name="github_robot_token_"):
    client = None
    if VAULT_URL:
        client = hvac.Client(url=VAULT_URL,token=VAULT_TOKEN)
        response=client.secrets.kv.read_secret_version(
            path=VAULT_PATH,
            mount_point=VAULT_MOUNT_POINT)
        parameters = [{"Name":p} for p in response['data']['data'] if p.startswith(token_prefix_env_name)]
    else:
        client = boto3.client("ssm", region_name="us-east-1")
        parameters = client.describe_parameters(
            ParameterFilters=[
                {"Key": "Name", "Option": "BeginsWith", "Values": [token_prefix_env_name]}
            ]
        )["Parameters"]
    assert parameters
    token = None

    for token_name in [p["Name"] for p in parameters]:
        value = get_parameter_from_ssm(token_name, True, client)
        gh = Github(value, per_page=100)
        # Do not spend additional request to API by accessin user.login unless
        # the token is chosen by the remaining requests number
        user = gh.get_user()
        rest, _ = gh.rate_limiting
        logging.info("Get token with %s remaining requests", rest)
        if token is None:
            token = Token(user, value, rest)
            continue
        if token.rest < rest:
            token.user, token.value, token.rest = user, value, rest

    assert token
    logging.info(
        "User %s with %s remaining requests is used", token.user.login, token.rest
    )

    return token.value
