#!/usr/bin/env python3
import os

S3_REGION = os.getenv("S3_REGION", "us-east-1")
VAULT_PATH = os.getenv("VAULT_PATH")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
VAULT_URL = os.getenv("VAULT_URL")
VAULT_MOUNT_POINT = os.getenv("VAULT_MOUNT_POINT", "secret")
