#!/usr/bin/env python
import os

VAULT_PATH = os.getenv("VAULT_PATH")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
VAULT_URL = os.getenv("VAULT_URL")
VAULT_MOUNT_POINT = os.getenv("VAULT_MOUNT_POINT", "secret")