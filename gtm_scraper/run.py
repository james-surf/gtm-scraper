import asyncio
import json
import os
import pickle
import random
from typing import List

import requests
from aiohttp import ClientResponseError, ClientSession
from get_all import get_gtm_accounts, get_tags_from_account
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from tqdm.asyncio import tqdm_asyncio

SCOPES = [
    "https://www.googleapis.com/auth/tagmanager.edit.containers",
    "https://www.googleapis.com/auth/tagmanager.readonly",
    "https://www.googleapis.com/auth/tagmanager.manage.accounts",
]

CREDENTIALS_PICKLE_FILE = "token.pickle"


def authenticate():
    credentials = None

    if os.path.exists(CREDENTIALS_PICKLE_FILE):
        with open(CREDENTIALS_PICKLE_FILE, "rb") as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "client_secrets.json", SCOPES
            )
            credentials = flow.run_local_server(port=0)

        with open(CREDENTIALS_PICKLE_FILE, "wb") as token:
            pickle.dump(credentials, token)

    return credentials


if __name__ == "__main__":
    creds = authenticate()
    all_tags = []

    all_surfside_accounts = get_gtm_accounts(creds)

    async def process_accounts():
        all_tags = []
        tasks = [
            get_tags_from_account(creds.token, account["path"])
            for account in all_surfside_accounts[:1]
        ]

        # Use tqdm for progress tracking
        results = await tqdm_asyncio.gather(*tasks)

        for i, result in enumerate(results):
            if isinstance(result, list):
                all_tags.extend(result)
            else:
                print(f"Error in account {all_surfside_accounts[i]['path']}: {result}")

        return all_tags

    all_tags = asyncio.run(process_accounts())

    print(f"Total tags: {len(all_tags)}")

    with open("data/tags.json", "w") as f:
        json.dump(all_tags, f, indent=2)
