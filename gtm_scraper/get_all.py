import asyncio
import json
import random
from typing import Dict, List

from aiohttp import ClientResponseError, ClientSession
from googleapiclient.discovery import build
from httplib2 import Credentials
from tqdm import tqdm

API_URL = "https://tagmanager.googleapis.com/tagmanager/v2"


async def fetch_with_retry(
    session: ClientSession, url: str, token: str, retries: int = 8
):
    """
    Fetches a URL with retries and exponential backoff.
    """
    headers = {"Authorization": f"Bearer {token}"}
    response = None
    max_delay = 240  # Maximum backoff delay in seconds, dont use anymore rly

    while True:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 429:  # Rate limit error
                    raise ClientResponseError(
                        response.request_info, response.history, status=429
                    )
                response.raise_for_status()
                data = await response.json()
                return data
        except ClientResponseError as e:
            if e.status == 429 and retries > 0:
                # Calculate exponential backoff with jitter
                backoff_time = min(60, max_delay) + random.uniform(
                    0, 2
                )  # Backoff time is 60 seconds, very long but some serious rate limits here
                tqdm.write(
                    f"Rate limit hit for {url}. Retrying in {backoff_time:.2f} seconds..."
                )
                # print(e)
                # print(f"Rate limit hit. Retrying in {backoff_time:.2f} seconds...")
                await asyncio.sleep(backoff_time)
                return await fetch_with_retry(session, url, token, retries - 1)
            else:
                raise e


def get_gtm_accounts(credentials: Credentials) -> List[Dict]:
    """
    Retrieves all GTM accounts with pagination.
    """
    service = build("tagmanager", "v2", credentials=credentials)
    accounts = []
    page_token = None

    while True:
        params = {"pageToken": page_token} if page_token else {}
        response = service.accounts().list(**params).execute()
        accounts.extend(response.get("account", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return accounts


async def fetch_containers(
    session: ClientSession,
    url: str,
    token: str,
) -> List[dict]:
    containers = []
    page_token = None

    while True:
        response = await fetch_with_retry(session, url, token)
        containers.extend(response.get("container", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return containers


async def fetch_workspace(
    session: ClientSession,
    url: str,
    token: str,
) -> List[dict]:
    workspaces = []
    page_token = None

    while True:
        response = await fetch_with_retry(session, url, token)
        workspaces.extend(response.get("workspace", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return workspaces


def filter_tags(tags: List[dict]):
    surf_tags = []
    for t in tags:
        if "surf" in json.dumps(t).lower():  # Filter tags with "surf" in them
            surf_tags.append(t)

    return surf_tags


async def fetch_surfside_tags(
    session: ClientSession, url: str, token: str
) -> List[dict]:
    tags = []
    page_token = None

    while True:
        response = await fetch_with_retry(session, url, token)
        tags.extend(response.get("tag", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return filter_tags(tags)


async def get_tags_from_account(token: str, account: str) -> List[Dict]:
    # try:
    all_tags = []
    async with ClientSession() as session:
        url = f"{API_URL}/{account}/containers"
        containers = await fetch_containers(session, url, token)

        for container in containers:
            url = f"{API_URL}/{container['path']}/workspaces"
            workspaces = await fetch_workspace(session, url, token)

            for workspace in workspaces:
                url = f"{API_URL}/{workspace['path']}/tags"
                tags = await fetch_surfside_tags(session, url, token)

                all_tags.extend(tags)

    return all_tags


# except Exception as e:
#     print(f"Error: {e}")
#     print(f"Account: {account}")
