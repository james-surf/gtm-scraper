import json

import psycopg2

# Database connection parameters
DB_CONFIG = {
    "dbname": "surfside",
    "user": "admin",
    "password": "password",
    "host": "localhost",
    "port": "5432",
}


# SQL statements
CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    path TEXT,
    account_id TEXT,
    container_id TEXT,
    workspace_id TEXT,
    tag_id TEXT,
    name TEXT,
    type TEXT,
    parameters JSONB,
    fingerprint TEXT,
    firing_trigger_ids TEXT[],
    tag_firing_option TEXT,
    tag_manager_url TEXT,
    paused BOOLEAN DEFAULT FALSE,
    consent_settings JSONB
);
"""

INSERT_QUERY = """
INSERT INTO tags (
    path, account_id, container_id, workspace_id, tag_id, name, type, parameters, fingerprint,
    firing_trigger_ids, tag_firing_option, tag_manager_url, paused, consent_settings
)
VALUES (
    %(path)s, %(accountId)s, %(containerId)s, %(workspaceId)s, %(tagId)s, %(name)s, %(type)s, %(parameters)s, %(fingerprint)s,
    %(firingTriggerId)s, %(tagFiringOption)s, %(tagManagerUrl)s, %(paused)s, %(consentSettings)s
);
"""


def push_to_db(data):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create table
        cur.execute(CREATE_TABLE_QUERY)
        conn.commit()

        # Insert data
        for tag in data:
            cur.execute(
                INSERT_QUERY,
                {
                    "path": tag.get("path"),
                    "accountId": tag.get("accountId"),
                    "containerId": tag.get("containerId"),
                    "workspaceId": tag.get("workspaceId"),
                    "tagId": tag.get("tagId"),
                    "name": tag.get("name"),
                    "type": tag.get("type"),
                    "parameters": json.dumps(tag.get("parameter", [])),
                    "fingerprint": tag.get("fingerprint"),
                    "firingTriggerId": tag.get("firingTriggerId", []),
                    "tagFiringOption": tag.get("tagFiringOption"),
                    "tagManagerUrl": tag.get("tagManagerUrl"),
                    "paused": tag.get("paused", False),
                    "consentSettings": json.dumps(tag.get("consentSettings", {})),
                },
            )

        # Commit and close
        conn.commit()
        cur.close()
        conn.close()
        print("Data pushed successfully!")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    with open("../tags.json", "r") as f:
        data = json.load(f)
        push_to_db(data)
