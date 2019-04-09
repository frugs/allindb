import concurrent.futures
import json
import os
import random

import firebase_admin
import firebase_admin.credentials
from firebase_admin.db import reference

import allindb.blizzard
import allindb.discord

CLIENT_ID = os.getenv("BATTLE_NET_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("BATTLE_NET_CLIENT_SECRET", "")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
GUILD_ID = os.getenv("GUILD_ID", "")
FULL_MEMBER_ROLE_ID = os.getenv("FULL_MEMBER_ROLE_ID", "")
API_KEY = os.getenv("BATTLE_NET_API_KEY", "")
FIREBASE_CONFIG = json.loads(os.getenv("FIREBASE_CONFIG", {}))
POOL_SIZE = int(os.getenv("POOL_SIZE", "32"))

firebase_admin.initialize_app(
    credential=firebase_admin.credentials.Certificate(FIREBASE_CONFIG.get("serviceAccount", {})),
    options=FIREBASE_CONFIG
)


def for_each_member(member_key: str):
    access_tokens_per_region, current_season_id_per_region = \
        allindb.blizzard.get_access_token_and_current_season_per_region(CLIENT_ID, CLIENT_SECRET)
    
    allindb.blizzard.update_characters_for_member(
        API_KEY, access_tokens_per_region, current_season_id_per_region, member_key
    )
    print("updated characters for member with id " + member_key)

    allindb.blizzard.update_ladder_summary_for_member(current_season_id_per_region, member_key)
    print("Updated ladder summary for member with id " + member_key)

    allindb.discord.update_discord_info_for_member(
        DISCORD_BOT_TOKEN, GUILD_ID, FULL_MEMBER_ROLE_ID, member_key
    )
    print("Updated discord info for member with id " + member_key)


def main():

    with concurrent.futures.ThreadPoolExecutor(POOL_SIZE) as executor:
        member_keys = list(reference().child("members").get(shallow=True))
        if not member_keys:
            member_keys = []
        else:
            random.shuffle(member_keys)

        concurrent.futures.wait(
            [executor.submit(for_each_member, member) for member in member_keys]
        )

    print("update complete.")


if __name__ == "__main__":
    main()
