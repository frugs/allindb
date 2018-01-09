import base64
import concurrent.futures
import gzip
import os
import pickle
import random

import pyrebase

import allindb.blizzard
import allindb.discord

CLIENT_ID = os.getenv("BATTLE_NET_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("BATTLE_NET_CLIENT_SECRET", "")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
GUILD_ID = os.getenv("GUILD_ID", "")
FULL_MEMBER_ROLE_ID = os.getenv("FULL_MEMBER_ROLE_ID", "")
API_KEY = os.getenv("BATTLE_NET_API_KEY", "")
FIREBASE_CONFIG = os.getenv("FIREBASE_CONFIG", "")
POOL_SIZE = int(os.getenv("POOL_SIZE", "32"))
REGIONS = ["us", "eu"]


def open_db_connection() -> pyrebase.pyrebase.Database:
    config = pickle.loads(gzip.decompress(base64.b64decode(FIREBASE_CONFIG)))

    firebase = pyrebase.initialize_app(config)
    return firebase.database()


def for_each_member(member_key: str):
    db = open_db_connection()

    access_tokens_per_region, current_season_id_per_region = \
        allindb.blizzard.get_access_token_and_current_season_per_region(CLIENT_ID, CLIENT_SECRET)

    allindb.blizzard.update_characters_for_member(
        db, API_KEY, access_tokens_per_region, current_season_id_per_region, member_key)
    print("updated characters for member with id " + member_key)

    allindb.blizzard.update_ladder_summary_for_member(db, current_season_id_per_region, member_key)
    print("Updated ladder summary for member with id " + member_key)

    allindb.discord.update_discord_info_for_member(db, DISCORD_BOT_TOKEN, GUILD_ID, FULL_MEMBER_ROLE_ID, member_key)
    print("Updated discord info for member with id " + member_key)


def main():

    with concurrent.futures.ThreadPoolExecutor(POOL_SIZE) as executor:
        db = open_db_connection()

        member_keys = list(db.child("members").shallow().get().val())
        if not member_keys:
            member_keys = []
        else:
            random.shuffle(member_keys)

        concurrent.futures.wait([executor.submit(for_each_member, member) for member in member_keys])

    print("update complete.")


if __name__ == "__main__":
    main()
