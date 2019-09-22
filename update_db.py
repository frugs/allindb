import concurrent.futures
import itertools
import functools
import json
import os
import random

import firebase_admin
import firebase_admin.credentials
from firebase_admin.db import reference

import allindb.blizzard
import allindb.discord
import allindb.executor

CLIENT_ID = os.getenv("BATTLE_NET_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("BATTLE_NET_CLIENT_SECRET", "")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
GUILD_ID = os.getenv("GUILD_ID", "")
FULL_MEMBER_ROLE_ID = os.getenv("FULL_MEMBER_ROLE_ID", "")
API_KEY = os.getenv("BATTLE_NET_API_KEY", "")
FIREBASE_CONFIG = json.loads(os.getenv("FIREBASE_CONFIG", {}))
POOL_SIZE = int(os.getenv("POOL_SIZE", "32"))
LEAGUE_IDS = range(7)
CLAN_IDS = [369458, 40715, 406747]
THREADED = os.getenv("THREADED", "true").casefold() == "true".casefold()

firebase_admin.initialize_app(
    credential=firebase_admin.credentials.Certificate(FIREBASE_CONFIG.get("serviceAccount", {})),
    options=FIREBASE_CONFIG
)


def _flatten(l) -> list:
    return list(itertools.chain.from_iterable(l))


def for_each_discord_member(
    access_tokens_per_region: dict, current_season_id_per_region: dict, mmrs_per_region: dict,
    member_key: str
):
    allindb.blizzard.update_characters_for_member(
        access_tokens_per_region, current_season_id_per_region, mmrs_per_region, member_key
    )
    print("updated characters for member with id " + member_key)

    allindb.blizzard.update_ladder_summary_for_member(current_season_id_per_region, member_key)
    print("Updated ladder summary for member with id " + member_key)

    allindb.discord.update_discord_info_for_member(
        DISCORD_BOT_TOKEN, GUILD_ID, FULL_MEMBER_ROLE_ID, member_key
    )
    print("Updated discord info for member with id " + member_key)


def update_unregistered_clan_members(
    current_season_id_per_region: dict, mmrs_per_region: dict, clan_members_per_region: dict,
    executor
):
    for region in clan_members_per_region.keys():
        concurrent.futures.wait(
            [
                executor.submit(
                    allindb.blizzard.update_unregistered_member_ladder_summary_for_member, region,
                    current_season_id_per_region[region], mmrs_per_region[region], clan_member
                ) for clan_member in clan_members_per_region[region]
            ]
        )


def main():
    access_tokens_per_region, current_season_id_per_region = allindb.blizzard.get_access_token_and_current_season_per_region(
        CLIENT_ID, CLIENT_SECRET
    )
    clan_ids_per_region = {"us": CLAN_IDS}

    with concurrent.futures.ThreadPoolExecutor(POOL_SIZE) as executor:

        if not THREADED:
            executor = allindb.executor.CurrentThreadExecutor()

        mmrs_per_region_per_league, clan_members_per_region_per_league = zip(
            *executor.map(
                functools.partial(
                    allindb.blizzard.fetch_mmrs_and_clan_members_for_each_league,
                    access_tokens_per_region, current_season_id_per_region, clan_ids_per_region
                ), LEAGUE_IDS
            )
        )

        mmrs_per_region = functools.reduce(
            lambda a, b: {region: a.get(region, []) + b.get(region, [])
                          for region in a.keys()}, mmrs_per_region_per_league,
            dict.fromkeys(access_tokens_per_region.keys(), [])
        )
        for region in mmrs_per_region:
            mmrs_per_region[region].sort()

        clan_members_per_region = functools.reduce(
            lambda a, b: {region: a.get(region, []) + b.get(region, [])
                          for region in a.keys()}, clan_members_per_region_per_league,
            dict.fromkeys(access_tokens_per_region.keys(), [])
        )

        print("Fetched MMRs and clan members.")

        discord_member_keys = list(reference().child("members").get(shallow=True).keys())
        if not discord_member_keys:
            discord_member_keys = []
        else:
            random.shuffle(discord_member_keys)
        print("Fetched members.")

        concurrent.futures.wait(
            [
                executor.submit(
                    for_each_discord_member, access_tokens_per_region, current_season_id_per_region,
                    mmrs_per_region, member
                ) for member in discord_member_keys
            ]
        )

        print("Updated registered members.")

        update_unregistered_clan_members(
            current_season_id_per_region, mmrs_per_region, clan_members_per_region, executor
        )

        print("Updated unregistered members.")

    print("update complete.")


if __name__ == "__main__":
    main()
