import itertools
import functools
import json
import os

import sc2gamedata
import firebase_admin
import firebase_admin.credentials
from firebase_admin.db import reference

CLIENT_ID = os.getenv("BATTLE_NET_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("BATTLE_NET_CLIENT_SECRET", "")
FIREBASE_CONFIG = json.loads(os.getenv("FIREBASE_CONFIG", {}))
LEAGUE_IDS = range(7)

firebase_admin.initialize_app(
    credential=firebase_admin.credentials.Certificate(
        FIREBASE_CONFIG.get("serviceAccount", {})
    ),
    options=FIREBASE_CONFIG,
)


def _flatten(l) -> list:
    return list(itertools.chain.from_iterable(l))


def _fetch_tier_boundaries_for_league(
    access_token: str, current_season_id: int, league_id: int
) -> list:
    league_data = sc2gamedata.get_league_data(
        access_token, current_season_id, league_id
    )
    return [
        {
            "type": "boundary",
            "tier": (league_id * 3) + tier_index,
            "min_mmr": tier_data.get("min_rating", 0),
            "max_mmr": tier_data.get("max_rating", 99999),
        }
        for tier_index, tier_data in enumerate(reversed(league_data.get("tier", [])))
    ]


def main():
    access_token, _ = sc2gamedata.get_access_token(CLIENT_ID, CLIENT_SECRET, "us")
    season_id = sc2gamedata.get_current_season_data(access_token)["id"]

    tier_boundaries = map(
        functools.partial(_fetch_tier_boundaries_for_league, access_token, season_id),
        LEAGUE_IDS,
    )
    flattened_tier_boundaries = _flatten(tier_boundaries)
    keyed_tier_boundaries = dict((x["tier"], x) for x in flattened_tier_boundaries)

    ref = firebase_admin.db.reference()
    ref.child("tier_boundaries").child("us").child(str(season_id)).set(keyed_tier_boundaries)


if __name__ == "__main__":
    main()
