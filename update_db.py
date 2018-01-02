import os
import pickle
import multiprocessing
import functools
import time
import requests
import sc2gamedata
import pyrebase


CLIENT_ID = os.getenv("BATTLE_NET_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("BATTLE_NET_CLIENT_SECRET", "")
API_KEY = os.getenv("BATTLE_NET_API_KEY", "")
FIREBASE_CONFIG_FILE = os.getenv("FIREBASE_CONFIG_FILE", "firebase.cfg")
POOL_SIZE = int(os.getenv("POOL_SIZE", "8"))
REGIONS = ["us", "eu"]


def open_db_connection() -> pyrebase.pyrebase.Database:
    with open(FIREBASE_CONFIG_FILE, "rb") as file:
        config = pickle.load(file)

    firebase = pyrebase.initialize_app(config)
    return firebase.database()


def update_matching_discord_member_ladder_stats(
        db: pyrebase.pyrebase.Database,
        discord_id: str,
        region: str,
        character: str,
        season: str,
        race: str,
        ladder_data: dict,
        team_data: dict):
    try:
        data = {
            "league_id": ladder_data["league"]["league_key"]["league_id"],
            "wins": team_data["wins"],
            "losses": team_data["losses"],
            "ties": team_data["ties"],
            "games_played": team_data["wins"] + team_data["losses"] + team_data["ties"],
            "mmr": team_data["rating"]
        }
        character_node = db.child("members").child(discord_id).child("characters").child(region).child(character)
        character_node.child("ladder_info").child(season).child(race).set(data)

    except requests.exceptions.HTTPError:
        update_matching_discord_member_ladder_stats(
            db, discord_id, region, character, season, race, ladder_data, team_data)


def for_each_member(member_key: str):
    try:
        db = open_db_connection()

        characters_query_result = db.child("members").child(member_key).child("characters").get().val()

        if not characters_query_result:
            return

        access_tokens_per_region = dict(
                (region, sc2gamedata.get_access_token(CLIENT_ID, CLIENT_SECRET, region)[0])
                for region
                in REGIONS)
        current_season_id_per_region = dict(
            (region, sc2gamedata.get_current_season_data(access_tokens_per_region[region], region)["id"])
            for region
            in REGIONS)

        for region in REGIONS:
            access_token = access_tokens_per_region[region]
            current_season_id = current_season_id_per_region[region]

            region_characters = characters_query_result.get(region, {})
            for character, character_data in region_characters.items():
                profile_ladder_data = sc2gamedata.get_profile_ladder_data(API_KEY, character, region)
                current_season_data = profile_ladder_data.get("currentSeason", [])

                current_season_ladders = [
                    x.get("ladder", [])[0]
                    for x
                    in current_season_data
                    if x.get("ladder", [])
                ]

                ladder_ids = [
                    x.get("ladderId", "")
                    for x
                    in current_season_ladders
                    if x.get("matchMakingQueue", "") == "LOTV_SOLO"
                ]

                ladders = [
                    sc2gamedata.get_ladder_data(access_token, x, region)
                    for x
                    in ladder_ids
                ]

                for ladder_data in ladders:
                    for team in ladder_data.get("team", {}):
                        for member in team.get("member", {}):
                            if "played_race_count" not in member or "legacy_link" not in member:
                                continue

                            legacy_link = member["legacy_link"]
                            if character == legacy_link.get("path", "")[9:].replace("/", "-"):

                                race = next(iter(member["played_race_count"][0]["race"].values()))

                                update_matching_discord_member_ladder_stats(
                                    db, member_key, region, character, current_season_id, race, ladder_data, team)

        print("fetched info for member with id " + member_key)

        characters_query_result = db.child("members").child(member_key).child("characters").get().val()

        if not characters_query_result:
            return

        highest_league_per_race = {"Zerg": 0, "Protoss": 0, "Terran": 0, "Random": 0}
        current_highest_league = None

        most_recent_season_id = -1
        season_games_played = {-1: 0}  # no season games played if you haven't played in any seasons

        for region in (x for x in characters_query_result if x in REGIONS):

            current_season_id = current_season_id_per_region[region]

            region_characters = characters_query_result.get(region, {})
            for character, character_data in region_characters.items():

                most_recent_season_id = max(most_recent_season_id, current_season_id)
                seasons = character_data.get("ladder_info", {})

                if seasons:
                    two_most_recent_seasons = list(
                        sorted(seasons.items(), key=lambda x: int(x[0]), reverse=True))[:2]

                    for season, season_data in two_most_recent_seasons:
                        for race, race_data in season_data.items():
                            race_league = race_data["league_id"]
                            highest_league_per_race[race] = max(highest_league_per_race[race], race_league)

                            season_id = int(season)
                            if season_id not in season_games_played:
                                season_games_played[season_id] = 0
                            season_games_played[season_id] += race_data["games_played"]

                            if season_id == current_season_id and \
                                    (not current_highest_league or current_highest_league < race_league):
                                current_highest_league = race_league

        highest_ranked_races = [
            race for race, league in highest_league_per_race.items()
            if league == max(highest_league_per_race.values())]

        data = {
            "zerg_player": "Zerg" in highest_ranked_races,
            "protoss_player": "Protoss" in highest_ranked_races,
            "terran_player": "Terran" in highest_ranked_races,
            "random_player": "Random" in highest_ranked_races,
            "current_season_games_played": season_games_played.get(most_recent_season_id),
            "previous_season_games_played": season_games_played.get(most_recent_season_id - 1),
            "last_updated": time.time()
        }

        if current_highest_league is not None:
            data["current_league"] = current_highest_league

        db.child("members").child(member_key).update(data)

        print("Updated member with id " + member_key)

    except requests.exceptions.HTTPError:
        for_each_member(member_key)


def main():

    with multiprocessing.Pool(POOL_SIZE) as pool:
        db = open_db_connection()

        member_keys = list(db.child("members").shallow().get().val())
        pool.map(functools.partial(for_each_member), member_keys)

    print("update complete.")


if __name__ == "__main__":
    main()
