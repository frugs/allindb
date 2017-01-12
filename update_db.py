import os
import pickle
import itertools
import multiprocessing
import functools
import time
import requests
import sc2gamedata
import pyrebase


ACCESS_TOKEN = os.getenv("BATTLE_NET_ACCESS_TOKEN", "")
FIREBASE_CONFIG_FILE = os.getenv("FIREBASE_CONFIG_FILE", "firebase.cfg")
POOL_SIZE = int(os.getenv("POOL_SIZE", "8"))
LEAGUE_IDS = range(7)
REGIONS = ["us", "eu"]


def open_db_connection() -> pyrebase.pyrebase.Database:
    with open(FIREBASE_CONFIG_FILE, "rb") as file:
        config = pickle.load(file)

    firebase = pyrebase.initialize_app(config)
    return firebase.database()


def update_matching_discord_member_ladder_stats(
        db: pyrebase.pyrebase.Database,
        caseless_battle_tag: str,
        region: str,
        season: str,
        race: str,
        ladder_data: dict,
        team_data:dict):
    try:
        db_result = db.child("members").order_by_child("caseless_battle_tag").equal_to(caseless_battle_tag).get()

        if db_result.pyres:
            discord_id = next(iter(db_result.val().values()))["discord_id"]

            data = {
                "league_id": ladder_data["league"]["league_key"]["league_id"],
                "wins": team_data["wins"],
                "losses": team_data["losses"],
                "ties": team_data["ties"],
                "games_played": team_data["wins"] + team_data["losses"] + team_data["ties"],
                "mmr": team_data["rating"]
            }
            db.child("members").child(discord_id).child("regions").child(region).child(season).child(race).set(data)

    except requests.exceptions.HTTPError:
        update_matching_discord_member_ladder_stats(db, caseless_battle_tag, region, season, race, ladder_data, team_data)


def for_each_division(region: str, season: str, member_caseless_battle_tags: set, division_data: dict):
    db = open_db_connection()

    ladder_id = division_data["ladder_id"]
    ladder_data = sc2gamedata.get_ladder_data(ACCESS_TOKEN, ladder_id, region)
    teams = ladder_data["team"]

    for team in teams:
        member = team["member"][0]
        if "character_link" not in member or "played_race_count" not in member:
            continue

        caseless_battle_tag = member["character_link"]["battle_tag"].casefold()
        race = next(iter(member["played_race_count"][0]["race"].values()))

        if caseless_battle_tag in member_caseless_battle_tags:
            update_matching_discord_member_ladder_stats(
                db, caseless_battle_tag, region, season, race, ladder_data, team)

    print("processed ladder {}".format(ladder_id))


def for_each_member(member_key: str):
    try:
        db = open_db_connection()
        regions_query_result = db.child("members").child(member_key).child("regions").shallow().get()

        if not regions_query_result.pyres:
            return

        highest_league_per_race = {"Zerg": 0, "Protoss": 0, "Terran": 0, "Random": 0}
        current_highest_league = None

        most_recent_season_id = -1
        season_games_played = {-1: 0}  # no season games played if you haven't played in any seasons
        for region in regions_query_result.val():
            current_season_id = sc2gamedata.get_current_season_data(ACCESS_TOKEN, region)["id"]
            most_recent_season_id = max(most_recent_season_id, current_season_id)
            seasons_query_result = db.child("members").child(member_key).child("regions").child(region).shallow().get()

            if seasons_query_result.pyres:
                for season in seasons_query_result.val():
                    race_stats_query_result = db.child("members").child(member_key).child("regions").child(region).child(season).get()

                    if race_stats_query_result.pyres:
                        race_stats = race_stats_query_result.val()
                        for race in race_stats.keys():
                            race_league = race_stats[race]["league_id"]
                            highest_league_per_race[race] = max(highest_league_per_race[race], race_league)

                            season_id = int(season)
                            if season_id not in season_games_played:
                                season_games_played[season_id] = 0
                            season_games_played[season_id] += race_stats[race]["games_played"]

                            if season == current_season_id and (not current_highest_league or current_highest_league < race_league):
                                current_highest_league = race_league

        highest_ranked_races = [
            race for race, league in highest_league_per_race.items()
            if league == max(highest_league_per_race.values())]

        data = {
            "zerg_player": "Zerg" in highest_ranked_races,
            "protoss_player": "Protoss" in highest_ranked_races,
            "terran_player": "Terran" in highest_ranked_races,
            "random_player": "Random" in highest_ranked_races,
            "current_season_games_played": season_games_played.get(most_recent_season_id, 0),
            "previous_season_games_played": season_games_played.get(most_recent_season_id - 1, 0),
            "last_updated": time.time()
        }

        if current_highest_league is not None:
            data["current_league"] = current_highest_league

        db.child("members").child(member_key).update(data)
    except requests.exceptions.HTTPError:
        for_each_member(member_key)


def get_member_caseless_battle_tags(db: pyrebase.pyrebase.Database) -> set:
    caseless_battle_tags = set()

    result = db.child("members").order_by_child("caseless_battle_tag").get()
    if result.pyres:
        for registered_member_data in result.val().values():
            caseless_battle_tags.add(registered_member_data["caseless_battle_tag"])

    return caseless_battle_tags


def main():

    with multiprocessing.Pool(POOL_SIZE) as pool:
        db = open_db_connection()
        member_caseless_battle_tags = get_member_caseless_battle_tags(db)

        for region in REGIONS:
            print("fetching data for region {}".format(region))

            current_season_id = sc2gamedata.get_current_season_data(ACCESS_TOKEN, region)["id"]
            leagues = [sc2gamedata.get_league_data(ACCESS_TOKEN, current_season_id, league_id, region)
                       for league_id in LEAGUE_IDS]
            tiers = itertools.chain(itertools.chain.from_iterable(league["tier"] for league in leagues))
            divisions = itertools.chain(itertools.chain.from_iterable(
                tier["division"] for tier in tiers if "division" in tier))

            map_func = functools.partial(for_each_division, region, current_season_id, member_caseless_battle_tags)
            pool.map(map_func, divisions)

        print("data fetch complete")

        member_keys = db.child("members").shallow().get().val()
        pool.map(for_each_member, member_keys)

    print("update complete.")


if __name__ == "__main__":
    main()
