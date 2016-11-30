import os
import pickle
import itertools
import multiprocessing
import functools
import time
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


def for_each_division(region: str, season: str, division_data: dict):
    db = open_db_connection()

    ladder_id = division_data["ladder_id"]
    ladder_data = sc2gamedata.get_ladder_data(ACCESS_TOKEN, ladder_id, region)
    teams = ladder_data["team"]

    for team in teams:
        member = team["member"][0]
        if "character_link" not in member:
            continue

        battle_tag = member["character_link"]["battle_tag"]
        race = next(iter(member["played_race_count"][0]["race"].values()))

        db_result = db.child("members").order_by_child("battle_tag").equal_to(battle_tag).get()

        if db_result.pyres:
            discord_id = next(iter(db_result.val().values()))["discord_id"]

            data = {
                "league_id": ladder_data["league"]["league_key"]["league_id"],
                "wins": team["wins"],
                "losses": team["losses"],
                "ties": team["ties"],
                "games_played": team["wins"] + team["losses"] + team["ties"],
                "mmr": team["rating"]
            }
            db.child("members").child(discord_id).child("regions").child(region).child(season).child(race).set(data)


def for_each_member(member_key: str):
    db = open_db_connection()
    regions_query_result = db.child("members").child(member_key).child("regions").shallow().get()

    if not regions_query_result.pyres:
        return

    seasons = ["current", "previous"]
    season_games_played = dict(zip(seasons, [0] * len(seasons)))
    highest_league_per_race = {"Zerg": 0, "Protoss": 0, "Terran": 0, "Random": 0}

    for region in regions_query_result.val():
        seasons_query_result = db.child("members").child(member_key).child("regions").child(region).shallow().get()

        if seasons_query_result.pyres:
            for season in seasons_query_result.val():
                race_stats_query_result = db.child("members").child(member_key).child("regions").child(region).child(season).get()

                if race_stats_query_result.pyres:
                    race_stats = race_stats_query_result.val()
                    for race in race_stats.keys():
                        highest_league_per_race[race] = max(highest_league_per_race[race], race_stats[race]["league_id"])
                        season_games_played[season] += race_stats[race]["games_played"]

    highest_ranked_races = [race for race, league in highest_league_per_race.items() if league == max(highest_league_per_race.values())]

    data = {
        "zerg_player": "Zerg" in highest_ranked_races,
        "protoss_player": "Protoss" in highest_ranked_races,
        "terran_player": "Terran" in highest_ranked_races,
        "random_player": "Random" in highest_ranked_races,
        "current_season_games_played": season_games_played["current"],
        "previous_season_games_played": season_games_played["previous"],
        "last_updated": time.time()
    }
    db.child("members").child(member_key).update(data)


def main():
    with multiprocessing.Pool(POOL_SIZE) as pool:

        for region in REGIONS:
            current_season_id = sc2gamedata.get_current_season_data(ACCESS_TOKEN, region)["id"]
            previous_season_id = current_season_id - 1

            seasons = {current_season_id: "current", previous_season_id: "previous"}

            for season_id in seasons.keys():
                leagues = [sc2gamedata.get_league_data(ACCESS_TOKEN, season_id, league_id, region)
                           for league_id in LEAGUE_IDS]
                tiers = itertools.chain(itertools.chain.from_iterable(league["tier"] for league in leagues))
                divisions = itertools.chain(itertools.chain.from_iterable(
                    tier["division"] for tier in tiers if "division" in tier))

                map_func = functools.partial(for_each_division, region, seasons[season_id])
                pool.map(map_func, divisions)

        db = open_db_connection()
        member_keys = db.child("members").shallow().get().val()
        pool.map(for_each_member, member_keys)


if __name__ == "__main__":
    main()
