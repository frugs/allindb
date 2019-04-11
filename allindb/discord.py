import time

from firebase_admin.db import reference
import requests

RETRIES = 5


def get_member_info(bot_token: str, guild_id: str, member_id: str) -> dict:
    url = "https://discordapp.com/api/guilds/{}/members/{}".format(guild_id, member_id)

    tries = 0
    failure = False
    while not failure and tries < RETRIES:
        response = requests.get(url, headers={"Authorization": "Bot " + bot_token})
        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            body = response.json()
            time.sleep(body.get("retry_after", 1000) * 0.001)
            tries += 1
        else:
            failure = True

    return {}


def update_discord_info_for_member(
        bot_token: str,
        guild_id: str,
        full_member_role_id: str,
        member_key: str):

    member_info = get_member_info(bot_token, guild_id, member_key)

    if member_info:
        discord_server_nick = member_info.get("nick", "")

        data = {
            "is_full_member": full_member_role_id in member_info.get("roles", []),
            "discord_username": member_info["user"]["username"]
        }

        if discord_server_nick:
            data["discord_server_nick"] = discord_server_nick
    else:
        data = {
            "is_full_member": False
        }

    reference().child("members").child(member_key).update(data)


