import pyquery
import requests


def get_teams():
    url = "https://nba.hupu.com/players"
    resp = requests.get(url)
    if resp.status_code == 200:
        teams = pyquery.PyQuery("span.team_name > a", resp.content)
        team_urls = []
        for team in teams.items():
            team_urls.append(team.attr("href"))
        return team_urls
    else:
        raise RuntimeError("获取列表失败")


def get_players(team_url):
    # 获取所有的分区列表
    all_player_names = []
    player_names = []
    resp = requests.get(team_url)
    if resp.status_code == 200:
        players = pyquery.PyQuery("tr > td.left:first > b:first", resp.content)
        for player in players.items():
            all_player_names.append(player.text())
            player_names.append(player.text())
            all_player_names.append(player.text().replace("-", ""))
            all_player_names.extend(player.text().split("-"))
    return all_player_names, player_names


def all_players_name():
    teams = get_teams()
    all_players_name = set()
    all_players_name_raw = set()
    for team in teams:
        players, players_raw = get_players(team)
        if players:
            all_players_name.update(set(players))
            all_players_name_raw.update(set(players_raw))
    all_players_name.remove("")
    # print(list(all_players_name))
    print(all_players_name_raw)


if __name__ == "__main__":
    all_players_name()
