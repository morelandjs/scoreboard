#!/usr/bin/env python3

import argparse
from collections import defaultdict, namedtuple
import datetime
from itertools import chain, product
import logging
from pathlib import Path
import dill
import time

import requests
from fuzzywuzzy import process
from bs4 import BeautifulSoup

"""
List of NBA team names:
COLUMNS: STD, ESPN, ESPN CITY 

"""
team_aliases = [
        ['ATL',  'atl',  'Atlanta'      ],
        ['BKN',  'bkn',  'Brooklyn'     ],     
        ['BOS',  'bos',  'Boston'       ],
        ['CHA',  'cha',  'Charlotte'    ],
        ['CHI',  'chi',  'Chicago'      ],
        ['CLE',  'cle',  'Cleveland'    ],
        ['DAL',  'dal',  'Dallas'       ],
        ['DEN',  'den',  'Denver'       ],
        ['DET',  'det',  'Detroit'      ],
        ['GSW',  'gs',   'Golden State' ],
        ['HOU',  'hou',  'Houston'      ],
        ['IND',  'ind',  'Indiana'      ],
        ['LAC',  'lac',  'LA'           ],
        ['LAL',  'lal',  'Los Angeles'  ],
        ['MEM',  'mem',  'Memphis'      ],
        ['MIA',  'mia',  'Miami'        ],      
        ['MIL',  'mil',  'Milwuakee'    ],
        ['MIN',  'min',  'Minnesota'    ],
        ['NOP',  'no',   'New Orleans'  ],
        ['NYK',  'ny',   'NY Knicks'    ],
        ['OKC',  'okc',  'Oklahoma City'],
        ['ORL',  'orl',  'Orlando'      ],
        ['PHI',  'phi',  'Philadelphia' ],
        ['PHX',  'phx',  'Phoenix'      ], 
        ['POR',  'por',  'Portland'     ],
        ['SAC',  'sac',  'Sacramento'   ],
        ['SAS',  'sa',   'San Antonio'  ],
        ['TOR',  'tor',  'Toronto'      ],    
        ['UTA',  'utah', 'Utah'         ],
        ['WAS',  'wsh',  'Washington'   ],
        ]

teams = [abbr for abbr, *other in team_aliases]

Game = namedtuple('Game', [
    'date', 'team', 'opp', 'home', 'won', 'score', 'home_team', 'away_team',
    'team_score', 'opp_score', 'home_score', 'away_score', 'winner', 'loser'
    ])

def abbr(team, espn=False):
    """
    Takes a variant team name and returns the standard
    (or ESPN) abbreviation

    """
    choices = list(chain(*team_aliases))
    name, confidence = process.extractOne(team, choices)

    for team in team_aliases:
        if name in team:
            return team[1] if espn else team[0]

def one_game(date, team, opp, home, won, score):
    """
    Store game attributes in an immutable named tuple object

    """

    home_team = team if home else opp
    away_team = opp if home else team

    score_winner, score_loser = score
    team_score = score_winner if won else score_loser
    opp_score = score_loser if won else score_winner
    home_score = team_score if home else opp_score
    away_score = opp_score if home else team_score

    winner = team if won else opp
    loser = opp if won else team

    return Game(
            date=date, team=team, opp=opp, home=home, won=won, score=score,
            home_team=home_team, away_team=away_team,
            team_score=team_score, opp_score=opp_score,
            home_score=home_score, away_score=away_score,
            winner=winner, loser=loser
            )

def fetch_games(team, season):
    """
    Returns the team's game data for the specific year from espn.go.com
    and updates the games cache.

    """
    BASEURL = 'http://espn.go.com/nba/team/schedule/_/name/{0}/year/{1}'
    r = requests.get(BASEURL.format(abbr(team, espn=True), season))
    table = BeautifulSoup(r.text, 'lxml').table

    try:
        entries = table.find_all('tr')[1:]
    except AttributeError:
        return

    for row in entries:
        try:
            # read a row of game data
            date, opponent, result, *other = row.find_all('td')

            # unpack game date
            weekday, month, day = date.text.split()
            month_number = datetime.datetime.strptime(month, '%b').month
            year = (season if month_number < 7 else season - 1)
            date_with_year = '{}, {}'.format(date.text, year)
            d = datetime.datetime.strptime(date_with_year, '%a, %b %d, %Y')
            date = datetime.date(d.year, d.month, d.day)

            # unpack teams
            opp = opponent.find_all('a')[1].text
            team, opp = map(abbr, [team, opp])

            # determine if team is home and if they won
            home = (opponent.li.text == 'vs')
            won = (result.span.text == 'W')

            # unpack game score
            score = list(map(int, result.a.text.split(' ')[0].split('-')))

            # create game object
            game = one_game(date, team, opp, home, won, score)

            # print progress to stdout
            fmt_date = '{}'.format(str(date))
            fmt_teams = '{:>4} {:<5}'.format(team, ('' if home else '@')+opp)
            fmt_score = '{:>3}-{:<3}'.format(game.team_score, game.opp_score)
            logging.info(fmt_date + fmt_teams + fmt_score)

            yield game

        except (ValueError, AttributeError) as e:
            pass


def update(cachefile, rebuild=False):
    """
    Scrape all NBA game scores, for every team, for years 2003–present
    from espn.com. Only update seasons with missing games unless
    rebuild=True

    """
    now = datetime.datetime.now()
    current_season = (now.year + 1 if now.month > 7 else now.year)
    scrape_seasons = range(2003, current_season + 1)

    if cachefile.exists() and not rebuild:
        games = dill.load(cachefile.open(mode='rb'))
        most_current_year = max(games.keys())
        scrape_seasons = range(most_current_year, current_season + 1)
    else:
        cachefile.parent.mkdir(exist_ok=True)
        games = defaultdict(dict)
        
    for season, (team, *aliases) in product(scrape_seasons, team_aliases):
        games[season][team] = list(fetch_games(team, season))
        dill.dump(games, cachefile.open(mode='wb'))


HOME = Path.home()
cachefile = HOME / ".cache/scoreboard/games.p"

if __name__ == "__main__":
    """
    Rebuild (or refresh) games cache

    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "--rebuild",
            action="store_true",
            default=False,
            help="rebuild game cache for all years 2003-present"
            )

    parser.add_argument(
            '--loglevel',
            choices={'debug', 'info', 'warning', 'error', 'critical'},
            default='info',
            help='log level (default: %(default)s)'
    )

    args = parser.parse_args()
    args_dict = vars(args)
    rebuild = args_dict['rebuild']

    logging.basicConfig(
            level=getattr(logging, args.loglevel.upper()),
            format='[%(levelname)s@%(relativeCreated)d] %(message)s',
            )

    logging.captureWarnings(True)

    start = datetime.datetime.now()
    logging.info('started at %s', start)

    update(cachefile, rebuild=rebuild)

    end = datetime.datetime.now()
    logging.info('finished at %s, %s elapsed', end, end - start)
else:
    """
    Load NBA games from cache.

    """
    if cachefile.exists():
        games = dill.load(cachefile.open(mode='rb'))
        years = games.keys()
        games_iter = chain(*[games[y][t] for (t, y) in product(teams, years)])
        games = sorted(games_iter, key=lambda g: g.date)
    else:
        logging.error("games.p not found, run python -m scoreboard")
        exit(1)
