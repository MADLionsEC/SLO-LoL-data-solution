import os
import urllib.request
import json
import pandas as pd
from pymongo import MongoClient
from itertools import chain
from riotwatcher import RiotWatcher
from tqdm import tqdm
from requests.exceptions import HTTPError
from converters.data2files import get_runes_reforged_json
from converters.data2frames import game_to_dataframe as g2df
from converters.data2frames import get_soloq_dataframe, get_league_dataframe
from datetime import datetime as dt, timedelta
from config.constants import MONGODB_CONN, SOLOQ, REGIONS, CUSTOM_PARTICIPANT_COLS, \
    STANDARD_POSITIONS, SCRIMS_POSITIONS_COLS, TOURNAMENT_GAME_ENDPOINT, EXPORTS_DIR, \
    RIFT_GAMES_QUEUES, TOURNAMENT_TL_ENDPOINT, LEAGUES_DATA_DICT, EXCEL_EXPORT_PATH, \
    DB_ITEMS, DB_CHANGE_TYPE


class DataBase:
    def __init__(self, api_key, region, league):
        self.rw = RiotWatcher(api_key)
        self.region = region
        self.league = league
        self.mongo_cnx = MongoClient(MONGODB_CONN)
        self.mongo_soloq_m_col = self.mongo_cnx.slds.soloq_m
        self.mongo_soloq_tl_col = self.mongo_cnx.slds.soloq_tl
        self.mongo_slo_m_col = self.mongo_cnx.slds.slo_m
        self.mongo_slo_tl_col = self.mongo_cnx.slds.slo_tl
        self.mongo_scrims_m_col = self.mongo_cnx.slds.scrims_m
        self.mongo_scrims_tl_col = self.mongo_cnx.slds.scrims_tl
        self.mongo_static_data = self.mongo_cnx.slds.static_data
        self.mongo_players = self.mongo_cnx.slds.players
        self.mongo_teams = self.mongo_cnx.slds.teams
        self.mongo_competitions = self.mongo_cnx.slds.competitions
        self.mongo_slo = self.mongo_cnx.slds.slo

    def get_old_and_new_game_ids(self, **kwargs):
        if self.league == 'SOLOQ':
            cursor = self.mongo_soloq_m_col.find({'platformId': self.region.upper()}, {'_id': 0,
                                                                                       'gameId': 1,
                                                                                       'platformId': 1})
            current_game_ids = [(gid['gameId'], gid['platformId']) for gid in cursor]
            acc_ids = self.get_account_ids(**kwargs)
            print('\t{} account ids found.'.format(len(acc_ids)))
            # New Solo Q game ids
            new_game_ids = self.get_game_ids(acc_ids=acc_ids, **kwargs)
        else:
            raw_data_coll_name = self.league.lower() + '_m'
            raw_data_coll = self.mongo_cnx.slds.get_collection(raw_data_coll_name)
            info_coll = self.mongo_cnx.slds.get_collection(self.league.lower())
            cursor1 = info_coll.find({}, {'_id': 0, 'game_id': 1, 'realm': 1, 'hash': 1})
            new_game_ids = [(record['game_id'], record['realm'], record['hash']) for record in cursor1]
            cursor2 = raw_data_coll.find({}, {'_id': 0, 'gameId': 1, 'platformId': 1})
            current_game_ids = [(record['gameId'], record['platformId']) for record in cursor2]

        return current_game_ids, new_game_ids

    def get_account_ids(self, **kwargs):
        players = self.mongo_players
        if kwargs['team_abbv'] is not None:
            print('\tLooking for account ids of {} players.'.format(kwargs['team_abbv'].replace(',', ' and ')))
            abbvs = kwargs['team_abbv'].split(',')
            if len(abbvs) == 1:
                team = abbvs[0]
            elif len(abbvs) > 1:
                team = {'$in': abbvs}
            else:
                print('No abbreviations selected. Check help for more information.')
                return
            cursor = players.find({'team_abbv': team}, {'_id': 0, 'account_id': 1})
        elif kwargs['competition'] is not None:
            print('\tLooking for account ids players competing in the {}.'.format(kwargs['competition']))
            competitions = kwargs['competition'].split(',')
            if len(competitions) == 1:
                competition = competitions[0]
            elif len(competitions) > 1:
                competition = {'$in': competitions}
            else:
                print('\tNo competitions selected. Check help for more information.')
                return
            cursor = players.aggregate([
                {
                    '$lookup': {'from': 'teams', 'localField': 'team_abbv', 'foreignField': 'key', 'as': 'team_info'}
                },
                {
                    '$match': {'team_info.competition': competition}
                }
            ])
        elif kwargs['region_filter'] is not None:
            print('\tLooking for account ids players competing in {}.'.format(kwargs['region_filter'].upper()
                                                                              .replace(',', ' and ')))
            regions = [REGIONS[region.upper()] for region in kwargs['region_filter'].split(',')]
            if len(regions) == 1:
                region = REGIONS[regions[0]]
            elif len(regions) > 1:
                region = {'$in': regions}
            else:
                print('\tNo region selected. Check help for more information.')
                return
            cursor = players.find({'region': region}, {'_id': 0, 'account_id': 1})
        else:
            print('\tLooking for account ids of every player in the DB.')
            cursor = players.find({}, {'_id': 0, 'account_id': 1})

        return [p['account_id'] for p in cursor]

    def get_new_ids(self, old, new):
        if self.league != SOLOQ:
            return [gid for gid in new if (gid[0], gid[1]) not in old]
        return list(set(new) - set(old))

    def download_games(self, current_game_ids, new_game_ids):
        def tournament_match_to_dict(id1, hash1, tournament):
            with urllib.request.urlopen(TOURNAMENT_GAME_ENDPOINT.format(tr=tournament, id=id1, hash=hash1)) as url:
                match = json.loads(url.read().decode())
            with urllib.request.urlopen(TOURNAMENT_TL_ENDPOINT.format(tr=tournament, id=id1, hash=hash1)) as url:
                tl = json.loads(url.read().decode())
            return match, tl

        ids_not_in_db = self.get_new_ids(current_game_ids, new_game_ids)
        print('\t{} new games to be downloaded.'.format(len(ids_not_in_db)))
        if ids_not_in_db:
            for item in tqdm(ids_not_in_db, desc='\tDownloading games'):
                try:
                    if item[1] not in REGIONS.values():
                        match, timeline = tournament_match_to_dict(item[0], item[2], item[1])
                    else:
                        match = self.rw.match.by_id(match_id=item[0], region=item[1])
                        timeline = self.rw.match.timeline_by_match(match_id=item[0], region=item[1])
                    data = {'match': match, 'timeline': timeline}
                    self.__save_match_raw_data(data=data)
                except HTTPError:
                    pass
        else:
            print('\tAll games already downloaded.')
        return None

    def get_game_ids(self, acc_ids, **kwargs):
        matchlist_kwargs = {k: kwargs[k] for k in ['begin_index', 'n_games']}
        matchlist_kwargs['queue'] = RIFT_GAMES_QUEUES
        matchlist_kwargs['region'] = self.region
        if matchlist_kwargs['n_games'] is not None and matchlist_kwargs['begin_index'] is not None:
            matchlist_kwargs['end_index'] = matchlist_kwargs['begin_index'] + matchlist_kwargs.pop('n_games')
        elif matchlist_kwargs['n_games'] is not None and matchlist_kwargs['begin_index'] is None:
            matchlist_kwargs['end_index'] = matchlist_kwargs.pop('n_games')
        elif matchlist_kwargs['n_games'] is None:
            matchlist_kwargs['end_index'] = matchlist_kwargs.pop('n_games')

        matches = list(chain.from_iterable(
            [self.rw.match.matchlist_by_account(account_id=acc, **matchlist_kwargs)['matches'] for acc in acc_ids]))
        result = list(set([(m['gameId'], m['platformId']) for m in matches]))
        return result

    def __save_match_raw_data(self, data):
        if isinstance(data, dict):
            game_id = str(data['match']['gameId'])
            platform_id = data['match']['platformId']
            data['timeline']['gameId'] = game_id
            data['timeline']['platformId'] = platform_id
            m_coll = self.mongo_cnx.slds.get_collection(self.league.lower() + '_m')
            tl_coll = self.mongo_cnx.slds.get_collection(self.league.lower() + '_tl')
            m_coll.insert_one(data['match'])
            tl_coll.insert_one(data['timeline'])
        else:
            raise TypeError('Dict expected at data param. Should be passed as shown here: {"match": match_dict, '
                            '"timeline": timeline_dict}.')

    def get_supported_leagues(self):
        cursor = self.mongo_competitions.find({}, {'_id': 0, 'key': 1})
        return [abbv['key'] for abbv in cursor]

    def get_supported_teams(self):
        cursor = self.mongo_teams.find({}, {'_id': 0, 'key': 1})
        return [abbv[0] for abbv in cursor]

    def concat_games(self, df):
        if self.league == 'SLO':
            return pd.concat([g2df(match=self.mongo_slo_m_col.find_one({'platformId': g[1]['realm'],
                                                                        'gameId': g[1]['game_id']}, {'_id': 0}),
                                   timeline=self.mongo_slo_tl_col.find_one({'platformId': str(g[1]['realm']),
                                                                            'gameId': str(g[1]['game_id'])},
                                                                           {'_id': 0}),
                                   custom_names=list(g[1][CUSTOM_PARTICIPANT_COLS].T),
                                   custom_positions=STANDARD_POSITIONS,
                                   team_names=list(g[1][['blue', 'red']]),
                                   custom=(g[1]['hash'] is None),
                                   week=g[1]['week'],
                                   database=self.mongo_static_data,
                                   split=g[1]['split'],
                                   season=g[1]['season']
                                   ) for g in tqdm(df.iterrows(), total=df.shape[0],
                                                   desc='\tTransforming JSON into XLSX')])
        elif self.league == 'SCRIMS':
            return pd.concat([g2df(match=self.mongo_scrims_m_col.find_one({'platformId': g[1]['realm'],
                                                                           'gameId': g[1]['game_id']}, {'_id': 0}),
                                   timeline=self.mongo_scrims_tl_col.find_one({'platformId': str(g[1]['realm']),
                                                                               'gameId': str(g[1]['game_id'])}),
                                   custom_positions=list(g[1][SCRIMS_POSITIONS_COLS]),
                                   team_names=list(g[1][['blue', 'red']]),
                                   custom_names=list(g[1][CUSTOM_PARTICIPANT_COLS]),
                                   custom=True, enemy=g[1]['enemy'], game_n=g[1]['game_n'], blue_win=g[1]['blue_win'],
                                   database=self.mongo_static_data
                                   ) for g in tqdm(df.iterrows(), total=df.shape[0],
                                                   desc='\tTransforming JSON into XLSX')])
        elif self.league == 'LCK':
            return pd.concat([g2df(match=None,
                                   timeline=None,
                                   week=g[1]['week'], custom=False,
                                   custom_positions=STANDARD_POSITIONS, database=self.mongo_static_data
                                   ) for g in tqdm(df.iterrows(), total=df.shape[0],
                                                   desc='\tTransforming JSON into XLSX')])
        elif self.league == 'SOLOQ':
            return pd.concat([g2df(match=self.mongo_soloq_m_col.find_one({'platformId': gid[1][1],
                                                                          'gameId': int(gid[1][0])}, {'_id': 0}),
                                   timeline=self.mongo_soloq_tl_col.find_one({'platformId': gid[1][1],
                                                                              'gameId': str(gid[1][0])}, {'_id': 0}),
                                   custom=False, database=self.mongo_static_data
                                   ) for gid in tqdm(df.iterrows(), total=df.shape[0],
                                                     desc='\tTransforming JSON into XLSX')])

    def get_stored_game_ids(self, **kwargs):
        mongo_query = {}
        if self.league == SOLOQ:
            game_id = 'gameId'
            realm = 'platformId'
            coll = self.mongo_cnx.slds.get_collection(self.league.lower() + '_m')
            if kwargs['patch'] is not None:
                patch = kwargs['patch']
                print('\tLooking for games played on patch {}.'.format(patch))
                regex_patch = '^' + '{}'.format(patch).replace('.', r'\.')
                mongo_query['gameVersion'] = {'$regex': regex_patch}
            if kwargs['team_abbv'] is not None or kwargs['competition'] is not None:
                acc_ids = self.get_account_ids(**kwargs)
                mongo_query['participantIdentities.player.currentAccountId'] = {'$in': acc_ids}
            if kwargs['begin_time'] is not None:
                print('\tLooking for games past {} at 00:00:00.'.format(kwargs['begin_time']))
                timestamp = self.__str_date_to_timestamp(kwargs['begin_time'])
                mongo_query['gameCreation'] = {}
                mongo_query['gameCreation']['$gte'] = timestamp
            if kwargs['end_time'] is not None:
                print('\tLooking for games before {} at 23:59:59.'.format(kwargs['end_time']))
                td1 = timedelta(hours=23, minutes=59, seconds=59)
                timestamp = self.__str_date_to_timestamp(kwargs['end_time'], td1)
                try:
                    mongo_query['gameCreation']['$lte'] = timestamp
                except KeyError:
                    mongo_query['gameCreation'] = {}
                    mongo_query['gameCreation']['$lte'] = timestamp
        else:
            game_id = 'game_id'
            realm = 'realm'
            coll = self.mongo_cnx.slds.get_collection(self.league.lower())
            if kwargs['split']:
                print('\tLooking for games played in {} split.'.format(kwargs['split']))
                mongo_query['split'] = kwargs['split']
            if kwargs['season']:
                print('\tLooking for games played in season {}.'.format(kwargs['season']))
                mongo_query['season'] = int(kwargs['season'])
            if kwargs['begin_time'] is not None:
                print('\tLooking for games past {} at 00:00:00.'.format(kwargs['begin_time']))
                timestamp = self.__str_date_to_timestamp(kwargs['begin_time'])
                mongo_query['timestamp'] = {}
                mongo_query['timestamp']['$gte'] = timestamp
            if kwargs['end_time'] is not None:
                print('\tLooking for games before {} at 23:59:59.'.format(kwargs['end_time']))
                td1 = timedelta(hours=23, minutes=59, seconds=59)
                timestamp = self.__str_date_to_timestamp(kwargs['end_time'], td1)
                try:
                    mongo_query['timestamp']['$lte'] = timestamp
                except KeyError:
                    mongo_query['timestamp'] = {}
                    mongo_query['timestamp']['$lte'] = timestamp

        games = coll.find(mongo_query, {'_id': 0, game_id: 1, realm: 1})
        return [(g[game_id], g[realm]) for g in games]

    def close_connections(self):
        self.mongo_cnx.close()

    @staticmethod
    def __str_date_to_timestamp(date, time_delta=None):
        if time_delta is not None:
            dt1 = dt.strptime(date, '%d-%m-%Y') + time_delta
        else:
            dt1 = dt.strptime(date, '%d-%m-%Y')
        return int(dt.timestamp(dt1) * 1e3)

    def generate_dataset(self):
        return None

    def save_static_data_files(self):
        versions = {'versions': self.rw.static_data.versions(region=self.region), '_id': 'versions'}
        self.mongo_static_data.replace_one(filter={'_id': 'versions'}, replacement=versions, upsert=True)
        champs = self.rw.static_data.champions(region=self.region, version=versions['versions'][0])
        champs['_id'] = 'champions'
        self.mongo_static_data.replace_one(filter={'_id': 'champions'}, replacement=champs, upsert=True)
        items = self.rw.static_data.items(region=self.region, version=versions['versions'][0])
        items['_id'] = 'items'
        self.mongo_static_data.replace_one(filter={'_id': 'items'}, replacement=items, upsert=True)
        summs = self.rw.static_data.summoner_spells(region=self.region, version=versions['versions'][0])
        summs['_id'] = 'summoner_spells'
        self.mongo_static_data.replace_one(filter={'_id': 'summoner_spells'}, replacement=summs, upsert=True)
        runes = {'runes': get_runes_reforged_json(versions), '_id': 'runes_reforged'}
        self.mongo_static_data.replace_one(filter={'_id': 'runes_reforged'}, replacement=runes, upsert=True)

    def modify_item_in_db(self, item_type, change_type, item):
        if item_type.lower() in DB_ITEMS and change_type.lower() in DB_CHANGE_TYPE:
            coll = self.mongo_cnx.slds.get_collection(item_type)
            if change_type.lower() == 'add':
                coll.insert_one(item)
            elif change_type.lower() == 'edit':
                coll.replace_one(filter={'key': item['key']}, replacement=item, upsert=True)
            elif change_type.lower() == 'remove':
                coll.delete_one(filter=item)


def create_dirs():
    if not os.path.exists(EXPORTS_DIR):
        os.makedirs(EXPORTS_DIR)


def parse_args(args, api_key):
    create_dirs()
    kwargs = vars(args)
    region = REGIONS[args.region.upper()]
    league = args.league.upper()
    db = DataBase(api_key, region, league)
    try:
        if args.update_static_data:
            db.save_static_data_files()
            print('Static data updated.')

        if args.download:
            print('Downloading.')
            current_game_ids, new_game_ids = db.get_old_and_new_game_ids(**kwargs)
            db.download_games(current_game_ids=current_game_ids, new_game_ids=new_game_ids)
            print("\tGames downloaded.")

        if args.export:
            print('Exporting.')
            stored_game_ids = db.get_stored_game_ids(**kwargs)
            print('\t{} games found.'.format(len(stored_game_ids)))
            if league != SOLOQ:
                info_df = get_league_dataframe(db.mongo_cnx.slds.get_collection(league.lower()))
                info_df['gid_realm'] = info_df.apply(lambda x: str(x['game_id']) + '_' + str(x['realm']), axis=1)
                ls1 = [str(g[0]) + '_' + str(g[1]) for g in stored_game_ids]
                df = info_df.loc[info_df['gid_realm'].isin(ls1)]
            else:
                df = pd.DataFrame(stored_game_ids).rename(columns={0: 'game_id', 1: 'realm'})
            concatenated_df = db.concat_games(df)
            final_df = concatenated_df

            # Merge Solo Q players info with data
            if league == SOLOQ:
                player_info_df = get_soloq_dataframe(db.mongo_players)
                final_df = final_df.merge(player_info_df, left_on='currentAccountId', right_on='account_id',
                                          how='left')

            final_df.to_excel(LEAGUES_DATA_DICT[league][EXCEL_EXPORT_PATH])
            print('\tGames exported.')

    finally:
        db.close_connections()
