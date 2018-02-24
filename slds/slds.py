from riotwatcher import RiotWatcher
from converters.data2frames import game_to_dataframe as g2df
from converters.data2files import write_json, read_json, save_runes_reforged_json
from config.constants import SLO_MATCHES_FILE_PATH, SLO_DATA_DTYPES, SLO_MATCHES_DATA_P_COLS, SLO_CUSTOM_POSITIONS, \
    API_KEY, SLO_GAMES_DIR, SLO_DATASETS_DIR, SLO_DATASET_CSV, STATIC_DATA_DIR
import pandas as pd
from datetime import datetime as dt
from tqdm import tqdm
import os


class Slds:
    def __init__(self, region):
        self.rw = RiotWatcher(API_KEY)
        self.region = region

    def generate_dataset(self, read_dir, force_update=False):
        df = pd.read_csv(SLO_MATCHES_FILE_PATH, dtype=SLO_DATA_DTYPES)
        df2 = pd.read_csv('{}{}'.format(SLO_DATASETS_DIR, SLO_DATASET_CSV), index_col=0)
        old = list(df2.gameId.unique())
        new = list(df.game_id.unique())
        new_ids = self.__get_new_ids(old, new)

        if not force_update:
            if new_ids:
                print('There are new ids {}, merging the old data with the new one.'.format(new_ids))
                df3 = df[df.game_id.isin(new_ids)]
                df4 = self.__concat_games(df3, read_dir)
                df_result = pd.concat([df2, df4.T.reset_index().drop_duplicates(subset='index',
                                                                                keep='first').set_index('index').T])
                return df_result.reset_index(drop=True)
            elif not new_ids:
                return None
        elif force_update:
            if new_ids:
                print('Updating current datasets but there are new ids found: {}'.format(new_ids))
                df_result = self.__concat_games(df, read_dir).T.reset_index()\
                    .drop_duplicates(subset='index', keep='first').set_index('index').T
            elif not new_ids:
                print('Forcing update of the current datasets even though there are not new ids.')
                df_result = self.__concat_games(df, read_dir).T.reset_index()\
                    .drop_duplicates(subset='index', keep='first').set_index('index').T
            return df_result.reset_index(drop=True)

    def download_games(self, ids, save_dir):
        file_names = os.listdir(save_dir)
        curr_ids = list(set([f.split('.')[0].split('_')[1] for f in file_names]))
        new_ids = self.__get_new_ids(curr_ids, ids)
        if new_ids:
            for item in tqdm(new_ids, desc='Downloading games'):
                match = self.rw.match.by_id(match_id=item, region=self.region)
                timeline = self.rw.match.timeline_by_match(match_id=item, region=self.region)
                data = {'match': match, 'timeline': timeline}
                self.__save_match_raw_data(data=data, save_dir=save_dir)
        else:
            print('All games already downloaded.')

    @staticmethod
    def __save_match_raw_data(data, save_dir):
        if isinstance(data, dict):
            game_id = data['match']['gameId']
            game_creation = dt.strftime(dt.fromtimestamp(data['match']['gameCreation'] / 1e3), '%d-%m-%y')
            write_json(data['match'], save_dir=save_dir, file_name='{date}_{id}'.format(date=game_creation, id=game_id))
            write_json(data['timeline'], save_dir=save_dir, file_name='{date}_{id}_tl'
                       .format(date=game_creation, id=game_id))
        else:
            raise TypeError('Dict expected at data param. Should be passed as shown here: {"match": match_dict, '
                            '"timeline": timeline_dict}.')

    @staticmethod
    def get_slo_games_ids():
        df = pd.read_csv(SLO_MATCHES_FILE_PATH, dtype=SLO_DATA_DTYPES)
        return list(df.game_id)

    @staticmethod
    def __get_file_names_from_match_id(m_id, save_dir):
        file_names = os.listdir(save_dir)
        match_filename = [val for val in file_names if str(m_id) in val and 'tl' not in val][0]
        tl_filename = [val for val in file_names if str(m_id) in val and 'tl' in val][0]
        return {'match_filename': match_filename.split('.')[0], 'tl_filename': tl_filename.split('.')[0]}

    def __get_new_ids(self, old, new):
        return list(set(map(int, new)) - set(map(int, old)))

    def __concat_games(self, df, read_dir):
        return pd.concat([g2df(match=read_json(save_dir=read_dir,
                                                    file_name=self.__get_file_names_from_match_id(m_id=g[1]['game_id'],
                                                                                                  save_dir=read_dir)
                                                    ['match_filename']),
                               timeline=read_json(save_dir=read_dir,
                                                  file_name=self.__get_file_names_from_match_id(
                                                      m_id=g[1]['game_id'], save_dir=read_dir)['tl_filename']),
                               custom_names=list(g[1][SLO_MATCHES_DATA_P_COLS].T),
                               custom_positions=SLO_CUSTOM_POSITIONS,
                               team_names=list(g[1][['blue', 'red']]),
                               week=g[1]['week']) for g in df.iterrows()])

    def save_static_data_files(self):
        versions = self.rw.static_data.versions(region=self.region)
        champs = self.rw.static_data.champions(region=self.region, version=versions[0])
        items = self.rw.static_data.items(region=self.region, version=versions[0])
        summs = self.rw.static_data.summoner_spells(region=self.region, version=versions[0])
        save_runes_reforged_json()
        write_json(versions, STATIC_DATA_DIR, file_name='versions')
        write_json(champs, STATIC_DATA_DIR, file_name='champions')
        write_json(items, STATIC_DATA_DIR, file_name='items')
        write_json(summs, STATIC_DATA_DIR, file_name='summoners')


def main():
    slds = Slds(region='EUW1')
    ids = slds.get_slo_games_ids()
    slds.download_games(ids=ids, save_dir=SLO_GAMES_DIR)
    # slds.save_static_data_files()
    df = slds.generate_dataset(read_dir=SLO_GAMES_DIR)
    if df is not None:
        df.to_excel('{}dataset_test.xlsx'.format(SLO_DATASETS_DIR))
        df.to_csv('{}dataset_test.csv'.format(SLO_DATASETS_DIR))
    else:
        print("Nothing has happened.")


if __name__ == '__main__':
    main()
