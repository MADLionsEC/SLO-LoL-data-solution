import argparse
import unittest
from connectors import database
import pandas as pd
from converters.data2frames import get_soloq_dataframe, get_league_dataframe
from config.constants import API_KEY, SOLOQ, REGIONS, SUPPORTED_LEAGUES, SLO, SCRIMS


def parse_args(league_params):
    parser = argparse.ArgumentParser(description='LoL solution to generate datasets from leagues, scrims and Solo Q'
                                                 ' matches.')
    # Groups
    mandatory = parser.add_argument_group('Mandatory', 'Mandatory commands to run the program.')
    shared = parser.add_argument_group('Common', 'Shared commands for all systems.')
    filesystem = parser.add_argument_group('File system', 'Commands used for the file system.')
    databases = parser.add_argument_group('Databases', 'Commands used for the databases system.')

    # Mandatory commands
    mandatory.add_argument('-l', '--league', help='Choose league. {}'.format(SUPPORTED_LEAGUES))
    mandatory.add_argument('-r', '--region', help='Choose region. {}'.format(list(REGIONS.keys())))
    mandatory.add_argument('-c', '--connector', help='Choose between Databases (DB) or File System (FS) connectors. {}')

    # Shared commands
    shared.add_argument('-e', '--export', help='Export data.', action='store_true')
    shared.add_argument('-d', '--download', help='Download new data if available.', action='store_true')
    shared.add_argument('-usd', '--update_static_data', help='Update static data information.', action='store_true')
    shared.add_argument('-ng', '--n_games', help='Set the number of games to download from Solo Q.', type=int)
    shared.add_argument('-bi', '--begin_index', help='Set the begin index of the Solo Q downloads.', type=int)
    shared.add_argument('-ms', '--merge_soloq', help='Merge SoloQ data with info of players.', action='store_true')

    # FS commands
    filesystem.add_argument('-xlsx', help='Export data as XLSX.', action='store_true')
    filesystem.add_argument('-csv', help='Export data as CSV.', action='store_true')
    filesystem.add_argument('-fu', '--force_update', help='Force the update of the exports datasets.',
                                   action='store_true')

    # DB commands
    databases.add_argument('-ta', '--team_abbv', help='Work with the data of one or more teams selected through '
                                                      'his abbreviation. {download and export}')
    databases.add_argument('-bt', '--begin_time', help='Set the start date limit of the export (day-month-year). '
                                                       '{download and export}')
    databases.add_argument('-et', '--end_time', help='Set the end date limit of the export (day-month-year). '
                                                     '{download and export}')
    databases.add_argument('-p', '--patch', help='Select the patch. {export}')
    databases.add_argument('-C', '--competition', help='Select the competition. {download and export}')
    databases.add_argument('-s', '--split', help='Select the split [spring, summer]. {leagues data export only]')
    databases.add_argument('-S', '--season', help='Select the season [int]. {leagues data export only]')
    databases.add_argument('-R', '--region_filter', help='Select the region to download and export the data.')

    return parser.parse_args(league_params.split(' '))


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.dict1 = {SOLOQ: {'params': '-c db -l {} -r euw -d -e -ta MAD -bt 1-3-2018 -et 2-3-2018'.format(SOLOQ)},
                      SLO: {'params': '-c db -r euw -l {} -d -e -bt 06-06-2018 -et 08-06-2018'.format(SLO)},
                      SCRIMS: {'params': '-c db -r euw -l {} -d -e -bt 06-06-2018 -et 08-06-2018'.format(SCRIMS)}}
        for key, value in self.dict1.items():
            args = parse_args(value['params'])
            kwargs = vars(args)
            region = REGIONS[args.region.upper()]
            league = args.league.upper()
            db = database.DataBase(API_KEY, region, league)
            try:
                if args.update_static_data:
                    db.save_static_data_files()
                    print('Static data updated.')

                if args.download:
                    print('Downloading.')
                    self.dict1[key]['current'], self.dict1[key]['new'] = db.get_old_and_new_game_ids(**kwargs)
                    db.download_games(current_game_ids=self.dict1[key]['current'], new_game_ids=self.dict1[key]['new'])
                    print("\tGames downloaded.")

                if args.export:
                    print('Exporting.')
                    self.dict1[key]['stored'] = db.get_stored_game_ids(**kwargs)
                    print('\t{} games found.'.format(len(self.dict1[key]['stored'])))
                    if league != SOLOQ:
                        info_df = get_league_dataframe(db.mongo_cnx.slds.get_collection(league.lower()))
                        info_df['gid_realm'] = info_df.apply(lambda x: str(x['game_id']) + '_' + str(x['realm']), axis=1)
                        ls1 = [str(g[0]) + '_' + str(g[1]) for g in self.dict1[key]['stored']]
                        df = info_df.loc[info_df['gid_realm'].isin(ls1)]
                    else:
                        df = pd.DataFrame(self.dict1[key]['stored']).rename(columns={0: 'game_id', 1: 'realm'})
                    concatenated_df = db.concat_games(df)
                    final_df = concatenated_df
                    self.dict1[key]['export_df'] = final_df

                    # Merge Solo Q players info with data
                    if league == SOLOQ:
                        player_info_df = get_soloq_dataframe(db.mongo_players)
                        self.dict1[key]['export_df'] = final_df.merge(player_info_df, left_on='currentAccountId', right_on='account_id',
                                                how='left')
                    print('\tGames exported.')
            finally:
                db.close_connections()
    
    def tearDown(self):
        pass
    
    def test_not_none(self):
        itera=0
        for key, value in self.dict1.items():
            self.assertIsNotNone(self.dict1[key]['current'])
            self.assertIsNotNone(self.dict1[key]['new'])
            self.assertIsNotNone(self.dict1[key]['stored'])
            self.assertIsNotNone(self.dict1[key]['export_df'])
            if itera == 0:
                row, col = self.dict1[key]['export_df'].shape
                self.assertEqual(row, 40, 'Incorrect ROW size.')
                self.assertEqual(col, 234, 'Incorrect COL size.')
                self.assertEqual(self.dict1[key]['export_df'].gameId.max(), 3550276067, 'Incorrect dataframe value. (Max gameId)')
                self.assertEqual(self.dict1[key]['export_df'].gameId.min(), 3549167260, 'Incorrect dataframe value. (Min gameId)')
                self.assertEqual(self.dict1[key]['export_df'].kills.max(), 17, 'Incorrect dataframe value. (Max kills)')
                self.assertEqual(self.dict1[key]['export_df'].kills.min(), 0, 'Incorrect dataframe value. (Min kills)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.max(), 19951, 'Incorrect dataframe value. (Max goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.min(), 2637, 'Incorrect dataframe value. (Min goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.max(), 2273, 'Incorrect dataframe value. (Max gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.min(), 1086, 'Incorrect dataframe value. (Min gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.max(), 100, 'Incorrect dataframe value. (Max visionScore)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.min(), 0, 'Incorrect dataframe value. (Min visionScore)')
            if itera == 1:
                row, col = self.dict1[key]['export_df'].shape
                self.assertEqual(row, 80, 'Incorrect ROW size.')
                self.assertEqual(col, 224, 'Incorrect COL size.')
                self.assertEqual(self.dict1[key]['export_df'].gameId.max(), 3659494518, 'Incorrect dataframe value. (Max gameId)')
                self.assertEqual(self.dict1[key]['export_df'].gameId.min(), 3659118018, 'Incorrect dataframe value. (Min gameId)')
                self.assertEqual(self.dict1[key]['export_df'].kills.max(), 13, 'Incorrect dataframe value. (Max kills)')
                self.assertEqual(self.dict1[key]['export_df'].kills.min(), 0, 'Incorrect dataframe value. (Min kills)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.max(), 17320, 'Incorrect dataframe value. (Max goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.min(), 4361, 'Incorrect dataframe value. (Min goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.max(), 2213, 'Incorrect dataframe value. (Max gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.min(), 963, 'Incorrect dataframe value. (Min gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.max(), 99, 'Incorrect dataframe value. (Max visionScore)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.min(), 11, 'Incorrect dataframe value. (Min visionScore)')
            if itera == 2:
                row, col = self.dict1[key]['export_df'].shape
                self.assertEqual(row, 40, 'Incorrect ROW size.')
                self.assertEqual(col, 224, 'Incorrect COL size.')
                self.assertEqual(self.dict1[key]['export_df'].gameId.max(), 3659376666, 'Incorrect dataframe value. (Max gameId)')
                self.assertEqual(self.dict1[key]['export_df'].gameId.min(), 3659301830, 'Incorrect dataframe value. (Min gameId)')
                self.assertEqual(self.dict1[key]['export_df'].kills.max(), 15, 'Incorrect dataframe value. (Max kills)')
                self.assertEqual(self.dict1[key]['export_df'].kills.min(), 0, 'Incorrect dataframe value. (Min kills)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.max(), 15803, 'Incorrect dataframe value. (Max goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].goldEarned.min(), 6324, 'Incorrect dataframe value. (Min goldEarned)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.max(), 2341, 'Incorrect dataframe value. (Max gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].gold_at_5.min(), 986, 'Incorrect dataframe value. (Min gold_at_5)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.max(), 76, 'Incorrect dataframe value. (Max visionScore)')
                self.assertEqual(self.dict1[key]['export_df'].visionScore.min(), 15, 'Incorrect dataframe value. (Min visionScore)')
            itera = itera+1


if __name__ == '__main__':
    unittest.main()
