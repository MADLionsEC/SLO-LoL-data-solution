import argparse
from connectors import filesystem, database
from config.constants import SUPPORTED_LEAGUES, SUPPORTED_CONNECTORS, REGIONS, PATCH_PATTERN, API_KEY


def parse_args():
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

    return parser.parse_args()


def main():
    args = parse_args()

    try:
        if not args.region:
            print('No region selected. Please, use -r or --region and select a region to work on from the following '
                  'list: {}'.format(REGIONS.keys()))
            return
        elif args.region.upper() not in REGIONS.keys():
            print('Region {} not supported. Try one of these: {}'.format(args.region.upper(), REGIONS.keys()))
            return
    except AttributeError:
        print('Please, use -r or --region and select a region to work on from the following list: {}'
              .format(REGIONS.keys()))
        return

    if not args.league:
        print('No league selected. Please, use -l or --league and select a league to work on from the following list: '
              '{}'.format(SUPPORTED_LEAGUES))
        return
    elif args.league.upper() not in SUPPORTED_LEAGUES:
        print('League {} not supported. Please, use -l or --league and select a league to work on from the following '
              'list: {}'.format(args.league.upper(), SUPPORTED_LEAGUES))

    if args.patch:
        if not PATCH_PATTERN.match(args.patch):
            print('Patch format is incorrect. Should be something like this:  \'8.9.1\', \'8.9\', \'8\' '
                  '(withouth the \' symbols).')
            return

    if not args.connector:
        print('No connector selected. To select a connector write down one of the following names just after the '
              'script call with region and league parameters set: {}. Something like that: \"python program.py '
              '-r EUW -l SOLOQ -c {}\"'.format(SUPPORTED_CONNECTORS, SUPPORTED_CONNECTORS[0]))
        return
    elif args.connector.upper() == 'FS':
        filesystem.parse_args(args)
    elif args.connector.upper() == 'DB':
        database.parse_args(args, API_KEY)
    else:
        print('That connector is not supported.')
        return


if __name__ == '__main__':
    main()
