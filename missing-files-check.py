import argparse
import fnmatch
import sys
import os
import MySQLdb
import csv
import timeit
import re
from datetime import datetime, timedelta

def main():
    args = _get_arguments()

    root = args.output_dir

    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    day_count = end_date - start_date

    flag_exist = False

    found_files = []
    missing_dates = []

    for single_date in (start_date + timedelta(days=n) for n in xrange(day_count.days)):

        flag_exist = False
        for path, subdirs, files in os.walk(root):
            for name in files:
                if fnmatch.fnmatch(name, '%s*' % single_date.isoformat()):
                    found_files.append(os.path.join(path, name))
                    flag_exist = True
                    break

        if not flag_exist:
            missing_dates.append(single_date)

    print("Missing:")
    for el in missing_dates:
        print el

    print("Found:")
    for el in found_files:
        print el

    return 0


def _get_arguments(argv=sys.argv[1:]):
    """
    Prepares and parses command line arguments for the script
    """
    
    parser = argparse.ArgumentParser(description='Initial parameters.')


    parser.add_argument('--start-date',
                        help='Start date')

    parser.add_argument('--end-date',
                        help='End date')

    parser.add_argument('--output-dir',
                        default="./output",
                        help='Output dir')

    args = parser.parse_args(argv)
    return args


def get_coords_filename( date):
    """

    """
    return "%s-%s-%s-coords.csv" % ( date.strftime('%Y'), date.strftime('%m'), date.strftime('%d'))


if __name__ == '__main__':
    sys.exit(main())
