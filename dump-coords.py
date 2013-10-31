#!/usr/bin/env python
# coding: utf-8

'''
--db-host = (required) - Default: 127.0.0.1
--db-database = (required)
--db-user = (required)
--db-pass = (required)
--db-port = Default: 3306
--db-table = (required)
--output-dir = директория, в която ще се записват резултатите. Default: ./output
--progress-file = файл, в който ще се записва до кое ID е стигнала обработката; Default: ./$0.progress
--max-records = максимален брой записи, които трябва да обработи. След като обработи толкова записи, скриптът трябва да спре; 0 - работи докато не свършат записите в базата. Default: 0
--batch-size = брой записи за извличане от базата на един пас; Default: 1000
--progress-on-every = брой обработени записи на които ще се извежда съобщение за прогрес; Default: 100'000
'''

import argparse
import sys
import os
import MySQLdb
import csv
import timeit
import re


def main():
    args = None
    try:
        args = _get_arguments()
        _dump_coords(args)
    finally:
        con = getattr(args, 'db_connection', None)
        if con:
            con.close()

    return 0


    
def _get_arguments(argv=sys.argv[1:]):
    """ 
    Prepares and parses command line arguments for the script
    """
    
    parser = argparse.ArgumentParser(description='Initial parameters.')

    parser.add_argument('--db-host',
                        default='127.0.0.1',
                        help='Databese IP')

    parser.add_argument('--db-database',
                        help='Database name', required=True)

    parser.add_argument('--db-user',
                        help='Database user', required=True)

    parser.add_argument('--db-pass',
                        help='Database password', required=True)

    parser.add_argument('--db-port',
                        default=3306,
                        type=int,
                        help='Database port (default: 3306)')

    parser.add_argument('--db-table',
                        help='Database table', required=True)

    parser.add_argument('--output-dir',
                        default='./output',
                        help='Output directory (default: ./output)')

    parser.add_argument('--progress-file',
                        default='./dump-coords.progress',
                        help='Name of the file whith progress data (default: ./dump-coords.progress)')

    parser.add_argument('--max-records',
                        default=0,
                        type=int,
                        help='Number of records to download (default: 0 - all records)')

    parser.add_argument('--batch-size',
                        default=1000,
                        type=int,
                        help='Number of records transfered in one pass (default: 1000)')

    parser.add_argument('--progress-on-every',
                        default=100000,
                        type=int,
                        help='Number of records to display progress message (default: 100000)')

    args = parser.parse_args(argv)

    _validate_argument(os.access(args.output_dir, os.W_OK) \
                            or os.access(os.path.dirname(args.output_dir), os.W_OK),
                       "Output directory must be writable: '{}'".format(args.output_dir))
    
    _validate_argument(os.access(args.progress_file, os.W_OK) \
                            or os.access(os.path.dirname(args.progress_file), os.W_OK),
                       "Progress file must be writable or its directory must allow for creating the file: '{}'".format(
                            args.progress_file
                        ))

    re_valid_table_name = re.compile('[^a-zA-Z_0-9]')
    args.db_table = re_valid_table_name.sub('', args.db_table)
    _validate_argument(len(args.db_table) > 0,
                       u"Error: Invalid table name provided")

    _validate_argument(args.max_records >= 0,
                       u"Error: Max records must be > 0")

    _validate_argument(args.batch_size > 0,
                       u"Error: Batch size must be > 0")

    _validate_argument(args.progress_on_every > 0,
                       u"Error: Progress every must be > 0")

    _validate_argument(args.max_records >= args.batch_size or args.max_records == 0,
                       u"Error: Batch size is bigger then number of records to download")

    _validate_argument(args.max_records % args.batch_size == 0,
                       u"Error: Number of records to download cannot be split into batch size")

    _validate_argument(args.batch_size <= args.progress_on_every or args.progress_on_every % args.batch_size == 0,
                       u'Error: Progress on every cannot be calculated')


    # validate db connection parameters and store a connection object into args
    try:
        args.db_connection = _connect_to_database(args)
    except Exception as e:
        _validate_argument(False, u"Cannot connect to database: '%s'" % (unicode(e)))


    print("Arguments:")
    print("\tDatabase host: {}".format(args.db_host))
    print("\tDatabase name: {}".format(args.db_database))
    print("\tDatabase user: {}".format(args.db_user))
    print("\tDatabase pass: {}".format(args.db_pass))
    print("\tDatabase port: {}".format(args.db_port))
    print("\tDatabase table: {}".format(args.db_table))
    print("\tOutput dir: {}".format(args.output_dir))
    print("\tProgress file: {}".format(args.progress_file))
    print("\tMax records: {}".format(args.max_records))
    print("\tBatch size: {}".format(args.batch_size))
    print("\tProgress on every: {}".format(args.progress_on_every))

    return args



def _validate_argument(condition, error_message):
    if not condition:
        print("Invalid argument: {}".format(error_message))
        sys.exit(1)


def _connect_to_database(args):
    """
    Tries to connect to the database and returns a connection
    
    :param args: object containing settings as attributes.
                 Requires: db_host, db_port, db_user, db_pass, db_table
    :return: DB connection
    :raises: Exception if the database connection fails
    """
    print 'Connecting to database ...',

    db_conn = MySQLdb.connect(args.db_host, args.db_user, args.db_pass, args.db_database)

    db_curr = None
    try:
        sql = '''
            SELECT
                1
            FROM
                %s
            LIMIT
                1
        ''' % args.db_table

        db_curr = db_conn.cursor()
        db_curr.execute(sql)
    except Exception as e:
        print 'Error in connection', e
        sys.exit(1)
    finally:
        if db_curr:
            db_curr.close()

    print 'Done'

    return db_conn


ID_COLUMN_INDEX = 0
TIMESTAMP_COLUMN_INDEX = 4


def _dump_coords(args):
    """
    """
    total_elapsed_time = 0
    total_records = 0

    # Just in case - should not fail
    assert args.max_records >= 0
    assert args.batch_size > 0
    assert args.progress_on_every > 0
    assert args.max_records >= args.batch_size or args.max_records == 0
    assert args.max_records % args.batch_size == 0
    assert args.batch_size <= args.progress_on_every or args.progress_on_every % args.batch_size == 0

    db_curr = args.db_connection.cursor()

    print ('Processing {} records'.format(args.max_records))
    count = 0
    max_id = int(load_progress(args.progress_file))
    while count < args.max_records or args.max_records <= 0:
        start = timeit.default_timer()

        coords = get_coords_batch(db_curr, args.db_table, max_id, args.batch_size)

        if not coords:
            break

        max_id, min_id = coords[-1][ID_COLUMN_INDEX]


        coords_dict = create_coords_dict(coords)

        for coords_date, value in coords_dict.iteritems():
            write_coords_to_csv(args.output_dir, value, coords_date)

        save_progress(args.progress_file, str(max_id))

        records = len(coords)
        count += records
        total_records += records

        stop = timeit.default_timer()
        elapsed_time = stop - start
        total_elapsed_time += elapsed_time

        if count % args.progress_on_every == 0:
            percent = 0
            if args.max_records > 0:
                percent = 100*float(count)/float(args.max_records)
                
            print '\t[ {:.2f}% ] {} records in {:.2f} secs;'.format(percent, total_records, elapsed_time),

            print 'reached {}; max_id {}'.format(coords_date.strftime('%Y-%m-%d'), max_id)



    print 'Done! \n Total time {:.3f} secs. \n Total records {}.'.format(total_elapsed_time, total_records)

    if db_curr:
        db_curr.close()


def create_coords_dict(coords):
    """
    Returns a dict containing rows for each date found in the coords list
        {
            datetime.date(2013, 10, 3): [
                (4324252,"", .....),
                (4324253,"", .....),
                (4324254,"", .....),
                (4324255,"", .....),
            ],

            datetime.date(2013, 10, 4): [
                (5324252,"", .....),
                (5324253,"", .....),
                (5324254,"", .....),
                (5324255,"", .....),
            ],
        }

    :param coords: tuple of tuples containing records as fetched from the DB
    :return: dict of lists, as described above
    """
    result = {}

    for row in coords:
        row_date = row[TIMESTAMP_COLUMN_INDEX].date()
        rows_for_date = result.setdefault(row_date, [])
        rows_for_date.append(row)

    return result


def save_progress(file_name, max_id):
    """
    Writes the output value to file
    :param file_name:
    :param max_id:
    :raises: IOError
    """
    with open(file_name, 'w') as f:
        f.write(max_id)


def load_progress(file_name):
    """
    Expects the file that contain only one integer value
    :param file_name:
    :return: Loaded value or 0 if value can`t be read
    """
    max_id = 0
    try:
        with open(file_name, 'r') as f:
            max_id = int(f.read())
    except:
        print ("No valid progress file found. Starts from 0")

    return max_id


def get_coords_filename(output_dir, date):
    """

    """
    return "%s/%s-%s-%s-coords.csv" % (output_dir, date.strftime('%Y'), date.strftime('%m'), date.strftime('%d'))


def write_coords_to_csv(output_dir, coords, coords_date):
    """
    :param output_dir:
    :param coords:
    :param coords_date:
    :returns:
    """
    output_dir_csv = os.path.join(output_dir, coords_date.strftime('%Y'), coords_date.strftime('%m'))
    if not os.path.exists(output_dir_csv):
        os.makedirs(output_dir_csv)

    with open(get_coords_filename(output_dir_csv, coords_date), 'a') as test_file:
        csv_writer = csv.writer(test_file)
        csv_writer.writerows(coords)


def get_coords_batch(db_curr, table, start_from, limit):
    sql = '''
        SELECT
            *
        FROM
            {}
        WHERE
            id > %s
        ORDER BY
            id
        LIMIT
            %s
    '''.format(table)

    db_curr.execute(sql, (start_from, limit))
    coords = db_curr.fetchall()
    return coords

if __name__ == '__main__':
    sys.exit(main())
