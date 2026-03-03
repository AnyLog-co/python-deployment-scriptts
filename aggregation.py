import argparse

from anylog_api.support import extract_credentials
from anylog_api.generic_rest import RestConn
from anylog_api.dbms import check_dbms
from anylog_api.dbms import get_tables, check_table
from anylog_api.dbms import list_columns
from anylog_api.dbms import connect_dbms
from anylog_api.data import set_aggregation
from anylog_api.data import set_ingestion

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("conn", type=str, default=None,
                        help="REST connection information to connect to AnyLog/EdgeLake to publish aggregation commands againsts")
    parser.add_argument("dbms", type=str, default=None, help="logical database to do aggregation against")
    parser.add_argument("--columns", type=str, default=None,
                        help="[table].[column] to define aggregation against, if not set define then do for all")
    parser.add_argument("--aggregation-timestamp", type=str, default="insert_timestamp",
                        help="timestamp column to do aggregations against")
    parser.add_argument("--intervals", type=int, default=10, help="Number of intervals to keep in aggregation")
    parser.add_argument("--interval-time", type=str, default="1 minute", help="time period per interval")
    parser.add_argument("--keep-source", type=bool, nargs='?', default=False, const=True, help="Store raw data into table(s)")
    parser.add_argument("--keep-aggregation", type=bool, nargs='?', default=False, const=True, help="Store aggregation insights into table(s)")

    args = parser.parse_args()

    # connect to AnyLog / EdgeLake
    broker, port, user, password = extract_credentials(credentials=args.conn)
    auth = ()
    if user and password:
        auth = (user, password)
    conn = RestConn(conn=f"{broker}:{port}", auth=auth, timeout=30)
    if not check_dbms(conn=conn, db_name=args.dbms):
        raise ValueError(f"Failed to locate logical database {args.dbms}")

    # Generate default timestamp aggregation column and table columns
    columns = {}
    if args.columns:
        for param in args.columns.split(','):
            table, column = param.strip().split('.')
            if table not in columns and check_table(conn=conn, db_name=args.dbms, table_name=table):
                columns[table] = {
                    "timestamp": None,
                    "columns": []
                }
            columns[table]["columns"].append(column)
    else:
        tables = get_tables(conn=conn, db_name=args.dbms, ignore_pars=True, is_json=True, get_help=False)
        for table in tables:
            columns[table] = {
                "timestamp": "insert_timestamp",
                "columns": []
            }
            table_columns = list_columns(conn=conn, db_name=args.dbms, table_name=table, return_list=False,
                                   ignore_internal_columns=True, is_json=True, get_help=False)
            for column in table_columns:
                if "timestamp" in table_columns[column] and columns[table]["timestamp"] == "insert_timestamp":
                    columns[table]["timestamp"] = column
                else:
                    columns[table]["columns"].append(column)

    if args.columns and args.aggregation_timestamp:
        for param in args.aggregation_timestamp.split(','):
            if '.' not in param:
                for table in columns:
                    columns[table]["timestamp"] = param
            else:
                table, column = param.strip().split('.')
                if columns.get(table):
                    columns[table]["timestamp"] = column

    if args.keep_aggregation:
        """
        :todo: 
        1. db type should be configurable 
        2. partition  
        3. drop partitions scheduled processes  
        """
        connect_dbms(conn=conn, db_name=f"agg_{args.dbms}", db_type="sqlite")

    for table in columns:
        timestamp = columns[table]["timestamp"]
        for column in columns[table]["columns"]:
            set_aggregation(conn=conn, db_name=args.dbms, table_name=table, value_column=column,
                            time_column=timestamp, intervals=args.intervals, interval_time=args.interval_time,
                            keep_aggregation=args.keep_aggregation, target_dbms=f"agg_{args.dbms}",
                            target_table=f"{table}_{column}")
        set_ingestion(conn=conn, db_name=args.dbms, table_name=table, keep_source=args.keep_source,
                      keep_aggregation=args.keep_aggregation)



if __name__ == "__main__":
    main()