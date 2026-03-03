from anylog_api.generic_rest import RestConn


def set_aggregation(conn:RestConn|None, db_name:str, table_name:str, value_column:str="*",
                    time_column:str="insert_timestamp", intervals:int=10, interval_time:str="1 minute",
                    keep_aggregation:bool=False, target_dbms:str=None, target_table:str=None, get_help:bool=False):
    """
    Set aggregation on a specific table / column
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database name
        table_name:str - logical table name
        value_column:str - column to aggregate against
        time_column:str - timestamp column to aggregate with
        intervals:int - number of aggregations to keep
        interval_time:str - time period for each interval
        keep_aggregation:bool - keep aggregations
        target_dbms:str - target database for aggregation
        target_table:str - target table for aggregation
        get_help:bool - print help for `set aggregation` instead of running command
    :params:
        headers:dict - REST headers
    :print:
        if get_help - print explanation for command
    """
    headers = {
        "command": f"""set aggregation where 
            dbms={db_name} and 
            table={table_name} and 
            time_column={time_column} and
            value_column={value_column} and
            intervals={intervals} and
            time={interval_time}""".replace("\n", " ").replace("\t", " ").strip(),
        "User-Agent": "AnyLog/1.23"
    }
    if get_help:
        conn.get_help(command=headers["command"])
        return

    if keep_aggregation:
        headers["command"] += f" and target_dbms={target_dbms}" if target_dbms is None else f" and target_dbms=agg_{db_name}"
        if target_table is None and value_column != "value":
            headers["command"] = f" and target_table={table_name.strip()}_{value_column.strip()}"
        elif target_table is None:
            headers["command"] = f" and target_table={table_name.strip()}"
        else:
            headers["command"] += f" and target_table={target_table}"

    conn.execute_post(headers=headers)

def set_ingestion(conn:RestConn, db_name:str, table_name:str="*", keep_source:bool=True, keep_aggregation:bool=False,
                  get_help:bool=False):
    """
    Set data ingestion - whether to only raw content, aggregation or both
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database name
        table_name:str - logical table name
        keep_source:bool - keep source data
        keep_aggregation:bool  - keep aggregation data
        get_help::bool - print help for `set aggregation` instead of running command
    :params:
        headers:dict - REST headers
    :print:
        if get_help - print explanation for command
    """
    headers  = {
        "command": f"""set aggregation ingest where 
            dbms={db_name} and 
            table={table_name} and 
            source={'true' if keep_source else 'false'} and
            derived={'true' if keep_aggregation else 'false'}""".replace("\n", " ").replace("\t", " ").strip(),
        "User-Agent": "AnyLog/1.23"
    }

    if get_help:
        conn.get_help(command=headers["command"])

    conn.execute_post(headers=headers)
