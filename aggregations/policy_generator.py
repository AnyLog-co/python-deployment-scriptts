import copy


def create_policy(db_name:str, table_name:str, timestamp_column:str, value_columns:list, interval:int,
                  interval_time:str, per_column:bool=False, keep_source:bool=True, keep_aggregation:bool=False,
                  agg_db_name:str=None, agg_table_name:str=None):
    """
    Create base for aggregation policy
    :args:
        db_name:str - logical database name
        table_name:str - logical table name
        timestamp_column:str - timestamp column
        value_columns:list - list of value column(s)
        interval:int - Number of intervals to keep in aggregation
        interval_time:int - how long each interval is for aggregating data
        per_column:bool - if per column is set to true include `value_column` info in new_policy
    :params:
        new_policy:dict - new policy
        command:str - `set aggregation where ...` command
    :return:
        new_policy
    """
    new_policy = {
        "aggregation": {
            "dbms": db_name,
            "table": table_name,
            **({"value_column": value_columns[0]} if per_column else {}),
            "script": [],
        }
    }

    base_cmd = f'set aggregation where dbms={db_name} and table={table_name} and intervals={interval} and time="{interval_time}" and time_column={timestamp_column} and value_column=%s'

    if isinstance(value_columns, str):
        command = copy.deepcopy(base_cmd) % value_columns
        if keep_source and keep_aggregation:
            if agg_db_name:
                command += f" and target_dbms={agg_db_name}"
            if agg_table_name:
                command += f" and target_table={agg_table_name}"
            else:
                command += f" and target_table=agg_{table_name}_{value_columns}"
        new_policy["aggregation"]["script"].append(command)

    elif isinstance(value_columns, list):
        for value_column in value_columns:
            command = copy.deepcopy(base_cmd) % value_column
            if keep_source and keep_aggregation:
                if agg_db_name:
                    command += f" and target_dbms={agg_db_name}"
                if agg_table_name:
                    command += f" and target_table={agg_table_name}_{value_column}"
                else:
                    command += f" and target_table=agg_{table_name}_{value_column}"
            new_policy["aggregation"]["script"].append(command)

    return new_policy


def define_ingestion(new_policy:dict, db_name:str, table_name:str, keep_source:bool=True, keep_aggregation:bool=False):
    """
    define data ingestion - if keeping both source and derived, then derived data would be stored under
    `agg_[table_name]_[value_column]`.
    :args:
        new_policy:dict - aggregation policy
        db_name:str - logical database name
        table_name:str - logical table name
        keep_source:bool - store raw data into operator node(s)
        keep_aggregation:bool - store aggregation results / derived data into operator node(s)
    :params:
        new_policy:dict - new policy
        command:str - `set aggregation ingest where ...` command
    :return:
        new_policy
    """
    command = f"set aggregation ingest where dbms={db_name} and table={table_name} and source={'true' if keep_source else 'false'} and derived={'true' if keep_aggregation else 'false'}"
    new_policy["aggregation"]["script"].append(command)

    return new_policy


def define_encoding(new_policy:dict, db_name:str, table_name:str, value_columns:list, encoding_type:str=None,
                    tolerance_level:int=None):
    """
    define data encoding
    :args:
        new_policy:dict - aggregation policy
        db_name:str - logical database name
        table_name:str - logical table name
        value_column:str - value column name
        encoding_type:str - encoding type
            * None
            * bounds
            * arel
        tolerance_level:int - percentage difference - Approximated Run-Length Encoding, allowable difference between
                              consecutive values while treating them as part of the same group or segment.
    :params:
        new_policy:dict - new policy
        command:str - `set aggregation ingest where ...` command
    :return:
        new_policy
    """
    command = f"set aggregation encoding where dbms={db_name} and table={table_name} and value_column=%s"
    if encoding_type:
        command += f" and encoding={encoding_type}"
    if tolerance_level:
        command += f" and tolerance={tolerance_level}"

    if isinstance(value_columns, str):
        new_policy["aggregation"]["script"].append(command % value_columns)
    else:
        for value_column in value_columns:
            new_policy["aggregation"]["script"].append(command % value_column)

    return new_policy


def define_thresholds(new_policy:dict, db_name:str, table_name:str, value_columns:list, min_value:float, max_value:float,
                      avg_value:float=None, event_count:int=None):
    """
    declare thresholds on each stream. These thresholds can be referenced by the rule engine to impact the processing
    of the stream and trigger operations that consider the thresholds.
    :args:
        new_policy:dict - aggregation policy
        db_name:str - logical database name
        table_name:str - logical table name
        value_column:str - value column name
        min_value:float - min value in threshold
        max_value:float - max value in threshold
        avg_value:float - avg value in threshold
        event_count:int - number of events per threshold
    :params:
        command:str - generated command
    :return;
        updated new_policy
    """
    command = f"set aggregation thresholds where dbms={db_name} and table={table_name} and column=%s and min={min_value} and max={max_value}"

    if avg_value:
        command += f" and avg={avg_value}"
    if event_count:
        command += f" and count={event_count}"

    if isinstance(value_columns, list):
        for value_column in value_columns:
            new_policy["aggregation"]["script"].append(command % value_column)
    elif isinstance(value_columns, str):
        new_policy["aggregation"]["script"].append(command % value_columns)

    return new_policy
