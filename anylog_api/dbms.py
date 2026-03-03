"""
Database specific commands
- list database -> check_dbms
- list tables -> check_table
- list columns in table
- connect to database
- disconnect from database (TBA)
- drop database (TBA)
"""
import copy
from anylog_api.generic_rest import RestConn

def list_dbms(conn:RestConn, is_json:bool=True, get_help:bool=False)->str|dict:
    """
    List logical databases
    :base-command:
        get databases
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        is_json:bool - return content in JSON format
        get_help:bool - return help information for the command
    :params:
        headers:dict - REST headers
        response - response from REST request
    :return:
        if get_help - prints help information for command
        else return response
    """
    headers = {
        "command": "get databases",
        "User-Agent": "AnyLog/1.23"
    }

    if get_help:
        conn.get_help(command=headers.get("command"))
        return None

    if is_json:
        headers["command"] += " where format=json"

    return conn.execute_get(headers=headers, parse_results=True)


def check_dbms(conn:RestConn, db_name:str)->bool:
    """
    Check whether logical database exists
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database to validate if exists
    :params:
        response:dict - response from `list_dbms`
    :return:
        True - exists
        False - DNE
    """
    response = list_dbms(conn=conn, is_json=True, get_help=False)
    for db_id in response:
        if db_id == db_name:
            return True
    return False


def get_tables(conn:RestConn, db_name:str, ignore_pars:bool=False, is_json:bool=True,
               get_help:bool=False)->str|list|dict|None:
    """
    List logical tables
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database name
        is_json:bool - return content in JSON format
        get_help:bool - return help information for the command
    :params:
        headers:dict - REST headers
        response - response from REST request
    :return:
        list of logical tables in a given database
    """
    headers = {
        "command": f"get tables where dbms={db_name}",
        "User-Agent": "AnyLog/1.23"
    }

    if get_help:
        conn.get_help(command=headers.get("command"))
        return None

    if  is_json:
        headers["command"] += " and format=json"
    
    response = conn.execute_get(headers=headers, parse_results=True)

    if response and isinstance(response, dict) and response.get(db_name):
        if ignore_pars:
            return [table for table in response[db_name] if not table.startswith("par_")]
        else:
            return [table for table in response[db_name]]

    return response

def check_table(conn:RestConn, db_name:str, table_name:str)->bool:
    """
    Check whether a table exists or not
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database name
        table_name:str - logical table name
    :return:
        if table exists in
    """
    response = get_tables(conn=conn, db_name=db_name, ignore_pars=False, is_json=True, get_help=False)
    return True if table_name in response else False


def list_columns(conn:RestConn, db_name:str, table_name:str|None, return_list:bool=True, ignore_internal_columns:bool=False,
                 is_json:bool=True, get_help:bool=False)->str|dict|list|None:
    """
    List columns in a given table
    :args:
        conn:RestConn - connection to AnyLog / EdgeLake
        db_name:str - logical database name
        return_list:bool - Return list of columns, otherwise return with their column type
        ignore_internal_columns:bool - ignore columns like row_id and tsd info
        is_json:bool - return content in JSON format
        get_help:bool - return help information for the command
    :params:
        headers:dict - REST headers
        response - response from REST request
    :return:
        column information
    """
    headers = {
        "command": f"get columns where dbms={db_name} and table={table_name}",
        "User-Agent": "AnyLog/1.23"
    }

    if get_help:
        conn.get_help(command=headers.get("command"))
        return None

    if is_json:
        headers["command"] += " and format=json"

    response = conn.execute_get(headers=headers, parse_results=True)
    if ignore_internal_columns:
       for key in ["row_id", "tsd_name", "tsd_id"]:
           del response[key]
       tmp_response = copy.deepcopy(response)
       for key, value in response.items():
           if "timestamp" in value and key != "insert_timestamp" and "insert_timestamp" in tmp_response:
               del tmp_response["insert_timestamp"]

       response = tmp_response

    return list(response.keys()) if return_list else response



def connect_dbms(conn:RestConn, db_name:str, db_type:str, host:str="!ip", port:int|None=None, user:str|None=None,
                 password:str|None=None, in_memory:bool=False, get_help:bool=False):
    """
    Connect to logical database if DNE
    :args:
        conn:RestConn - connection to AnyLog
        db_name:str - logical database to connect to
        db_type:str - physical database type
        host:str - database IP
        port:int - database port
        user:str / password:str - database login credentials
    :params:
        headers:dict - REST headers:
    :issues:
        if user doesn't specify database information, it should automatically be derived from the node
    """
    headers = {
        "command": f"connect dbms {db_name} where type={db_type}",
        "User-Agent": "AnyLog/1.23"
    }

    if get_help:
        conn.get_help(command=headers["command"])
        return None

    if db_type != "sqlite":
        if host:
            headers["command"] += f" and ip={host}"
        if port:
            headers["command"] += f" and port={port}"
        if user:
            headers["command"] += f" and user={user}"
        if password:
            headers["command"] += f" and password={password}"
    if in_memory:
        headers["command"] += " and memory=true"

    if not check_dbms(conn=conn, db_name=db_name):
        conn.execute_post(headers=headers, data_payload=None, json_payload=None)
    return None
