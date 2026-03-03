import ast
import json
from support import rest_call

def get_policy_id(conn:str, db_name:str, table_name:str, value_column:str=None, per_column:bool=False):
    """
    Based on a few WHERE conditions, get the policy ID if policy exists
    :args:
        conn:str - REST conn
        db_name:str - logical database name
        table_name:str - logical table name
        value_column:str - specific column if aggregation is unique per column
    :params:
        headers:dict - REST header
        response:requests.response - response from REST
    :return:
        policy ID if policy exists else empty list
    """
    headers = {
        "command": f"blockchain get aggregation where dbms={db_name} and table={table_name} bring [*][id]",
        "User-Agent": "AnyLog/1.23"
    }
    if per_column:
        headers["command"] = headers["command"].replace(f"table={table_name}", f"table={table_name} and value_column={value_column}")

    response = rest_call(request_type="GET", conn=conn, headers=headers)
    try:
        return ast.literal_eval(response.text)
    except:
        return response.text


def declare_policy(conn:str, new_policy:dict):
    """
    Declare policy on the blockchain
    :args:
        conn:str - REST connection info
        new_policy:dict - generate aggregation policy
    :params:
        payload:str - payload to publish policy
        headers:dict - REST headers
    """
    payload = f"<new_policy={json.dumps(new_policy)}>"
    headers = {
        "command": "blockchain insert where policy=!new_policy and local=true and master=!ledger_conn",
        "User-Agent": "AnyLog/1.23"
    }

    rest_call(request_type="POST", conn=conn, headers=headers, payload=payload)

def config_policy(conn:str, policy_id:str):
    """
    Execute config from policy based on policy ID
    :args:
        conn:str - REST connection info
        policy_id:str - blockchain ID for the aggregation policy
    :params:
        headers:dict - REST headers
    """
    headers = {
        "command": f"config from policy where id={policy_id}",
        "User-Agent": "AnyLog/1.23"
    }

    rest_call(request_type="POST", conn=conn, headers=headers)