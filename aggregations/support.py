import requests

def rest_call(request_type:str, conn:str, headers:dict, payload:str=None):
    """
    Basic REST call
    :args:
        request_type:str - REST request type
        conn:str - REST conn
        headers:dict - REST headers
        payload:str - content to publish (new_policy)
    :params:
        response:requests.requests - response from RESt request
    :return:
        response
    """
    try:
        response = requests.request(method=request_type.upper(), url=f"http://{conn}", headers=headers, data=payload)
        response.raise_for_status()
    except Exception as error:
        raise Exception(f"Failed to execute {request_type.upper()} against {conn} (Error: {error})")
    return response
