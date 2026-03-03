def extract_credentials(credentials:str):
    """
    Extract credentials from user input
    :args:
        credentials:str - connection information
            [ip]:[port]
            [user]:[password]@[ip]:[port]
    :params:
        broker:str -  IP
        port:int - port associated with IP
        user:str - user used for security
        password:str - password associated with user
    :return:
        broker, port, user, password
    """
    user = None
    password = None
    broker, port = credentials.split(':')
    if '@' in credentials:
        creds, conn = credentials.split('@')
        user, password = creds.split(':')
        broker, port = conn.split(":")
    try:
        port = int(port)
    except:
        pass

    return broker, port, user, password