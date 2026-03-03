import requests
import pprint

from anylog_api.error_codes import HTTP_STATUS_CODES
from anylog_api.error_codes import REST_EXCEPTION_CODES
from anylog_api.error_codes import REQUEST_EXCEPTION_MAP


class RestConn:
    def __init__(self, conn:str, auth:tuple|None=None, timeout:int=30):
        """
        Generic connection to AnyLog/EdgeLake REST API
        :args:
            conn:str - url base
            auth:tuple - authentication information
            timeout:int - REST timeout
        :params:
            self.url:str - full URL
            self.auth:tuple - authentication information
            self.timeout:int - REST timeout
        """
        self.url = f"http://{conn}" if not conn.startswith("http") else conn
        self.auth = auth
        self.timeout  = timeout

    def execute_command(self, method:str, headers:dict, json_payload:dict|None=None, data_payload:str|None=None)->requests.Response:
        """
        Execute request against the default URL
        :args:
            method:str -  Method to execute
            headers:dict -REST headers
            json_payload:dict - non-serialized (dictionary) payload to publish
            data_payload:str - serialized payload to publish
        :params:
            response:requests.Response - response from REST request
        :raise:
            - not 200 <= status code < 300
            - other  / unknown
        return:
            response
        """
        try:
            response = requests.request(method=method.upper(), url=self.url, headers=headers, data=data_payload,
                                        json=json_payload)
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:

            status_code = error.response.status_code
            status_msg = HTTP_STATUS_CODES.get(status_code)
            if not status_msg:
                # fallback to first-digit mapping to REST_EXCEPTION_CODES
                first_digit = int(str(status_code)[0])
                status_msg = REST_EXCEPTION_CODES.get(first_digit, "Unknown REST error")

            error_msg = (
                f"Failed to execute {method.upper()} against {self.url} "
                f"(Network Error {status_code}: {status_msg} | Response: {error.response.text})"
            )
            raise requests.exceptions.HTTPError(error_msg, response=error.response) from error

        except Exception as error:
            # Any transport/network errors (ConnectionError, Timeout, etc.)

            error_type = type(error).__name__
            error_code = REQUEST_EXCEPTION_MAP.get(error_type, 899)
            error_msg = REST_EXCEPTION_CODES.get(error_code, str(error))

            raise Exception(
                f"Failed to execute {method.upper()} against {self.url} "
                f"(Transport Error {error_code}: {error_msg})"
            ) from error

        return response


    def execute_post(self, headers:dict, json_payload:dict|None=None, data_payload:str|None=None)->requests.Response:
        """
        Execute POST command using `execute_command`
        :args:
            method:str -  Method to execute
            headers:dict -REST headers
            json_payload:dict - non-serialized (dictionary) payload to publish
            data_payload:str - serialized payload to publish
        :params:
            response:requests.Response - response from REST request
        :raise:
            - not 200 <= status code < 300
            - other  / unknown
        return:
            response
        """
        return self.execute_command(method="POST", headers=headers, json_payload=json_payload, data_payload=data_payload)

    def execute_put(self, headers:dict, json_payload:dict|None=None, data_payload:str|None=None)->requests.Response:
        """
        Execute PUT command using `execute_command`
        :args:
            method:str -  Method to execute
            headers:dict -REST headers
            json_payload:dict - non-serialized (dictionary) payload to publish
            data_payload:str - serialized payload to publish
        :params:
            response:requests.Response - response from REST request
        :raise:
            - not 200 <= status code < 300
            - other  / unknown
        return:
            response
        """
        return self.execute_command(method="PUT", headers=headers, json_payload=json_payload, data_payload=data_payload)


    def execute_get(self, headers:dict, parse_results:bool=True)->dict|str|requests.Response:
        """
        Execute GET command using `execute_command` 
        :args:
            method:str -  Method to execute
            headers:dict -REST headers
            parse_results:bool - Parse / extract from response
        :params:
            response:requests.Response - response from REST request
            output:dict|str - parsed content
        :raise:
            - not 200 <= status code < 300
            - other  / unknown
        return:
            output | response
        """
        response = self.execute_command(method="GET", headers=headers, json_payload=None, data_payload=None)

        if parse_results:
            try:
                return response.json()
            except:
                try:
                    return response.content
                except Exception as error:
                    raise Exception(f"Failed to parse content from {self.url} for command {headers.get("command")} (Error: {error})")

        return response

    def get_help(self, command:str):
        """
        Get help for a command
        :args:
            command:str - command to get help for
        :params:
           headers:dict - REST header to execute `help` for the command
        :return:
            print response for `help` against command
        """
        headers = {
            "command": f"help {command}",
            "User-Agent": "AnyLog/1.23"
        }

        response = self.execute_get(headers=headers, parse_results=True)
        pprint.pprint(response)


