import logging
from typing import Tuple, Optional, Dict, Any

from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_base import (
    _TokenAuth,
    AirflowException,
    AuthBase,
    HTTPBasicAuth,
    RetryError,
    USER_AGENT_HEADER,
    requests,
    requests_exceptions,
)

logger = logging.getLogger(__file__)


class DatabricksHookUpdated(DatabricksHook):
    """Overwrites airflow.contrib.hooks.databricks_hook.DatabricksHook() to be able to upload files.
    DatabricksHook not able to POST files to api, due to requests realisation. This class changes it.
    Original:
        request_func(url,
                     json=json if method in ('POST', 'PATCH') else None,
                     params=json if method == 'GET' else None,
                     auth=auth,
                     headers=headers,
                     timeout=self.timeout_seconds,
     New:
         request_func(url,
                      json=json if method in ('POST', 'PATCH') else None,
                      params=json if method == 'GET' else None,
                      files=payload,
                      auth=auth,
                      headers=headers,
                      timeout=self.timeout_seconds,

     https://docs.databricks.com/dev-tools/api/latest/dbfs.html#put
    """

    def __init__(self):
        super().__init__()

    def _do_api_call(
            self,
            endpoint_info: Tuple[str, str],
            json: Optional[Dict[str, Any]] = None,
            wrap_http_errors: bool = True,
            payload: Optional[Any] = None,
    ):
        """
        Utility function to perform an API call with retries

        :param endpoint_info: Tuple of method and endpoint
        :param json: Parameters for this API call.
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        method, endpoint = endpoint_info

        # TODO: get rid of explicit 'api/' in the endpoint specification
        url = f'https://{self.host}/{endpoint}'

        aad_headers = self._get_aad_headers()
        headers = {**USER_AGENT_HEADER.copy(), **aad_headers}

        auth: AuthBase
        token = self._get_token()
        if token:
            auth = _TokenAuth(token)
        else:
            self.log.info('Using basic auth.')
            auth = HTTPBasicAuth(self.databricks_conn.login, self.databricks_conn.password)

        request_func: Any
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        elif method == 'PATCH':
            request_func = requests.patch
        elif method == 'DELETE':
            request_func = requests.delete
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        try:
            for attempt in self._get_retry_object():
                with attempt:
                    response = request_func(
                        url,
                        json=json if method in ('POST', 'PATCH') else None,
                        params=json if method == 'GET' else None,
                        files=payload,
                        auth=auth,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
        except RetryError:
            raise AirflowException(f'API requests to Databricks failed {self.retry_limit} times. Giving up.')
        except requests_exceptions.HTTPError as e:
            if wrap_http_errors:
                raise AirflowException(
                    f'Response: {e.response.content}, Status Code: {e.response.status_code}'
                )
            else:
                raise e


class DBFS:
    def cp_to_dbfs(self, source_file: str, target_path: str, overwrite: str = "true"):
        with open(source_file, "rb") as file:
            file_content = file.read()
        hook = DatabricksHookUpdated()
        DBFS_API_PUT_ENDPOINT = ("POST", "api/2.0/dbfs/put")
        try:
            hook._do_api_call(
                DBFS_API_PUT_ENDPOINT,
                {"path": target_path, "overwrite": overwrite},
                payload={"file": file_content},
            )
        except Exception as e:
            logger.exception(e)
