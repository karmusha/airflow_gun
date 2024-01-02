import sys

from urllib.request import HTTPPasswordMgrWithPriorAuth
from airflow.models.connection import Connection
from airflow.exceptions import AirflowNotFoundException


class AirflowHTTPConnectionPasswordMgr(HTTPPasswordMgrWithPriorAuth):
    """
    Менеджер паролей, который умеет доставать данные из Airflow connections
    Менеджер паролей можно передать в любой модуль авторизации:
        HTTPBasicAuthHandler,
        HTTPDigestAuthHandler,
        PreemptiveBasicAuthHandler,
        InformaticaAuthHandler,
        HTTPNtlmAuthHandler
    и другой авторизационный модуль, который вызывает
    password manager для получения учётных записей
    """

    def __init__(self, http_conn_url: str, *args, **kwargs):
        self.http_conn_url = http_conn_url
        self._http_connection_processed = False

        super().__init__(*args, **kwargs)

    def add_credentials_from_connection(self, authuri):
        """
        Функция забирает креды из airflow connection и добавляет в password_mgr
        """
        try:
            conn = Connection.get_connection_from_secrets(self.http_conn_url)
        except AirflowNotFoundException as e:
            print(e, file=sys.stderr)
            raise e
        except Exception as e:
            print(e, file=sys.stderr)
            raise e

        realm = None
        if conn.extra_dejson and hasattr(conn.extra_dejson, "realm"):
            realm = conn.extra_dejson.realm

        uri = []
        if authuri:
            uri.append(authuri)
        if hasattr(conn, "host"):
            uri.append(conn.host)

        username = None
        if hasattr(conn, "login"):
            username = conn.login

        password = None
        if hasattr(conn, "password"):
            password = conn.password

        self.add_password(realm, uri, username, password, is_authenticated=False)

    def find_user_password(self, realm, authuri):
        if not self._http_connection_processed:
            self.add_credentials_from_connection(authuri)
            self._http_connection_processed = True

        return super().find_user_password(realm, authuri)
