from base64 import b64encode

from urllib.request import (
    HTTPBasicAuthHandler,
)


class PreemptiveBasicAuthHandler(HTTPBasicAuthHandler):
    """
    Basic авторизация с привентивным выставлением Authorization заголовка, не дожидаясь ошибки 401
      При обычной basic авторизации сначала выполняется тестовый запрос
      Если возвращается ошибка 401, то формируется второй запрос с Authorization заголовком
    PreemptiveBasic формирует authorization заголовок заранее и посылает его в первом же запросе
    """

    def http_request(self, req):
        url = req.get_full_url()
        realm = None
        user, passwd = self.passwd.find_user_password(realm, url)
        if passwd:
            raw = f"{user}:{passwd}"
            raw = raw.encode("ascii")
            auth = b64encode(raw)
            auth = auth.decode("ascii")
            auth = f"Basic {auth}"
            req.add_unredirected_header(self.auth_header, auth)
        return req

    https_request = http_request
