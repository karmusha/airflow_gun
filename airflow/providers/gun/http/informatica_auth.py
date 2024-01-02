from urllib.request import (
    BaseHandler,
    HTTPPasswordMgr,
)


class InformaticaAuthHandler(BaseHandler):
    """
    Авторизационный модуль для Informatica
    Фомирует три заголовка:
        username - имя пользователя. Имя пользователя может быть в двух вариантах написания
            - username - непосредственно только username.
                В таком случае securitydomain станет равен Native
            - securitydomain\\username - передан securitydomain
                и username в одном моле через разделитель \\
        encryptedpassword   - зашифрованный пароль пользователя. 
            Зашифровать можно с помощью pmpasswd утилиты, которая расположена
            на сервере с установленной informatiuca
            - https://knowledge.informatica.com/s/article/528158?language=en_US
            - INFA_HOME/server/bin/pmpasswd <password to encrypt> -e CRYPT_DATA
        securitydomain      - доменное имя (как в Informatica Web Administator),
            чувстительно к регистру. По умолчанию используется Native
    Авторизация в Informatica работает аналогично preemptive варианту
        - заголовки подставляются в запрос не дожидаясь повторного взапроса из-за ошибки 401
    """

    auth_header_username = "username"
    auth_header_encryptedpassword = "encryptedpassword"
    auth_header_securitydomain = "securitydomain"

    def __init__(self, password_mgr=None):
        if password_mgr is None:
            password_mgr = HTTPPasswordMgr()
        self.passwd = password_mgr
        self.add_password = self.passwd.add_password

    def http_request(self, req):
        """
        Добавляет авторизационные заголовки в http request
        """
        url = req.get_full_url()
        realm = None
        user, passwd = self.passwd.find_user_password(realm, url)
        if passwd:
            user_parts = user.split("\\", 1)
            if len(user_parts) == 1:
                domain_name = "Native"
                user_name = user_parts[0]
            else:
                domain_name = user_parts[0]
                user_name = user_parts[1]

            req.add_unredirected_header(self.auth_header_username, user_name)
            req.add_unredirected_header(self.auth_header_encryptedpassword, passwd)
            req.add_unredirected_header(self.auth_header_securitydomain, domain_name)
        return req

    https_request = http_request
