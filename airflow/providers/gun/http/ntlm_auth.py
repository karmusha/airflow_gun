"""
Модуль ntlm авторизации для http клиента
"""

import socket
import re
from urllib.request import HTTPPasswordMgr, URLError, BaseHandler, addinfourl
from http.client import HTTPConnection, HTTPSConnection

from airflow.providers.gun.http.ntlm import ntlm


class AbstractNtlmAuthHandler:
    """AbstractNtlmAuthHandler"""

    def __init__(self, password_mgr=None):
        if password_mgr is None:
            password_mgr = HTTPPasswordMgr()
        self.passwd = password_mgr
        self.add_password = self.passwd.add_password

    def http_error_authentication_required(self, auth_header_field, req, fp, headers):
        """http_error_authentication_required"""

        auth_header_value_list = headers.get_all(auth_header_field)
        if auth_header_value_list:
            if any([hv.lower() == "ntlm" for hv in auth_header_value_list]):
                fp.close()
                return self.retry_using_http_NTLM_auth(
                    req, auth_header_field, None, headers
                )

    def retry_using_http_NTLM_auth(self, req, auth_header_field, realm, headers):
        """retry_using_http_NTLM_auth"""

        user, pw = self.passwd.find_user_password(realm, req.get_full_url())
        if pw is not None:
            user_parts = user.split("\\", 1)
            if len(user_parts) == 1:
                UserName = user_parts[0]
                DomainName = ""
                type1_flags = (
                    ntlm.NTLM_TYPE1_FLAGS & ~ntlm.NTLM_NegotiateOemDomainSupplied
                )
            else:
                DomainName = user_parts[0].upper()
                UserName = user_parts[1]
                type1_flags = ntlm.NTLM_TYPE1_FLAGS
            # ntlm secures a socket, so we must use the same socket for the complete handshake
            headers = dict(req.headers)
            headers.update(req.unredirected_hdrs)
            auth = f"NTLM {ntlm.create_NTLM_NEGOTIATE_MESSAGE(user, type1_flags)}"
            if req.headers.get(auth_header_field, None) == auth:
                return None
            headers[auth_header_field] = auth
            host = req.host
            if not host:
                raise URLError("no host given")
            h = None
            if req.get_full_url().startswith("https://"):
                h = HTTPSConnection(host)  # will parse host:port
            else:
                h = HTTPConnection(host)  # will parse host:port
            # we must keep the connection because NTLM authenticates the connection, not single requests
            headers["Connection"] = "Keep-Alive"
            headers = dict((name.title(), val) for name, val in list(headers.items()))
            h.request(req.get_method(), req.selector, req.data, headers)
            r = h.getresponse()
            r.begin()
            r._safe_read(int(r.getheader("content-length")))
            try:
                if r.getheader("set-cookie"):
                    # this is important for some web applications that store authentication-related info in cookies (it took a long time to figure out)
                    headers["Cookie"] = r.getheader("set-cookie")
            except TypeError:
                pass
            r.fp = None  # remove the reference to the socket, so that it can not be closed by the response object (we want to keep the socket open)
            auth_header_value = r.getheader(auth_header_field, None)

            # some Exchange servers send two WWW-Authenticate headers, one with the NTLM challenge
            # and another with the 'Negotiate' keyword - make sure we operate on the right one
            m = re.match("(NTLM [A-Za-z0-9+\-/=]+)", auth_header_value)
            if m:
                (auth_header_value,) = m.groups()

            (ServerChallenge, NegotiateFlags) = ntlm.parse_NTLM_CHALLENGE_MESSAGE(
                auth_header_value[5:]
            )
            auth = f"NTLM {ntlm.create_NTLM_AUTHENTICATE_MESSAGE(ServerChallenge, UserName, DomainName, pw, NegotiateFlags)}"
            headers[auth_header_field] = auth
            headers["Connection"] = "Close"
            headers = dict((name.title(), val) for name, val in list(headers.items()))
            try:
                h.request(req.get_method(), req.selector, req.data, headers)
                # none of the configured handlers are triggered, for example redirect-responses are not handled!
                response = h.getresponse()

                def notimplemented():
                    raise NotImplementedError

                response.readline = notimplemented
                return addinfourl(
                    response, response.msg, req.get_full_url(), response.code
                )
            except socket.error as err:
                raise URLError("socket error") from err
        else:
            return None


class HTTPNtlmAuthHandler(AbstractNtlmAuthHandler, BaseHandler):
    """Модуль ntlm авторизации"""

    auth_header = "Authorization"

    def http_error_401(self, req, fp, _code, _msg, headers):
        """ "Обработка ошибки 401"""
        return self.http_error_authentication_required(
            "www-authenticate", req, fp, headers
        )


class ProxyNtlmAuthHandler(AbstractNtlmAuthHandler, BaseHandler):
    """
    CAUTION: this class has NOT been tested at all!!!
    use at your own risk
    """

    auth_header = "Proxy-authorization"

    def http_error_407(self, req, fp, _code, _msg, headers):
        """ "Обработка ошибки 407"""
        return self.http_error_authentication_required(
            "proxy-authenticate", req, fp, headers
        )
