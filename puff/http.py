from typing import Optional, Dict, Any
from . import wrap_async, rust_objects


DEFAULT_TIMEOUT = 30 * 1000


class HttpResponse:
    def __init__(self, rust_response):
        self.rust_response = rust_response
        self._headers = None
        self._cookies = None

    @property
    def status(self) -> int:
        return self.rust_response.status()

    @property
    def headers(self) -> Dict[str, bytes]:
        """
        Access and parse all headers
        """
        if self._headers is None:
            self._headers = self.rust_response.headers()
        return self._headers

    @property
    def cookies(self) -> Dict[str, str]:
        """
        Access all cookies.
        """
        if self._cookies is None:
            self._cookies = self.rust_response.cookies()
        return self._cookies

    def header(self, header_name: str) -> Optional[bytes]:
        """
        Access and parse a single header. (more efficient if you don't need all headers).
        """
        return self.rust_response.header(header_name)

    def json(self):
        """
        Consume the response and return the body as json (parsed in Rust).
        You will be unable to get new data (cookie, headers, body) after.
        """
        return wrap_async(lambda r: self.rust_response.json(r))

    def body(self) -> bytes:
        """
        Consume the response and return the body as bytes.
        You will be unable to get new data (cookie, headers, body) after.
        """
        return wrap_async(lambda r: self.rust_response.body(r))

    def text(self) -> str:
        """
        Consume the response and return the body as a string.
        You will be unable to get new data (cookie, headers, body) after.
        """
        return wrap_async(lambda r: self.rust_response.body_text(r))


class HttpClient:
    def __init__(self, rust_http):
        self.rust_http = rust_http

    def request(
        self,
        method: str,
        url: str,
        headers=None,
        json: Optional[Any] = None,
        body: Optional[bytes] = None,
        data: Optional[Dict[str, str]] = None,
        files=None,
        timeout_ms=DEFAULT_TIMEOUT,
    ) -> HttpResponse:
        """Constructs and sends a request
        :param method: method for the new :class:`Request` object: ``GET``, ``OPTIONS``, ``HEAD``, ``POST``, ``PUT``, ``PATCH``, or ``DELETE``.
        :param url: URL for the new :class:`Request` object.
        :param json: (optional) Data to send as a JSON request (uses self.json)
        :param body: (optional) Raw bytes to send as the body.
        :param data: (optional) Dictionary to send in the body encoded as a form.
        :param headers: (optional) Dictionary of HTTP Headers to send with the :class:`Request`.
        :param files: (optional) Dictionary of ``'name': file-like-objects`` (or ``{'name': file-tuple}``) for multipart encoding upload.
        ``file-tuple`` can be a 2-tuple ``('filename', fileobj)``.
        :param timeout_ms: (optional) How many milliseconds to wait for the server to send data before giving up.
        :type timeout_ms: int
        :return: :class:`HttpResponse <HttpResponse>` object
        :rtype: puff.http.HttpResponse
        """

        if json is not None:
            return self.json(method, url, headers, json)

        def run_request(r):
            def new_r(v, e):
                if e is None:
                    v = HttpResponse(v)
                return r(v, e)

            return self.rust_http.request(
                new_r, method, url, headers, body, data, files, timeout_ms
            )

        return wrap_async(run_request)

    def json(
        self, method: str, url: str, headers=None, json=None, timeout=DEFAULT_TIMEOUT
    ) -> HttpResponse:
        """
        Send JSON request to an endpoint.

        This method is optimized by serializing python objects in Rust
        """

        def run_json(r):
            def new_r(v, e):
                if e is None:
                    v = HttpResponse(v)
                return r(v, e)

            return self.rust_http.request_json(
                new_r, method, url, headers, json, timeout
            )

        return wrap_async(run_json)

    def get(self, url: str, **kwargs) -> HttpResponse:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> HttpResponse:
        return self.request("POST", url, **kwargs)

    def patch(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PATCH", url, **kwargs)

    def put(self, url: str, **kwargs) -> HttpResponse:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> HttpResponse:
        return self.request("DELETE", url, **kwargs)

    def head(self, url: str, **kwargs) -> HttpResponse:
        return self.request("HEAD", url, **kwargs)

    def options(self, url: str, **kwargs) -> HttpResponse:
        return self.request("OPTIONS", url, **kwargs)


global_http_client = HttpClient(rust_objects.global_http_client_getter())
