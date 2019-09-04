import logging
from requests.models import Response

from EntityLoader import LoadBehaviour


def get_url_params(params: dict) -> str:
    _params = '&'.join(['{}={}'.format(k, v) for k, v in params.items()])
    return '?{}'.format(_params) if len(_params) > 0 else ''


class GithubLoadBehaviour(LoadBehaviour):
    def __init__(self,
                 _token: str,
                 per_page: int,
                 _logger: logging.Logger):
        super().__init__()
        self._per_page = per_page
        self._token = _token
        self._logger = _logger

    def _get_url_params(self, params: dict) -> str:
        _params = '&'.join(['{}={}'.format(k, v) for k, v in params.items()])
        return '?{}'.format(_params) if len(_params) > 0 else ''

    def _get_remaining_limit(self, resp: Response) -> int:
        limit = resp.headers.get('X-RateLimit-Remaining')
        if limit:
            return int(limit)
        return 0

    def _get_params(self, page) -> dict:
        return {
            'per_page': self._per_page,
            'page': page,
            'state': 'all'
        }

    def _get_headers(self) -> dict:
        return {
            'Authorization': 'token {}'.format(self._token)
        }

    def _get_next_page(self, cur_page: int, resp: Response) -> int:
        if resp.status_code < 400:
            return cur_page + 1
        if resp.status_code == 403:
            return cur_page
        return cur_page

    def _is_last_page(self, return_object_count: int, resp: Response) -> bool:
        resp_status = int(resp.status_code)
        if (return_object_count < self._per_page) and resp_status < 400:
            return True
        if resp_status == 404:
            return True
        return False
