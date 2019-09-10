import json
import logging
import requests

from EntityLoader import LoadContext, Loading
from github_loading import GithubLoadBehaviour


class SimplePageableBehaviour(GithubLoadBehaviour):
    def __init__(self,
                 _token: str,
                 per_page: int,
                 _logger: logging.Logger,
                 _loading_obj: str,
                 _base_url: str,
                 _headers: str,
                 _params: str,
                 _token_id: int,
                 _proc_uuid: str):
        super().__init__(_token, per_page, _logger)
        self._loading_obj_name = _loading_obj
        self._base_url = _base_url
        self._headers = _headers
        self._params = _params
        self._token_id = _token_id
        self._proc_uuid = _proc_uuid

    def _build_url(self) -> str:
        return self._base_url

    def handle_error(self, obj: LoadContext, e: Exception, loading: Loading):
        self._logger.error('url: {}, loading_id: {}, error with message: {}'.format(obj.url, loading.id, str(e)))

    def get_load_context(self):
        return LoadContext(
            self._build_url(),
            params=self._get_params(None),
            headers=self._get_headers(),
            obj={'page': 1, 'remaining': -1, 'token_id': self._token_id, 'proc_uuid': self._proc_uuid}
        )

    def _get_params(self, page: int) -> dict:
        _prms = json.loads(self._params)
        if page:
            _prms['page'] = page
        return _prms

    def _get_headers(self) -> dict:
        _hdrs = json.loads(self._headers)
        _hdrs['Authorization'] = 'token {}'.format(self._token)
        return _hdrs

    def load(self, obj: LoadContext, loading: Loading):
        current_page = obj.obj['page']
        _token_id = obj.obj.get('token_id', None)
        _proc_uuid = obj.obj.get('proc_uuid', None)
        url = '{}{}'.format(loading.url, self._get_url_params(obj.params))

        resp = requests.get(url, headers=obj.headers)

        resp_status = int(resp.status_code)
        remaining_limit = self._get_remaining_limit(resp)

        next_page = self._get_next_page(current_page, resp)

        rv_objs = []
        if resp_status < 400:
            rv_objs = json.loads(resp.text)

        self._logger.info('token_id: {}, proc_uuid: {}, type: {}, state: {}, page: {}, count: {}, limit: {}, url: {}'.format(
            _token_id, _proc_uuid,
            self._loading_obj_name, resp.status_code, current_page,
            len(rv_objs), remaining_limit, url
        ))

        if int(remaining_limit if remaining_limit else 1) <= 0:
            self._logger.warn('token_id {} is expired'.format(_token_id))

        load_result = obj.get_simplified_load_result(
            rv_objs,
            LoadContext(
                self._build_url(),
                params=self._get_params(next_page),
                headers=self._get_headers(),
                obj={'page': next_page, 'remaining': -1}
            ) if not self._is_last_page(len(rv_objs), resp) else None
        )
        load_result.resp_headers = dict(resp.headers)
        load_result.resp_text_data = resp.text
        load_result.resp_status = resp_status

        return load_result
