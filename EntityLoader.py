from typing import List, Dict, Optional, Callable
from loading import Loading, create_loading, finish_loading


class LoadContext:
    def __init__(self, url: str, params, headers, obj=None):
        self.url = url
        self.params = params
        self.headers = headers
        self.obj = obj

    @staticmethod
    def get_simplified(url: str, obj):
        return LoadContext(
            url,
            None,
            None,
            obj
        )

    def next(self) -> Optional:
        pass

    def get_simplified_load_result(
            self,
            result,
            next_load_context
    ):
        return self.get_load_result(
            result, None, None, None, None, next_load_context
        )

    def get_load_result(
            self,
            result,
            resp_status: int,
            resp_headers: Optional[Dict] = None,
            resp_raw_data: Optional[Dict] = None,
            resp_text_data: Optional[str] = None,
            next_load_context=None
    ):
        return LoadResult(
            result,
            self,
            resp_status,
            resp_headers,
            resp_raw_data,
            resp_text_data,
            next_load_context
        )


class LoadResult:
    def __init__(
            self,
            result,
            current_context: LoadContext,
            resp_status: int,
            resp_headers: Optional[Dict],
            resp_raw_data: Optional[Dict],
            resp_text_data: Optional[str],
            next_load_context: Optional[LoadContext]
    ):
        self.result = result
        self.current_context = current_context
        self.resp_status = resp_status  # type: int
        self.resp_headers = resp_headers  # type: Dict
        self.resp_raw_data = resp_raw_data
        self.resp_text_data = resp_text_data
        self.next_load_context = next_load_context  # type: Optional[LoadContext]

    @staticmethod
    def get_end_load_result(obj: LoadContext):
        return LoadResult(
            None, obj, None, None, None, None, None
        )


class LoadBehaviour:
    def __init__(self):
        pass

    def get_load_context(self) -> LoadContext:
        pass

    def load(self, obj: LoadContext, loading: Loading) -> Optional[LoadResult]:
        pass

    def handle_error(self, obj: LoadContext, e: Exception, loading: Loading) -> LoadResult:
        return LoadResult.get_end_load_result(obj)

    def pre_load(self, obj: LoadContext):
        pass

    def post_load(self, load_result: LoadResult):
        pass


class EntityLoader:
    def __init__(
        self,
        load_behaviour: LoadBehaviour
    ):
        assert load_behaviour
        self._load_behaviour = load_behaviour

    def load(self) -> Optional[LoadResult]:
         return self.__load()

    def __load(self) -> Optional[LoadResult]:

        current_load_context = self._load_behaviour.get_load_context()
        load_result = None

        if current_load_context:
            _loading = create_loading(
                current_load_context.url,
                current_load_context.params,
                current_load_context.headers
            )
            try:
                self._load_behaviour.pre_load(current_load_context)
                load_result = self._load_behaviour.load(current_load_context, _loading)
                if load_result:
                    _loading.set_finish_data(
                        load_result.resp_status,
                        load_result.resp_headers,
                        load_result.resp_raw_data,
                        load_result.resp_text_data
                    )
                self._load_behaviour.post_load(load_result)
            except Exception as e:
                _loading.error = str(e)
                load_result = self._load_behaviour.handle_error(current_load_context, e, _loading)
            finally:
                finish_loading(_loading)
        return load_result
