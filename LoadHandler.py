from TokenRepository import TokenRepostory
from EntityLoader import EntityLoader, LoadResult
from SimplePageableBehaviour import SimplePageableBehaviour
from ObjectQueue import QueueRepository, QueueEntry, QueueState, MAX_RETRY_COUNT

from datetime import datetime
from tzlocal import get_localzone

from main import get_logger


class LoadHandler(object):
    def __init__(self):
        self.__queue_repository = QueueRepository()  # type: QueueRepository
        self.__token_repository = TokenRepostory()  # type: TokenRepostory

    def _handle_ok(self, queue_object: QueueEntry, load_result: LoadResult):
        # TODO: use transaction
        self.__queue_repository.move_to_object_pool(queue_object)
        self.__queue_repository.remove_by_id(queue_object.id)
        if load_result.next_load_context:
            next_loading = load_result.next_load_context
            self.__queue_repository.add_entry(
                QueueEntry(
                    next_loading.url,
                    queue_object.token_id,
                    uuid=None,
                    entry_type=queue_object.entry_type,
                    execute_at=None,
                    base_url=queue_object.base_url
                )
            )

    def _handle_error(self, queue_object: QueueEntry, load_result: LoadResult, error_text: str):
        # TODO: use transaction
        if queue_object.retry_count >= MAX_RETRY_COUNT:
            queue_object.state = QueueState.UNPROCESSED.value
            queue_object.closed_at = datetime.now(get_localzone())
            self.__queue_repository.save_error(queue_object, error_text)
            self.__queue_repository.remove_by_id(queue_object.id)
        else:
            self.__queue_repository.move_entry_to_end(queue_object)
            self.__queue_repository.save_error(queue_object, error_text)
        if load_result and load_result.resp_status in (403, 429):
            self.__queue_repository.shift_by_token(queue_object.token_id)

    def handle(self, object_queue_id: int):
        current_obj = self.__queue_repository.by_id(object_queue_id)
        try:
            token = self.__token_repository.by_id(current_obj.token_id)
            logger = get_logger(None)
            load_result = EntityLoader(SimplePageableBehaviour(
                token,
                100,
                logger,
                current_obj.entry_type,
                current_obj.url
            )).load()

            if load_result:
                if load_result.resp_status < 400:
                    self._handle_ok(current_obj, load_result)
                elif load_result.resp_status >= 400:
                    self._handle_error(current_obj, load_result, load_result.resp_text_data)

        except Exception as ex:
            self._handle_error(current_obj, None, str(ex))
            raise ex
