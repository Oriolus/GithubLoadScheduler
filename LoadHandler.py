from TokenRepository import TokenRepostory
from EntityLoader import EntityLoader, LoadResult
from SimplePageableBehaviour import SimplePageableBehaviour
from ObjectQueue import QueueRepository, QueueEntry, QueueState, MAX_RETRY_COUNT

from uuid import uuid4
from datetime import datetime
from tzlocal import get_localzone
from threading import local

from config import Config


class LoadHandler(object):
    def __init__(self, logger, config: Config = None):
        self.__queue_repository = QueueRepository()  # type: QueueRepository
        self.__token_repository = TokenRepostory()  # type: TokenRepostory
        self.__logger = logger
        self.__config = config  # type: Config
        self.__thread_local_store = local()

    def _handle_ok(self, queue_object: QueueEntry, load_result: LoadResult):
        cur_uuid = self.__thread_local_store.cur_uuid
        self.__logger.debug('LoadHandler._handle_ok: start. uuid: {}'.format(cur_uuid))
        # TODO: use transaction
        self.__queue_repository.move_to_object_history(queue_object)
        self.__logger.debug('LoadHandler._handle_ok: moved to history. uuid: {}'.format(cur_uuid))
        self.__queue_repository.remove_by_id(queue_object.id)
        self.__logger.debug('LoadHandler._handle_ok: removed from queue. uuid: {}'.format(cur_uuid))
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
            self.__logger.debug('LoadHandler._handle_ok: added next page. uuid: {}'.format(cur_uuid))

    def _handle_error(self, queue_object: QueueEntry, load_result: LoadResult, error_text: str):
        cur_uuid = self.__thread_local_store.cur_uuid
        # TODO: use transaction
        self.__logger.debug('LoadHandler._handle_error: start. uuid: {}'.format(cur_uuid))
        if queue_object.retry_count >= MAX_RETRY_COUNT:
            queue_object.state = QueueState.UNPROCESSED.value
            queue_object.closed_at = datetime.now(get_localzone())
            self.__queue_repository.save_error(queue_object, error_text)
            self.__logger.debug('LoadHandler._handle_error: error saved. uuid: {}'.format(cur_uuid))
            self.__queue_repository.remove_by_id(queue_object.id)
            self.__logger.debug('LoadHandler._handle_error: removed from queue. uuid: {}'.format(cur_uuid))
        else:
            self.__queue_repository.move_entry_to_end(queue_object)
            self.__logger.debug('LoadHandler._handle_error: moved to end. uuid: {}'.format(cur_uuid))
            self.__queue_repository.save_error(queue_object, error_text)
            self.__logger.debug('LoadHandler._handle_error: error saved. uuid: {}'.format(cur_uuid))
        if load_result and load_result.resp_status in (403, 429):
            self.__queue_repository.shift_by_token(queue_object.token_id)
            self.__logger.debug('LoadHandler._handle_error: object shifted. uuid: {}'.format(cur_uuid))

    def handle(self, object_queue_id: int):
        self.__thread_local_store.cur_uuid = uuid4()
        _cur_uuid = self.__thread_local_store.cur_uuid
        self.__logger.debug('LoadHandler.handle: start. uuid: {}'.format(_cur_uuid))
        current_obj = self.__queue_repository.by_id(object_queue_id)
        if current_obj:
            try:
                self.__logger.info('.type: {}, url: {}. uuid: {}'\
                                   .format(current_obj.entry_type, current_obj.url, _cur_uuid))
                token = self.__token_repository.by_id(current_obj.token_id)
                load_result = EntityLoader(SimplePageableBehaviour(
                    token,
                    self.__config.gh_per_page if self.__config else 100,
                    self.__logger,
                    current_obj.entry_type,
                    current_obj.url
                )).load()
                self.__logger.debug('LoadHandler.handle: loaded. uuid: {}'.format(_cur_uuid))

                if load_result:
                    if load_result.resp_status < 400:
                        self._handle_ok(current_obj, load_result)
                    elif load_result.resp_status >= 400:
                        self._handle_error(current_obj, load_result, load_result.resp_text_data)

            except Exception as ex:
                self._handle_error(current_obj, None, str(ex))
                self.__logger.error('type: {}, url: {}, error: {}. uuid: {}'\
                                    .format(current_obj.entry_type, current_obj.url, str(ex), _cur_uuid)
                                    )
                # raise ex
        else:
            self.__logger.warn('there is no object in object_queue with object_id: {}'.format(object_queue_id))
        self.__logger.debug('LoadHandler.handle: end. uuid: {}'.format(_cur_uuid))

