from EntityLoader import EntityLoader, LoadResult
from SimplePageableBehaviour import SimplePageableBehaviour
from ObjectQueue import QueueRepository, ObjectHistoryRepository, ObjectQueue, QueueEntry, QueueState, MAX_RETRY_COUNT

from uuid import uuid4
from json import dumps
from copy import deepcopy
from threading import local
from datetime import datetime
from tzlocal import get_localzone

from config import Config


class LoadHandler(object):
    def __init__(self, logger, config: Config = None):
        self.__object_queue = ObjectQueue(config)
        self.__queue_repository = QueueRepository()  # type: QueueRepository
        self.__obj_history_repository = ObjectHistoryRepository()  # type: ObjectHistoryRepository
        self.__logger = logger
        self.__config = config  # type: Config
        self.__thread_local_store = local()

    def _handle_ok(self, queue_object: QueueEntry, load_result: LoadResult):
        cur_uuid = self.__thread_local_store.cur_uuid
        self.__logger.debug('LoadHandler._handle_ok: start. uuid: {}'.format(cur_uuid))
        queue_object.updated_at = datetime.now(get_localzone())
        queue_object.closed_at = datetime.now(get_localzone())
        queue_object.state = QueueState.PROCESSED.value
        self.__object_queue.enqueue_ok(queue_object)
        if load_result.next_load_context:
            _new_entry = deepcopy(queue_object)
            _headers = deepcopy(load_result.next_load_context.headers)
            del _headers['Authorization']
            _new_entry.headers = dumps(_headers)
            _new_entry.params = dumps(load_result.next_load_context.params)
            _new_entry.url = load_result.next_load_context.url
            self.__queue_repository.add_entry(_new_entry)
            self.__logger.debug('LoadHandler._handle_ok: added next page. uuid: {}'.format(cur_uuid))
        self.__logger.debug('LoadHandler._handle_ok: enqueue done. uuid: {}'.format(cur_uuid))

    def _handle_error(self, queue_object: QueueEntry, load_result: LoadResult, error_text: str):
        cur_uuid = self.__thread_local_store.cur_uuid
        # TODO: use transaction
        self.__logger.debug('LoadHandler._handle_error: start. uuid: {}'.format(cur_uuid))
        queue_object.state = QueueState.UNPROCESSED.value
        queue_object.updated_at = datetime.now(get_localzone())
        queue_object.error = error_text
        queue_object.retry_count += 1
        if queue_object.retry_count >= MAX_RETRY_COUNT:
            queue_object.closed_at = datetime.now(get_localzone())
            self.__object_queue.enqueue_with_error(queue_object)
            self.__logger.debug('LoadHandler._handle_error: enqueued with error. uuid: {}'.format(cur_uuid))
        else:
            self.__object_queue.move_to_end_with_error(queue_object)
            self.__logger.debug('LoadHandler._handle_error: moved to end with error. uuid: {}'.format(cur_uuid))
        if load_result and load_result.resp_status in (403, 429):
            self.__queue_repository.shift_by_token(queue_object.token_id)
            self.__logger.debug('LoadHandler._handle_error: token_id: {}, object shifted. uuid: {}'.format(
                queue_object.token_id, cur_uuid
            ))

    def handle(self, object_queue_id: int):
        self.__thread_local_store.cur_uuid = uuid4()
        _cur_uuid = self.__thread_local_store.cur_uuid
        self.__logger.debug('LoadHandler.handle: start. uuid: {}'.format(_cur_uuid))
        current_obj = self.__queue_repository.by_id(object_queue_id)
        if current_obj:
            try:
                self.__logger.info('type: {}, token_id: {}, url: {}. uuid: {}'.format(
                    current_obj.entry_type
                    , current_obj.token_id
                    , current_obj.url,
                    _cur_uuid)
                )
                load_result = EntityLoader(SimplePageableBehaviour(
                    current_obj.token,
                    self.__config.gh_per_page if self.__config else 100,
                    self.__logger,
                    current_obj.entry_type,
                    current_obj.url,
                    current_obj.headers,
                    current_obj.params,
                    current_obj.token_id,
                    str(_cur_uuid)
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
        else:
            self.__logger.warn('there is no object in object_queue with object_id: {}'.format(object_queue_id))
        self.__logger.debug('LoadHandler.handle: end. uuid: {}'.format(_cur_uuid))

