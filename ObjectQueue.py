from enum import Enum
from uuid import uuid4
from typing import List
from datetime import datetime
from threading import Lock

from tzlocal import get_localzone
from psycopg2.extras import DictCursor, RealDictCursor

from config import Config
from main import get_logger, transaction


class QueueState(Enum):
    UNPROCESSED = 'unprocessed'
    TO_PROCESS = 'to_process'
    PROCESSED = 'processed'


OBJECTS_PER_TOKEN = 150
QUEUE_THRESHOLD = 50
MAX_RETRY_COUNT = 10
MU = 0.1


class QueueEntry(object):
    def __init__(self, **kwargs):
        self.__entry_type = None  # type: str
        self.__id = None  # type: int
        self.__url = None  # type: str
        self.__token_id = None  # type: int
        self.__token = None  # type: str
        self.__uuid = None  # type: str
        self.__execute_at = None  # type: datetime
        self.__base_url = None  # type: str
        self.__closed_at = None  # type: datetime
        self.__state = None  # type: str
        self.__retry_count = None  # type: int
        self.__created_at = None  # type: datetime
        self.__updated_at = None  # type: datetime
        self.__execute_at = None  # type: datetime
        self.__headers = None  # type: str
        self.__params = None  # type: str
        self.__error = None  # type: str

    @property
    def entry_type(self) -> str:
        return self.__entry_type

    @entry_type.setter
    def entry_type(self, value: str):
        self.__entry_type = value

    @property
    def url(self) -> str:
        return self.__url

    @url.setter
    def url(self, value: str):
        self.__url = value

    @property
    def id(self) -> int:
        return self.__id

    @id.setter
    def id(self, value: int):
        self.__id = value

    @property
    def token_id(self) -> int:
        return self.__token_id

    @token_id.setter
    def token_id(self, value: int):
        self.__token_id = value

    @property
    def base_url(self) -> str:
        return self.__base_url

    @base_url.setter
    def base_url(self, value: str):
        self.__base_url = value

    @property
    def closed_at(self) -> datetime:
        return self.__closed_at

    @closed_at.setter
    def closed_at(self, value: datetime):
        self.__closed_at = value

    @property
    def state(self) -> str:
        return self.__state

    @state.setter
    def state(self, value: str):
        self.__state = value

    @property
    def retry_count(self) -> int:
        return self.__retry_count

    @retry_count.setter
    def retry_count(self, value: int):
        self.__retry_count = value

    @property
    def created_at(self) -> datetime:
        return self.__created_at

    @created_at.setter
    def created_at(self, value: datetime):
        self.__created_at = value

    @property
    def updated_at(self) -> datetime:
        return self.__updated_at

    @updated_at.setter
    def updated_at(self, value: datetime):
        self.__updated_at = value

    @property
    def uuid(self) -> str:
        return self.__uuid

    @uuid.setter
    def uuid(self, value: str):
        self.__uuid = value

    @property
    def execute_at(self) -> datetime:
        return self.__execute_at

    @execute_at.setter
    def execute_at(self, value: datetime):
        self.__execute_at = value

    @property
    def headers(self) -> str:
        return self.__headers

    @headers.setter
    def headers(self, value: str):
        self.__headers = value

    @property
    def params(self) -> str:
        return self.__params

    @params.setter
    def params(self, value: str):
        self.__params = value

    @property
    def token(self) -> str:
        return self.__token

    @token.setter
    def token(self, value: str):
        self.__token = value

    @property
    def error(self) -> str:
        return self.__error

    @error.setter
    def error(self, value: str):
        self.__error = value


class ObjectHistoryRepository(object):
    def __init__(self):
        pass

    def __get_connection(self):
        return transaction()

    def save_history(self, obj: QueueEntry):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.save_history_traned(obj, conn)

    def save_history_traned(self, obj: QueueEntry, conn):
        with conn.cursor() as cur:
            query = '''
                insert into
                    stg.object_history
                (
                    base_object_url
                    , object_url
                    , object_type
                    , created_at
                    , updated_at
                    , closed_at
                    , state
                    , retry_count
                    , headers
                    , params
                    , token_id
                )
                values
                (
                    %(base_object_url)s
                    , %(url)s
                    , %(object_type)s
                    , %(created_at)s
                    , %(updated_at)s
                    , %(closed_at)s
                    , %(state)s
                    , %(retry_count)s + 1
                    , %(headers)s
                    , %(params)s
                    , %(token_id)s
                )
            '''
            cur.execute(query, {
                'base_object_url': obj.base_url,
                'url': obj.url,
                'object_type': obj.entry_type,
                'created_at': obj.created_at,
                'updated_at': obj.updated_at,
                'closed_at': obj.closed_at,
                'state': obj.state,
                'retry_count': obj.retry_count,
                'headers': obj.headers,
                'params': obj.params,
                'token_id': obj.token_id
            })


class QueueRepository(object):
    def __init__(self):
        pass

    def __get_connection(self):
        return transaction()

    def remove_by_id_traned(self, _id: int, conn):
        with conn.cursor() as cur:
            query = '''
                delete from
                    stg.object_queue
                where
                    id = %s
            '''
            cur.execute(query, (_id,))

    def mark_issues_done_traned(self, url: str, conn):
        with conn.cursor() as cur:
            query = '''
            update
                stg.issue_loading
            set
                comment_state = 'DONE'
            where
                url = %s
            '''
            cur.execute(query, (url,))

    def __shift_by_token(self, token_id: int, conn, shift_seconds: int = 7):
        with conn.cursor() as cur:
            query = '''
                update
                    stg.object_queue
                set
                    execute_at = execute_at + interval '1 second' * %(shift_secs)s
                where
                    token_id = %(token_id)s
                    '''
            cur.execute(query, {'shift_secs': shift_seconds, 'token_id': token_id})

    def move_entry_to_end_traned(self, entry: QueueEntry, conn):
        with conn.cursor() as cur:
            query = '''
                update
                    stg.object_queue
                set
                    execute_at =
                    (
                        select
                            max(execute_at) + interval '1 second' * 0.72
                        from
                            stg.object_queue
                        where
                            token_id = %(token_id)s
                    )
                    , uuid = null
                    , retry_count = %(retry_count)s
                    , state = %(state)s
                where
                    id = %(entry_id)s
                    '''
            cur.execute(query, {
                'token_id': entry.token_id,
                'entry_id': entry.id,
                'retry_count': entry.retry_count,
                'state': QueueState.UNPROCESSED.value
            })

    def shift_by_token(self, token_id: int, shift_seconds: int = 7):
        # проблема: обновляем время у всех объектов.
        # параллельно обновляется время у объекта этого же токена
        # одно из изменений теряем. Если теряем массовое изменение -
        # все объекты сдвинутся ещё на n секунд
        with self.__get_connection() as conn:
            self.__shift_by_token(token_id, conn, shift_seconds)

    def add_entry(self, entry: QueueEntry):
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    insert into
                        stg.object_queue
                    (
                        token_id
                        , url
                        , base_object_url
                        , created_at
                        , updated_at
                        , retry_count
                        , object_type
                        , execute_at
                        , state
                        , headers
                        , params
                    )
                    values
                    (
                        %(_token_id)s
                        , %(_url)s
                        , %(_base_url)s
                        , now()::timestamptz(3)
                        , now()::timestamptz(3)
                        , 0
                        , %(_obj_type)s
                        , (select coalesce(max(execute_at), now()::timestamptz(3)) + interval '1 second' * 0.72 from stg.object_queue where token_id = %(_token_id)s)
                        , %(_state)s
                        , %(_headers)s
                        , %(_params)s
                    )
                '''
                cur.execute(query, {
                    '_token_id': entry.token_id,
                    '_url': entry.url,
                    '_base_url': entry.base_url,
                    '_obj_type': entry.entry_type,
                    '_state': QueueState.UNPROCESSED.value,
                    '_headers': entry.headers,
                    '_params': entry.params
                })
                conn.commit()

    def remove_by_id(self, _id: int):
        with self.__get_connection() as conn:
            self.remove_by_id_traned(conn)

    def mark_issues_done(self, url: str):
        with self.__get_connection() as conn:
            self.mark_issues_done_traned(url, conn)

    def move_entry_to_end(self, entry: QueueEntry):
        with self.__get_connection() as conn:
            self.move_entry_to_end_traned(entry, conn)

    def fill(self, queue_threshold: int, objects_per_token: int) -> int:
        query = '''
            with token_to_enqueue as
            (
                /*
                    find token with count objects in object_queue lower then threshold
                    find token without objects in object_queue
                */
    
                select
                    tkn.id token_id
                    , coalesce(max(q.execute_at), (now() + interval '1 second' * 3)::timestamp(3) with time zone) last_execute
                from
                    log.token tkn
    
                    left join stg.object_queue q on
                        q.token_id = tkn.id
                where
                    tkn.is_enable = 1::bit
                group by
                    tkn.id
                having
                    count(1) <= %(queue_threshold)s
            )
            , numbered_token as
            (
                select
                    *
                    , row_number() over (order by token_id) rn
                from
                    token_to_enqueue
            )
            , numbered as
            (
                /*
                    find objects neither in object_queue nor in object_history
                */
                select
                    is_load.url base_obj_url
                    , is_load.url || '/comments' url
                    , now()::timestamp(3) with time zone created_at
                    , now()::timestamp(3) with time zone updated_at
                    , null::timestamp(3) with time zone closed_at
                    , 0 retry_count
                    , 'comments' object_type
                    , row_number() over (order by is_load.url asc) rn
                from
                    stg.issue_loading is_load
                    
                    left join stg.object_queue oq on
                        oq.base_object_url = is_load.url
    
                where
                    is_load.comment_state = 'TO_DO'
                    and
                    oq.base_object_url is null
                limit (select count(1) * %(objects_per_token)s * 2 from token_to_enqueue)
            )
            , joint_object as
            (
                select
                    n_tkn.token_id
                    , n_tkn.last_execute
                    , obj.base_obj_url
                    , obj.url
                    , obj.created_at
                    , obj.updated_at
                    , obj.closed_at
                    , obj.retry_count
                    , obj.object_type
                    , row_number() over (partition by n_tkn.token_id order by obj.url) rn
                from
                    numbered obj
    
                    -- loosing first record
                    inner join numbered_token n_tkn on
                        n_tkn.rn = (obj.rn / %(objects_per_token)s + 1) 
                where
                    obj.rn <= (select count(1)  * %(objects_per_token)s from token_to_enqueue)
            )
            insert into
                stg.object_queue
            (
                token_id
                , base_object_url
                , url
                , created_at
                , updated_at
                , closed_at
                , retry_count
                , object_type
                , execute_at
                , state
                , headers
                , params
            )
            select
                token_id
                , base_obj_url
                , url
                , created_at
                , updated_at
                , closed_at
                , retry_count
                , object_type
                , last_execute + (rn * interval '1 second' * 0.72) execute_at
                , %(start_status)s
                , \'{}\'
                , \'{"per_page": 100, "page": 1}\'
            from
                joint_object
        '''
        affected = 0
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {
                    'queue_threshold': queue_threshold,
                    'objects_per_token': objects_per_token,
                    'start_status': QueueState.UNPROCESSED.value
                })
                affected = cur.rowcount
                conn.commit()
        return affected

    def mark_objects(self,
                     _uuid: str,
                     stmp: datetime,
                     mark_timestamp_delta: float = MU
                     ) -> int:
        query = '''
            update
                stg.object_queue
            set
                updated_at = now()::timestamp(3) with time zone
                , state = %(to_state)s
                , uuid = %(uuid)s
            where
                execute_at >= %(cur_timestamp)s - interval '1 second' * %(mu)s
                and
                execute_at < %(cur_timestamp)s + interval '1 second' * %(mu)s
                and
                state = %(from_state)s
                and
                uuid is null
        '''
        affected = 0
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {
                    'uuid': _uuid,
                    'cur_timestamp': stmp,
                    'from_state': QueueState.UNPROCESSED.value,
                    'to_state': QueueState.TO_PROCESS.value,
                    'mu': mark_timestamp_delta
                })
                affected = cur.rowcount
                conn.commit()
        return affected

    def by_id(self, id: int) -> QueueEntry:
        result = None
        with self.__get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = '''
                    select
                        obj.id
                        , obj.token_id
                        , obj.url
                        , obj.object_type
                        , obj.base_object_url
                        , obj.retry_count
                        , obj.created_at
                        , obj.updated_at
                        , obj.closed_at
                        , obj.state
                        , obj.uuid
                        , obj.execute_at
                        , obj.headers
                        , obj.params
                        , tkn.value as token
                    from
                        stg.object_queue obj
                        
                        inner join log.token tkn on
                            tkn.id = obj.token_id
                        
                    where
                        obj.id = %s
                '''
                cur.execute(query, (id, ))

                raw = cur.fetchone()
                if raw:
                    result = QueueEntry()
                    result.id = raw['id']
                    result.token_id = raw['token_id']
                    result.url = raw['url']
                    result.entry_type = raw['object_type']
                    result.base_url = raw['base_object_url']
                    result.retry_count = raw['retry_count']
                    result.created_at = raw['created_at']
                    result.updated_at = raw['updated_at']
                    result.closed_at = raw['closed_at']
                    result.uuid = raw['uuid']
                    result.headers = raw['headers']
                    result.params = raw['params']
                    result.token = raw['token']
        return result

    def by_uuid(self, _uuid: str) -> List[QueueEntry]:
        query = '''
            select
                obj.id
                , obj.token_id
                , obj.url
                , obj.object_type
                , obj.base_object_url
                , obj.retry_count
                , obj.created_at
                , obj.updated_at
                , obj.closed_at
                , obj.state
                , obj.uuid
                , obj.execute_at
                , obj.headers
                , obj.params
                , tkn.value as token
            from
                stg.object_queue obj
                
                inner join log.token tkn on
                    tkn.id = obj.token_id
            where
                obj.uuid = %s
        '''
        res = []
        with self.__get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (_uuid, ))
                for raw in cur.fetchall():
                    result = QueueEntry()
                    result.id = raw['id']
                    result.token_id = raw['token_id']
                    result.url = raw['url']
                    result.entry_type = raw['object_type']
                    result.base_url = raw['base_object_url']
                    result.retry_count = raw['retry_count']
                    result.created_at = raw['created_at']
                    result.updated_at = raw['updated_at']
                    result.closed_at = raw['closed_at']
                    result.uuid = raw['uuid']
                    result.headers = raw['headers']
                    result.params = raw['params']
                    result.token = raw['token']
                    res.append(result)
        return res

    def clear(self) -> int:
        affected = 0
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = 'truncate table stg.object_queue'
                cur.execute(query)
                affected = cur.rowcount
                conn.commit()
        return affected

    def delete_ancient_entries(self, depth_secs: int) -> int:
        affected = 0
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    delete from
                        stg.object_queue
                    where
                        execute_at < now()::timestamptz(3) - interval '1 second' * %(depth)s
                            '''
                cur.execute(query, {'depth': depth_secs})
                affected = cur.rowcount
                conn.commit()
        return affected


class ObjectQueue(object):
    def __init__(self, config: Config):
        self.__queue_repository = QueueRepository()  # type: QueueRepository
        self.__obj_hst_repository = ObjectHistoryRepository()  # type: ObjectHistoryRepository
        self.__get_executing_lock = Lock()
        self.__logger = get_logger()
        self.__config = config

    def __get_connection(self):
        return transaction()

    def clear(self):
        self.__queue_repository.clear()

    def delete_ancient_entries(self, depth_secs: int = 120):
        affected = self.__queue_repository.delete_ancient_entries(depth_secs)
        self.__logger.info('removing ancient records: {}'.format(affected))

    def fill(self):
        _cur_uuid = uuid4()
        self.__logger.debug('ObjectQueue.fill: start. uuid: {}'.format(_cur_uuid))
        affected = self.__queue_repository.fill(
            self.__config.sched_queue_threshold if self.__config.sched_queue_threshold else QUEUE_THRESHOLD,
            self.__config.sched_object_per_token if self.__config.sched_object_per_token else OBJECTS_PER_TOKEN
        )
        self.__logger.debug('ObjectQueue.fill: end. affected rows: {}. uuid: {}'.format(affected, _cur_uuid))

    def next_entries_by_current_timestamp(self) -> List[QueueEntry]:
        _cur_uuid = str(uuid4())
        self.__logger.debug('ObjectQueue.next_entries_by_current_timestamp: start. uuid: {}'.format(_cur_uuid))
        with self.__get_executing_lock:
            cur_timestamp = datetime.now(get_localzone())
            self.__queue_repository.mark_objects(_cur_uuid, cur_timestamp,
                                                 self.__config.sched_mark_timestamp_delta if self.__config.sched_mark_timestamp_delta else MU
                                                 )
            self.__logger.debug('ObjectQueue.next_entries_by_current_timestamp: marked. uuid: {}'.format(_cur_uuid))
        return self.__queue_repository.by_uuid(_cur_uuid)

    def move_to_end_with_error(self, entry: QueueEntry):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__obj_hst_repository.save_history_traned(entry, conn)
            self.__queue_repository.move_entry_to_end_traned(entry, conn)

    def enqueue_with_error(self, entry: QueueEntry):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__obj_hst_repository.save_history_traned(entry, conn)
            self.__queue_repository.remove_by_id_traned(entry.id, conn)
            self.__queue_repository.mark_issues_done_traned(entry.base_url, conn)

    def enqueue_ok(self, queue_object: QueueEntry):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__obj_hst_repository.save_history_traned(queue_object, conn)
            self.__queue_repository.mark_issues_done_traned(queue_object.base_url, conn)
            self.__queue_repository.remove_by_id_traned(queue_object.id, conn)
