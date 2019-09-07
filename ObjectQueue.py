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
        self.__uuid = None  # type: str
        self.__execute_at = None  # type: datetime
        self.__base_url = None  # type: str
        self.__closed_at = None  # type: datetime
        self.__state = None  # type: str
        self.__retry_count = None  # type: int
        self.__created_at = None  # type: datetime
        self.__updated_at = None  # type: datetime
        self.__execute_at = None  # type: datetime

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


class QueueRepository(object):
    def __init__(self):
        pass

    def __get_connection(self):
        return transaction()

    def __remove_by_id(self, _id: int, conn):
        with conn.cursor() as cur:
            query = '''
                delete from
                    stg.object_queue
                where
                    id = %s
            '''
            cur.execute(query, (_id,))

    def __move_to_object_history(self, obj: QueueEntry, conn):
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
                'retry_count': obj.retry_count
            })

    def __mark_issues_done(self, url: str, conn):
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

    def __save_error(self, obj: QueueEntry, error_text: str, conn):
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
                    , error_text
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
                    , %(retry_count)s
                    , %(error_text)s
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
                'error_text': error_text
            })

    def __move_entry_to_end(self, entry: QueueEntry, conn):
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
                    , retry_count = retry_count + 1
                    , uuid = null
                where
                    id = %(entry_id)s
                    '''
            cur.execute(query, {
                'token_id': entry.token_id,
                'entry_id': entry.id
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
                        , (select coalesce(max(execute_at), now()::timestamptz(0)) + interval '1 second' * 0.72 from stg.object_queue where token_id = %(_token_id)s)
                        , %(_state)s
                    )
                '''
                cur.execute(query, {
                    '_token_id': entry.token_id,
                    '_url': entry.url,
                    '_base_url': entry.base_url,
                    '_obj_type': entry.entry_type,
                    '_state': QueueState.UNPROCESSED.value
                })
                conn.commit()

    def remove_by_id(self, _id: int):
        with self.__get_connection() as conn:
            self.__remove_by_id(conn)

    def save_error(self, obj: QueueEntry, error_text: str):
        with self.__get_connection() as conn:
            self.__save_error(obj, error_text, conn)

    def move_to_object_history(self, obj: QueueEntry):
        with self.__get_connection() as conn:
            self.__move_to_object_history(obj, conn)

    def mark_issues_done(self, url: str):
        with self.__get_connection() as conn:
            self.__mark_issues_done(url, conn)

    def move_entry_to_end(self, entry: QueueEntry):
        with self.__get_connection() as conn:
            self.__move_entry_to_end(entry, conn)

    def enqueue_with_error(self, entry: QueueEntry, error_text: str):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__save_error(entry, error_text, conn)
            self.__remove_by_id(entry.id, conn)
            conn.commit()

    def move_to_end_with_error(self, entry: QueueEntry, error_text):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__save_error(entry, error_text, conn)
            self.__move_entry_to_end(entry, conn)
            conn.commit()

    def enqueue_ok(self, queue_object: QueueEntry):
        with self.__get_connection() as conn:
            conn.set_session(autocommit=False)
            self.__move_to_object_history(queue_object, conn)
            self.__mark_issues_done(queue_object.base_url, conn)
            self.__remove_by_id(queue_object.id, conn)
            conn.commit()

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
                        id
                        , token_id
                        , url
                        , object_type
                        , base_object_url
                        , retry_count
                        , created_at
                        , updated_at
                        , closed_at
                        , state
                        , uuid
                        , execute_at
                    from
                        stg.object_queue
                    where
                        id = %s
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
        return result

    def by_uuid(self, _uuid: str) -> List[QueueEntry]:
        query = '''
            select
                id
                , token_id
                , url
                , object_type
                , base_object_url
                , retry_count
                , created_at
                , updated_at
                , closed_at
                , state
                , uuid
                , execute_at
            from
                stg.object_queue
            where
                uuid = %s
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
        self._queue_repository = QueueRepository()  # type: QueueRepository
        self._get_executing_lock = Lock()
        self._logger = get_logger()
        self._config = config

    def clear(self):
        self._queue_repository.clear()

    def delete_ancient_entries(self, depth_secs: int = 120):
        affected = self._queue_repository.delete_ancient_entries(depth_secs)
        self._logger.info('removing ancient records: {}'.format(affected))

    def fill(self):
        _cur_uuid = uuid4()
        self._logger.debug('ObjectQueue.fill: start. uuid: {}'.format(_cur_uuid))
        affected = self._queue_repository.fill(
            self._config.sched_queue_threshold if self._config.sched_queue_threshold else QUEUE_THRESHOLD,
            self._config.sched_object_per_token if self._config.sched_object_per_token else OBJECTS_PER_TOKEN
        )
        self._logger.debug('ObjectQueue.fill: end. affected rows: {}. uuid: {}'.format(affected, _cur_uuid))

    def next_entries_by_current_timestamp(self) -> List[QueueEntry]:
        _cur_uuid = str(uuid4())
        self._logger.debug('ObjectQueue.next_entries_by_current_timestamp: start. uuid: {}'.format(_cur_uuid))
        with self._get_executing_lock:
            # date_str = "2019-09-03 00:14:08.192"
            # cur_timestamp = get_localzone().localize(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f'))
            cur_timestamp = datetime.now(get_localzone())
            self._queue_repository.mark_objects(_cur_uuid, cur_timestamp,
                self._config.sched_mark_timestamp_delta if self._config.sched_mark_timestamp_delta else MU
            )
            self._logger.debug('ObjectQueue.next_entries_by_current_timestamp: marked. uuid: {}'.format(_cur_uuid))
        return self._queue_repository.by_uuid(_cur_uuid)


