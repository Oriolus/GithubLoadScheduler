from enum import Enum
from uuid import uuid4
from typing import List
from datetime import datetime
from threading import Lock

from tzlocal import get_localzone

from main import get_dwh_connection



class QueueState(Enum):
    UNPROCESSED = 'unprocessed'
    TO_PROCESS = 'to_process'
    PROCESSED = 'processed'


MAX_ENTRY_BY_QUEUE = 100
MAX_RETRY_COUNT = 10


class QueueEntry(object):
    def __init__(self,
                 url: str = None,
                 token_id: int = None,
                 uuid: str = None,
                 entry_type: str = None,
                 execute_at: datetime = None,
                 base_url: str = None,
                 closed_at: datetime = None,
                 state: str = None,
                 retry_count: int = None
                 ):
        self.__entry_type = entry_type
        self.__id = None  # type: int
        self.__url = url  # type: str
        self.__token_id = token_id  # type: int
        self.__uuid = uuid  # type: str
        self.__execute_at = execute_at  # type: datetime
        self.__base_url = base_url  # type: str
        self.__closed_at = closed_at  # type: datetime
        self.__state = state  # type: str
        self.__retry_count = retry_count # type: int

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


class QueueRepository(object):
    def __init__(self):
        pass

    def __get_connection(self):
        return get_dwh_connection()

    def shift_by_token(self, token_id: int, shift_seconds: int = 7):
        # проблема: обновляем время у всех объектов.
        # параллельно обновляется время у объекта этого же токена
        # одно из изменений теряем. Если теряем массовое изменение -
        # все объекты сдвинутся ещё на n секунд
        with self.__get_connection() as conn:
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
                conn.commit()

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
                        %(token_id)s
                        , %(url)s
                        , %(base_url)s
                        , now()::timestamptz(3)
                        , now()::timestamptz(3)
                        , 0
                        , %(obj_type)%
                        , (select max(execute_at) + interval '1 second' * 0.72 from stg.object_queue where token_id = %(token_id)s)
                        , %(state)s
                    )
                '''
                cur.execute(query, {
                    'token_id': entry.token_id,
                    'url': entry.url,
                    'base_url': entry.base_url,
                    'obj_type': entry.entry_type,
                    'state': QueueState.UNPROCESSED.value
                })
                conn.commit()

    def remove_by_id(self, id: int):
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    delete from
                        stg.object_queue
                    where
                        id = %s
                '''
                cur.execute(query, (id, ))
                conn.commit()

    def move_entry_to_end(self, entry: QueueEntry):
        with self.__get_connection() as conn:
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
                    where
                        id = %(entry_id)s
                        '''
                cur.execute(query, {
                    'token_id': entry.token_id,
                    'entry_id': entry.id
                })
                conn.commit()

    def by_id(self, id: int) -> QueueEntry:
        result = None
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    select
                        id
                        , token_id
                        , url
                        , object_type
                        , base_object_url
                        , retry_count
                        , state
                    from
                        stg.object_queue
                    where
                        id = %s
                '''
                cur.execute(query, (id, ))
                raw = cur.fetchone()
                result = QueueEntry(
                    raw[2],
                    raw[1],
                    uuid=None,
                    entry_type=raw[3],
                    execute_at=None,
                    base_url=raw[4],
                    retry_count=raw[5],
                    state=raw[6]
                )
                result.id = raw[0]
        return result

    def save_error(self, obj: QueueEntry, error_text: str):
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    insert into
                        stg.object_done_pool
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
                    select
                        base_object_url
                        , url
                        , object_type
                        , created_at
                        , %(updated_at)s
                        , %(closed_at)s
                        , %(state)s
                        , retry_count
                        , %(error_text)s
                    from
                        stg.object_queue
                    where
                        id = %(obj_id)s
                '''
                cur.execute(query, {
                    'obj_id': obj.id,
                    'updated_at': datetime.now(get_localzone()),
                    'closed_at': obj.closed_at,
                    'state': obj.state,
                    'error_text': error_text
                })
                conn.commit()

    def move_to_object_pool(self, obj: QueueEntry):
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    insert into
                        stg.object_done_pool
                    (
                        base_object_url
                        , object_url
                        , object_type
                        , created_at
                        , closed_at
                        , state
                        , retry_count
                    )
                    select
                        base_object_url
                        , url
                        , object_type
                        , created_at
                        , %(closed_at)s
                        , %(state)s
                        , retry_count + 1
                    from
                        stg.object_queue
                    where
                        id = %(obj_id)s
                '''
                cur.execute(query, {
                    'obj_id': obj.id,
                    'closed_at': datetime.now(get_localzone()),
                    'state': QueueState.PROCESSED.value,
                })
                conn.commit()

    def fill(self):
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
                    find objects neither in object_queue nor in object_done_pull
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
    
                    left join stg.object_queue que on
                        que.base_object_url = is_load.url
    
                    left join stg.object_done_pull done on
                        done.base_object_url = is_load.url
                        and
                        done.state = 'unprocessed'
    
                where
                    que.url is null
                    and
                    done.base_object_url is null
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
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {
                    'queue_threshold': 9,
                    'objects_per_token': 10,
                    'start_status': QueueState.UNPROCESSED.value
                })
                conn.commit()
        pass

    def mark_objects(self, _uuid: str, stmp: datetime):
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
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, {
                    'uuid': _uuid,
                    'cur_timestamp': stmp,
                    'from_state': QueueState.UNPROCESSED.value,
                    'to_state': QueueState.TO_PROCESS.value,
                    'mu': 0.15
                })
                conn.commit()

    def by_uuid(self, _uuid: str) -> List[QueueEntry]:
        query = '''
            select
                id
                , token_id
                , uuid
                , url
                , execute_at
                , object_type
            from
                stg.object_queue
            where
                uuid = %s
        '''
        res = []
        with self.__get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (_uuid, ))
                for raw in cur.fetchall():
                    _obj = QueueEntry(
                        raw[3],
                        raw[1],
                        raw[2],
                        raw[5],
                        raw[4]
                    )
                    _obj.id = raw[0]
                    res.append(_obj)
        return res


class ObjectQueue(object):
    def __init__(self):
        self._queue_repository = QueueRepository()  # type: QueueRepository
        self._get_executing_lock = Lock()

    def fill(self):
        self._queue_repository.fill()

    def next_entries(self) -> List[QueueEntry]:
        _cur_uuid = str(uuid4())
        with self._get_executing_lock:
            date_str = "2019-09-03 00:14:08.192"
            cur_timestamp = get_localzone().localize(datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f'))
            # cur_timestamp = datetime.now(get_localzone())
            self._queue_repository.mark_objects(_cur_uuid, cur_timestamp)
        return self._queue_repository.by_uuid(_cur_uuid)


