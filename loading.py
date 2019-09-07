import json
import uuid
from main import transaction
from typing import Dict, Optional


def get_db_connection():
    return transaction()


class Loading:
    def __init__(self):
        self.id = 0

        self.url = None
        self.req_params = {}
        self.req_headers = {}

        self.resp_status = 0
        self.resp_headers = {}
        self.resp_raw = {}
        self.resp_text = None

        self.begin_timestamp = None
        self.end_timestamp = None
        self.guid = None
        self.error = None

    def set_finish_data(self,
                        resp_status: int,
                        resp_headers: Optional[Dict[str, str]],
                        resp_raw: Optional[Dict[str, str]],
                        resp_text: Optional[str]):
        self.resp_status = resp_status
        self.resp_headers = resp_headers.copy() if resp_headers else None
        self.resp_raw = resp_raw.copy() if resp_raw else None
        self.resp_text = resp_text[:] if resp_text else None


def create_loading(
        url: str,
        params: Optional[Dict[str, str]],
        headers: Optional[Dict[str, str]]) -> Loading:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            insert_sql = '''
    INSERT INTO log.loading
        (url, req_params, req_headers, "begin_timestamp", guid)
        VALUES (%s, %s, %s, now(), %s)
    RETURNING id, begin_timestamp;
'''
            obj = Loading()
            _guid = str(uuid.uuid4())
            cur.execute(insert_sql, (url, json.dumps(params), json.dumps(headers), _guid))
            conn.commit()
            row = cur.fetchall()[0]
            obj.id = row[0]
            obj.begin_timestamp = row[1]
            obj.url = url
            obj.req_params = params
            obj.guid = _guid
            return obj


def update_request_info(obj: Loading):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            sql = '''
    update
        log.loading
    set
        req_params = %s,
        req_headers = %s
    where
        id = %s
'''
            cur.execute(sql, (
                json.dumps(obj.req_params) if obj.req_params else None,
                json.dumps(obj.req_headers) if obj.req_headers else None,
                obj.id
            ))
            conn.commit()


def finish_loading(obj: Loading) -> Loading:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            update_script = '''
    update log.loading
    set
        resp_status = %s
        ,resp_headers = %s
        ,resp_text = %s
        ,resp_raw = %s
        ,end_timestamp = now()
        ,error = %s
    where
        id = %s
    returning end_timestamp
            '''
            cur.execute(update_script, (
                obj.resp_status,
                json.dumps(obj.resp_headers) if obj.resp_headers else None,
                obj.resp_text if obj.resp_text else None,
                json.dumps(obj.resp_raw) if obj.resp_raw else None,
                obj.error[:4096] if obj.error else None,
                str(obj.id)
            ))
            obj.end_timestamp = cur.fetchone()[0]
            conn.commit()
    return obj


# CREATE TABLE log.loading
# (
#     id serial NOT NULL,
#     guid uuid,
#     url varchar(1024),
#     req_params varchar(16384),
#     req_headers varchar(16384),
#     begin_timestamp timestamp(6) with time zone,
#     resp_status integer,
#     resp_headers varchar(16384),
#     resp_text text,
#     resp_raw varchar(1048576),
#     end_timestamp timestamp(6) with time zone,
#     error varchar(4096),
#     CONSTRAINT loading_ext_pkey PRIMARY KEY (id)
# )
