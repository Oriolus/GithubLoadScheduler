from main import transaction


class TokenRepostory(object):
    def __init__(self):
        pass

    def __get_db_connection(self):
        return transaction()

    def by_id(self, id: int) -> str:
        result = None
        with self.__get_db_connection() as conn:
            with conn.cursor() as cur:
                query = '''
                    select
                        value
                    from
                        log.token
                    where
                        id = %s
                '''
                cur.execute(query, (id,))
                result = cur.fetchone()[0]
        return result
