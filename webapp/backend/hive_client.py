from pyhive import hive
from thrift.transport.TTransport import TTransportException
import time
import socket
import config

_MAX_RETRIES = 3
_RETRY_DELAY = 1  # secondes, doublé à chaque tentative

# Prevent blocking workers indefinitely when HiveServer2 accepts TCP but stalls.
socket.setdefaulttimeout(config.HIVE_SOCKET_TIMEOUT_SECONDS)


def get_connection():
    delay = _RETRY_DELAY
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return hive.Connection(
                host=config.HIVE_HOST,
                port=config.HIVE_PORT,
                database=config.HIVE_DATABASE,
                auth=config.HIVE_AUTH,
            )
        except Exception as e:
            err = str(e)
            is_transport_error = isinstance(e, TTransportException) or (
                "Could not connect to any of" in err
            )

            if not is_transport_error:
                raise

            if attempt == _MAX_RETRIES:
                raise RuntimeError(
                    f"HiveServer2 injoignable après {_MAX_RETRIES} tentatives : {e}"
                ) from e

            time.sleep(delay)
            delay *= 2


def run_query(sql: str) -> list[dict]:
    """Execute HiveQL and return list of row dicts."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        if cursor.description is None:
            return []
        columns = [desc[0].split(".")[-1] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()
