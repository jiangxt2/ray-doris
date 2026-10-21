"""Bounded readiness for the shared metadata and database services."""

import socket
import time

import pyarrow.fs as fs
import pymysql

deadline = time.monotonic() + 300
while True:
    try:
        for port in [9000, 9083]:
            with socket.create_connection(("hms", port), timeout=2):
                pass
        fs.HadoopFileSystem("hms", 9000, user="root").get_file_info("/")
        with pymysql.connect(
            host="fe",
            port=9030,
            user="root",
            connect_timeout=2,
            cursorclass=pymysql.cursors.DictCursor,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW BACKENDS")
                assert any(str(row["Alive"]).lower() == "true" for row in cursor.fetchall())
        break
    except Exception:
        if time.monotonic() >= deadline:
            raise
        time.sleep(2)
print("Doris backend, HMS and HDFS are ready")
