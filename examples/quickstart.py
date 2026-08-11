"""Read a projected Doris table into Ray Data."""

import os

from ray_doris import read_doris

dataset = read_doris(
    table=os.environ.get("DORIS_TABLE", "analytics.events"),
    host=os.environ.get("DORIS_HOST", "127.0.0.1"),
    mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "9030")),
    http_port=int(os.environ.get("DORIS_HTTP_PORT", "8030")),
    http_scheme=os.environ.get("DORIS_HTTP_SCHEME", "http"),
    user=os.environ.get("DORIS_USER", "root"),
    password_env="DORIS_PASSWORD",
    columns=["event_id", "created_at", "score"],
    filter="score >= 80",
    tablet_size=32,
)

print(dataset.schema())
print(dataset.take(5))
