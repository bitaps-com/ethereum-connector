import asyncio
import asyncpg
from .utils import *
import traceback

async def init_db(app,conn):
    level = await conn.fetchval("SHOW TRANSACTION ISOLATION LEVEL;")
    if level != "repeatable read":
        app.log.warning("RECOMMENDED Postgres isolation level is REPEATABLE READ! Current isolation level is %s" % level.upper())
    await conn.execute(f"""
                       CREATE TABLE IF NOT EXISTS {app.block_table} (
                           height BIGINT DEFAULT NULL,
                           hash bytea NOT NULL PRIMARY KEY,
                           previous_hash bytea,
                           timestamp INT4   DEFAULT 0
                        );
                        """)

    await conn.execute(f"""
                       CREATE INDEX IF NOT EXISTS {app.block_table}_height ON {app.block_table} USING BTREE (height desc);
                       """)

    await conn.execute(f"""
                       CREATE TABLE IF NOT EXISTS {app.transaction_table} (
                           height INT4  DEFAULT NULL,
                           hash bytea NOT NULL  PRIMARY KEY ,
                           timestamp INT4   DEFAULT 0,
                           last_timestamp INT4   DEFAULT 0,
                           affected BIT(1) DEFAULT 'B0'
                        );
                       """)

    await conn.execute(f"""
                       CREATE INDEX IF NOT EXISTS {app.transaction_table}_height ON {app.transaction_table} USING BTREE (height desc);
                       """)

async def create_pool(app):
    app.db_pool = await asyncpg.create_pool(dsn=app.postgresql_dsn, loop=app.loop,
                                             min_size=1, max_size=app.postgresql_pool_max_size)

async def ping(app):
    async with app.db_pool.acquire() as conn:
        await conn.fetchval("SELECT 1")

async def cache_load_handler(app, conn):
    stmt = await conn.prepare(f"SELECT hash, height, last_timestamp FROM {app.transaction_table} WHERE height IS NOT NULL ORDER BY height DESC, last_timestamp DESC LIMIT $1;")
    rows = await stmt.fetch(DEFAULT_CONFIRMED_TX_CACHE_SIZE)
    rows = sorted(rows, key=lambda i: i["height"])
    [app.confirmed_tx_cache.set(row["hash"], (row["height"], row["last_timestamp"])) for row in rows]
    stmt = await conn.prepare(f"SELECT hash, height, last_timestamp FROM {app.transaction_table} WHERE height IS NULL ORDER BY last_timestamp DESC;")
    rows = await stmt.fetch()
    rows = sorted(rows, key=lambda i: i["last_timestamp"])
    [app.pending_tx_cache.set(row["hash"],  (-1, row["last_timestamp"])) for row in rows]
    stmt = await conn.prepare(f"SELECT hash, height FROM {app.block_table} ORDER BY height DESC LIMIT $1;")
    rows = await stmt.fetch(DEFAULT_BLOCK_CACHE_SIZE)
    rows = sorted(rows, key=lambda i: i["height"])
    [app.block_cache.set(row["hash"], row["height"]) for row in rows]

async def block_handler(app, block, conn):
    await conn.execute(f"INSERT INTO {app.block_table} (hash, height, previous_hash, timestamp) VALUES ($1, $2, $3, $4);", hex_to_bytes(block["hash"]), int(block["number"], 16), hex_to_bytes(block["parentHash"]) if "parentHash" in block else None, int(block['timestamp'], 16))
    tx_hash_list = [hex_to_bytes(tx["hash"]) for tx in block["transactions"]]
    await conn.execute(f"UPDATE {app.transaction_table} SET height = $1, timestamp=$2  WHERE hash = ANY ($3);", int(block["number"], 16), int(block['timestamp'], 16), tx_hash_list)

async def tx_handler(app, tx, conn):
    inserted = await conn.fetchval(f"INSERT INTO {app.transaction_table} (hash, timestamp, last_timestamp, affected) VALUES ($1, $2, $3, $4) ON CONFLICT (hash) DO NOTHING RETURNING 1;", hex_to_bytes(tx["hash"]), tx["timestamp"], tx["timestamp"], asyncpg.BitString('1') if tx["handler_result"] else asyncpg.BitString('0'))
    return inserted is not None

async def pending_tx_update_handler(app, bin_tx_hash,last_seen_timestamp,conn):
    await conn.execute(f"UPDATE {app.transaction_table} SET last_timestamp = $1  WHERE hash=$2;",last_seen_timestamp,bin_tx_hash)

async def pending_tx_expire_handler(app, expired_hash_list, conn):
    await conn.execute(f"DELETE FROM {app.transaction_table} WHERE hash = ANY ($1)", expired_hash_list)

async def confirmed_tx_expire_handler(app, conn, block_exp = 100000):
    await conn.execute(f"DELETE FROM {app.transaction_table} WHERE height < ($1) and affected != 'B1'", app.last_block_height-block_exp)

async def orphan_handler(app, orphan_block_height,orphan_bin_block_hash, conn):
    await conn.execute(f"DELETE FROM {app.block_table} WHERE hash = $1;", orphan_bin_block_hash)
    await conn.execute(f"UPDATE {app.transaction_table} SET height = NULL WHERE height=$1;", orphan_block_height)


