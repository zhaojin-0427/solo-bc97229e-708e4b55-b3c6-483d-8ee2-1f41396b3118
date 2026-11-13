"""同步引擎：源库 -> 副本库的持续增量复制。

正确性策略
==========

1. 变更捕获（源端）
   specimens 上的 AFTER INSERT/UPDATE/DELETE 触发器把每次行变更追加到
   _change_log（单调 AUTOINCREMENT 序号 seq）。触发器与 DML 同事务：
     - 提交 -> 行与日志原子可见；回滚 -> 一起消失，不传播。

2. 事务边界即发布单位（本模块的核心不变量）
   SQLite 里一个源事务的所有 _change_log 行在 COMMIT 时原子可见，读端
   要么看到该事务的全部行、要么一行也看不到。再结合 SQLite 的单写者
   保证（同一时刻只有一个事务持有写锁，序号分配顺序与提交顺序一致，
   不可能出现“序号更小的事务更晚提交”），因此：
     * 在源端 *单个只读事务* 中读到的 MAX(seq)，必然是某个完整已提交
       事务最后一条日志的序号（事务边界）。若此刻存在未提交写事务，
       它占住写锁、其它事务无法提交，且其行在本快照中不可见，MAX 仍
       停在上一个已提交事务的末尾；其后的 seq 要么不存在，要么属于
       当时尚未提交、在本快照中不可见的事务。
   同步器只把副本发布到这样的边界上：
     * 每轮先开一个只读短事务取一致快照边界 snapshot_max 并立即关闭；
     * 在副本的 *单个* 写事务内分批重放 (last, snapshot_max] 的日志，
       保留 LIMIT CHANGE_BATCH_SIZE 的分批读取，内存不随积压量增长；
     * 只有重放终点恰好等于 snapshot_max（到达完整边界）才与进度一起
       COMMIT；若最后一批的尾序号小于 snapshot_max，说明取边界之后又
       有事务提交、当前副本事务停在某个源事务内部 -> 整体 ROLLBACK，
       下一轮重新取边界后整段重做。
   于是：
     - 一个源事务的增删改永远一起可见，绝不会出现“500 条新鉴定 +
       1 条旧鉴定”的半次更正；
     - 停止 / 进程中断只可能发生在发布点之间：尚未发布的副本事务整体
       回滚，重启后从持久化的完整发布位置接续；
     - 已就绪副本在同步期间始终可查询上一个完整发布的旧状态（未提交
       的写事务对其它连接不可见）；
     - 对源库只有只读短事务，WAL 下源写入无需等待同步。

3. 首次后台复制（bootstrap，允许源库并发提交）
   a. 记下源库当前最大序号 w0；
   b. 按主键分页把 specimens 快照写入副本（每页与进度同事务提交，
      不持有长读事务、不反复全表扫描）；
   c. 快照结束记下最大序号 w1；
   d. 按上面的事务边界协议重放日志直到覆盖 w1（INSERT OR REPLACE /
      DELETE），完成后 last_applied_seq 与数据在同一事务提交，副本才
      置为 RUNNING（就绪前不供查询）。
   区间内的日志覆盖快照期间发生的一切并发变更：
     - 快照后插入的行：其 I 事件在 (w0, w1]，重放插入；
     - 快照后删除的行：D 事件在 (w0, w1]，重放删除，不会“复活”；
     - 快照后更正的行：U 事件携带最新值，重放覆盖旧鉴定；
     - 区间起点从 1 开始，对快照已有行是幂等覆盖，无副作用。

4. 持续增量
   只拉取 seq > last_applied_seq 的新日志（索引点查，不扫描全表）。
   每轮取一致快照边界后，在副本单事务内分批应用、到边界才与进度共同
   提交；没有新日志则短轮询等待。

5. 原子性与续跑
   每次发布都是“数据变更 + sync_meta + _applied_changes”同一事务提交，
   且发布点保证是源事务边界。中断重启以持久化的 last_applied_seq 为准：
     - 仍为 0（初次复制未完成）：重做 bootstrap（reset 后重来，幂等）；
     - 大于 0（曾经就绪）：无论上次是 RUNNING/STOPPED/ERROR，都按持久
       进度接续增量，绝不重做快照、绝不重复传播。
   last_applied_seq 永远指向某个完整源事务的最后一条日志，因此副本在
   任何持久化时刻都对应源库的一个事务一致前缀。

6. 不阻塞源写入
   对源库全部使用只读短事务（含取边界与分批读取），WAL 允许读写并发；
   同步快慢不影响鉴定员提交。
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from . import replica_db
from .source_db import (
    CHANGE_BATCH_SIZE,
    POLL_INTERVAL,
    SNAPSHOT_PAGE_SIZE,
    connect as source_connect,
)


class SyncEngine:
    """后台同步线程的生命周期与状态管理。"""

    def __init__(self, source_path: str, replica_path: str,
                 poll_interval: float = POLL_INTERVAL,
                 batch_size: int = CHANGE_BATCH_SIZE):
        self.source_path = source_path
        self.replica_path = replica_path
        self.poll_interval = poll_interval
        # 分批大小保持小而固定：分批读取始终保留，发布原子性由
        # “只在事务边界提交”保证，而不是靠调大批大小绕过。
        self.batch_size = batch_size
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # bootstrap 开始/完成事件，便于外部（如测试）确定性地观察阶段
        self.bootstrap_started = threading.Event()
        self.bootstrap_completed = threading.Event()
        # 每批日志在副本上应用完成、尚未提交时的观察钩子（主要供测试
        # 确定性控制时序：跨批大事务、处理中停止）。钩子不得抛异常。
        self.batch_applied_hook: Optional[Callable[[int, int], None]] = None
        # 仅供 UI 展示的瞬时计数器（权威进度在副本 sync_meta 中）
        self._lock = threading.Lock()
        self._stats = {
            "running": False,
            "last_batch_at": None,
            "last_batch_changes": 0,
            "total_batches": 0,
        }

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def start(self) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self.bootstrap_started.clear()
            self.bootstrap_completed.clear()
            self._stats.update(running=True, last_error=None)
            t = threading.Thread(
                target=self._run_safely, name="sync-worker", daemon=True
            )
            self._thread = t
            t.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                return False
            self._stop.set()
            return True

    def wait_until_idle(self, timeout: Optional[float] = None) -> None:
        """供测试使用：等待同步线程退出。"""
        t = self._thread
        if t is not None:
            t.join(timeout)

    def status(self) -> dict:
        meta = replica_db.get_meta(self.replica_path)
        with self._lock:
            stats = dict(self._stats)
            alive = self._thread is not None and self._thread.is_alive()
        return {
            **meta,
            "worker_alive": alive,
            "stop_requested": self._stop.is_set(),
            **stats,
        }

    # ------------------------------------------------------------------ #
    # 线程主体
    # ------------------------------------------------------------------ #
    def _run_safely(self) -> None:
        try:
            self._run()
        except Exception as exc:  # 记录到持久 meta，UI 可见，线程退出
            self._record_error(exc)
        finally:
            with self._lock:
                self._stats["running"] = False

    def _record_error(self, exc: Exception) -> None:
        try:
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    UPDATE sync_meta
                       SET phase = ?, last_error = ?,
                           updated_at = datetime('now')
                     WHERE id = 1
                    """,
                    (replica_db.PHASE_ERROR, f"{type(exc).__name__}: {exc}"),
                )
                conn.execute("COMMIT")
        except Exception:
            # 副本本身不可写时，错误至少保留在线程状态里
            pass
        with self._lock:
            self._stats["last_error"] = f"{type(exc).__name__}: {exc}"

    def _run(self) -> None:
        replica_db.init_replica(self.replica_path)
        meta = replica_db.get_meta(self.replica_path)

        # 只要曾经完成过初次复制（进度已持久化），无论上次是 RUNNING 还是
        # STOPPED/ERROR 退出，重启都按持久进度接续，绝不重做快照。
        # 只有初次复制未完成（last_applied_seq 仍为 0）时才重做 bootstrap。
        if meta["last_applied_seq"] > 0:
            self._mark_running()
            self._incremental_loop(meta["last_applied_seq"])
        else:
            # 初次复制，或上次初次复制中断 -> 可安全重做
            self._bootstrap()
            if self._stop.is_set():
                return
            self.bootstrap_completed.set()
            meta = replica_db.get_meta(self.replica_path)
            self._incremental_loop(meta["last_applied_seq"])

    # ------------------------------------------------------------------ #
    # 初次复制
    # ------------------------------------------------------------------ #
    def _bootstrap(self) -> None:
        self.bootstrap_started.set()
        replica_db.reset_for_bootstrap(self.replica_path)

        src = source_connect(self.source_path, readonly=True)
        try:
            w0 = self._source_max_seq(src)

            # --- 分页快照：按主键游标推进，每页独立短事务 ---------------
            last_id = 0
            copied = 0
            while not self._stop.is_set():
                rows = src.execute(
                    """
                    SELECT id, taxon, locality
                      FROM specimens
                     WHERE id > ?
                     ORDER BY id ASC
                     LIMIT ?
                    """,
                    (last_id, SNAPSHOT_PAGE_SIZE),
                ).fetchall()
                if not rows:
                    break
                with replica_db.open_replica(self.replica_path) as rconn:
                    rconn.execute("BEGIN IMMEDIATE")
                    rconn.executemany(
                        "INSERT OR REPLACE INTO specimens(id, taxon, locality)"
                        " VALUES (?, ?, ?)",
                        [(r["id"], r["taxon"], r["locality"]) for r in rows],
                    )
                    copied += len(rows)
                    rconn.execute(
                        """
                        UPDATE sync_meta
                           SET bootstrap_copied = ?,
                               source_max_seq = ?,
                               updated_at = datetime('now')
                         WHERE id = 1
                        """,
                        (copied, w0),
                    )
                    rconn.execute("COMMIT")
                last_id = rows[-1]["id"]
                if len(rows) < SNAPSHOT_PAGE_SIZE:
                    break

            if self._stop.is_set():
                self._mark_stopped()
                return

            # 快照结束时的源端最大序号：重放到覆盖 w1 的事务边界，
            # 并发窗口内的一切变更都会被完整事务地补齐
            w1 = self._source_max_seq(src)
        finally:
            src.close()

        self._catch_up(w1)

    # ------------------------------------------------------------------ #
    # 增量循环
    # ------------------------------------------------------------------ #
    def _incremental_loop(self, last_seq: int) -> None:
        while not self._stop.is_set():
            # 每轮重新取一致快照边界：源端持续写入时边界不断前移，
            # 发布始终只落在完整事务上。
            boundary = self._read_source_boundary()

            if boundary <= last_seq:
                # 只更新“源端水位”，不改动 last_applied_seq
                self._touch_watermark(boundary)
                self._wait_poll()
                continue

            advanced = self._publish_to_boundary(last_seq, boundary)
            if advanced is None:
                return  # 被停止
            if advanced == last_seq:
                # 边界被更新的事务挡住、本轮整段回滚；短暂退让后重取边界，
                # 避免源端高频提交时空转占满 CPU。
                self._wait_poll()
                continue
            last_seq = advanced

        self._mark_stopped()

    def _wait_poll(self) -> None:
        # 可被 stop 立即唤醒
        self._stop.wait(self.poll_interval)

    # ------------------------------------------------------------------ #
    # 事务边界发布（bootstrap 收尾与增量共用）
    # ------------------------------------------------------------------ #
    def _read_source_boundary(self) -> int:
        """在源端单个只读事务内取一致快照的最大日志序号。

        SQLite 保证该序号是某个完整已提交事务的最后一条日志：其后的
        序号要么不存在，要么属于本快照不可见的未提交事务。只读短事务
        不阻塞 WAL 下的源端写入。
        """
        src = source_connect(self.source_path, readonly=True)
        try:
            src.execute("BEGIN")
            try:
                boundary = self._source_max_seq(src)
            finally:
                src.execute("COMMIT")
            return boundary
        finally:
            src.close()

    def _catch_up(self, target_seq: int) -> None:
        """重放日志直到 last_applied_seq 到达覆盖 target_seq 的事务边界。

        bootstrap 用它把快照与并发窗口的全部日志补齐；只有数据与进度
        一起提交完成、副本真正一致后才进入 RUNNING（就绪前不供查询）。
        target_seq 本身可能落在某事务中间，因此循环按一致快照边界推进，
        最终边界 >= target_seq，且每一次发布都是完整事务。
        """
        last_seq = replica_db.get_meta(self.replica_path)["last_applied_seq"]
        while not self._stop.is_set():
            if last_seq >= target_seq:
                break
            boundary = self._read_source_boundary()
            if boundary <= last_seq:
                # target 尚不可见（理论上不会发生），等待源端可见后重试
                self._wait_poll()
                continue
            advanced = self._publish_to_boundary(last_seq, boundary)
            if advanced is None:
                return  # 被停止
            if advanced == last_seq:
                # 本轮整段回滚、无进展：退让后重新取边界再整段重做
                self._wait_poll()
                continue
            last_seq = advanced

        if self._stop.is_set():
            self._mark_stopped()
            return

        final_boundary = self._read_source_boundary()
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, last_applied_seq = ?,
                       source_max_seq = ?, last_error = NULL,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_RUNNING, last_seq,
                 max(final_boundary, last_seq)),
            )
            rconn.execute("COMMIT")

    def _publish_to_boundary(
        self, last_seq: int, boundary: int
    ) -> Optional[int]:
        """把 (last_seq, boundary] 的日志在副本 *单事务* 内分批发布。

        只在重放终点恰好等于 boundary（完整事务边界）时提交数据、审计
        流水与进度；若最后一批止于 boundary 之前（取边界后又有新事务
        提交，当前事务被拦在某个源事务内部），整体回滚并返回原进度，
        由调用方重新取边界后整段重做。

        返回发布后的 last_applied_seq；整段回滚返回原 last_seq；
        被停止返回 None（副本事务一定已回滚，绝不发布半次更正）。
        """
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            try:
                cursor = last_seq
                applied_in_tx = 0
                n_batches = 0
                while cursor < boundary:
                    if self._stop.is_set():
                        rconn.execute("ROLLBACK")
                        self._mark_stopped()
                        return None

                    # 每批仍是有界的 LIMIT 点查：不随积压日志量增长内存，
                    # 也不要求调大批大小或全表重复制。
                    src = source_connect(self.source_path, readonly=True)
                    try:
                        changes = src.execute(
                            """
                            SELECT seq, op, id, taxon, locality
                              FROM _change_log
                             WHERE seq > ? AND seq <= ?
                             ORDER BY seq ASC
                             LIMIT ?
                            """,
                            (cursor, boundary, self.batch_size),
                        ).fetchall()
                    finally:
                        src.close()

                    if not changes:
                        # boundary 来自可见快照，区间内却读不到行：
                        # 只能是源库被外部裁剪/替换。防御性回滚交由上层报错。
                        rconn.execute("ROLLBACK")
                        raise RuntimeError(
                            "change log gap: boundary seq "
                            f"{boundary} not readable from source"
                        )

                    for ch in changes:
                        if ch["op"] in ("I", "U"):
                            # INSERT OR REPLACE：以日志携带的最新鉴定为准，
                            # 旧鉴定不会覆盖新值（日志按 seq 升序应用）
                            rconn.execute(
                                "INSERT OR REPLACE INTO"
                                " specimens(id, taxon, locality)"
                                " VALUES (?, ?, ?)",
                                (ch["id"], ch["taxon"], ch["locality"]),
                            )
                        else:  # 'D'：撤回，删除且不会被后续快照复活
                            rconn.execute(
                                "DELETE FROM specimens WHERE id = ?",
                                (ch["id"],),
                            )
                        rconn.execute(
                            "INSERT OR REPLACE INTO"
                            " _applied_changes(seq, op, specimen_id)"
                            " VALUES (?, ?, ?)",
                            (ch["seq"], ch["op"], ch["id"]),
                        )

                    batch_end = int(changes[-1]["seq"])
                    cursor = batch_end
                    applied_in_tx += len(changes)
                    n_batches += 1

                    if self.batch_applied_hook is not None:
                        # 批已应用但事务尚未提交：测试可在此确定性地
                        # 观察“旧状态仍完整可见”或请求停止。
                        self.batch_applied_hook(cursor, boundary)
                        if self._stop.is_set():
                            rconn.execute("ROLLBACK")
                            self._mark_stopped()
                            return None

                if cursor != boundary:
                    # 正常不可达：boundary 来自已提交快照，区间内日志持久
                    # 可读，分批重放必然恰好到达 boundary。只有源库文件被
                    # 外部替换/日志被裁剪才会落在这里。此时绝不发布部分
                    # 前缀，整体回滚并交由上层按错误处理/重试。
                    rconn.execute("ROLLBACK")
                    return last_seq

                rconn.execute(
                    """
                    UPDATE sync_meta
                       SET last_applied_seq = ?,
                           source_max_seq = MAX(source_max_seq, ?),
                           last_error = NULL,
                           updated_at = datetime('now')
                     WHERE id = 1
                    """,
                    (boundary, boundary),
                )
                rconn.execute("COMMIT")
            except Exception:
                # 任何意外都不允许留下半截写事务
                rconn.execute("ROLLBACK")
                raise

        with self._lock:
            self._stats["last_batch_at"] = time.time()
            self._stats["last_batch_changes"] = applied_in_tx
            self._stats["total_batches"] += n_batches
        return boundary

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #
    @staticmethod
    def _source_max_seq(src) -> int:
        row = src.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM _change_log"
        ).fetchone()
        return int(row["m"])

    def _touch_watermark(self, source_max: int) -> None:
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET source_max_seq = ?, updated_at = datetime('now')
                 WHERE id = 1
                """,
                (source_max,),
            )
            rconn.execute("COMMIT")

    def _mark_stopped(self) -> None:
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_STOPPED,),
            )
            rconn.execute("COMMIT")

    def _mark_running(self) -> None:
        """重启续跑时把阶段从 STOPPED/ERROR 切回 RUNNING（进度保持不变）。"""
        with replica_db.open_replica(self.replica_path) as rconn:
            rconn.execute("BEGIN IMMEDIATE")
            rconn.execute(
                """
                UPDATE sync_meta
                   SET phase = ?, last_error = NULL,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (replica_db.PHASE_RUNNING,),
            )
            rconn.execute("COMMIT")


# 便捷函数：一次性同步到覆盖当前源端水位的事务边界（主要用于测试/脚本）
def sync_once(source_path: str, replica_path: str) -> dict:
    engine = SyncEngine(source_path, replica_path)
    replica_db.init_replica(replica_path)
    meta = replica_db.get_meta(replica_path)
    # 与 _run 的续跑语义一致：只有从未完成初次复制时才做 bootstrap
    if meta["last_applied_seq"] == 0:
        engine._bootstrap()  # noqa: SLF001
    meta = replica_db.get_meta(replica_path)
    target = engine._read_source_boundary()  # noqa: SLF001
    if target > meta["last_applied_seq"]:
        engine._catch_up(target)  # noqa: SLF001
    return replica_db.get_meta(replica_path)
