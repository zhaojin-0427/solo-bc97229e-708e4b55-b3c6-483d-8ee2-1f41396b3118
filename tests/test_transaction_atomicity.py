"""事务原子发布回归测试。

覆盖本修复的核心不变量：
  1. 一次源事务（501 条更正，跨默认 500 的批次边界）必须整体可见——
     任何时刻副本要么停留在完整旧状态，要么整体到达新状态，绝不出现
     “500 条新鉴定 + 1 条旧鉴定”的半次更正；
  2. 同步处理中停止 / 模拟进程中断，不得发布半次更正，副本数据与已
     发布进度共同生效或共同回滚；
  3. 重启（新引擎实例）从完整发布位置接续，最终一致，且不重做快照；
  4. 分批读取始终保留，内存不随积压日志量增长（不依赖调大批大小）。

时序通过 SyncEngine.batch_applied_hook（批已应用、事务尚未提交时回调）
与 threading.Event 确定性控制，不使用 sleep 碰运气。
"""
import sqlite3
import threading
import time

import pytest

from app import replica_db, source_db
from app.source_db import connect as source_connect
from app.sync import SyncEngine


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
@pytest.fixture()
def paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    source_db.init_demo(src, rows=[(i, f"taxon-{i}", f"loc-{i}") for i in range(1, 9)])
    return src, rep


def replica_rows(path):
    with replica_db.open_replica(path) as conn:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}


def source_rows(path):
    conn = source_connect(path, readonly=True)
    try:
        return {r["id"]: (r["taxon"], r["locality"])
                for r in conn.execute("SELECT * FROM specimens ORDER BY id")}
    finally:
        conn.close()


def wait_until(deadline_s, predicate):
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def do_big_correction(src, n=501, start_id=1000, mark="rev1"):
    """鉴定员在 *单个源事务* 内更正/登记 n 条标本。

    返回 (first_seq, last_seq)，即该事务在 _change_log 中占据的闭区间。
    """
    conn = source_db.connect(src)
    try:
        before = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) s FROM _change_log").fetchone()["s"]
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT INTO specimens(id, taxon, locality) VALUES (?, ?, ?)",
            [(start_id + i, f"{mark}-taxon-{i}", f"{mark}-loc-{i}")
             for i in range(n)],
        )
        conn.execute("COMMIT")
        after = conn.execute("SELECT MAX(seq) s FROM _change_log").fetchone()["s"]
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    assert after - before == n  # 一个事务 n 条连续日志
    return before + 1, after


def start_engine(src, rep, batch_size, poll=0.02):
    engine = SyncEngine(src, rep, poll_interval=poll, batch_size=batch_size)
    engine.start()
    return engine


def wait_running_caught_up(rep, src, timeout=20):
    assert wait_until(timeout, lambda: (
        replica_db.get_meta(rep)["phase"] == replica_db.PHASE_RUNNING
        and replica_db.get_meta(rep)["last_applied_seq"]
        >= source_db.max_change_seq(src)
    )), "副本未能在时限内就绪并追上源端"


# --------------------------------------------------------------------------- #
# 1. 大事务跨批：一次源事务的增删改必须整体可见
# --------------------------------------------------------------------------- #
class TestTransactionAtomicVisibility:
    def test_501_row_transaction_never_split_across_500_sized_batches(self, paths):
        """场景复现：501 条更正 + 每批 500，旧实现会暴露 500 新 + 1 旧。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=500)
        try:
            wait_running_caught_up(rep, src)

            first_seq, last_seq = do_big_correction(src, n=501)
            assert last_seq - first_seq + 1 == 501

            assert wait_until(20, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= last_seq)
        finally:
            engine.stop()
            engine.wait_until_idle(10)

        rows = replica_rows(rep)
        # 501 条要么全部是新鉴定，绝不能出现部分新部分旧
        for i in range(501):
            assert rows[1000 + i] == (f"rev1-taxon-{i}", f"rev1-loc-{i}")
        meta = replica_db.get_meta(rep)
        # 进度精确落在该事务最后一条上，而不是事务内部的第 500 条
        assert meta["last_applied_seq"] == last_seq
        assert replica_rows(rep) == source_rows(src)

    def test_replica_keeps_complete_old_state_while_publish_in_progress(self, paths):
        """发布事务进行中（已应用首批、尚未提交），外部读者仍见完整旧状态。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
            old_rows = replica_rows(rep)

            first_seq, last_seq = do_big_correction(src, n=350)

            reached_first_batch = threading.Event()
            release = threading.Event()

            def hook(cursor, boundary):  # noqa: ANN001
                if boundary >= last_seq and not reached_first_batch.is_set():
                    reached_first_batch.set()
                    release.wait(10)

            engine.batch_applied_hook = hook
            assert reached_first_batch.wait(10), "未进入大事务的发布事务"

            # 发布事务尚未提交：独立连接必须读到完整且一致的旧状态
            mid_rows = replica_rows(rep)
            assert mid_rows == old_rows
            assert all(1000 + i not in mid_rows for i in range(350))
            meta = replica_db.get_meta(rep)
            assert meta["last_applied_seq"] < first_seq  # 进度未前移

            release.set()
            assert wait_until(10, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= last_seq)
        finally:
            engine.batch_applied_hook = None
            release.set()
            engine.stop()
            engine.wait_until_idle(10)

        # 提交后整体切换到新状态
        rows = replica_rows(rep)
        for i in range(350):
            assert rows[1000 + i] == (f"rev1-taxon-{i}", f"rev1-loc-{i}")

    def test_progress_always_lands_on_committed_transaction_boundary(self, paths):
        """持续多个大小不一的事务，每次持久化进度都必须是某事务末条序号。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=64)
        try:
            # 先就绪
            wait_running_caught_up(rep, src)
            seed_end = replica_db.get_meta(rep)["last_applied_seq"]

            txn_ends = []
            sizes = [1, 70, 130, 5, 200, 333]  # 含跨多批的大事务
            next_id = 2000
            for k, n in enumerate(sizes):
                _, end = do_big_correction(src, n=n, start_id=next_id, mark=f"t{k}")
                txn_ends.append(end)
                next_id += n + 10

            # 合法发布位置：初始播种事务末条 + 每个大事务末条。
            # 采样期间进度只能取这些值之一，绝不能停在某事务内部序号上。
            valid_positions = {seed_end, *txn_ends}
            observed = []
            sampler_stop = threading.Event()

            def sampler():
                while not sampler_stop.is_set():
                    observed.append(
                        replica_db.get_meta(rep)["last_applied_seq"])
                    time.sleep(0.002)

            t = threading.Thread(target=sampler)
            t.start()
            try:
                assert wait_until(20, lambda:
                    replica_db.get_meta(rep)["last_applied_seq"] >= txn_ends[-1])
            finally:
                sampler_stop.set()
                t.join()

            illegal = [s for s in observed if s not in valid_positions]
            assert not illegal, (
                f"发现落在源事务内部的发布进度: {sorted(set(illegal))[:10]}")
            meta = replica_db.get_meta(rep)
            assert meta["last_applied_seq"] == txn_ends[-1]
            with replica_db.open_replica(rep) as conn:
                applied = [r["seq"] for r in conn.execute(
                    "SELECT seq FROM _applied_changes ORDER BY seq")]
            for end in txn_ends:
                assert end in applied
            assert replica_rows(rep) == source_rows(src)
        finally:
            engine.stop()
            engine.wait_until_idle(10)

    def test_mixed_insert_update_delete_in_one_transaction_is_atomic(self, paths):
        """一个源事务里同时含增、改、删，副本不得只看到其中一部分。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=50)
        try:
            wait_running_caught_up(rep, src)

            conn = source_db.connect(src)
            try:
                before = conn.execute(
                    "SELECT COALESCE(MAX(seq),0) s FROM _change_log").fetchone()["s"]
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(
                    "INSERT INTO specimens VALUES (?, ?, ?)",
                    [(3000 + i, f"new-{i}", "nl") for i in range(120)])
                conn.execute("UPDATE specimens SET taxon='corrected', locality='cl'"
                             " WHERE id IN (1,2,3,4,5,6,7,8)")
                conn.execute("DELETE FROM specimens WHERE id IN (4,5,6)")
                conn.execute("COMMIT")
                last_seq = conn.execute(
                    "SELECT MAX(seq) s FROM _change_log").fetchone()["s"]
            finally:
                conn.close()

            assert wait_until(15, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= last_seq)
        finally:
            engine.stop()
            engine.wait_until_idle(10)

        rows = replica_rows(rep)
        # 新增 120 条全部可见
        assert all(3000 + i in rows for i in range(120))
        # 更正整体生效
        assert rows[1] == ("corrected", "cl") and rows[2] == ("corrected", "cl")
        # 删除整体生效，不复活
        assert 4 not in rows and 5 not in rows and 6 not in rows
        assert replica_rows(rep) == source_rows(src)


# --------------------------------------------------------------------------- #
# 2. 处理中停止：不得发布半次更正
# --------------------------------------------------------------------------- #
class TestStopDuringProcessing:
    def test_stop_mid_publish_rolls_back_whole_transaction(self, paths):
        """在跨批大事务的发布事务中途停止：整段回滚，副本停留完整旧状态。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
            progress_before = replica_db.get_meta(rep)["last_applied_seq"]
            old_rows = replica_rows(rep)

            first_seq, last_seq = do_big_correction(src, n=450)

            entered = threading.Event()

            def hook(cursor, boundary):  # noqa: ANN001
                if boundary >= last_seq and cursor >= first_seq + 99 \
                        and not entered.is_set():
                    entered.set()
                    engine.stop()  # 在首批已应用、未提交时请求停止

            engine.batch_applied_hook = hook
            assert entered.wait(10), "停止钩子未触发"
            engine.wait_until_idle(10)
            assert not (engine._thread and engine._thread.is_alive())
        finally:
            engine.batch_applied_hook = None

        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        # 进度停在发布前的完整位置，半次更正没有任何一部分生效
        assert meta["last_applied_seq"] == progress_before
        assert replica_rows(rep) == old_rows
        assert all(1000 + i not in replica_rows(rep) for i in range(450))

    def test_stop_between_poll_cycles_keeps_last_complete_publish(self, paths):
        """停止发生在空闲轮询时：已发布的完整事务保留，未发布的不生效。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100, poll=0.02)
        try:
            wait_running_caught_up(rep, src)
            _, end1 = do_big_correction(src, n=120, start_id=4000, mark="a")
            assert wait_until(10, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= end1)
            engine.stop()
            engine.wait_until_idle(10)

            # 停止后源库再来一个大事务，副本绝不动
            _, end2 = do_big_correction(src, n=300, start_id=5000, mark="b")
            time.sleep(0.3)
            meta = replica_db.get_meta(rep)
            assert meta["phase"] == replica_db.PHASE_STOPPED
            assert meta["last_applied_seq"] == end1
            assert all(5000 + i not in replica_rows(rep) for i in range(300))
            assert end2 > end1
        finally:
            if engine._thread and engine._thread.is_alive():
                engine.stop()
                engine.wait_until_idle(5)

    def test_commit_failure_leaves_no_half_transaction(self, paths, monkeypatch):
        """发布事务提交点崩溃（故障注入）：数据与进度共同回滚。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
        finally:
            engine.stop()
            engine.wait_until_idle(10)

        progress_before = replica_db.get_meta(rep)["last_applied_seq"]
        _, last_seq = do_big_correction(src, n=250)

        real_factory = replica_db._ReplicaConnection  # noqa: SLF001
        crash = {"done": False}

        class CrashingConnection(real_factory):
            def execute(self, sql, *args):
                if (not crash["done"] and isinstance(sql, str)
                        and sql.strip().upper() == "COMMIT"):
                    crash["done"] = True
                    raise sqlite3.OperationalError("simulated crash at commit")
                return super().execute(sql, *args)

        monkeypatch.setattr(replica_db, "_ReplicaConnection", CrashingConnection)
        crashed_engine = SyncEngine(src, rep, poll_interval=0.02, batch_size=100)
        crashed_engine.start()
        assert wait_until(10, lambda: crash["done"])
        crashed_engine.wait_until_idle(10)
        monkeypatch.undo()

        assert replica_db.get_meta(rep)["last_applied_seq"] == progress_before
        assert all(1000 + i not in replica_rows(rep) for i in range(250))


# --------------------------------------------------------------------------- #
# 3. 重启续接：从完整发布位置接续，不重做快照
# --------------------------------------------------------------------------- #
class TestRestartContinuation:
    def test_restart_after_mid_publish_stop_resumes_and_finishes_atomically(self, paths):
        """中途停止 -> 新引擎模拟进程重启 -> 从完整位置接续，最终整体一致。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
            progress_before = replica_db.get_meta(rep)["last_applied_seq"]

            first_seq, last_seq = do_big_correction(src, n=450)
            entered = threading.Event()

            def hook(cursor, boundary):  # noqa: ANN001
                if boundary >= last_seq and not entered.is_set():
                    entered.set()
                    engine.stop()

            engine.batch_applied_hook = hook
            assert entered.wait(10)
            engine.wait_until_idle(10)
        finally:
            engine.batch_applied_hook = None

        assert replica_db.get_meta(rep)["last_applied_seq"] == progress_before

        # 模拟进程重启：全新引擎实例，按持久化进度接续
        engine2 = SyncEngine(src, rep, poll_interval=0.02, batch_size=100)
        engine2.start()
        try:
            assert wait_until(15, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= last_seq)
        finally:
            engine2.stop()
            engine2.wait_until_idle(10)

        meta = replica_db.get_meta(rep)
        assert meta["phase"] == replica_db.PHASE_STOPPED
        assert meta["last_applied_seq"] == last_seq
        # 没有重做首次复制快照
        assert meta["bootstrap_copied"] == 8
        rows = replica_rows(rep)
        for i in range(450):
            assert rows[1000 + i] == (f"rev1-taxon-{i}", f"rev1-loc-{i}")
        assert replica_rows(rep) == source_rows(src)

    def test_restart_after_crash_resumes_without_republish(self, paths, monkeypatch):
        """提交点崩溃后重启：从旧进度接续，不重复传播、不留空洞。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
        finally:
            engine.stop()
            engine.wait_until_idle(10)

        progress_before = replica_db.get_meta(rep)["last_applied_seq"]
        _, last_seq = do_big_correction(src, n=250)

        real_factory = replica_db._ReplicaConnection  # noqa: SLF001
        crash = {"done": False}

        class CrashingConnection(real_factory):
            def execute(self, sql, *args):
                if (not crash["done"] and isinstance(sql, str)
                        and sql.strip().upper() == "COMMIT"):
                    crash["done"] = True
                    raise sqlite3.OperationalError("simulated crash at commit")
                return super().execute(sql, *args)

        monkeypatch.setattr(replica_db, "_ReplicaConnection", CrashingConnection)
        crashed = SyncEngine(src, rep, poll_interval=0.02, batch_size=100)
        crashed.start()
        assert wait_until(10, lambda: crash["done"])
        crashed.wait_until_idle(10)
        monkeypatch.undo()

        # 重启
        engine2 = SyncEngine(src, rep, poll_interval=0.02, batch_size=100)
        engine2.start()
        try:
            assert wait_until(15, lambda:
                replica_db.get_meta(rep)["last_applied_seq"] >= last_seq)
        finally:
            engine2.stop()
            engine2.wait_until_idle(10)

        assert replica_db.get_meta(rep)["last_applied_seq"] == last_seq
        # 审计流水无重复序号
        with replica_db.open_replica(rep) as conn:
            seqs = [r["seq"] for r in conn.execute(
                "SELECT seq FROM _applied_changes ORDER BY seq")]
        assert len(seqs) == len(set(seqs))
        assert replica_rows(rep) == source_rows(src)

    def test_restart_with_deeper_backlog_publishes_whole_transactions(self, paths):
        """停止期间积压多个跨批大事务，重启后仍逐事务完整发布。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=80)
        try:
            wait_running_caught_up(rep, src)
            engine.stop()
            engine.wait_until_idle(10)

            ends = []
            nid = 6000
            for k, n in enumerate([250, 301, 175]):
                _, end = do_big_correction(src, n=n, start_id=nid, mark=f"bk{k}")
                ends.append(end)
                nid += n + 5

            engine2 = SyncEngine(src, rep, poll_interval=0.02, batch_size=80)
            engine2.start()
            try:
                assert wait_until(20, lambda:
                    replica_db.get_meta(rep)["last_applied_seq"] >= ends[-1])
            finally:
                engine2.stop()
                engine2.wait_until_idle(10)

            assert replica_db.get_meta(rep)["last_applied_seq"] == ends[-1]
            assert replica_rows(rep) == source_rows(src)
        finally:
            if engine._thread and engine._thread.is_alive():
                engine.stop()
                engine.wait_until_idle(5)


# --------------------------------------------------------------------------- #
# 4. 分批读取保留 + 不阻塞源写入
# --------------------------------------------------------------------------- #
class TestBatchingAndNonBlocking:
    def test_batched_reads_are_retained_under_huge_backlog(self, paths):
        """积压远超批大小：分批读取生效，单次源查询始终受 LIMIT 约束。"""
        src, rep = paths
        # 先就绪再停止，制造停止后的大积压
        engine = start_engine(src, rep, batch_size=128)
        try:
            wait_running_caught_up(rep, src)
            engine.stop()
            engine.wait_until_idle(10)
        finally:
            pass

        do_big_correction(src, n=2000)  # 远大于批大小 128

        seen_batch_sql = threading.Event()
        limits = []

        import app.sync as sync_mod
        real_connect = sync_mod.source_connect

        class _SpyingConn:
            """只包装 execute 的轻量代理（sqlite3 连接的 execute 不可实例覆盖）。"""

            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, params=()):
                if isinstance(sql, str) and "FROM _change_log" in sql \
                        and "LIMIT" in sql.upper():
                    limits.append(params[-1] if params else None)
                    seen_batch_sql.set()
                return self._inner.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self._inner, name)

            def close(self):
                self._inner.close()

        def spy_connect(path, *, readonly=False, timeout=30.0):  # noqa: ANN001
            conn = real_connect(path, readonly=readonly, timeout=timeout)
            return _SpyingConn(conn) if readonly else conn

        original = sync_mod.source_connect
        sync_mod.source_connect = spy_connect
        engine2 = SyncEngine(src, rep, poll_interval=0.02, batch_size=128)
        engine2.start()
        try:
            assert seen_batch_sql.wait(10)
            assert wait_until(20, lambda:
                replica_db.get_meta(rep)["last_applied_seq"]
                >= source_db.max_change_seq(src))
        finally:
            sync_mod.source_connect = original
            engine2.stop()
            engine2.wait_until_idle(10)

        assert limits, "没有观察到带 LIMIT 的分批日志读取"
        assert all(lim == 128 for lim in limits), limits
        assert replica_rows(rep) == source_rows(src)

    def test_source_writes_not_blocked_while_large_publish_runs(self, paths):
        """大事务发布期间，源库写入立即成功，不等待同步结束。"""
        src, rep = paths
        engine = start_engine(src, rep, batch_size=100)
        try:
            wait_running_caught_up(rep, src)
            do_big_correction(src, n=1000)

            gate = threading.Event()
            entered = threading.Event()

            def hook(cursor, boundary):  # noqa: ANN001
                if not entered.is_set() and boundary >= source_db.max_change_seq(src):
                    entered.set()
                    gate.wait(10)

            engine.batch_applied_hook = hook
            assert entered.wait(10)

            # 发布事务卡在未提交状态时，源端写入必须快速成功
            t0 = time.time()
            source_db.insert_specimen(src, 9000, "parallel", "write")
            elapsed = time.time() - t0
            gate.set()
            assert elapsed < 15.0

            assert wait_until(15, lambda:
                replica_db.get_meta(rep)["last_applied_seq"]
                >= source_db.max_change_seq(src))
        finally:
            engine.batch_applied_hook = None
            gate.set()
            engine.stop()
            engine.wait_until_idle(10)

        assert replica_rows(rep) == source_rows(src)
