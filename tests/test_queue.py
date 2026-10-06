"""Tests for the lane-based queue. Stdlib only; never touches the live queue DB or log.

Run: python -m unittest discover -s tests -v
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="rpq-test-")
os.environ["ROBLOX_PLAYTEST_QUEUE_DB"] = os.path.join(TMP, "import.db")
os.environ["ROBLOX_PLAYTEST_QUEUE_LOG"] = os.path.join(TMP, "queue.log")
os.environ["ROBLOX_PLAYTEST_AUDIO_STATE"] = os.path.join(TMP, "audio-state.json")
os.environ.pop("ROBLOX_PLAYTEST_MUTE_AUDIO", None)
sys.path.insert(0, ROOT)
import server  # noqa: E402

LOG = os.environ["ROBLOX_PLAYTEST_QUEUE_LOG"]


def tearDownModule():
    shutil.rmtree(TMP, ignore_errors=True)


class Call(threading.Thread):
    """Run a function in a thread and keep its result or error."""
    def __init__(self, fn, *a, **kw):
        super().__init__(daemon=True)
        self.fn, self.a, self.kw, self.result, self.error = fn, a, kw, None, None
        self.start()

    def run(self):
        try:
            self.result = self.fn(*self.a, **self.kw)
        except BaseException as e:  # noqa: BLE001
            self.error = e

    def get(self, timeout=5):
        self.join(timeout)
        assert not self.is_alive(), "call did not finish"
        if self.error: raise self.error
        return self.result


def wait_for(pred, timeout=5):
    end = time.time() + timeout
    while time.time() < end:
        if pred(): return True
        time.sleep(0.01)
    raise AssertionError("condition not reached")


class QueueCase(unittest.TestCase):
    def setUp(self):
        self._saved = {k: getattr(server, k) for k in (
            "DB", "POLL_SECONDS", "MAX_WAIT_SECONDS", "GRACE_SECONDS", "LIVE_SECONDS", "LEASE_SECONDS",
            "CAMERA_SECONDS", "PROCESS_TIMEOUT_SECONDS", "QUEUE_WAIT_SECONDS")}
        server.DB = os.path.join(TMP, self.id().split(".")[-1] + ".db")
        server.POLL_SECONDS = 0.03
        server.MAX_WAIT_SECONDS = 0.25
        server.GRACE_SECONDS = 60
        server.LIVE_SECONDS = 30
        server.LEASE_SECONDS = 30
        server.CAMERA_SECONDS = 120
        self.c = server.db()

    def tearDown(self):
        for k, v in self._saved.items(): setattr(server, k, v)
        self.c.close()

    # helpers
    def row(self, job_id):
        return self.c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    def state(self, job_id):
        r = self.row(job_id)
        return r["state"] if r else None

    def acq(self, agent, job_id, **kw):
        return server.acquire({"agent": agent, "job_id": job_id, **kw})

    def bg(self, agent, job_id, **kw):
        """Start a long-waiting acquire in a thread; later calls use the short default wait."""
        server.MAX_WAIT_SECONDS = 10
        t = Call(self.acq, agent, job_id, **kw)
        wait_for(lambda: self.row(job_id) is not None)
        server.MAX_WAIT_SECONDS = 0.25
        return t

    def rel(self, agent, job_id, **kw):
        return server.release({"agent": agent, "job_id": job_id, **kw})

    def log(self):
        with open(LOG, encoding="utf-8") as f: return f.read()


class Lanes(QueueCase):
    def test_play_fifo(self):
        self.assertEqual(self.acq("a", "A")["status"], "granted")
        b = self.bg("b", "B")
        c = self.bg("c", "C")
        time.sleep(0.2)
        self.assertEqual(self.state("B"), "queued"); self.assertEqual(self.state("C"), "queued")
        self.rel("a", "A")
        self.assertEqual(b.get()["status"], "granted")
        self.assertEqual(self.state("C"), "queued")
        self.rel("b", "B")
        self.assertEqual(c.get()["status"], "granted")

    def test_old_style_acquire_is_play(self):
        r = self.acq("a", "A")
        self.assertEqual((r["status"], r["lane"], r["job_id"], r["lease_id"]), ("granted", "play", "A", "A"))
        self.assertIn("Ready. Use the same agent and job_id=A", r["text"])
        self.assertEqual(self.acq("e", "E", lane="edit", scope=["Workspace.Map"])["status"], "queued")
        with self.assertRaises(ValueError): self.acq("", "X")

    def test_null_lane_rows_count_as_play(self):
        now = time.time()
        self.c.execute("INSERT INTO jobs(id,agent,state,created,started,lease_until) VALUES('OLD','old','active',?,?,?)", (now, now, now + 60))
        self.assertEqual(self.acq("e", "E", lane="edit", scope=["Workspace.Map"])["status"], "queued")
        self.assertEqual(self.acq("k", "K", lane="camera")["status"], "queued")

    def test_disjoint_edits_run_together(self):
        a = self.acq("a", "A", lane="edit", scope=["Workspace.Map"])
        b = self.acq("b", "B", lane="edit", scope=["StarterGui.Building"])
        c = self.acq("c", "C", lane="edit", scope=["Workspace.MapExtras"])  # dot boundary: not a child of Workspace.Map
        self.assertEqual([a["status"], b["status"], c["status"]], ["granted"] * 3)

    def test_overlapping_edits_wait(self):
        self.assertEqual(self.acq("a", "A", lane="edit", scope=["Workspace.Map"])["status"], "granted")
        child = self.bg("b", "B", lane="edit", scope=["Workspace.Map.Tower"])
        whole = self.bg("c", "C", lane="edit")  # no scope = whole place
        time.sleep(0.2)
        self.assertEqual((self.state("B"), self.state("C")), ("queued", "queued"))
        self.rel("a", "A")
        self.assertEqual(child.get()["status"], "granted")
        self.assertEqual(self.state("C"), "queued")  # whole place overlaps the child lease
        self.rel("b", "B")
        self.assertEqual(whole.get()["status"], "granted")

    def test_later_disjoint_edit_passes_earlier_waiting_edit(self):
        self.acq("a", "A", lane="edit", scope=["Workspace.Map"])
        blocked = self.bg("b", "B", lane="edit", scope=["Workspace.Map"])
        self.assertEqual(self.acq("c", "C", lane="edit", scope=["Workspace.Other"])["status"], "granted")
        self.rel("a", "A")
        self.assertEqual(blocked.get()["status"], "granted")

    def test_play_waits_for_earlier_edits_and_blocks_later_ones(self):
        self.acq("a", "A", lane="edit", scope=["Workspace.Map"])
        play = self.bg("p", "P")
        self.assertEqual(self.acq("b", "B", lane="edit", scope=["Workspace.Other"])["status"], "queued")  # nothing passes a waiting play
        self.assertEqual(self.acq("k", "K", lane="camera")["status"], "queued")  # camera conflicts with play too
        self.rel("a", "A")
        self.assertEqual(play.get()["status"], "granted")
        self.assertEqual(self.acq("b", "B", lane="edit", scope=["Workspace.Other"])["status"], "queued")
        self.rel("p", "P")
        self.assertEqual(self.acq("b", "B", lane="edit", scope=["Workspace.Other"])["status"], "granted")

    def test_edit_ahead_of_play_is_not_blocked_by_it(self):
        self.acq("a", "A", lane="edit", scope=["Workspace.Map"])
        early = self.bg("e", "E", lane="edit", scope=["Workspace.Map"])
        self.bg("p", "P")
        self.rel("a", "A")
        self.assertEqual(early.get()["status"], "granted")  # queued before the play, so it goes first
        self.assertEqual(self.state("P"), "queued")

    def test_edit_renew_yields_then_refuses(self):
        self.acq("a", "A", lane="edit", scope=["Workspace.Map"])
        self.assertNotIn("play request", server.renew({"agent": "a", "job_id": "A"})["text"])
        self.bg("p", "P")
        r = server.renew({"agent": "a", "job_id": "A"})
        self.assertTrue(r["yield"]); self.assertIn("release at the next safe point", r["text"])
        self.c.execute("UPDATE jobs SET queued_at=? WHERE id='P'", (time.time() - 10 * server.LEASE_SECONDS,))
        with self.assertRaisesRegex(ValueError, "Renew refused"):
            server.renew({"agent": "a", "job_id": "A"})

    def test_play_renew_is_never_refused_and_reports_overrun(self):
        self.acq("a", "A", minutes=0.005)  # 0.3 s
        self.bg("b", "B")
        time.sleep(0.6)
        text = server.renew({"agent": "a", "job_id": "A"})["text"]
        self.assertIn("% of your", text); self.assertIn("1 waiting", text)

    def test_camera_alongside_edits_blocked_by_play_and_not_renewable(self):
        self.acq("e", "E", lane="edit", scope=["Workspace.Map"])
        k = self.acq("k", "K", lane="camera")
        self.assertEqual(k["status"], "granted"); self.assertLessEqual(k["lease_seconds"], 120)
        self.assertEqual(self.acq("k2", "K2", lane="camera")["status"], "queued")  # one camera lease at a time
        with self.assertRaisesRegex(ValueError, "not renewable"):
            server.renew({"agent": "k", "job_id": "K"})
        self.rel("k", "K")
        self.assertEqual(self.acq("k2", "K2", lane="camera")["status"], "granted")
        self.rel("k2", "K2"); self.rel("e", "E")
        self.acq("p", "P")
        self.assertEqual(self.acq("k3", "K3", lane="camera")["status"], "queued")

    def test_camera_lease_is_capped(self):
        server.CAMERA_SECONDS = 9999
        self.assertLessEqual(self.acq("k", "K", lane="camera")["lease_seconds"], 120)

    def test_invalid_lane(self):
        with self.assertRaises(ValueError): self.acq("a", "A", lane="bogus")


class BoundedWait(QueueCase):
    def test_returns_queued_and_keeps_place(self):
        self.acq("a", "A")
        r = self.acq("b", "B", purpose="x", minutes=3)
        self.assertEqual((r["status"], r["position"]), ("queued", 1))
        self.assertIn("call acquire again", r["text"].lower()); self.assertIn("same agent and job_id=B", r["text"])
        created = self.row("B")["created"]
        self.assertEqual(self.state("B"), "queued")
        self.assertEqual(self.acq("b", "B")["status"], "queued")  # re-call, same place
        self.assertEqual(self.row("B")["created"], created)
        self.rel("a", "A")
        self.assertEqual(self.acq("b", "B")["status"], "granted")

    def test_abandoned_after_grace(self):
        server.GRACE_SECONDS = 0.3
        self.acq("a", "A")
        self.assertEqual(self.acq("b", "B")["status"], "queued")
        time.sleep(0.5)
        server.cleanup(self.c)
        self.assertEqual(self.state("B"), "abandoned")
        with self.assertRaisesRegex(ValueError, "abandoned"):
            self.acq("b", "B")
        self.assertIn("abandon job=B agent=b", self.log())

    def test_away_waiter_does_not_block_others_but_keeps_priority(self):
        server.LIVE_SECONDS = 0.1
        self.acq("a", "A")
        self.assertEqual(self.acq("b", "B")["status"], "queued")
        time.sleep(0.3)  # B is away: not polled for longer than LIVE_SECONDS
        self.rel("a", "A")
        self.assertEqual(self.acq("c", "C")["status"], "granted")
        self.rel("c", "C")
        self.assertEqual(self.acq("b", "B")["status"], "granted")

    def test_only_the_polling_thread_grants(self):
        self.acq("a", "A")
        self.assertEqual(self.acq("b", "B")["status"], "queued")
        self.rel("a", "A")
        time.sleep(0.2)
        server.cleanup(self.c)
        self.assertEqual(self.state("B"), "queued")  # nobody polls for B, so nobody gets the lease on its behalf

    def test_queue_age_expiry_applies_only_to_old_code_rows(self):
        server.QUEUE_WAIT_SECONDS = 0.2
        self.acq("a", "A")
        self.acq("b", "B")  # new-code row: has last_seen
        self.c.execute("INSERT INTO jobs(id,agent,state,created) VALUES('OLDQ','old','queued',?)", (time.time(),))
        time.sleep(0.4)
        self.acq("b", "B")  # still polling: never age-expired
        server.cleanup(self.c)
        self.assertEqual((self.state("B"), self.state("OLDQ")), ("queued", "expired"))
        self.assertIn("expire job=OLDQ agent=old", self.log())

    def test_default_live_seconds_is_90(self):
        self.assertEqual(self._saved["LIVE_SECONDS"], 90)


class SupersedeCancel(QueueCase):
    def test_supersede(self):
        self.acq("a", "A")
        old = self.bg("x", "X1")
        new = self.bg("x", "X2")
        res = old.get()
        self.assertEqual(res["status"], "superseded")
        self.assertEqual(self.state("X1"), "superseded"); self.assertEqual(self.state("X2"), "queued")
        self.assertIn("supersede job=X1 agent=x", self.log())
        self.rel("a", "A")
        self.assertEqual(new.get()["status"], "granted")

    def test_supersede_leaves_active_lease_and_says_so(self):
        self.acq("x", "X1")
        r = self.acq("x", "X2")
        self.assertEqual(r["status"], "queued")
        self.assertIn("still hold an active lease", r["text"]); self.assertIn("X1", r["text"])
        self.assertEqual(self.state("X1"), "active")

    def test_cancel_queued_and_active(self):
        self.acq("a", "A")
        b = self.bg("b", "B")
        self.assertTrue(server.cancel({"agent": "b", "job_id": "B"})["cancelled"])
        self.assertEqual(b.get()["status"], "cancelled")
        self.assertEqual(self.state("B"), "cancelled")
        self.assertTrue(server.cancel({"agent": "a", "job_id": "A"})["cancelled"])
        self.assertEqual(self.state("A"), "released")
        self.assertFalse(server.cancel({"agent": "a", "job_id": "A"})["cancelled"])
        with self.assertRaises(ValueError): server.cancel({"agent": "zzz", "job_id": "A"})
        self.assertIn("cancel job=B agent=b", self.log())

    def test_notifications_cancelled_through_handle(self):
        replies = []
        orig = server.reply
        server.reply = lambda i, result=None, error=None: replies.append((i, result, error))
        try:
            self.acq("a", "A")
            server.MAX_WAIT_SECONDS = 10
            msg = {"jsonrpc": "2.0", "id": 77, "method": "tools/call",
                   "params": {"name": "acquire", "arguments": {"agent": "b", "job_id": "B"}}}
            server.register_inflight(msg)
            t = Call(server.handle, msg)
            wait_for(lambda: self.row("B") is not None)
            server.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 77}})
            t.get()
            self.assertEqual(self.state("B"), "cancelled")
            self.assertEqual([r for r in replies if r[0] == 77], [])  # cancelled requests get no response
            self.assertNotIn(77, server.INFLIGHT)
            # a cancel for a request that already finished changes nothing
            server.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 77}})
            self.assertEqual(self.state("A"), "active")
        finally:
            server.reply = orig

    def test_notifications_cancelled_releases_a_just_granted_lease(self):
        replies = []
        orig = server.reply
        server.reply = lambda i, result=None, error=None: replies.append((i, result, error))
        try:
            msg = {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                   "params": {"name": "acquire", "arguments": {"agent": "b", "job_id": "B"}}}
            server.register_inflight(msg)
            server.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": 5}})
            server.handle(msg)  # cancel arrived first: no job is created, no reply sent
            self.assertIsNone(self.row("B")); self.assertEqual(replies, [])
        finally:
            server.reply = orig


class ReviewFixes(QueueCase):
    def test_job_id_of_another_agent_is_refused(self):
        # two agents that pick the same job_id must not both be told they hold Studio
        self.assertEqual(self.acq("a", "J")["status"], "granted")
        with self.assertRaisesRegex(ValueError, "another agent"):
            self.acq("b", "J")
        self.assertEqual(self.row("J")["agent"], "a")
        self.assertEqual(self.acq("x", "X")["status"], "queued")
        with self.assertRaisesRegex(ValueError, "another agent"):
            self.acq("y", "X")
        self.assertEqual((self.state("X"), self.row("X")["agent"]), ("queued", "x"))

    def _cancel_during_first_poll(self, msg):
        """Deliver notifications/cancelled after acquire passed its cancel check but before its first transaction."""
        orig = server.cleanup
        fired = []

        def cleanup(c):
            if not fired:
                fired.append(1)
                server.handle({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": msg["id"]}})
            return orig(c)
        server.cleanup = cleanup
        return orig

    def test_cancel_racing_first_poll_does_not_leave_a_lease(self):
        replies = []
        orig_reply = server.reply
        server.reply = lambda i, result=None, error=None: replies.append((i, result, error))
        msg = {"jsonrpc": "2.0", "id": 91, "method": "tools/call",
               "params": {"name": "acquire", "arguments": {"agent": "b", "job_id": "B"}}}
        server.register_inflight(msg)
        orig_cleanup = self._cancel_during_first_poll(msg)
        try:
            server.handle(msg)
        finally:
            server.cleanup = orig_cleanup; server.reply = orig_reply
        self.assertEqual(replies, [])
        self.assertNotIn(self.state("B"), ("active", "queued"))  # nobody is listening for this lease
        self.assertEqual(self.acq("c", "C")["status"], "granted")

    def test_cancel_racing_first_poll_does_not_leave_a_queued_row(self):
        self.acq("a", "A")
        server.MAX_WAIT_SECONDS = 10
        msg = {"jsonrpc": "2.0", "id": 92, "method": "tools/call",
               "params": {"name": "acquire", "arguments": {"agent": "b", "job_id": "B"}}}
        server.register_inflight(msg)
        orig_reply = server.reply
        server.reply = lambda *a, **k: None
        orig_cleanup = self._cancel_during_first_poll(msg)
        try:
            Call(server.handle, msg).get()
        finally:
            server.cleanup = orig_cleanup; server.reply = orig_reply
        self.assertNotEqual(self.state("B"), "queued")

    def test_scope_game_prefix_and_comma_string_overlap(self):
        self.assertEqual(self.acq("a", "A", lane="edit", scope=["game.Workspace.Map"])["status"], "granted")
        self.assertEqual(self.acq("b", "B", lane="edit", scope=["Workspace.Map.Tower"])["status"], "queued")
        self.assertEqual(self.acq("c", "C", lane="edit", scope="StarterGui.Building, ReplicatedStorage")["status"], "granted")
        self.assertEqual(self.acq("d", "D", lane="edit", scope=["ReplicatedStorage.Config"])["status"], "queued")
        self.assertEqual(server.parse_scope(["game"]), [])  # the whole game means the whole place


class Sessions(QueueCase):
    def test_dead_process_cleanup(self):
        now = time.time()
        self.c.execute("INSERT INTO processes(id,pid,started,last_seen) VALUES('dead',1,?,?)", (now - 500, now - 500))
        self.c.execute("INSERT INTO processes(id,pid,started,last_seen) VALUES('alive',2,?,?)", (now, now))
        ins = "INSERT INTO jobs(id,agent,state,created,started,lease_until,proc,lane) VALUES(?,?,?,?,?,?,?,'play')"
        self.c.execute(ins, ("DA", "d", "active", now - 100, now - 100, now + 500, "dead"))
        self.c.execute(ins, ("DQ", "d2", "queued", now - 50, None, None, "dead"))
        self.c.execute(ins, ("LQ", "l", "queued", now - 10, None, None, "alive"))
        server.cleanup(self.c)
        self.assertEqual((self.state("DA"), self.state("DQ"), self.state("LQ")), ("expired", "expired", "queued"))
        self.assertIsNone(self.c.execute("SELECT 1 FROM processes WHERE id='dead'").fetchone())
        self.assertIn("expire job=DA agent=d", self.log())

    def test_heartbeat_registers_process(self):
        self.acq("a", "A")
        self.assertIsNotNone(self.c.execute("SELECT 1 FROM processes WHERE id=?", (server.PROC_ID,)).fetchone())

    def test_shutdown_releases_and_cancels_own_jobs(self):
        self.acq("a", "A")
        self.acq("b", "B")  # queued
        self.c.execute("INSERT INTO jobs(id,agent,state,created,proc) VALUES('OTHER','o','queued',?,'someone-else')", (time.time(),))
        server.shutdown_process()
        self.assertEqual((self.state("A"), self.state("B"), self.state("OTHER")), ("released", "cancelled", "queued"))

    def test_rejoin_priority(self):
        self.acq("a", "A")
        old_created = self.row("A")["created"]
        b = self.bg("b", "B")
        self.rel("a", "A", rejoin_seconds=60)
        self.assertEqual(b.get()["status"], "granted")  # nothing is reserved meanwhile
        c = self.bg("c", "C")
        again = self.bg("a", "A2")
        self.assertEqual(self.row("A2")["created"], self.row("A")["released"])
        self.assertGreater(self.row("A2")["created"], old_created)
        self.rel("b", "B")
        self.assertEqual(again.get()["status"], "granted")  # front of the queue, ahead of C
        self.assertEqual(self.state("C"), "queued")
        self.rel("a", "A2")
        self.assertEqual(c.get()["status"], "granted")

    def test_rejoin_never_jumps_a_request_that_was_waiting(self):
        # Two agents alternating play leases with rejoin used to starve a camera request forever.
        self.acq("a", "A")
        b = self.bg("b", "B")
        t = self.bg("t", "T", lane="camera")
        self.rel("a", "A", rejoin_seconds=60)
        self.assertEqual(b.get()["status"], "granted")
        again = self.bg("a", "A2")
        self.rel("b", "B", rejoin_seconds=60)
        self.assertEqual(t.get()["status"], "granted")  # T was waiting before A stepped out
        self.assertEqual(self.state("A2"), "queued")
        self.rel("t", "T")
        self.assertEqual(again.get()["status"], "granted")

    def test_rejoin_expires_and_is_used_once(self):
        self.acq("a", "A")
        self.rel("a", "A", rejoin_seconds=0.2)
        time.sleep(0.4)
        self.acq("z", "Z")
        self.acq("a", "A2")
        self.assertGreater(self.row("A2")["created"], self.row("Z")["created"])
        with self.assertRaises(ValueError): self.rel("z", "Z", rejoin_seconds=301)

    def test_studio_down_and_up(self):
        self.acq("a", "A")
        server.report_down({"agent": "a", "reason": "MCP disconnected"})
        r = self.acq("b", "B")
        self.assertEqual(r["status"], "down")
        self.assertIn("Studio reported down by a at", r["text"]); self.assertIn("MCP disconnected", r["text"])
        self.assertIn("Stop and tell your orchestrator or user.", r["text"])
        self.assertNotEqual(self.state("B"), "queued")  # a down report never leaves a request waiting
        self.assertEqual(server.renew({"agent": "a", "job_id": "A"})["job_id"], "A")  # active leases continue
        self.assertIn("DOWN", server.status({})["text"])
        server.report_up({"agent": "a"})
        self.rel("a", "A")
        self.assertEqual(self.acq("b", "B2")["status"], "granted")
        log = self.log()
        self.assertIn("WARNING studio down by=a", log); self.assertIn("INFO studio up by=a", log)

    def test_down_wakes_waiting_acquire(self):
        self.acq("a", "A")
        b = self.bg("b", "B")
        server.report_down({"agent": "a", "reason": "crashed"})
        self.assertEqual(b.get()["status"], "down")


class Visibility(QueueCase):
    def test_status_table_and_eta(self):
        self.acq("a", "A", lane="edit", scope=["Workspace.Map", "StarterGui.Building"], purpose="build tower", minutes=10)
        self.acq("b", "B", lane="edit", scope=["Workspace.Map"], purpose="fence", minutes=4)
        self.acq("c", "C", lane="play", purpose="no estimate")
        text = server.status({"job_id": "B"})["text"]
        self.assertIn("ACTIVE (1)", text); self.assertIn("QUEUED (2)", text)
        self.assertIn("Workspace.Map,StarterGui.Building", text); self.assertIn("build tower", text)
        self.assertRegex(text, r"Your job B: position 1 of 2, ETA ~(9|10) min")
        self.acq("d", "D", lane="edit", scope=["Workspace.Other"], minutes=2)  # waits behind C, which gave no estimate
        self.assertIn("ETA unknown", server.status({"job_id": "D"})["text"])

    def test_release_notes_handover(self):
        self.acq("a", "A")
        self.rel("a", "A", notes="camera moved to the tower; debug pause off")
        r = self.acq("b", "B")
        self.assertIn("Previous holder a", r["text"]); self.assertIn("camera moved to the tower", r["text"])
        self.assertIn("Before starting, make sure Studio is stopped and in the state your project's docs require.", r["text"])
        self.rel("b", "B")
        self.assertNotIn("left notes: camera", self.acq("a", "A2")["text"].replace("Previous holder a", ""))  # own notes not echoed back
        self.c.execute("UPDATE jobs SET released=? WHERE id='A'", (time.time() - 3600,))
        self.rel("a", "A2");
        self.assertNotIn("left notes", self.acq("c", "C")["text"])  # older than 30 minutes

    def test_log_lines_keep_prefixes(self):
        self.acq("agent-x", "J1", lane="edit", scope=["Workspace.Map"], purpose="fix walls")
        self.rel("agent-x", "J1")
        log = self.log()
        self.assertRegex(log, r"INFO acquire job=J1 agent=agent-x lane=edit scope=Workspace\.Map purpose=fix walls")
        self.assertRegex(log, r"INFO release job=J1 agent=agent-x lane=edit scope=Workspace\.Map purpose=fix walls")


class Migration(unittest.TestCase):
    def test_old_schema_db_migrates_additively(self):
        path = os.path.join(TMP, "old-schema.db")
        old = sqlite3.connect(path)
        old.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, agent TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL, started REAL, lease_until REAL, released REAL, UNIQUE(id))")
        now = time.time()
        old.execute("INSERT INTO jobs VALUES('OLDACTIVE','oldagent','active',?,?,?,NULL)", (now - 10, now - 10, now + 100))
        old.execute("INSERT INTO jobs VALUES('OLDQUEUED','oldagent2','queued',?,NULL,NULL,NULL)", (now - 5,))
        old.execute("INSERT INTO jobs VALUES('OLDDONE','oldagent3','released',?,?,?,?)", (now - 500, now - 500, now - 400, now - 450))
        old.commit(); old.close()
        saved = server.DB
        server.DB = path
        try:
            c = server.db()
            cols = {r[1] for r in c.execute("PRAGMA table_info(jobs)")}
            self.assertTrue({"lane", "scope", "purpose", "minutes", "proc", "last_seen", "queued_at", "notes", "rejoin_until"} <= cols)
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue({"processes", "flags"} <= tables)
            self.assertEqual(c.execute("SELECT count(*) FROM jobs").fetchone()[0], 3)  # old rows intact
            server.migrate(c); server.migrate(c)  # idempotent
            # the old active row (NULL lane) behaves as play, so it blocks; old queued row stays ahead in order
            saved_max = server.MAX_WAIT_SECONDS; server.MAX_WAIT_SECONDS = 0.2
            try:
                r = server.acquire({"agent": "new", "job_id": "NEW", "lane": "edit", "scope": ["Workspace.Map"]})
                self.assertEqual(r["status"], "queued")
                # an old-code process sees only additive changes: its own INSERT form still works
                c.execute("INSERT OR IGNORE INTO jobs(id,agent,state,created) VALUES('OLD2','o','queued',?)", (time.time(),))
                # cancelling an old-process row writes only states the old code understands
                self.assertTrue(server.cancel({"agent": "oldagent2", "job_id": "OLDQUEUED"})["cancelled"])
                self.assertIn(c.execute("SELECT state FROM jobs WHERE id='OLDQUEUED'").fetchone()[0], server.OLD_STATES)
            finally:
                server.MAX_WAIT_SECONDS = saved_max
            c.close()
        finally:
            server.DB = saved


class EndToEnd(unittest.TestCase):
    def test_stdio_round_trip(self):
        db = os.path.join(TMP, "e2e.db")
        env = {k: v for k, v in os.environ.items() if k != "ROBLOX_PLAYTEST_MUTE_AUDIO"}
        env.update(ROBLOX_PLAYTEST_QUEUE_DB=db, ROBLOX_PLAYTEST_QUEUE_LOG=os.path.join(TMP, "e2e.log"),
                   ROBLOX_PLAYTEST_AUDIO_STATE=os.path.join(TMP, "e2e-audio.json"))
        p = subprocess.Popen([sys.executable, os.path.join(ROOT, "server.py")], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, env=env, cwd=ROOT, text=True)
        timer = threading.Timer(60, p.kill); timer.start()
        ids = iter(range(1, 100))

        def rpc(method, params=None):
            i = next(ids)
            p.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method, "params": params or {}}) + "\n"); p.stdin.flush()
            out = json.loads(p.stdout.readline())
            self.assertEqual(out["id"], i)
            return out["result"]

        def call(name, **args):
            res = rpc("tools/call", {"name": name, "arguments": args})
            return res["content"][0]["text"], bool(res.get("isError"))

        try:
            init = rpc("initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}})
            self.assertEqual(init["serverInfo"]["version"], "0.3.0")
            self.assertIn("requesting a position in the Roblox Studio queue", init["instructions"])
            p.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"); p.stdin.flush()
            names = [t["name"] for t in rpc("tools/list")["tools"]]
            self.assertEqual(names, ["acquire", "release", "renew", "cancel", "status", "report_down", "report_up"])
            text, err = call("acquire", agent="e2e", job_id="J1", purpose="round trip", minutes=1)
            self.assertFalse(err); self.assertIn("Ready.", text)
            self.assertIn("e2e", call("status")[0])
            self.assertIn("renewed", call("renew", agent="e2e", job_id="J1")[0])
            text, err = call("release", agent="e2e", job_id="J1", notes="clean")
            self.assertFalse(err); self.assertIn("released", text)
            text, err = call("release", agent="e2e", job_id="J1")
            self.assertTrue(err)
            text, err = call("acquire", agent="e2e", job_id="J2", lane="edit", scope=["Workspace.Map"])
            self.assertFalse(err)
            p.stdin.close()  # session ends without releasing J2
            self.assertEqual(p.wait(timeout=20), 0)
        finally:
            timer.cancel()
            if p.poll() is None: p.kill()
            p.stdout.close()
        c = sqlite3.connect(db)
        states = dict(c.execute("SELECT id,state FROM jobs").fetchall())
        c.close()
        self.assertEqual(states, {"J1": "released", "J2": "released"})  # EOF released the open lease


if __name__ == "__main__":
    unittest.main()
