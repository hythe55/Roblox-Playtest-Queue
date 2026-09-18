import json, os, threading
from contextlib import contextmanager

MUTE_ENABLED = os.environ.get("ROBLOX_PLAYTEST_MUTE_AUDIO", "false").lower() in ("1", "true", "yes", "on")
PROCESS_NAME = os.environ.get("ROBLOX_PLAYTEST_AUDIO_PROCESS", "RobloxStudioBeta.exe")
WATCH_INTERVAL = float(os.environ.get("ROBLOX_PLAYTEST_AUDIO_POLL_SECONDS", "0.5"))
_saved = {}
_state_lock = threading.RLock()
_watch_stop = None
_watch_thread = None
STATE_FILE = os.environ.get("ROBLOX_PLAYTEST_AUDIO_STATE", os.path.join(os.path.dirname(__file__), "audio-state.json"))

@contextmanager
def com_context():
    import comtypes
    comtypes.CoInitialize()
    try:
        yield
    finally:
        comtypes.CoUninitialize()

def is_muted(volume):
    value = volume.GetMute
    return bool(value() if callable(value) else value)

def process_pid(process):
    value = process.pid
    return int(value() if callable(value) else value)

def is_studio_process(process):
    if not process:
        return False
    value = process.name
    name = value() if callable(value) else value
    return name.lower() == PROCESS_NAME.lower()

def write_state(job_id):
    with _state_lock:
        sessions = [{"pid": pid, "muted": was_muted} for pid, (_, was_muted) in _saved.items()]
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"job_id": job_id, "sessions": sessions}, f)

def mute_sessions(job_id=None):
    global _saved
    muted = 0
    new_sessions = 0
    with com_context():
        from pycaw.pycaw import AudioUtilities
        for session in AudioUtilities.GetAllSessions():
            process = session.Process
            if not is_studio_process(process):
                continue
            pid = process_pid(process)
            volume = session.SimpleAudioVolume
            with _state_lock:
                previous = _saved.get(pid)
                was_muted = previous[1] if previous else is_muted(volume)
                _saved[pid] = (volume, was_muted)
            if previous is None:
                new_sessions += 1
            volume.SetMute(1, None)
            muted += 1
    if job_id and new_sessions:
        write_state(job_id)
    return muted, new_sessions

def stop_watcher():
    global _watch_stop, _watch_thread
    with _state_lock:
        stop = _watch_stop
        thread = _watch_thread
        _watch_stop = None
        _watch_thread = None
    if stop:
        stop.set()
    if thread and thread is not threading.current_thread():
        thread.join(timeout=max(1.0, WATCH_INTERVAL * 2))

def watch_sessions(job_id, stop):
    while not stop.wait(WATCH_INTERVAL):
        try:
            mute_sessions(job_id)
        except Exception:
            pass

def mute_studio(job_id=None):
    global _saved, _watch_stop, _watch_thread
    if not MUTE_ENABLED:
        return {"enabled": False, "muted": 0}
    try:
        stop_watcher()
        with _state_lock:
            _saved = {}
        muted, _ = mute_sessions(job_id)
        if job_id:
            write_state(job_id)
            stop = threading.Event()
            thread = threading.Thread(target=watch_sessions, args=(job_id, stop), daemon=True)
            with _state_lock:
                _watch_stop = stop
                _watch_thread = thread
            thread.start()
        return {"enabled": True, "muted": muted, "watching": bool(job_id)}
    except Exception as exc:
        return {"enabled": True, "muted": 0, "warning": f"Audio mute unavailable: {exc}"}

def restore_studio(job_id=None):
    global _saved
    stop_watcher()
    restored = 0
    restored_pids = set()
    persisted = {}
    if job_id and os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            if data.get("job_id") == job_id:
                persisted = {int(x["pid"]): bool(x["muted"]) for x in data.get("sessions", [])}
        except Exception:
            pass
    if persisted:
        try:
            with com_context():
                from pycaw.pycaw import AudioUtilities
                for session in AudioUtilities.GetAllSessions():
                    process = session.Process
                    pid = process_pid(process) if is_studio_process(process) else None
                    if pid in persisted:
                        session.SimpleAudioVolume.SetMute(1 if persisted[pid] else 0, None)
                        restored += 1
                        restored_pids.add(pid)
        except Exception:
            pass
    with _state_lock:
        saved = list(_saved.items())
    try:
        with com_context():
            for pid, (volume, was_muted) in saved:
                if pid in restored_pids:
                    continue
                try:
                    volume.SetMute(1 if was_muted else 0, None)
                    restored += 1
                except Exception:
                    pass
    except Exception:
        pass
    with _state_lock:
        _saved = {}
    if job_id:
        try: os.remove(STATE_FILE)
        except FileNotFoundError: pass
    return {"enabled": MUTE_ENABLED, "restored": restored}
