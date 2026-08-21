import json, os

MUTE_ENABLED = os.environ.get("ROBLOX_PLAYTEST_MUTE_AUDIO", "false").lower() in ("1", "true", "yes", "on")
PROCESS_NAME = os.environ.get("ROBLOX_PLAYTEST_AUDIO_PROCESS", "RobloxStudioBeta.exe")
_saved = []
STATE_FILE = os.environ.get("ROBLOX_PLAYTEST_AUDIO_STATE", os.path.join(os.path.dirname(__file__), "audio-state.json"))

def mute_studio(job_id=None):
    global _saved
    if not MUTE_ENABLED:
        return {"enabled": False, "muted": 0}
    try:
        from pycaw.pycaw import AudioUtilities
        _saved = []
        persisted = []
        for session in AudioUtilities.GetAllSessions():
            process = session.Process
            if process and process.name().lower() == PROCESS_NAME.lower():
                volume = session.SimpleAudioVolume
                was_muted = bool(volume.GetMute())
                pid = int(process.pid())
                _saved.append((volume, was_muted))
                persisted.append({"pid": pid, "muted": was_muted})
                volume.SetMute(1, None)
        if job_id:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({"job_id": job_id, "sessions": persisted}, f)
        return {"enabled": True, "muted": len(_saved)}
    except Exception as exc:
        return {"enabled": True, "muted": 0, "warning": f"Audio mute unavailable: {exc}"}

def restore_studio(job_id=None):
    global _saved
    restored = 0
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
            from pycaw.pycaw import AudioUtilities
            for session in AudioUtilities.GetAllSessions():
                process = session.Process
                if process and int(process.pid()) in persisted and process.name().lower() == PROCESS_NAME.lower():
                    session.SimpleAudioVolume.SetMute(1 if persisted[int(process.pid())] else 0, None)
                    restored += 1
        except Exception:
            pass
    for volume, was_muted in _saved:
        try:
            volume.SetMute(1 if was_muted else 0, None)
            restored += 1
        except Exception:
            pass
    _saved = []
    if job_id:
        try: os.remove(STATE_FILE)
        except FileNotFoundError: pass
    return {"enabled": MUTE_ENABLED, "restored": restored}
