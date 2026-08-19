import os

MUTE_ENABLED = os.environ.get("ROBLOX_PLAYTEST_MUTE_AUDIO", "false").lower() in ("1", "true", "yes", "on")
PROCESS_NAME = os.environ.get("ROBLOX_PLAYTEST_AUDIO_PROCESS", "RobloxStudioBeta.exe")
_saved = []

def mute_studio():
    global _saved
    if not MUTE_ENABLED:
        return {"enabled": False, "muted": 0}
    try:
        from pycaw.pycaw import AudioUtilities
        _saved = []
        for session in AudioUtilities.GetAllSessions():
            process = session.Process
            if process and process.name().lower() == PROCESS_NAME.lower():
                volume = session.SimpleAudioVolume
                _saved.append((volume, bool(volume.GetMute())))
                volume.SetMute(1, None)
        return {"enabled": True, "muted": len(_saved)}
    except Exception as exc:
        return {"enabled": True, "muted": 0, "warning": f"Audio mute unavailable: {exc}"}

def restore_studio():
    global _saved
    restored = 0
    for volume, was_muted in _saved:
        try:
            volume.SetMute(1 if was_muted else 0, None)
            restored += 1
        except Exception:
            pass
    _saved = []
    return {"enabled": MUTE_ENABLED, "restored": restored}
