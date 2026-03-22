from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import streamlit as st


@dataclass
class StoredAudioRef:
    storage_provider: str
    storage_path: str
    public_url: str
    file_name: str
    mime_type: str
    size_bytes: int
    created_at: str


class AudioStorageAdapter:
    def store(
        self,
        *,
        session_id: str,
        player_id: str,
        file_name: str,
        mime_type: str,
        content: bytes,
    ) -> StoredAudioRef:
        raise NotImplementedError


class LocalAudioStorageAdapter(AudioStorageAdapter):
    def __init__(self, base_dir: str = "data/audio_notes"):
        self.base_dir = Path(base_dir)

    def store(
        self,
        *,
        session_id: str,
        player_id: str,
        file_name: str,
        mime_type: str,
        content: bytes,
    ) -> StoredAudioRef:
        target_dir = self.base_dir / session_id / player_id
        target_dir.mkdir(parents=True, exist_ok=True)
        suffix = Path(file_name).suffix or ".wav"
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        target_path = target_dir / f"audio-{ts}{suffix}"
        target_path.write_bytes(content)
        abs_path = str(target_path.resolve())
        return StoredAudioRef(
            storage_provider="local",
            storage_path=abs_path,
            public_url=abs_path,
            file_name=file_name,
            mime_type=mime_type,
            size_bytes=len(content),
            created_at=datetime.utcnow().isoformat() + "Z",
        )


def get_audio_storage_adapter() -> AudioStorageAdapter:
    cfg = st.secrets.get("audio_storage", {})
    provider = str(cfg.get("provider", "local")).strip().lower()
    if provider == "local":
        base_dir = str(cfg.get("base_dir", "data/audio_notes")).strip() or "data/audio_notes"
        return LocalAudioStorageAdapter(base_dir=base_dir)
    # Fallback: until cloud/IPFS adapters are implemented.
    return LocalAudioStorageAdapter()
