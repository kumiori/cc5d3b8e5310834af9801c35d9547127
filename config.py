import os
from dataclasses import dataclass, field
from typing import Dict


def _as_bytes(megabytes: float) -> int:
    return int(megabytes * 1024 * 1024)


@dataclass(frozen=True)
class Settings:
    match_webhook_url: str = os.getenv("MATCH_WEBHOOK_URL", "")
    proof_webhook_url: str = os.getenv("PROOF_WEBHOOK_URL", "")
    webhook_secret: str = os.getenv("THRESHOLD_WEBHOOK_SECRET", "")
    default_locale: str = os.getenv("LOCALE", "en")
    default_utm: Dict[str, str] = field(
        default_factory=lambda: {"source": "qr", "campaign": "street_v0"}
    )
    default_token: str = os.getenv("THRESHOLD_TOKEN", "")
    proof_max_image_bytes: int = _as_bytes(float(os.getenv("PROOF_IMAGE_MAX_MB", "5")))
    proof_max_voice_bytes: int = _as_bytes(float(os.getenv("PROOF_VOICE_MAX_MB", "2")))
    request_timeout_s: float = float(os.getenv("REQUEST_TIMEOUT_S", "12.0"))
    show_debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    app_title: str = os.getenv("APP_TITLE", "Ice Ice Baby")
    accent_color: str = os.getenv("ACCENT_COLOR", "#1f1f1f")


settings = Settings()
