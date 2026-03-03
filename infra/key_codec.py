from __future__ import annotations

import math
import re
import secrets
from typing import Dict, List

DEFAULT_KEY_BYTES = 16  # 128 bits of entropy
DEFAULT_HEX_LENGTH = DEFAULT_KEY_BYTES * 2

# Curated emoji alphabet (64 symbols) chosen for clarity, neutral meaning, and cross-platform support.
EMOJI_ALPHABET: List[str] = [
    "🌑",
    "🌒",
    "🌓",
    "🌔",
    "🌕",
    "🌖",
    "🌗",
    "🌘",
    "⭐",
    "🌟",
    "✨",
    "⚡",
    "🔥",
    "💧",
    "🌊",
    "🌬️",
    "🌀",
    "🌈",
    "❄️",
    "☄️",
    "🌋",
    "💎",
    "🧊",
    "🪐",
    "🌌",
    "🎇",
    "🎆",
    "🎈",
    "🎉",
    "🎯",
    "🎲",
    "🧠",
    "🫧",
    "🧬",
    "🔮",
    "🪄",
    "🛰️",
    "🚀",
    "🛸",
    "🛠️",
    "⚙️",
    "📡",
    "🔑",
    "🗝️",
    "📀",
    "💾",
    "🧲",
    "🪙",
    "🥇",
    "🎖️",
    "🔰",
    "♾️",
    "🪬",
    "🔺",
    "🔻",
    "🔷",
    "🔶",
    "⬛",
    "⬜",
    "🟥",
    "🟩",
    "🟦",
    "🟨",
]
EMOJI_BASE = len(EMOJI_ALPHABET)
EMOJI_INDEX = {symbol: idx for idx, symbol in enumerate(EMOJI_ALPHABET)}
EMOJI_SYMBOLS_PER_KEY = math.ceil(
    DEFAULT_KEY_BYTES * 8 / math.log2(EMOJI_BASE)
)

# Generate 256 punchy composite words by combining 16 adjectives and 16 nouns.
ADJECTIVES = [
    "solar",
    "lunar",
    "crystal",
    "shadow",
    "ember",
    "sonic",
    "quantum",
    "wild",
    "neon",
    "iron",
    "cipher",
    "scarlet",
    "plasma",
    "velvet",
    "static",
    "mythic",
]
NOUNS = [
    "vault",
    "pulse",
    "glyph",
    "arc",
    "spike",
    "flare",
    "crown",
    "delta",
    "prism",
    "drift",
    "forge",
    "orbit",
    "circuit",
    "veil",
    "signal",
    "quartz",
]
WORDLIST_256 = [f"{adj}{noun}" for adj in ADJECTIVES for noun in NOUNS]
WORD_INDEX: Dict[str, int] = {word: idx for idx, word in enumerate(WORDLIST_256)}

HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")
EMOJI_PATTERN = re.compile(
    "|".join(sorted((re.escape(sym) for sym in EMOJI_ALPHABET), key=len, reverse=True))
)


def generate_hex_key() -> str:
    """Return the canonical 128-bit hex key (uppercase)."""
    return secrets.token_hex(DEFAULT_KEY_BYTES).upper()


def _int_to_base_symbols(value: int, alphabet: List[str], pad: int) -> str:
    """Convert an integer to a string in the provided base using the alphabet."""
    if value == 0:
        return alphabet[0] * max(1, pad)
    digits: List[str] = []
    base = len(alphabet)
    while value > 0:
        value, rem = divmod(value, base)
        digits.append(alphabet[rem])
    while len(digits) < pad:
        digits.append(alphabet[0])
    return "".join(reversed(digits))


def _symbols_to_int(symbols: str, alphabet_index: Dict[str, int]) -> int:
    value = 0
    base = len(alphabet_index)
    for symbol in symbols:
        value = value * base + alphabet_index[symbol]
    return value


def hex_to_emoji(hex_key: str) -> str:
    """
    Convert the canonical hex key into an emoji string.

    Bijection justification:
        - Both encodings represent the same integer value.
        - Hex digits encode the integer in base-16; the emoji alphabet encodes the exact
          same integer in base-|alphabet|.
        - Because both conversions are deterministic, invertible base changes without loss,
          every hex value maps to exactly one emoji string (with padding ensuring fixed
          length), and every emoji string maps back to the original hex. No information is
          lost or duplicated in either direction.
    """
    value = int(hex_key, 16)
    return _int_to_base_symbols(value, EMOJI_ALPHABET, EMOJI_SYMBOLS_PER_KEY)


def emoji_to_hex(emoji_key: str) -> str:
    if len(emoji_key) != EMOJI_SYMBOLS_PER_KEY:
        raise ValueError(
            f"Emoji keys must be {EMOJI_SYMBOLS_PER_KEY} symbols long."
        )
    value = _symbols_to_int(emoji_key, EMOJI_INDEX)
    return f"{value:0{DEFAULT_HEX_LENGTH}X}"


def hex_to_phrase(hex_key: str, separator: str = "-") -> str:
    data = bytes.fromhex(hex_key)
    if len(data) != DEFAULT_KEY_BYTES:
        raise ValueError("Hex key must represent 16 bytes for phrase projection.")
    words = [WORDLIST_256[b] for b in data]
    return separator.join(words)


def phrase_to_hex(phrase: str) -> str:
    tokens = re.split(r"[\s,_-]+", phrase.strip().lower())
    tokens = [tok for tok in tokens if tok]
    if not tokens:
        raise ValueError("No words detected in passphrase.")
    if len(tokens) != DEFAULT_KEY_BYTES:
        raise ValueError(
            f"Passphrase must contain {DEFAULT_KEY_BYTES} words (found {len(tokens)})."
        )
    bytes_out = bytearray()
    for token in tokens:
        if token not in WORD_INDEX:
            raise ValueError(f"Unknown token '{token}' in phrase.")
        bytes_out.append(WORD_INDEX[token])
    return bytes_out.hex().upper()


def normalize_access_key(raw: str) -> str:
    """
    Attempt to interpret the user input as:
        1. canonical hex (with/without 0x, spaces)
        2. emoji projection (only emoji alphabet characters)
        3. passphrase projection (words from WORDLIST_256)
    Returns uppercase hex string if successful; raises ValueError otherwise.
    """
    candidate = raw.strip()
    if not candidate:
        raise ValueError("Empty access key.")

    hex_guess = candidate.replace(" ", "").replace("-", "")
    if hex_guess.lower().startswith("0x"):
        hex_guess = hex_guess[2:]
    if HEX_PATTERN.fullmatch(hex_guess) and len(hex_guess) % 2 == 0:
        if len(hex_guess) != DEFAULT_HEX_LENGTH:
            raise ValueError(
                f"Hex keys must be {DEFAULT_HEX_LENGTH} characters long."
            )
        return hex_guess.upper()

    if all(ch in EMOJI_INDEX for ch in candidate):
        return emoji_to_hex(candidate)

    tokens = re.split(r"[\s,_-]+", candidate.lower())
    tokens = [tok for tok in tokens if tok]
    if tokens and all(tok in WORD_INDEX for tok in tokens):
        return phrase_to_hex(candidate)

    raise ValueError("Access key format not recognized.")


def split_emoji_symbols(raw: str) -> List[str]:
    """Split a string into emoji symbols from the curated alphabet."""
    if not raw:
        return []
    symbols: List[str] = []
    idx = 0
    while idx < len(raw):
        match = EMOJI_PATTERN.match(raw, idx)
        if not match:
            return []
        symbols.append(match.group(0))
        idx = match.end()
    return symbols
