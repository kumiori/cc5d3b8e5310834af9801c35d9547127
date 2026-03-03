from __future__ import annotations

from io import BytesIO
from typing import List, Tuple
from datetime import datetime, UTC

from PIL import Image, ImageDraw, ImageFont

from infra.key_codec import split_emoji_symbols


def _load_font(candidates: List[str], size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _glyph_hash(font: ImageFont.ImageFont, symbol: str) -> bytes:
    canvas = Image.new("L", (80, 80), 255)
    draw = ImageDraw.Draw(canvas)
    draw.text((8, 8), symbol, fill=0, font=font)
    bbox = canvas.getbbox()
    if not bbox:
        return b""
    return canvas.crop(bbox).tobytes()


def _pick_emoji_font(symbols: List[str], size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Apple Color Emoji.ttc",
        "/System/Library/Fonts/Supplemental/Apple Symbols.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
    best_font: ImageFont.FreeTypeFont | ImageFont.ImageFont = ImageFont.load_default()
    best_score = 0
    sample = symbols[: min(len(symbols), 10)]
    if not sample:
        return _load_font(candidates, size)

    for path in candidates:
        try:
            font = ImageFont.truetype(path, size)
        except Exception:
            continue
        hashes = {_glyph_hash(font, s) for s in sample}
        score = len([h for h in hashes if h])
        if score > best_score:
            best_score = score
            best_font = font
    return best_font


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> List[str]:
    if not text:
        return [""]
    words = text.split(" ")
    lines: List[str] = []
    current = ""
    for word in words:
        trial = word if not current else f"{current} {word}"
        if draw.textlength(trial, font=font) <= max_width:
            current = trial
            continue
        if current:
            lines.append(current)
            current = ""
        # split overlong token
        token = word
        while token and draw.textlength(token, font=font) > max_width:
            cut = max(1, min(len(token), 16))
            while cut > 1 and draw.textlength(token[:cut], font=font) > max_width:
                cut -= 1
            lines.append(token[:cut])
            token = token[cut:]
        current = token
    if current:
        lines.append(current)
    return lines or [""]


def build_credentials_pdf(
    *,
    access_key: str,
    emoji: str,
    phrase: str,
    nickname: str,
    role: str,
    title: str = "Carte d'acces",
) -> bytes:
    symbols = split_emoji_symbols(emoji)
    suffix4_symbols = symbols[-4:] if len(symbols) >= 4 else []
    suffix6_symbols = symbols[-6:] if len(symbols) >= 6 else []

    grouped_key = " ".join(access_key[i : i + 4] for i in range(0, len(access_key), 4))
    spaced_emoji = " ".join(symbols) if symbols else emoji
    spaced_emoji4 = " ".join(suffix4_symbols) if suffix4_symbols else "—"
    spaced_emoji6 = " ".join(suffix6_symbols) if suffix6_symbols else "—"
    phrase_wrapped = phrase.replace("-", " - ")

    text_font = _load_font(
        [
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttc",
            "/Library/Fonts/Arial Unicode.ttf",
        ],
        18,
    )
    emoji_font = _pick_emoji_font(symbols + suffix4_symbols + suffix6_symbols, 26)
    title_font = _load_font(
        [
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
            "/System/Library/Fonts/Supplemental/Helvetica.ttc",
            "/Library/Fonts/Arial Unicode.ttf",
        ],
        30,
    )

    width = 680
    margin = 28
    line_gap = 10
    header_h = 34

    probe = Image.new("RGB", (width, 400), "white")
    draw = ImageDraw.Draw(probe)

    content: List[Tuple[str, str, ImageFont.ImageFont]] = [
        ("Date UTC", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"), text_font),
        ("Nom", nickname or "—", text_font),
        ("Role", role or "guest", text_font),
        ("Cle d'acces", grouped_key, text_font),
        ("Emoji", spaced_emoji or "—", emoji_font),
        ("Emoji-4", spaced_emoji4, emoji_font),
        ("Emoji-6", spaced_emoji6, emoji_font),
        ("Phrase secrete", phrase_wrapped or "—", text_font),
    ]

    y = margin + header_h * 2
    max_text_width = width - (2 * margin) - 150
    measured: List[Tuple[str, List[str], ImageFont.ImageFont]] = []
    for label, value, font in content:
        lines = _wrap_text(draw, value, font, max_text_width)
        measured.append((label, lines, font))
        y += (len(lines) * 28) + line_gap
    emoji_rows = max(1, (len(symbols) + 7) // 8)
    suffix_rows = 1 if suffix4_symbols else 0
    suffix6_rows = 1 if suffix6_symbols else 0
    y += (emoji_rows * 44) + (suffix_rows * 44) + (suffix6_rows * 44) + 54
    height = max(520, y + margin)

    image = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(image)

    y = margin
    draw.rectangle([(0, 0), (width, 110)], fill=(235, 245, 255))
    draw.text((margin, y), "LES AFFRANCHIS", fill=(15, 48, 87), font=title_font)
    y += header_h
    draw.text((margin, y), title, fill=(15, 48, 87), font=text_font)
    y += header_h + 16

    label_x = margin
    value_x = margin + 150
    for label, lines, font in measured:
        draw.text((label_x, y), f"{label} :", fill=(36, 52, 71), font=text_font)
        for i, line in enumerate(lines):
            if font is emoji_font:
                draw.text((value_x, y + (i * 28)), line, fill="black", font=font, embedded_color=True)
            else:
                draw.text((value_x, y + (i * 28)), line, fill="black", font=font)
        y += (len(lines) * 28) + line_gap

    # Emoji chips in color for readability even when glyph fallback is imperfect.
    chip_palette = [
        (255, 235, 238),
        (232, 245, 233),
        (227, 242, 253),
        (255, 243, 224),
        (243, 229, 245),
        (224, 247, 250),
    ]
    x = value_x
    y += 2
    draw.text((label_x, y), "Jetons emoji :", fill=(36, 52, 71), font=text_font)
    for idx, symbol in enumerate(symbols):
        if x + 72 > width - margin:
            x = value_x
            y += 44
        bg = chip_palette[idx % len(chip_palette)]
        draw.rounded_rectangle([(x, y), (x + 64, y + 34)], radius=8, fill=bg, outline=(160, 160, 160), width=1)
        draw.text((x + 8, y + 3), symbol, fill=(20, 20, 20), font=emoji_font, embedded_color=True)
        draw.text((x + 44, y + 9), str(idx + 1), fill=(70, 70, 70), font=text_font)
        x += 72
    y += 46

    if suffix4_symbols:
        x = value_x
        draw.text((label_x, y), "Jetons emoji-4 :", fill=(36, 52, 71), font=text_font)
        for idx, symbol in enumerate(suffix4_symbols):
            bg = chip_palette[(idx + 2) % len(chip_palette)]
            draw.rounded_rectangle([(x, y), (x + 64, y + 34)], radius=8, fill=bg, outline=(160, 160, 160), width=1)
            draw.text((x + 8, y + 3), symbol, fill=(20, 20, 20), font=emoji_font, embedded_color=True)
            draw.text((x + 44, y + 9), str(idx + 1), fill=(70, 70, 70), font=text_font)
            x += 72
        y += 46

    if suffix6_symbols:
        x = value_x
        draw.text((label_x, y), "Jetons emoji-6 :", fill=(36, 52, 71), font=text_font)
        for idx, symbol in enumerate(suffix6_symbols):
            bg = chip_palette[(idx + 3) % len(chip_palette)]
            draw.rounded_rectangle([(x, y), (x + 64, y + 34)], radius=8, fill=bg, outline=(160, 160, 160), width=1)
            draw.text((x + 8, y + 3), symbol, fill=(20, 20, 20), font=emoji_font, embedded_color=True)
            draw.text((x + 44, y + 9), str(idx + 1), fill=(70, 70, 70), font=text_font)
            x += 72
        y += 46

    draw.line([(margin, y), (width - margin, y)], fill="black", width=1)
    y += 14
    draw.text((margin, y), "Conservez ce document en lieu sur.", fill="black", font=text_font)

    buffer = BytesIO()
    image.save(buffer, format="PDF", resolution=150.0)
    return buffer.getvalue()
