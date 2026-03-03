# cc5d3b8e5310834af9801c35d9547127

Backlog.
Affranchis task.
Created scaffold 18:00
Created database pages (Notion, by hand)
Run the bootstrapper
App running local 21:00

# Les Affranchi•e•s · Cuisine (Streamlit + Notion)

A lightweight webapp to collect constraints, preferences, and cravings for **Les Affranchis**, backed by Notion databases.

## Language rules

- **Developer-facing code/comments/docs:** English (this README included).
- **User-facing UI text (Streamlit):** French, for now.  
  Titles, labels, buttons, helper text, errors, and debug messages shown in the UI initally in French.

---

## What this app does (v0)

### Guest flow

Guests log in and answer a short form:

1. Contraintes (régime, allergènes, ingrédients “non”)
2. Préférences (piquant, texture, condiments)
3. Envies (ressentis) rendered as pills
4. Notes publiques (bio persistante + “note de ce soir” par session)

### Host view

- aggregates (counts, distributions)
- per-person details (kitchen-ready)
- optional export (CSV + copy/paste summary)

---

## Notion architecture

The app assumes Notion databases already exist and are connected to the integration token.

Core entities:

- `players` (users; persistent bio + persistent constraints)
- `sessions` (one gathering = one session/event; can be active)
- `questions` (session-scoped question catalog)
- `responses` (answers linked to player + session + question)

In the future:

- `statements`, `votes`, `decisions`

### Notes model (two layers)

- `players.notes_public` = bio note (persistent across sessions)
- `responses.notes_public` (or a dedicated “note ce soir” question) = tonight note (per session)

---

## Environment variables

Required:

- `NOTION_TOKEN`

Database IDs:

- `AFF_PLAYERS_DB_ID`
- `AFF_SESSIONS_DB_ID`
- `AFF_QUESTIONS_DB_ID`
- `AFF_RESPONSES_DB_ID`

Optional, if still present in repo:

- `AFF_STATEMENTS_DB_ID`
- `AFF_VOTES_DB_ID`
- `AFF_DECISIONS_DB_ID`

Example:

```bash
export NOTION_TOKEN="..."
export AFF_PLAYERS_DB_ID="..."
export AFF_SESSIONS_DB_ID="..."
export AFF_QUESTIONS_DB_ID="..."
export AFF_RESPONSES_DB_ID="..."
```
