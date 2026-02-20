import re
import acoustid
import musicbrainzngs
from pathlib import Path
from datetime import datetime, timezone
from mutagen.mp3 import MP3
from pymongo import MongoClient
import time

from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3, APIC, error
import os
import time


# ==== CONFIG ====
DOWNLOAD_DIR = Path("../music-miner/downloads")
ACOUSTID_API_KEY = "d2VmByYshF"

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "musicdb"
COLLECTION_NAME = "tracks"

# ==== INIT ====
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# MusicBrainz requires a user agent
musicbrainzngs.set_useragent("music-indexer", "1.0", "you@example.com")


# We normalise strings to search across consistant fields.
def normalise_string(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9\s]", "", value)
    return value


def is_file_stable(path: Path, wait_seconds=5):
    initial_size = path.stat().st_size
    time.sleep(wait_seconds)
    return path.stat().st_size == initial_size


def write_id3_tags(
    file_path: Path,
    title: str,
    artist: str,
    album: str,
    genres: list,
    release_date: str,
):
    try:
        try:
            audio = EasyID3(file_path)
        except error:
            audio = MP3(file_path)
            audio.add_tags()
            audio.save()
            audio = EasyID3(file_path)

        audio["title"] = title
        audio["artist"] = artist

        if album:
            audio["album"] = album

        if genres:
            audio["genre"] = genres

        if release_date:
            audio["date"] = release_date

        audio.save()
        print(f"Updated ID3 tags for {file_path.name}")

    except Exception as e:
        print(f"Failed writing tags for {file_path.name}: {e}")


# ==== MUSICBRAINZ ENRICHMENT ====
def fetch_musicbrainz_metadata(recording_id):
    try:
        result = musicbrainzngs.get_recording_by_id(
            recording_id, includes=["artists", "releases", "tags"]
        )

        rec = result.get("recording", {})

        # ----- Genres / tags -----
        tags = [tag["name"] for tag in rec.get("tag-list", [])]

        # ----- Artist -----
        artist = None
        artists = rec.get("artist-credit", [])
        if artists:
            artist = "".join(
                a["artist"]["name"] + a.get("joinphrase", "")
                for a in artists
                if isinstance(a, dict)
            )

        # ----- Album / Release -----
        releases = rec.get("release-list", [])
        album = releases[0]["title"] if releases else None

        release_date = None
        if releases and "date" in releases[0]:
            release_date = releases[0]["date"]

        return {
            "mb_artist": artist,
            "mb_artist_lower": artist.lower() if artist else None,
            "genres": tags,
            "genres_lower": [t.lower() for t in tags],
            "album": album,
            "album_lower": album.lower() if album else None,
            "release_date": release_date,
        }

    except Exception as e:
        print("MusicBrainz lookup failed:", e)
        return {}


# ==== PROCESS SINGLE FILE ====
def process_file(file_path: Path):
    print(f"Processing {file_path.name}")

    if collection.find_one({"music_file": str(file_path.resolve())}):
        print(f"Skipping {file_path.name} (already indexed)")
        return

    if not is_file_stable(file_path):
        print(f"Skipping {file_path.name} (still being written)")
        return

    # ----- Audio metadata -----
    audio = MP3(file_path)
    duration = int(audio.info.length)
    bitrate = int(audio.info.bitrate / 1000)

    # ----- AcoustID lookup -----
    music_id = None
    title = file_path.stem
    artist = "Unknown"
    acoustid_score = None

    try:
        fp_duration, fingerprint = acoustid.fingerprint_file(str(file_path))
        results = acoustid.lookup(ACOUSTID_API_KEY, fingerprint, fp_duration)

        for score, uuid, rec_title, rec_artist in acoustid.parse_lookup_result(results):
            music_id = uuid
            title = rec_title or title
            artist = rec_artist or artist
            acoustid_score = score
            break

    except acoustid.AcoustidError as e:
        print(f"AcoustID error for {file_path.name}: {e}")
        music_id = file_path.stem

    # ----- MusicBrainz enrichment -----
    mb_data = {}
    if music_id and music_id != file_path.stem:
        mb_data = fetch_musicbrainz_metadata(music_id)

        # Respect MB rate limiting (~1 request/sec recommended)
        time.sleep(1)

    final_artist = mb_data.get("mb_artist") or artist
    final_genres = mb_data.get("genres", [])

    write_id3_tags(
        file_path=file_path,
        title=title,
        artist=final_artist,
        album=mb_data.get("album"),
        genres=final_genres,
        release_date=mb_data.get("release_date"),
    )

    doc = {
        "_id": str(file_path.resolve()),
        "music_id": music_id,
        "music_file": str(file_path.resolve()),
        "title": title,
        "title_lower": normalise_string(title),
        "artist": final_artist,
        "artist_lower": normalise_string(final_artist),
        "album": mb_data.get("album"),
        "album_lower": normalise_string(mb_data["album"])
        if mb_data.get("album")
        else None,
        "genres": final_genres,
        "genres_lower": [normalise_string(g) for g in final_genres],
        "release_date": mb_data.get("release_date"),
        "audio_features": {"duration_seconds": duration, "bitrate_kbps": bitrate},
        "sources": {"acoustid_score": acoustid_score, "musicbrainz_id": music_id},
        "date_added": datetime.now(timezone.utc),
        "notes": "Auto-indexed",
    }

    # ----- Upsert -----
    collection.update_one({"_id": str(file_path.resolve())}, {"$set": doc}, upsert=True)
    print(f"Indexed {file_path.name}")


# ==== INDEX ALL FILES ====
def index_all():
    for file_path in DOWNLOAD_DIR.glob("*.mp3"):
        process_file(file_path)


if __name__ == "__main__":
    index_all()
    print("Indexing complete.")
