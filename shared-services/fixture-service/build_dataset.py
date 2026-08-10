"""Build `books.json.gz` for fixture-service from Open Library.

Writes `test-data/books.json.gz`, a **build artefact**: gitignored, mounted into
the service for local runs, and uploaded to S3 by `terraform/aws/s3.tf` for the
Kubernetes and Lambda paths. The service itself never touches Open Library -- it
reads the mount, or downloads the object once on boot.

Build it once and keep it: a 5k run is on the order of an hour against a flaky
upstream. `--resume` extends an existing file rather than starting over.

**Source and licence.** Open Library's bibliographic metadata is dedicated to the
public domain under **CC0 1.0**, so it can be redistributed here without
restriction. Attribution is not required but is recorded anyway: the data comes
from <https://openlibrary.org/search.json> and the bulk equivalents documented at
<https://openlibrary.org/developers/dumps>. Records are real -- real titles,
authors, publication years, publishers, ISBNs and page counts.

Why the Search API rather than the monthly bulk dumps: the dumps are the right
tool for millions of records (`ol_dump_works` alone is ~2.9 GB compressed), which
is far past what a fixture set needs. A few thousand records over the API is a
couple of MB gzipped.

Goodreads is not an option: Amazon retired its public API in December 2020 and
stopped issuing keys, so there is no legitimate programmatic source there.

Usage:

    uv run --no-project shared-services/fixture-service/build_dataset.py
    uv run --no-project shared-services/fixture-service/build_dataset.py --target 20000

Open Library returns intermittent 503s under load, so every request is retried
with backoff and requests are paced to stay a polite client.
"""

import argparse
import gzip
import json
import logging
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

SEARCH_URL = "https://openlibrary.org/search.json"
USER_AGENT = (
    "orchestration-bakeoff/1.0 (workflow-orchestrator evaluation fixtures; "
    "+https://openlibrary.org/developers)"
)
FIELDS = ",".join(
    [
        "key",
        "title",
        "author_name",
        "author_key",
        "first_publish_year",
        "edition_count",
        "publisher",
        "isbn",
        "subject",
        "number_of_pages_median",
        "ratings_average",
        "ratings_count",
        "language",
    ]
)

# A broad subject spread, so the corpus isn't all one genre. Paginated over.
SUBJECTS = [
    "fiction", "science_fiction", "fantasy", "mystery", "historical_fiction",
    "biography", "poetry", "philosophy", "science", "mathematics",
    "computer_science", "programming", "economics", "psychology", "art",
    "music", "cooking", "travel", "medicine", "law",
    "education", "religion", "sports", "nature", "architecture",
    "drama", "humor", "adventure", "romance", "thriller",
    "war", "politics", "business", "engineering", "physics",
    "chemistry", "biology", "astronomy", "geography", "linguistics",
]

PAGE_SIZE = 100
MAX_SUBJECTS_PER_PAGE = 8
MAX_ISBNS = 3
MAX_PUBLISHERS = 3


def fetch(subject: str, page: int, attempts: int = 5) -> list[dict]:
    """One page of works for a subject, retrying Open Library's frequent 503s."""
    params = urllib.parse.urlencode(
        {
            "q": f"subject:{subject}",
            "page": page,
            "limit": PAGE_SIZE,
            "fields": FIELDS,
            "sort": "editions",
        }
    )
    request = urllib.request.Request(
        f"{SEARCH_URL}?{params}", headers={"User-Agent": USER_AGENT}
    )
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read()).get("docs", [])
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            wait = 2 * attempt
            logger.warning(
                "%s p%d attempt %d/%d failed (%s); retrying in %ds",
                subject, page, attempt, attempts, exc, wait,
            )
            time.sleep(wait)
    logger.error("giving up on %s page %d", subject, page)
    return []


def pick_isbn13(isbns: list[str]) -> str | None:
    for value in isbns:
        cleaned = value.replace("-", "").strip()
        if len(cleaned) == 13 and cleaned.isdigit():
            return cleaned
    return None


def normalize(doc: dict, subject_hint: str) -> dict | None:
    """Open Library's search doc -> this service's book record."""
    key = (doc.get("key") or "").rsplit("/", 1)[-1]
    title = doc.get("title")
    if not key or not title:
        return None

    names = doc.get("author_name") or []
    keys = doc.get("author_key") or []
    authors = [
        {"id": keys[i] if i < len(keys) else f"{key}A{i}", "name": name}
        for i, name in enumerate(names)
    ]
    if not authors:
        return None

    subjects = [s.lower() for s in (doc.get("subject") or [])][:MAX_SUBJECTS_PER_PAGE]
    if not subjects:
        subjects = [subject_hint.replace("_", " ")]

    isbns = doc.get("isbn") or []
    return {
        "id": key,
        "title": title,
        "authors": authors,
        "first_publish_year": doc.get("first_publish_year"),
        "publishers": (doc.get("publisher") or [])[:MAX_PUBLISHERS],
        "isbn_13": pick_isbn13(isbns),
        "edition_count": doc.get("edition_count") or 1,
        "languages": doc.get("language") or [],
        "subjects": subjects,
        "page_count": doc.get("number_of_pages_median"),
        "average_rating": (
            round(doc["ratings_average"], 2) if doc.get("ratings_average") else None
        ),
        "ratings_count": doc.get("ratings_count") or 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--target", type=int, default=5000, help="records to collect")
    parser.add_argument(
        "--pace", type=float, default=1.0, help="seconds to wait between requests"
    )
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        # test-data/, so the one compose mount (/data) serves this and DAG 1's ZIP.
        # Gitignored: it is a build artefact, uploaded to S3 for deployed runs.
        default=pathlib.Path(__file__).parents[2] / "test-data" / "books.json.gz",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="start from scratch instead of extending an existing dataset",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    books: dict[str, dict] = {}
    # Resume support: a full build is ~an hour of flaky upstream requests, so
    # never throw away what has already been collected.
    if args.out.is_file() and args.resume:
        with gzip.open(args.out, "rt", encoding="utf-8") as handle:
            books = {b["id"]: b for b in json.load(handle)}
        logger.info("resuming from %d existing records in %s", len(books), args.out)

    page = 1
    # Round-robin the subjects a page at a time so an early subject with deep
    # results can't dominate the corpus.
    while len(books) < args.target and page <= 20:
        for subject in SUBJECTS:
            if len(books) >= args.target:
                break
            for doc in fetch(subject, page):
                record = normalize(doc, subject)
                if record and record["id"] not in books:
                    books[record["id"]] = record
            logger.info("page %d / %s: %d records", page, subject, len(books))
            # Checkpoint after every page. Writing only at the end would mean a
            # crash at record 4,999 loses the whole build.
            write(books, args.out)
            time.sleep(args.pace)
        page += 1

    size_mb = args.out.stat().st_size / 1_048_576
    logger.info("wrote %d books to %s (%.2f MB gzipped)", len(books), args.out, size_mb)


def write(books: dict[str, dict], out: pathlib.Path) -> None:
    """Write the corpus, sorted by id so the artefact is byte-stable.

    Writes to a temp file and renames, so an interrupted checkpoint cannot leave
    a truncated dataset behind.
    """
    tmp = out.with_suffix(out.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as handle:
        json.dump([books[k] for k in sorted(books)], handle, separators=(",", ":"))
    tmp.replace(out)


if __name__ == "__main__":
    main()
