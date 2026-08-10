"""Fixture Service -- a mock Books API, plus DAG 1's input archive.

Backed by **real Open Library metadata** (~5k works), whose bibliographic data is
dedicated to the public domain under **CC0 1.0** and so can be redistributed here
freely. Field names and identifier formats mirror Open Library's own
(`OL...W` work keys, `OL...A` author keys, `first_publish_year`, `edition_count`),
so the fixture looks like the real service it stands in for. Refresh the dataset
with `build_dataset.py`; the service itself never touches the network.

Goodreads is deliberately not the model: Amazon retired its public API in
December 2020, so nothing can legitimately be built against it today.

Why this service exists at all:

* **DAG 2 (every tool).** The spec only asks for "items, each with its own detail
  URL". Implementations reached for `api.github.com/orgs/<org>/repos` because it
  happens to have that shape, but unauthenticated GitHub allows 60 requests/hour
  per IP and a default run costs 1 + 30 -- two runs exhaust the budget for every
  orchestrator at once, and the resulting 403s look like flow bugs.
* **DAG 1 (Kestra especially).** Kestra's `dag1_csv_etl` takes a `zip_url` input
  and downloads it, where Prefect/Airflow read a local path. A Kestra script task
  runs in its own throwaway container, so the ZIP must be fetchable over HTTP.

**Pagination is GitHub-style on purpose**: `/books` returns a *bare JSON array*
with `X-Total-Count` and `Link` headers, rather than an envelope. DAG 2's
normalize step in several orchestrators tests `isinstance(body, list)`, so an
envelope would silently yield zero items. `/search.json` is also provided for
callers that want Open Library's real `{numFound, start, docs}` shape.

Detail URLs are built from the requesting URL, so a caller inside compose gets
`fixture-service:8099` links and a host caller gets `localhost:8099` ones with no
configuration. Override with `?base=` or `FIXTURE_BASE_URL` when the caller that
fetches the collection and the caller that fetches the details sit on different
networks.
"""

import gzip
import json
import logging
import pathlib
import tempfile
import urllib.request
from collections import Counter
from contextlib import asynccontextmanager
from urllib.parse import quote

import boto3
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Resolve both data files on boot, downloading them if not bind-mounted."""
    _load_corpus()
    _resolve(ZIP_NAME, settings.fixture_sample_zip_url, "DAG 1 archive")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Fixture Books API",
    description=(
        "Mock bibliographic API backing DAG 2, plus DAG 1's input archive. "
        "Data: Open Library (CC0 1.0)."
    ),
)

# Both data files are build artefacts: never committed, never baked into the image.
# Each is resolved at startup as
#   1. the compose bind mount  (local dev, from test-data/)
#   2. the download cache      (deployed: pulled from S3 on boot)
# The corpus is *essential* -- without it /books and /health report 503 rather than
# pretending an empty library is healthy. DAG 1's ZIP is *optional*: DAG 2 works
# fine without it, so a missing archive only fails /sample-data.zip.
MOUNTED_DIR = pathlib.Path("/data")
ZIP_NAME = "sample-data.zip"
CORPUS_NAME = "books.json.gz"


class Settings(BaseSettings):
    """Service configuration, sourced from environment variables."""

    # Overrides the base URL used to build detail links. Leave unset to derive it
    # per request, which is correct whenever the caller that fetches the
    # collection and the caller that fetches the details share a network.
    fixture_base_url: str | None = None
    # DAG 2's fan-out width when the caller doesn't pass ?per_page.
    fixture_default_per_page: int = 5
    fixture_max_per_page: int = 500
    # Explicit corpus path. Normally unset: the mount / cache / download order
    # below resolves it. Set to pin a specific file.
    fixture_dataset: pathlib.Path | None = None
    # Where to fetch each artefact when it is not bind-mounted. Accepts an
    # `s3://bucket/key` URI (needs credentials -- the bucket blocks public access)
    # or a plain https URL. Terraform seeds both objects; see terraform/aws/s3.tf.
    fixture_sample_zip_url: str | None = None
    fixture_books_url: str | None = None
    # Where the downloaded archive is cached. Defaults under the system temp dir so
    # it works whether or not the process runs as root; point it at a volume to
    # persist across restarts.
    fixture_cache_dir: pathlib.Path = pathlib.Path(tempfile.gettempdir()) / "fixture"
    aws_region: str | None = None

    model_config = SettingsConfigDict(case_sensitive=False)


settings = Settings()


class Author(BaseModel):
    id: str
    name: str


class BookSummary(BaseModel):
    """Collection-endpoint shape. `url` is what DAG 2's fan-out fetches."""

    id: str
    title: str
    authors: list[Author]
    first_publish_year: int | None = None
    subjects: list[str] = []
    url: str


class Book(BookSummary):
    """Detail-endpoint shape: the summary plus the full record."""

    publishers: list[str] = []
    isbn_13: str | None = None
    edition_count: int = 1
    languages: list[str] = []
    page_count: int | None = None
    average_rating: float | None = None
    ratings_count: int = 0


class SubjectFacet(BaseModel):
    subject: str
    book_count: int


class SearchResponse(BaseModel):
    """Open Library's own envelope, for callers that prefer it to a bare array."""

    # camelCase deliberately: this mirrors Open Library's field name exactly.
    numFound: int
    start: int
    docs: list[BookSummary]


# Populated by the lifespan hook, because the corpus may have to be downloaded
# first. Empty means unprovisioned, which /health and /books report as 503 rather
# than quietly serving an empty library.
BOOKS: list[dict] = []
BOOKS_BY_ID: dict[str, dict] = {}
AUTHOR_NAMES: dict[str, str] = {}
SUBJECT_COUNTS: Counter = Counter()
CORPUS_PATH: pathlib.Path | None = None


def _load_corpus() -> None:
    """Resolve and index the corpus. Leaves it empty if unavailable."""
    global BOOKS, BOOKS_BY_ID, AUTHOR_NAMES, SUBJECT_COUNTS, CORPUS_PATH

    path = settings.fixture_dataset
    if path is not None and not path.is_file():
        logger.error("FIXTURE_DATASET=%s does not exist", path)
        path = None
    if path is None:
        path = _resolve(CORPUS_NAME, settings.fixture_books_url, "book corpus")
    if path is None:
        logger.error(
            "no book corpus: mount %s, or set FIXTURE_BOOKS_URL. "
            "Build one with build_dataset.py.",
            MOUNTED_DIR / CORPUS_NAME,
        )
        return

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        BOOKS = json.load(handle)
    CORPUS_PATH = path
    BOOKS_BY_ID = {b["id"]: b for b in BOOKS}
    # Author id -> name, built once. Authors recur across works, so this is much
    # smaller than the corpus and makes /authors/{id} a dict lookup.
    AUTHOR_NAMES = {a["id"]: a["name"] for b in BOOKS for a in b["authors"]}
    SUBJECT_COUNTS = Counter(s for b in BOOKS for s in b["subjects"])
    logger.info("loaded %d books from %s", len(BOOKS), path)


def _require_corpus() -> None:
    if not BOOKS:
        raise HTTPException(
            503,
            "book corpus not provisioned. Locally: build it with "
            "shared-services/fixture-service/build_dataset.py (compose mounts "
            f"test-data/ at {MOUNTED_DIR}). Deployed: set FIXTURE_BOOKS_URL to the "
            "S3 object (terraform output fixture_books_url).",
        )


def _resolve(name: str, url: str | None, label: str) -> pathlib.Path | None:
    """Locate an artefact, downloading it into the cache if necessary.

    Best-effort by design: every failure path is logged and reported by /health
    rather than raised. This runs from the lifespan hook, where an escaping
    exception aborts startup entirely.
    """
    mounted = MOUNTED_DIR / name
    if mounted.is_file():
        return mounted

    cached = settings.fixture_cache_dir / name
    if cached.is_file():
        return cached
    if not url:
        return None

    try:
        # Inside the try: an unwritable cache directory must degrade, not crash.
        cached.parent.mkdir(parents=True, exist_ok=True)
        if url.startswith("s3://"):
            bucket, _, key = url[len("s3://") :].partition("/")
            boto3.client("s3", region_name=settings.aws_region).download_file(
                bucket, key, str(cached)
            )
        else:
            with urllib.request.urlopen(url, timeout=120) as response:
                cached.write_bytes(response.read())
        logger.info("downloaded %s from %s to %s", label, url, cached)
        return cached
    except Exception:
        # .unlink() so a partial file is never mistaken for a good one.
        cached.unlink(missing_ok=True)
        logger.exception("could not fetch %s from %s", label, url)
        return None


def _zip_path() -> pathlib.Path | None:
    """DAG 1's archive, if it was resolved at startup."""
    for candidate in (MOUNTED_DIR / ZIP_NAME, settings.fixture_cache_dir / ZIP_NAME):
        if candidate.is_file():
            return candidate
    return None


def _base_url(request: Request, base: str | None) -> str:
    if base:
        return base.rstrip("/")
    if settings.fixture_base_url:
        return settings.fixture_base_url.rstrip("/")
    return str(request.base_url).rstrip("/")


def _summary(book: dict, base: str) -> BookSummary:
    return BookSummary(
        id=book["id"],
        title=book["title"],
        authors=[Author(**a) for a in book["authors"]],
        first_publish_year=book.get("first_publish_year"),
        subjects=book.get("subjects", []),
        url=f"{base}/books/{book['id']}",
    )


def _matches(
    book: dict,
    subject: str | None,
    author: str | None,
    q: str | None,
    year_from: int | None,
    year_to: int | None,
) -> bool:
    if subject and subject.lower() not in book.get("subjects", []):
        return False
    if author and not any(
        author.lower() in a["name"].lower() for a in book["authors"]
    ):
        return False
    if q and q.lower() not in book["title"].lower():
        return False
    year = book.get("first_publish_year")
    if year_from is not None and (year is None or year < year_from):
        return False
    if year_to is not None and (year is None or year > year_to):
        return False
    return True


def _filtered(
    subject: str | None,
    author: str | None,
    q: str | None,
    year_from: int | None,
    year_to: int | None,
) -> list[dict]:
    if not any([subject, author, q, year_from, year_to]):
        return BOOKS
    return [
        b for b in BOOKS if _matches(b, subject, author, q, year_from, year_to)
    ]


def _link_header(base: str, page: int, per_page: int, total: int, extra: str) -> str:
    """GitHub-style Link header, so pagination is discoverable from the response."""
    last = max(1, -(-total // per_page))
    parts = []
    if page < last:
        parts.append(f'<{base}/books?page={page + 1}&per_page={per_page}{extra}>; rel="next"')
    if page > 1:
        parts.append(f'<{base}/books?page={page - 1}&per_page={per_page}{extra}>; rel="prev"')
    parts.append(f'<{base}/books?page=1&per_page={per_page}{extra}>; rel="first"')
    parts.append(f'<{base}/books?page={last}&per_page={per_page}{extra}>; rel="last"')
    return ", ".join(parts)


@app.get("/books")
async def list_books(
    request: Request,
    response: Response,
    page: int = Query(1, ge=1),
    per_page: int | None = Query(None, ge=1),
    subject: str | None = None,
    author: str | None = None,
    q: str | None = Query(None, description="case-insensitive title substring"),
    year_from: int | None = None,
    year_to: int | None = None,
    base: str | None = Query(None, description="override the base URL of detail links"),
) -> list[BookSummary]:
    """DAG 2's initial fetch: a page of book summaries, each with a detail URL.

    Returns a bare array with `X-Total-Count` / `Link` headers rather than an
    envelope -- see the module docstring.
    """
    _require_corpus()
    size = per_page or settings.fixture_default_per_page
    if size > settings.fixture_max_per_page:
        raise HTTPException(422, f"per_page exceeds {settings.fixture_max_per_page}")

    resolved = _base_url(request, base)
    matched = _filtered(subject, author, q, year_from, year_to)
    start = (page - 1) * size
    window = matched[start : start + size]

    # Percent-encode: these values are echoed into the Link header, and HTTP
    # headers are latin-1, so a non-ASCII title search (the corpus is
    # multilingual) would raise UnicodeEncodeError on the way out.
    extra = "".join(
        f"&{k}={quote(str(v), safe='')}"
        for k, v in (
            ("subject", subject),
            ("author", author),
            ("q", q),
            ("year_from", year_from),
            ("year_to", year_to),
        )
        if v is not None
    )
    response.headers["X-Total-Count"] = str(len(matched))
    response.headers["Link"] = _link_header(resolved, page, size, len(matched), extra)
    return [_summary(b, resolved) for b in window]


@app.get("/search.json")
async def search(
    request: Request,
    q: str | None = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1),
    base: str | None = None,
) -> SearchResponse:
    """Open Library-shaped search, for callers that want the `docs` envelope."""
    _require_corpus()
    if limit > settings.fixture_max_per_page:
        raise HTTPException(422, f"limit exceeds {settings.fixture_max_per_page}")
    resolved = _base_url(request, base)
    matched = _filtered(None, None, q, None, None)
    start = (page - 1) * limit
    return SearchResponse(
        numFound=len(matched),
        start=start,
        docs=[_summary(b, resolved) for b in matched[start : start + limit]],
    )


@app.get("/books/{book_id}")
async def get_book(request: Request, book_id: str, base: str | None = None) -> Book:
    """DAG 2's per-item detail fetch: the full bibliographic record."""
    _require_corpus()
    book = BOOKS_BY_ID.get(book_id)
    if book is None:
        raise HTTPException(404, f"no book with id {book_id}")
    resolved = _base_url(request, base)
    return Book(**{**book, "url": f"{resolved}/books/{book['id']}"})


@app.get("/subjects")
async def list_subjects(limit: int = Query(50, ge=1)) -> list[SubjectFacet]:
    """Subject facets, most common first -- useful for `/books?subject=`."""
    _require_corpus()
    return [
        SubjectFacet(subject=s, book_count=n)
        for s, n in SUBJECT_COUNTS.most_common(limit)
    ]


@app.get("/authors/{author_id}")
async def get_author(request: Request, author_id: str, base: str | None = None) -> dict:
    """An author plus their works -- a second fan-out shape if a DAG wants one."""
    _require_corpus()
    name = AUTHOR_NAMES.get(author_id)
    if name is None:
        raise HTTPException(404, f"no author with id {author_id}")
    resolved = _base_url(request, base)
    written = [
        _summary(b, resolved)
        for b in BOOKS
        if any(a["id"] == author_id for a in b["authors"])
    ]
    return {"id": author_id, "name": name, "book_count": len(written), "books": written}


@app.get("/sample-data.zip")
async def sample_data() -> FileResponse:
    """DAG 1's input archive: the `test-data/` mount if present, else bundled."""
    path = _zip_path()
    if path is None:
        raise HTTPException(
            500,
            f"sample-data.zip not found at {MOUNTED_DIR / ZIP_NAME} or "
            f"{settings.fixture_cache_dir / ZIP_NAME}. "
            "Locally: run test-data/make-sample-data.py (compose mounts test-data/). "
            "Deployed: set FIXTURE_SAMPLE_ZIP_URL to the S3 object "
            "(terraform output dag1_bucket / dag1_sample_zip_key).",
        )
    return FileResponse(path, media_type="application/zip")


@app.get("/health")
async def health(response: Response) -> dict:
    # 503 when the corpus is missing, so a Kubernetes readiness probe fails loudly
    # instead of the service quietly serving an empty library.
    ok = bool(BOOKS)
    response.status_code = 200 if ok else 503
    return {
        "status": "ok" if ok else "no-corpus",
        "book_count": len(BOOKS),
        "corpus_source": str(CORPUS_PATH) if CORPUS_PATH else None,
        "author_count": len(AUTHOR_NAMES),
        "subject_count": len(SUBJECT_COUNTS),
        "default_per_page": settings.fixture_default_per_page,
        "sample_data_present": _zip_path() is not None,
        "sample_data_source": str(_zip_path()) if _zip_path() else None,
        "data_source": "Open Library (CC0 1.0)",
    }
