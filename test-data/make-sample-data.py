"""Generate `sample-data.zip`, DAG 1's input archive.

The archive is a build artefact, not source: it is gitignored, generated here for
local runs, and uploaded to S3 by `terraform/aws/s3.tf` for the Kubernetes and
Lambda paths (where there is no host bind mount). Keeping the generator in git
rather than the binary means the fixture is diffable and reviewable.

**Byte-stability matters.** Terraform tracks the object with `filemd5()`, and
`fixture-service` may cache it by ETag, so a rebuild must not produce a different
archive from identical inputs. ZIP entries therefore carry a pinned timestamp and
fixed compression — without that, `zipfile` stamps the current mtime and every
regeneration would look like a change.

**Do not change the columns casually.** All eleven DAG 1 implementations join
`orders`/`customers`/`products` on these exact column names, and several assert
the 10-row result. Adding rows or columns means updating every implementation's
SQL transform.

    uv run --no-project test-data/make-sample-data.py
"""

import argparse
import logging
import pathlib
import zipfile

logger = logging.getLogger(__name__)

# Pinned so the archive is byte-identical across rebuilds (see docstring).
ZIP_TIMESTAMP = (2026, 1, 1, 0, 0, 0)

CSVS = {
    "customers.csv": """customer_id,customer_name,email
C001,Alice Johnson,alice@example.com
C002,Bob Smith,bob@example.com
C003,Carol Davis,carol@example.com
C004,Dan Wilson,dan@example.com
C005,Eve Martinez,eve@example.com
""",
    "products.csv": """product_id,product_name,category,price
P001,Widget Alpha,Widgets,19.99
P002,Widget Beta,Widgets,29.99
P003,Gadget Gamma,Gadgets,49.99
P004,Gadget Delta,Gadgets,99.99
P005,Doohickey Epsilon,Doohickeys,9.99
""",
    "orders.csv": """order_id,customer_id,product_id,quantity,order_date
O001,C001,P001,2,2026-06-01
O002,C001,P003,1,2026-06-02
O003,C002,P002,5,2026-06-03
O004,C003,P005,10,2026-06-04
O005,C004,P004,1,2026-06-05
O006,C005,P001,3,2026-06-06
O007,C002,P003,2,2026-06-07
O008,C003,P002,1,2026-06-08
O009,C004,P005,4,2026-06-09
O010,C005,P004,2,2026-06-10
""",
}


def build(out: pathlib.Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        # Sorted, so entry order is stable too.
        for name in sorted(CSVS):
            info = zipfile.ZipInfo(filename=name, date_time=ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            # CRLF, matching both RFC 4180 and the archive this replaced. The
            # literals above are LF for readability in this file.
            archive.writestr(info, CSVS[name].replace("\n", "\r\n"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path(__file__).parent / "sample-data.zip",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build(args.out)
    logger.info(
        "wrote %s (%d bytes, %d CSVs)", args.out, args.out.stat().st_size, len(CSVS)
    )


if __name__ == "__main__":
    main()
