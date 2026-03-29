#!/usr/bin/env python3
"""Download DailyMed bulk SPL zip files for local processing.

DailyMed (NLM) publishes complete monthly snapshots of all human drug labels
as ZIP archives. This script downloads them so build_osmotic_index.py can
process them offline — no per-request API calls needed.

Typical total download size: ~20 GB  (4 Rx parts + 2 OTC parts).
Files are saved outside your project folder by default to avoid OneDrive sync.

Usage
-----
    python scripts/download_spl_zips.py
    python scripts/download_spl_zips.py --dest D:/DailyMed/spl_zips
    python scripts/download_spl_zips.py --rx-only
    python scripts/download_spl_zips.py --otc-only

Files already downloaded at the correct size are skipped automatically.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# DailyMed bulk-download URLs.
# Source: https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm
# NLM updates these files monthly. If a URL returns 404, check the page above.
# Note: files are served from dailymed-data.nlm.nih.gov, not dailymed.nlm.nih.gov.
# ---------------------------------------------------------------------------
_BASE = "https://dailymed-data.nlm.nih.gov/public-release-files"

RX_URLS = [
    f"{_BASE}/dm_spl_release_human_rx_part1.zip",
    f"{_BASE}/dm_spl_release_human_rx_part2.zip",
    f"{_BASE}/dm_spl_release_human_rx_part3.zip",
    f"{_BASE}/dm_spl_release_human_rx_part4.zip",
    f"{_BASE}/dm_spl_release_human_rx_part5.zip",
    f"{_BASE}/dm_spl_release_human_rx_part6.zip",
]

OTC_URLS = [
    f"{_BASE}/dm_spl_release_human_otc_part1.zip",
    f"{_BASE}/dm_spl_release_human_otc_part2.zip",
    f"{_BASE}/dm_spl_release_human_otc_part3.zip",
    f"{_BASE}/dm_spl_release_human_otc_part4.zip",
    f"{_BASE}/dm_spl_release_human_otc_part5.zip",
    f"{_BASE}/dm_spl_release_human_otc_part6.zip",
    f"{_BASE}/dm_spl_release_human_otc_part7.zip",
    f"{_BASE}/dm_spl_release_human_otc_part8.zip",
    f"{_BASE}/dm_spl_release_human_otc_part9.zip",
    f"{_BASE}/dm_spl_release_human_otc_part10.zip",
    f"{_BASE}/dm_spl_release_human_otc_part11.zip",
]

_DEFAULT_DEST = Path.home() / ".excipient_finder" / "spl_zips"


def _fmt_bytes(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1e9:.1f} GB"
    if n >= 1_000_000:
        return f"{n / 1e6:.0f} MB"
    return f"{n / 1e3:.0f} KB"


def download_file(url: str, dest_dir: Path) -> None:
    """Download a single file with progress reporting. Skips if already complete."""
    try:
        import httpx
    except ImportError:
        print("ERROR: httpx is required. Install it with: pip install httpx")
        sys.exit(1)

    filename = url.rsplit("/", 1)[-1]
    dest_path = dest_dir / filename

    with httpx.Client(timeout=None, follow_redirects=True) as client:
        head = client.head(url)
        if head.status_code == 404:
            print(f"  SKIP (404 — URL may have changed): {filename}")
            return
        remote_size = int(head.headers.get("content-length", 0))

        if dest_path.exists():
            local_size = dest_path.stat().st_size
            # Reject files that look like HTML error pages (< 1 MB for a supposed zip).
            if local_size < 1_000_000:
                print(f"  Removing bad download ({_fmt_bytes(local_size)}, expected zip): {filename}")
                dest_path.unlink()
            elif remote_size and local_size == remote_size:
                print(f"  Already complete ({_fmt_bytes(remote_size)}): {filename}")
                return
            else:
                print(f"  Resuming (local {_fmt_bytes(local_size)}, remote {_fmt_bytes(remote_size)}): {filename}")
        else:
            size_str = f" ({_fmt_bytes(remote_size)})" if remote_size else ""
            print(f"  Downloading{size_str}: {filename}")

        downloaded = dest_path.stat().st_size if dest_path.exists() else 0
        headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}

        with (
            client.stream("GET", url, headers=headers) as response,
            open(dest_path, "ab" if downloaded else "wb") as f,
        ):
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if remote_size:
                    pct = downloaded / remote_size * 100
                    print(f"\r    {pct:5.1f}%  {_fmt_bytes(downloaded)} / {_fmt_bytes(remote_size)}", end="", flush=True)
                else:
                    print(f"\r    {_fmt_bytes(downloaded)}", end="", flush=True)

    print(f"\r    Done: {dest_path}          ")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download DailyMed bulk SPL zip files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=_DEFAULT_DEST,
        metavar="PATH",
        help=f"Directory to save zip files (default: {_DEFAULT_DEST})",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--rx-only", action="store_true", help="Download only prescription drug labels")
    group.add_argument("--otc-only", action="store_true", help="Download only OTC drug labels")
    args = parser.parse_args()

    urls = RX_URLS + OTC_URLS
    if args.rx_only:
        urls = RX_URLS
    elif args.otc_only:
        urls = OTC_URLS

    args.dest.mkdir(parents=True, exist_ok=True)
    print(f"Destination : {args.dest}")
    print(f"Files       : {len(urls)}")
    print(f"Approx size : ~{len(urls) * 2}-{len(urls) * 3} GB")
    print()

    for url in urls:
        try:
            download_file(url, args.dest)
        except KeyboardInterrupt:
            print("\nInterrupted. Re-run to resume.")
            sys.exit(1)
        except Exception as exc:
            print(f"\n  ERROR: {exc}")

    print("\nAll downloads complete.")
    print(f"Next step: python scripts/build_osmotic_index.py --zips {args.dest}")


if __name__ == "__main__":
    main()
