import hashlib
import json
import logging
import os
import re
import sys
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, TypedDict

import httpx
import humanize
import typer
import yaml
from fastavro import reader as avro_reader
from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
if os.environ.get("ARCHIVE_HTTPX_LOG_LEVEL", "").upper() not in {"INFO", "DEBUG"}:
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_all_filenames_with_checksums(client: httpx.Client) -> dict[str, str]:
    """
    Download and parse the MD5SUMS file, returning {filename: checksum}.
    Ignores comment lines and strips any path prefix.
    """
    url = "https://ztf.uw.edu/alerts/public/MD5SUMS"

    try:
        response = client.get(url, timeout=30).raise_for_status()
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Failed to fetch MD5SUMS: {exc}")

    result: dict[str, str] = {}
    for line in response.text.splitlines():
        line = line.strip()

        if not line or line.startswith("#"):
            continue

        parts = line.split()

        if len(parts) == 2:
            checksum, filename = parts
            filename = os.path.basename(filename)
            result[filename] = checksum
    return result


def get_all_filenames(client: httpx.Client) -> list[str]:
    """
    Return just the list of filenames from the MD5SUMS file.
    """
    return list(get_all_filenames_with_checksums(client).keys())


def get_file_info(client: httpx.Client, url: str) -> tuple[str, str]:
    """
    Retrieve the Last-Modified and Content-Length headers for a file at the given URL.

    Args:
        client: An httpx.Client instance.
        url: The URL of the file.

    Returns:
        A tuple (last_modified, content_length).

    Raises:
        FileInfoError: If the file info cannot be retrieved.
    """
    try:
        response = client.head(url)

        if response.status_code != 200:
            raise RuntimeError(f"Failed to get file info: HTTP {response.status_code}")

        last_modified = str(response.headers.get("Last-Modified", "Unknown"))
        content_length = str(response.headers.get("Content-Length", "Unknown"))

        return last_modified, content_length
    except httpx.HTTPError as exc:
        raise RuntimeError(f"HTTP error occurred: {exc}") from exc


def download_file(
    client: httpx.Client,
    url: str,
    output_path: Path,
    total_size: int | None = None,
) -> None:
    """
    Download a file from the given URL to the specified output path, with tqdm progress bar.

    Args:
        client: An httpx.Client instance.
        url: The URL of the file.
        output_path: The local file path to save the file.
        total_size: The total size of the file in bytes (if known).

    Raises:
        DownloadError: If the download fails.
    """
    try:
        with client.stream("GET", url) as response:
            if response.status_code != 200:
                raise RuntimeError(
                    f"Failed to download file: HTTP {response.status_code}"
                )
            with (
                open(output_path, "wb") as f,
                tqdm(
                    total=total_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=output_path.name,
                    leave=False,
                ) as pbar,
            ):
                for chunk in response.iter_bytes():
                    if not chunk:
                        continue
                    _ = f.write(chunk)
                    _ = pbar.update(len(chunk))
    except httpx.HTTPError as exc:
        raise RuntimeError(f"HTTP error occurred: {exc}") from exc
    except OSError as exc:
        raise OSError(f"File write error: {exc}") from exc


app = typer.Typer(
    help="Download ZTF public alert files by date.", rich_markup_mode=None
)


@app.command()
def list_files(
    proxy: Annotated[
        str | None,
        typer.Option(
            help="SOCKS/HTTP proxy URL (single URL, e.g., socks5://localhost:1080)",
            envvar="ARCHIVE_DOWNLOAD_PROXY",
        ),
    ] = None,
) -> None:
    """
    List all available files in the archive.
    """
    with httpx.Client(proxy=proxy or None) as client:
        filenames = get_all_filenames(client)
        for name in filenames:
            typer.echo(name)


def download_files_batch(
    client: httpx.Client,
    filenames: list[str],
    output_dir: Path,
    overwrite: bool = False,
    delay: float = 0.0,
    desc: str = "Files",
) -> None:
    """
    Download a batch of files, skipping those already downloaded unless overwrite is True.
    """
    output_dir.mkdir(exist_ok=True)

    for filename in tqdm(filenames, desc=desc, unit="file"):
        url = f"https://ztf.uw.edu/alerts/public/{filename}"
        output_path = output_dir / filename

        try:
            _, content_length = get_file_info(client, url)
            expected_size = int(content_length) if content_length.isdigit() else None
        except Exception as exc:
            logger.warning(f"Skipping {filename}: {exc}")
            continue

        if output_path.exists() and not overwrite:
            file_size = output_path.stat().st_size
            if expected_size is not None and file_size == expected_size:
                logger.info(f"Skipping {filename}: already downloaded.")
                continue
            logger.info(f"Mismatched file found for {filename}, re-downloading.")

        typer.echo(f"Downloading {filename} ...")

        try:
            download_file(client, url, output_path, total_size=expected_size)
            typer.echo(f"Downloaded {filename}")
        except Exception as exc:
            logger.error(f"Failed to download {filename}: {exc}")
            continue

        if delay > 0:
            time.sleep(delay)


@app.command()
def download_all(
    delay: Annotated[
        float, typer.Argument(help="Delay (seconds) between downloads")
    ] = 5.0,
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite/--no-overwrite",
            help="Overwrite existing files",
            is_flag=True,
        ),
    ] = True,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-d",
            help="Directory containing downloaded files to validate (full check)",
            envvar="ARCHIVE_DOWNLOAD_DIR",
        ),
    ] = None,
    proxy: Annotated[
        str | None,
        typer.Option(
            help="SOCKS/HTTP proxy URL (e.g., socks5://localhost:1080)",
            envvar="ARCHIVE_DOWNLOAD_PROXY",
        ),
    ] = None,
) -> None:
    """
    Download all files into a directory specified by ARCHIVE_DOWNLOAD_DIR.
    Skips files that already exist and match expected size.
    """
    if not output_dir:
        logger.error("ARCHIVE_DOWNLOAD_DIR environment variable not set.")
        raise typer.Exit(code=1)

    proxy_url = proxy if proxy else None

    with httpx.Client(proxy=proxy_url) as client:
        filenames = get_all_filenames(client)
        download_files_batch(
            client,
            filenames,
            output_dir,
            overwrite=overwrite,
            delay=delay,
            desc="Total files",
        )


@app.command()
def download_one(
    name: Annotated[
        str, typer.Argument(help="Date in YYYYMMDD format or full filename")
    ],
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-d",
            help="Directory containing downloaded files to validate (full check)",
            envvar="ARCHIVE_DOWNLOAD_DIR",
        ),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite/--no-overwrite",
            help="Overwrite existing files",
            is_flag=True,
        ),
    ] = True,
    proxy: Annotated[
        str | None,
        typer.Option(
            help="SOCKS/HTTP proxy URL (e.g., socks5://localhost:1080)",
            envvar="ARCHIVE_DOWNLOAD_PROXY",
        ),
    ] = None,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt",
        ),
    ] = False,
) -> None:
    """
    Download a ZTF public alert file by date (YYYYMMDD) or full filename.

    Args:
        name: Date in YYYYMMDD format or full filename (e.g., ztf_public_20240101.tar.gz).
    """
    if not output_dir:
        logger.error("ARCHIVE_DOWNLOAD_DIR environment variable not set.")
        raise typer.Exit(code=1)

    filename = name if name.endswith(".tar.gz") else f"ztf_public_{name}.tar.gz"
    url = f"https://ztf.uw.edu/alerts/public/{filename}"

    with httpx.Client(proxy=proxy or None) as client:
        last_modified, content_length = get_file_info(client, url)
        typer.echo(f"File: {filename}")
        typer.echo(f"Last-Modified: {last_modified}")
        typer.echo(f"Size: {humanize.naturalsize(content_length)}")

        if not yes:
            confirm = typer.confirm("Download this file?")
            if not confirm:
                return

        try:
            download_files_batch(
                client,
                [filename],
                output_dir,
                overwrite=overwrite,
                delay=0.0,
                desc="Single file",
            )
            typer.echo(f"Downloaded {filename} to {output_dir / filename}")
        except Exception as exc:
            logger.error(str(exc))
            raise typer.Exit(code=1)


@app.command()
def download_sample(
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-d",
            help="Directory containing downloaded files to validate (full check)",
            envvar="ARCHIVE_DOWNLOAD_DIR",
        ),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option(
            "--overwrite/--no-overwrite",
            help="Overwrite existing files",
            is_flag=True,
        ),
    ] = True,
    proxy: Annotated[
        str | None,
        typer.Option(
            help="SOCKS/HTTP proxy URL (e.g., socks5://localhost:1080)",
            envvar="ARCHIVE_DOWNLOAD_PROXY",
        ),
    ] = None,
) -> None:
    """
    Download a sample of the archive (one file every 6 months) into the download folder.
    """
    if not output_dir:
        logger.error("ARCHIVE_DOWNLOAD_DIR environment variable not set.")
        raise typer.Exit(code=1)

    with httpx.Client(proxy=proxy or None) as client:
        filenames = get_all_filenames(client)

        date_pattern = re.compile(r"ztf_public_(\d{8})\.tar\.gz")

        dated_files: list[tuple[datetime, str]] = []
        for filename in filenames:
            m = date_pattern.match(filename)
            if m:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
                dated_files.append((dt, filename))

        dated_files.sort()

        selected: list[str] = []
        last_date: datetime | None = None
        for date, filename in dated_files:
            if not last_date or (date - last_date).days >= 180:
                selected.append(filename)
                last_date = date

        if len(selected) == 0:
            logger.error("No files found for sampling.")
            raise typer.Exit(code=1)

        typer.echo(f"Selected {len(selected)} files for sample download.")
        download_files_batch(
            client,
            selected,
            output_dir,
            overwrite=overwrite,
            delay=0.0,
            desc="Sample files",
        )


class Status(str, Enum):
    OK = "OK"
    ERROR = "ERROR"
    NOT_FOUND = "NOT_FOUND"
    INVALID = "INVALID"


yaml.add_representer(
    Status,
    lambda dumper, data: dumper.represent_scalar("tag:yaml.org,2002:str", str(data)),  # pyright: ignore[reportUnknownMemberType]
)


class OutputFormat(str, Enum):
    HUMAN = "human"
    JSON = "json"
    YAML = "yaml"


class MD5Result(TypedDict):
    status: Status
    expected: str | None
    got: str | None
    error: str | None


class ExtractResult(TypedDict):
    status: Status
    error: str | None


class AvroResult(TypedDict):
    file: str
    status: Status
    error: str | None


class FileReport(TypedDict):
    md5: MD5Result
    extract: ExtractResult | None
    avro: list[AvroResult]


def validate_single_file(
    file_path: Path,
    checksums: dict[str, str],
    skip_on_md5_fail: bool,
) -> tuple[str, FileReport]:
    md5_result: MD5Result = {
        "status": Status.NOT_FOUND,
        "expected": None,
        "got": None,
        "error": None,
    }
    extract_result: ExtractResult | None = None
    avro_results: list[AvroResult] = []

    has_checksum = file_path.name in checksums

    if not has_checksum and skip_on_md5_fail:
        return file_path.name, {"md5": md5_result, "extract": None, "avro": []}

    if has_checksum:
        try:
            md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    md5.update(chunk)
            file_md5 = md5.hexdigest()
            if file_md5 == checksums[file_path.name]:
                md5_result["status"] = Status.OK
            else:
                md5_result["status"] = Status.INVALID
                md5_result["expected"] = checksums[file_path.name]
                md5_result["got"] = file_md5
        except Exception as exc:
            md5_result["status"] = Status.ERROR
            md5_result["error"] = str(exc)

    if md5_result["status"] in (Status.OK, Status.NOT_FOUND) or not skip_on_md5_fail:
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(path=tmpdir, filter="data")
                extract_result = {"status": Status.OK, "error": None}
                for avro_file in Path(tmpdir).rglob("*.avro"):
                    avro_entry: AvroResult = {
                        "file": avro_file.name,
                        "status": Status.OK,
                        "error": None,
                    }
                    try:
                        with open(avro_file, "rb") as af:
                            _ = list(avro_reader(af))
                    except Exception as exc:
                        avro_entry["status"] = Status.ERROR
                        avro_entry["error"] = str(exc)
                    avro_results.append(avro_entry)
        except Exception as exc:
            extract_result = {"status": Status.ERROR, "error": str(exc)}

    return file_path.name, {
        "md5": md5_result,
        "extract": extract_result,
        "avro": avro_results,
    }


@app.command()
def validate(
    files: Annotated[
        list[Path] | None,
        typer.Argument(help="Files to validate (reads from stdin if piped)"),
    ] = None,
    output_dir: Annotated[
        Path | None,
        typer.Option(
            "--output-dir",
            "-d",
            help="Directory containing downloaded files to validate",
            envvar="ARCHIVE_DOWNLOAD_DIR",
        ),
    ] = None,
    proxy: Annotated[
        str | None,
        typer.Option(
            "--proxy",
            "-p",
            help="SOCKS/HTTP proxy URL (e.g., socks5://localhost:1080)",
            envvar="ARCHIVE_DOWNLOAD_PROXY",
        ),
    ] = None,
    output_format: Annotated[
        OutputFormat,
        typer.Option(
            "--output-format",
            "-o",
            help="Output format: human, json, or yaml",
            show_choices=True,
            case_sensitive=False,
        ),
    ] = OutputFormat.HUMAN,
    skip_on_md5_fail: Annotated[
        bool,
        typer.Option(
            "--skip-on-md5-fail/--no-skip-on-md5-fail",
            "-x",
            help="Skip extraction and Avro validation when MD5 check fails",
        ),
    ] = False,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            "-w",
            help="Number of parallel validation workers",
        ),
    ] = 4,
) -> None:
    """
    Validate .tar.gz files against MD5SUMS, check extraction, and validate Avro contents.

    Accepts file paths as positional arguments, from stdin (one per line),
    or validates all .tar.gz in --output-dir.
    """
    files_to_check: list[Path] = []

    if files:
        files_to_check = files
    elif not sys.stdin.isatty():
        for line in sys.stdin:
            stripped = line.strip()
            if stripped:
                files_to_check.append(Path(stripped))

    if not files_to_check and output_dir is not None:
        files_to_check = list(output_dir.glob("*.tar.gz"))

    if not files_to_check:
        typer.echo("Provide files as arguments, via stdin, or use --output-dir.")
        raise typer.Exit(code=1)

    with httpx.Client(proxy=proxy or None) as client:
        checksums = get_all_filenames_with_checksums(client)

    reports: dict[str, FileReport] = {}
    use_tqdm = sys.stderr.isatty() and len(files_to_check) > 1

    if workers <= 1 or len(files_to_check) == 1:
        iterator = (
            tqdm(files_to_check, desc="Validating", unit="file", file=sys.stderr)
            if use_tqdm
            else files_to_check
        )
        for fp in iterator:
            name, report = validate_single_file(fp, checksums, skip_on_md5_fail)
            reports[name] = report
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    validate_single_file, fp, checksums, skip_on_md5_fail
                ): fp
                for fp in files_to_check
            }
            with tqdm(
                total=len(futures),
                desc="Validating",
                unit="file",
                file=sys.stderr,
                disable=not use_tqdm,
            ) as pbar:
                for future in as_completed(futures):
                    name, report = future.result()
                    reports[name] = report
                    pbar.update(1)

    ordered: dict[str, FileReport] = {}
    for fp in files_to_check:
        if fp.name in reports:
            ordered[fp.name] = reports[fp.name]
    reports = ordered

    if output_format == OutputFormat.JSON:
        typer.echo(json.dumps(reports, indent=2, default=str))
    elif output_format == OutputFormat.YAML:
        typer.echo(yaml.dump(reports, sort_keys=False, default_flow_style=False))
    else:
        report_lines: list[str] = []
        for fname, status in reports.items():
            report_lines.append(f"File: {fname}")
            md5 = status["md5"]
            report_lines.append(f"  MD5: {md5['status']}")
            if md5.get("expected") or md5.get("got"):
                report_lines.append(f"    expected: {md5.get('expected')}")
                report_lines.append(f"    got: {md5.get('got')}")
            if md5.get("error"):
                report_lines.append(f"    error: {md5['error']}")
            extract = status["extract"]
            if extract:
                report_lines.append(f"  Extract: {extract['status']}")
                if extract.get("error"):
                    report_lines.append(f"    error: {extract['error']}")
            avros = status["avro"]
            if avros:
                report_lines.append("  Avro files:")
                for avro in avros:
                    report_lines.append(
                        f"    {avro.get('file', '<unknown>')}: {avro['status']}"
                    )
                    if avro.get("error"):
                        report_lines.append(f"      error: {avro['error']}")
            report_lines.append("")
        typer.echo("\n".join(report_lines))


if __name__ == "__main__":
    app()
