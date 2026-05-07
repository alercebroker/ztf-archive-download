import json
import logging
import shutil
import tempfile
from pathlib import Path

from git import Repo
from tqdm import tqdm

REPO_URL = "https://github.com/ZwickyTransientFacility/ztf-avro-alert"
SCHEMA_DIR = Path("./schema")
OUTPUT_BASE = Path("./schemas")


def extract_version(alert_avsc_path: Path) -> str:
    with open(alert_avsc_path, "r") as f:
        data = json.load(f)
    version = data.get("version")
    if not version:
        raise ValueError(f"No 'version' field found in {alert_avsc_path}")
    return str(version)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        logging.info(f"Cloning repo into temp dir: {tmppath}")
        repo = Repo.clone_from(REPO_URL, tmppath)
        alert_avsc_rel = SCHEMA_DIR / "alert.avsc"
        alert_avsc_full = tmppath / alert_avsc_rel

        # Map version -> (commit.hexsha, commit.committed_date)
        version_commits: dict[str, tuple[str, int]] = {}

        logging.info("Collecting unique schema versions from git history...")
        for commit in tqdm(list(repo.iter_commits(paths=alert_avsc_rel))):
            repo.git.checkout(commit.hexsha)
            try:
                version = extract_version(Path(alert_avsc_full))
            except Exception as e:
                logging.warning(
                    f"Failed to extract version at commit {commit.hexsha}: {e}"
                )
                continue
            # If duplicate, keep the latest (by commit date)
            if (
                version not in version_commits
                or commit.committed_date > version_commits[version][1]
            ):
                version_commits[version] = (commit.hexsha, commit.committed_date)

        logging.info(f"Found {len(version_commits)} unique schema versions.")
        OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

        for version, (hexsha, _) in tqdm(
            sorted(version_commits.items(), key=lambda x: x[1][1])
        ):
            repo.git.checkout(hexsha)
            out_dir = OUTPUT_BASE / version
            out_dir.mkdir(parents=True, exist_ok=True)
            for avsc_file in (tmppath / SCHEMA_DIR).glob("*.avsc"):
                dest = out_dir / f"ztf.alert.{avsc_file.stem}.avsc"
                if dest.exists():
                    logging.warning(f"Overwriting existing file: {dest}")
                _ = shutil.copy2(avsc_file, dest)
                logging.info(f"Stored schema version {version} at {out_dir}")

        logging.info("Schema extraction complete.")


if __name__ == "__main__":
    main()
