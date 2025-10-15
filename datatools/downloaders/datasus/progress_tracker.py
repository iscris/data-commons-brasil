import json
import logging
import os
from pathlib import Path
from typing import Optional, Set

import json5

logger = logging.getLogger("downloader:DATASUS:progress")


class ProgressTracker:
    """Tracks download progress to enable resuming interrupted downloads."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        # Save progress in parent directory to persist across runs
        # output_dir is like: /app/output/downloader/datasus/tmp_uuid
        # We want: /app/output/downloader/datasus/.datasus_progress.json
        parent_dir = os.path.dirname(output_dir)
        self.progress_file = os.path.join(parent_dir, ".datasus_progress.json")
        self.completed_datasets: Set[str] = set()
        self._load_progress()

    def _load_progress(self) -> None:
        """Load progress from disk if it exists."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.completed_datasets = set(data.get("completed_datasets", []))
                    logger.info(
                        f"Loaded progress: {len(self.completed_datasets)} datasets already completed"
                    )
            except Exception as e:
                logger.warning(f"Failed to load progress file: {e}")
                self.completed_datasets = set()
        else:
            logger.info("No previous progress found, starting fresh")

    def _save_progress(self) -> None:
        """Save current progress to disk."""
        try:
            # Ensure parent directory exists
            os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(
                    {"completed_datasets": list(self.completed_datasets)},
                    f,
                    indent=2,
                    ensure_ascii=False,
                )
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")

    def is_completed(self, dataset_name: str) -> bool:
        """Check if a dataset has already been completed."""
        return dataset_name in self.completed_datasets

    def mark_completed(self, dataset_name: str) -> None:
        """Mark a dataset as completed and save progress."""
        self.completed_datasets.add(dataset_name)
        self._save_progress()
        logger.info(
            f"Marked {dataset_name} as completed. Total: {len(self.completed_datasets)}"
        )

    def _check_openapi_file_complete(self, filepath: Path) -> bool:
        """
        Check if an OpenAPI JSON file is complete by verifying metadata status.
        Returns True if file has status='complete', False otherwise.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json5.load(f)
                metadata = data.get("metadata", {})
                status = metadata.get("status", "")

                if status == "complete":
                    logger.debug(
                        f"OpenAPI file {filepath.name} is complete "
                        f"({metadata.get('total_records', 0)} records, "
                        f"{metadata.get('pages_downloaded', 0)} pages)"
                    )
                    return True
                else:
                    logger.warning(
                        f"OpenAPI file {filepath.name} is incomplete "
                        f"(status: {status})"
                    )
                    return False
        except Exception as e:
            logger.warning(f"Failed to check OpenAPI file {filepath.name}: {e}")
            return False

    def _verify_files_in_directory(
        self, directory: Path, dataset_name: str, check_openapi: bool = False
    ) -> bool:
        """
        Verify if dataset files exist and are complete in a directory.

        Args:
            directory: Directory to check
            dataset_name: Name of the dataset
            check_openapi: If True, also verify OpenAPI file completeness

        Returns:
            True if files exist (and are complete for OpenAPI), False otherwise
        """
        matching_files = list(directory.glob(f"{dataset_name}_*"))

        if not matching_files:
            return False

        # For OpenAPI files, check completeness
        if check_openapi:
            json_files = [f for f in matching_files if f.suffix == ".json"]
            if json_files:
                # All JSON files must be complete
                all_complete = all(
                    self._check_openapi_file_complete(f) for f in json_files
                )
                if not all_complete:
                    logger.warning(
                        f"Dataset {dataset_name} has incomplete OpenAPI files"
                    )
                    return False

        logger.info(
            f"Found {len(matching_files)} complete files for dataset {dataset_name} "
            f"in {directory}"
        )
        return True

    def verify_dataset_files(self, dataset_name: str, output_dir: str) -> bool:
        """
        Verify if dataset files already exist on disk and are complete.
        This helps resume downloads that were interrupted.
        Checks both current tmp directory and previous timestamped directories.

        For OpenAPI JSON files, verifies that metadata.status == 'complete'.
        """
        # Check current output directory (tmp_uuid)
        path = Path(output_dir)
        if path.exists():
            if self._verify_files_in_directory(path, dataset_name, check_openapi=True):
                return True

        # Also check parent directory for timestamped folders from previous runs
        parent_dir = Path(output_dir).parent
        if parent_dir.exists():
            # Look for timestamped directories (format: YYYYMMDD_HHMMSS)
            for subdir in parent_dir.iterdir():
                if subdir.is_dir() and not subdir.name.startswith("tmp_"):
                    if self._verify_files_in_directory(
                        subdir, dataset_name, check_openapi=True
                    ):
                        return True

        return False

    def get_stats(self) -> dict:
        """Get current progress statistics."""
        return {"completed_datasets": len(self.completed_datasets)}
