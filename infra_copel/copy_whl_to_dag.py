from pathlib import Path
import shutil
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    # Directories
    current_dir = Path(__file__).parent.resolve()
    dist_dir = current_dir / "dist"
    target_dir = current_dir.parent / "dags" / "install"

    # Find the most recent .whl file
    whl_files = list(dist_dir.glob("*.whl"))
    if not whl_files:
        raise FileNotFoundError("No .whl files found in the dist directory.")

    latest_whl = max(whl_files, key=lambda p: p.stat().st_mtime)

    # Extract the base name (until the first '-')
    base_name = latest_whl.stem.split("-")[0]

    # Remove files in target directory that match the base name
    for file in target_dir.glob(f"{base_name}-*.whl"):
        try:
            file.unlink()  # Remove the file
            logging.info(f"Removed existing file: {file}")
        except Exception as e:
            logging.error(f"Error removing file {file}: {e}")

    # Copy the file to the target directory
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(latest_whl, target_dir)
        logging.info(f"File {latest_whl} copied to {target_dir}")
    except PermissionError as e:
        logging.error(f"Permission error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
