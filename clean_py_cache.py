import os
import shutil
import argparse

def clean_pycache_and_pyc(root_dir=".", dry_run=False):
    """
    Recursively removes all __pycache__ directories and .pyc files
    starting from the given root_dir.
    """
    cleaned_dirs_count = 0
    cleaned_files_count = 0

    print(f"Scanning for __pycache__ directories and .pyc files in: {os.path.abspath(root_dir)}")
    if dry_run:
        print("--- DRY RUN MODE: No files or directories will be deleted. ---")

    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Remove __pycache__ directories
        if "__pycache__" in dirnames:
            pycache_path = os.path.join(dirpath, "__pycache__")
            print(f"{'[DRY RUN] Would remove' if dry_run else 'Removing'} directory: {pycache_path}")
            if not dry_run:
                try:
                    shutil.rmtree(pycache_path)
                    cleaned_dirs_count += 1
                except OSError as e:
                    print(f"Error removing directory {pycache_path}: {e}")
            # Important: Remove __pycache__ from dirnames to prevent os.walk
            # from trying to descend into it after it's been (notionally) removed
            dirnames.remove("__pycache__")


        # Remove .pyc files
        for filename in filenames:
            if filename.endswith(".pyc"):
                pyc_file_path = os.path.join(dirpath, filename)
                print(f"{'[DRY RUN] Would remove' if dry_run else 'Removing'} file: {pyc_file_path}")
                if not dry_run:
                    try:
                        os.remove(pyc_file_path)
                        cleaned_files_count += 1
                    except OSError as e:
                        print(f"Error removing file {pyc_file_path}: {e}")

    print("\n--- Summary ---")
    if dry_run:
        print("Dry run complete. No changes were made.")
        # To give an idea of what would happen, you'd need to count during the scan
        # For simplicity in dry run, we'll just state no changes.
        # A more elaborate dry run would count potential deletions.
    else:
        print(f"Successfully removed {cleaned_dirs_count} __pycache__ director(y/ies).")
        print(f"Successfully removed {cleaned_files_count} .pyc file(s).")
    print("----------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean __pycache__ directories and .pyc files from a project.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="The root directory of the project to clean. Defaults to current directory."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and print what would be removed, but don't actually delete anything."
    )
    args = parser.parse_args()

    clean_pycache_and_pyc(args.directory, args.dry_run)