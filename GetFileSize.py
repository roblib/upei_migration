#!/usr/bin/env python3

import os
import argparse


def human_readable_size(size_bytes):
    """Convert bytes into a human-readable format (KB, MB, GB, etc.)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def total_size_of_files(pattern, directory):
    """Calculate total size of all files in subdirectories that contain 'pattern' in their filename."""
    total_size = 0
    for root, _, files in os.walk(directory):
        for file in files:
            if pattern in file:  # Match the pattern in filename
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)

    return human_readable_size(total_size)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate total size of files matching a pattern in a directory.")
    parser.add_argument("--pattern", required=True, help="Substring to match in filenames.")
    parser.add_argument("--directory", required=True, help="Directory to search in.")

    args = parser.parse_args()

    total_size = total_size_of_files(args.pattern, args.directory)
    print(f"Total size of all files containing '{args.pattern}' in '{args.directory}': {total_size}")
