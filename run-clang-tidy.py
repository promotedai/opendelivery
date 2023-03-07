#!/usr/bin/env python3

# clang-tidy is a tool meant to run on a single file at a time. clang-tidy
# comes with a run-clang-tidy.py to facilitate running it over an entire
# workspace, but it's effectively useless because it has no way to plainly
# ignore subdirectories.

import os, sys, subprocess, multiprocessing
manager = multiprocessing.Manager()
failed_files = manager.list()

source_dir = os.path.realpath(os.getcwd()).rstrip(os.sep)
build_dir = source_dir + os.sep + "build"
ignore_dirs = tuple([build_dir, source_dir + os.sep + "submodules"])
extensions = tuple([".h", ".cc"])

def run_clang_tidy(file_path):
    proc = subprocess.Popen("clang-tidy --quiet -p=" + build_dir + " " + file_path, shell=True)
    if proc.wait() != 0:
        failed_files.append(file_path)

def collect_files(dir, ignore, exts):
    collected_files = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            file_path = root + os.sep + file
            if not file_path.startswith(ignore) and file_path.endswith(exts):
                collected_files.append(file_path)
    return collected_files

pool = multiprocessing.Pool()
pool.map(run_clang_tidy, collect_files(source_dir, ignore_dirs, extensions))
pool.close()
pool.join()
if len(failed_files) > 0:
    print("Errors in " + str(len(failed_files)) + " files.")
    sys.exit(1)
print("No errors found.")
sys.exit(0)
