"""Regenerates the auto-stats block in README.md: test count (from pytest) and lines of
tracked Python source (from git ls-files, so build artifacts/DBs never get counted).

Run this before a commit when you want the README's numbers refreshed - it is NOT wired into
a git hook or CI on purpose, since this repo isn't pushed to a CI-backed remote yet. Re-running
is safe any number of times: it only rewrites the block between the AUTO-STATS markers below,
never anything else in the file.
"""
import re
import subprocess
import sys
from pathlib import Path

README_PATH = Path(__file__).parent / "README.md"
START_MARKER = "<!-- AUTO-STATS:START -->"
END_MARKER = "<!-- AUTO-STATS:END -->"

EXCLUDE_PREFIXES = ("test_",)
EXCLUDE_DIR_PARTS = ("tests",)


def count_tracked_lines() -> int:
    result = subprocess.run(["git", "ls-files"], cwd=Path(__file__).parent, capture_output=True, text=True, check=True)
    files = result.stdout.splitlines()
    total = 0
    for f in files:
        path = Path(__file__).parent / f
        if not path.is_file() or path.suffix != ".py":
            continue
        if path.name.startswith(EXCLUDE_PREFIXES):
            continue
        if any(part in EXCLUDE_DIR_PARTS for part in path.parts):
            continue
        try:
            total += sum(1 for _ in path.open(encoding="utf-8", errors="ignore"))
        except OSError:
            continue
    return total


def count_tests() -> int:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=Path(__file__).parent, capture_output=True, text=True,
    )
    match = re.search(r"(\d+) tests? collected", result.stdout)
    return int(match.group(1)) if match else 0


def build_stats_block() -> str:
    lines_of_code = count_tracked_lines()
    test_count = count_tests()
    return (
        f"{START_MARKER}\n"
        f"![Lines of source](https://img.shields.io/badge/source-{lines_of_code}_lines-c9a24b)\n"
        f"![Tests](https://img.shields.io/badge/tests-{test_count}-4a8a5c)\n"
        f"{END_MARKER}"
    )


def main() -> None:
    content = README_PATH.read_text(encoding="utf-8")
    new_block = build_stats_block()
    if START_MARKER in content and END_MARKER in content:
        pattern = re.compile(re.escape(START_MARKER) + r".*?" + re.escape(END_MARKER), re.DOTALL)
        content = pattern.sub(new_block, content)
    else:
        content = new_block + "\n\n" + content
    README_PATH.write_text(content, encoding="utf-8")
    print(f"README stats updated: {new_block}")


if __name__ == "__main__":
    main()
