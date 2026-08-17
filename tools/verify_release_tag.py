"""Verify that an immutable remote tag still resolves to the candidate commit."""

from __future__ import annotations

import argparse
import subprocess
from collections.abc import Sequence

_TAG_REFERENCE_FIELDS = 2


def main(arguments: Sequence[str] | None = None) -> int:
    """Fail closed unless the remote tag identity matches the candidate SHA."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-sha", required=True)
    options = parser.parse_args(arguments)
    if not options.tag.startswith("v") or any(char.isspace() for char in options.tag):
        raise SystemExit("release tag must start with v and contain no whitespace")
    reference = f"refs/tags/{options.tag}"
    result = subprocess.run(
        ["git", "ls-remote", "origin", reference, f"{reference}^{{}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit("remote release tag lookup failed")
    identities = {}
    for line in result.stdout.splitlines():
        values = line.split()
        if len(values) != _TAG_REFERENCE_FIELDS:
            raise SystemExit("remote release tag lookup returned malformed output")
        identities[values[1]] = values[0]
    resolved = identities.get(f"{reference}^{{}}", identities.get(reference, ""))
    if not resolved or resolved != options.expected_sha:
        raise SystemExit("remote release tag no longer matches the release candidate")
    print(f"remote release tag verified: {options.tag} -> {options.expected_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
