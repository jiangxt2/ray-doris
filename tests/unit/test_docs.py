from __future__ import annotations

from pathlib import Path

import pytest
from tools.check_docs import validate_compatibility_matrices

MATRIX = (
    ("3.9", "2.49.2"),
    ("3.10", "2.57.0"),
    ("3.11", "2.58.0"),
    ("3.12", "2.58.0"),
    ("3.13", "2.58.0"),
)


def _ci_text(matrix: tuple[tuple[str, str], ...] = MATRIX) -> str:
    entries = "\n".join(
        f'          - python: "{python}"\n            ray: "{ray}"' for python, ray in matrix
    )
    return (
        "name: CI\n\n"
        "jobs:\n"
        "  unit:\n"
        "    strategy:\n"
        "      matrix:\n"
        "        include:\n"
        f"{entries}\n"
        "  quality:\n"
        "    runs-on: ubuntu-latest\n"
    )


def _markdown_text(matrix: tuple[tuple[str, str], ...] = MATRIX) -> str:
    rows = "\n".join(f"| {python} | {ray} | verification |" for python, ray in matrix)
    return f"| Python | Ray | Verification |\n| --- | --- | --- |\n{rows}\n"


def _write_sources(
    tmp_path: Path,
    *,
    ci_text: str | None = None,
    readme_matrix: tuple[tuple[str, str], ...] = MATRIX,
    compatibility_matrix: tuple[tuple[str, str], ...] = MATRIX,
) -> tuple[Path, Path, Path]:
    ci_path = tmp_path / "ci.yml"
    readme_path = tmp_path / "README.md"
    compatibility_path = tmp_path / "compatibility.md"
    ci_path.write_text(_ci_text() if ci_text is None else ci_text, encoding="utf-8")
    readme_path.write_text(_markdown_text(readme_matrix), encoding="utf-8")
    compatibility_path.write_text(
        _markdown_text(compatibility_matrix),
        encoding="utf-8",
    )
    return ci_path, readme_path, compatibility_path


def test_repository_compatibility_matrices_match() -> None:
    root = Path(__file__).parents[2]
    assert (
        validate_compatibility_matrices(
            root / ".github" / "workflows" / "ci.yml",
            root / "README.md",
            root / "doc" / "source" / "compatibility.md",
        )
        == []
    )


def test_compatibility_matrices_accept_matching_sources(tmp_path: Path) -> None:
    assert validate_compatibility_matrices(*_write_sources(tmp_path)) == []


@pytest.mark.parametrize(
    ("source", "matrix"),
    [
        ("readme", MATRIX[:-1]),
        ("compatibility", (*MATRIX, ("3.14", "2.58.0"))),
        ("readme", (MATRIX[0], MATRIX[2], MATRIX[1], *MATRIX[3:])),
        ("compatibility", (*MATRIX[:2], ("3.11", "2.57.0"), *MATRIX[3:])),
    ],
)
def test_compatibility_matrices_reject_document_drift(
    tmp_path: Path,
    source: str,
    matrix: tuple[tuple[str, str], ...],
) -> None:
    kwargs = {f"{source}_matrix": matrix}
    errors = validate_compatibility_matrices(*_write_sources(tmp_path, **kwargs))
    assert any("expected CI matrix" in error for error in errors)


def test_compatibility_matrices_reject_duplicate_entries(tmp_path: Path) -> None:
    matrix = (*MATRIX[:2], MATRIX[1], *MATRIX[2:])
    errors = validate_compatibility_matrices(*_write_sources(tmp_path, compatibility_matrix=matrix))
    assert any("duplicate Python/Ray compatibility entries" in error for error in errors)


def test_compatibility_matrices_reject_ci_indentation_error(tmp_path: Path) -> None:
    malformed = _ci_text().replace('          - python: "3.9"', '        - python: "3.9"')
    errors = validate_compatibility_matrices(*_write_sources(tmp_path, ci_text=malformed))
    assert any("expected CI matrix" in error for error in errors)


def test_compatibility_matrices_reject_missing_unit_job(tmp_path: Path) -> None:
    missing_unit = _ci_text().replace("  unit:\n", "  missing-unit:\n")
    errors = validate_compatibility_matrices(*_write_sources(tmp_path, ci_text=missing_unit))
    assert any("missing or malformed" in error for error in errors)
