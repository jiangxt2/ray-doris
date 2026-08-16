"""Validate documentation navigation, API targets, and Python examples."""

from __future__ import annotations

import argparse
import importlib
import re
import sys
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DOCS_ROOT = REPOSITORY_ROOT / "doc" / "source"

_AUTODOC_PATTERN = re.compile(
    r"^```\{(?:autoclass|autoexception|autofunction)\}\s+(?P<target>\S+)\s*$",
    re.MULTILINE,
)
_PYTHON_BLOCK_PATTERN = re.compile(
    r"(?P<skip><!-- docs-check: skip-python -->\s*)?"
    r"```python\s*\n(?P<code>.*?)^```\s*$",
    re.MULTILINE | re.DOTALL,
)
_TOCTREE_PATTERN = re.compile(
    r"^```\{toctree\}\s*\n(?P<body>.*?)^```\s*$",
    re.MULTILINE | re.DOTALL,
)
_HTML_META_PATTERN = re.compile(
    r"\A---\s*\n.*?html_meta:\s*\n\s+description:\s*.+?\n---\s*\n",
    re.DOTALL,
)

_EXPECTED_NAVIGATION = (
    "quickstart",
    "key-concepts",
    "user-guide/index",
    "examples/index",
    "faq",
    "api/api",
    "compatibility",
    "architecture",
    "contributing/index",
)
_EXPECTED_API_TARGETS = frozenset(
    {
        "ray_doris.DorisAuthenticationError",
        "ray_doris.DorisAmbiguousWriteError",
        "ray_doris.DorisConfigurationError",
        "ray_doris.DorisConnection",
        "ray_doris.DorisDatasource",
        "ray_doris.DorisDatasink",
        "ray_doris.DorisError",
        "ray_doris.DorisLabelExistsError",
        "ray_doris.DorisMetadataError",
        "ray_doris.DorisPermissionError",
        "ray_doris.DorisPlanningError",
        "ray_doris.DorisReadError",
        "ray_doris.DorisSchemaError",
        "ray_doris.DorisTable",
        "ray_doris.DorisTableCompatibilityError",
        "ray_doris.DorisWriteError",
        "ray_doris.DorisWriteOptions",
        "ray_doris.DorisWriteResult",
        "ray_doris.read_doris",
        "ray_doris.write_doris",
    }
)


def _toctree_targets(text: str) -> tuple[str, ...]:
    match = _TOCTREE_PATTERN.search(text)
    if match is None:
        return ()
    targets: list[str] = []
    for raw_line in match.group("body").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(":"):
            continue
        titled_target = re.fullmatch(r".+?\s*<(?P<target>[^>]+)>", line)
        targets.append(titled_target.group("target") if titled_target else line)
    return tuple(targets)


def _resolve_target(target: str) -> Any:
    parts = target.split(".")
    for boundary in range(len(parts), 0, -1):
        module_name = ".".join(parts[:boundary])
        try:
            obj: Any = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            raise
        for attribute in parts[boundary:]:
            obj = getattr(obj, attribute)
        return obj
    raise ImportError(f"cannot resolve documented target {target!r}")


def validate_navigation(docs_root: Path) -> list[str]:
    errors: list[str] = []
    index_path = docs_root / "index.md"
    targets = _toctree_targets(index_path.read_text(encoding="utf-8"))
    if targets != _EXPECTED_NAVIGATION:
        errors.append(f"{index_path}: navigation is {targets!r}; expected {_EXPECTED_NAVIGATION!r}")
    for target in _EXPECTED_NAVIGATION:
        source = docs_root / target
        if not source.with_suffix(".md").is_file():
            errors.append(f"{index_path}: navigation target does not exist: {target}")
    return errors


def validate_page_metadata(docs_root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(docs_root.rglob("*.md")):
        if not _HTML_META_PATTERN.search(path.read_text(encoding="utf-8")):
            errors.append(f"{path}: missing MyST html_meta description")
    return errors


def validate_api_reference(path: Path, *, import_objects: bool) -> list[str]:
    text = path.read_text(encoding="utf-8")
    targets = tuple(match.group("target") for match in _AUTODOC_PATTERN.finditer(text))
    errors: list[str] = []
    if set(targets) != _EXPECTED_API_TARGETS:
        errors.append(
            f"{path}: API targets are {sorted(set(targets))!r}; "
            f"expected {sorted(_EXPECTED_API_TARGETS)!r}"
        )
    if len(targets) != len(set(targets)):
        errors.append(f"{path}: duplicate autodoc targets")
    if not import_objects:
        return errors
    for target in targets:
        try:
            _resolve_target(target)
        except (AttributeError, ImportError) as exc:
            errors.append(f"{target}: import failed: {type(exc).__name__}: {exc}")
    package = importlib.import_module("ray_doris")
    exported = {f"ray_doris.{name}" for name in package.__all__}
    if exported != _EXPECTED_API_TARGETS:
        errors.append(
            f"ray_doris.__all__ does not match the documented public API: {sorted(exported)!r}"
        )
    return errors


def validate_python_examples(docs_root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(docs_root.rglob("*.md")):
        text = path.read_text(encoding="utf-8")
        for match in _PYTHON_BLOCK_PATTERN.finditer(text):
            if match.group("skip"):
                continue
            line = text.count("\n", 0, match.start()) + 1
            try:
                compile(match.group("code"), f"{path}:{line}", "exec")
            except SyntaxError as exc:
                errors.append(f"{path}:{line}: invalid Python example: {exc.msg}")
    example = REPOSITORY_ROOT / "examples" / "quickstart.py"
    try:
        compile(example.read_text(encoding="utf-8"), str(example), "exec")
    except SyntaxError as exc:
        errors.append(f"{example}: invalid Python example: {exc.msg}")
    return errors


def run_checks(docs_root: Path, *, static_only: bool) -> list[str]:
    errors = validate_navigation(docs_root)
    errors.extend(validate_page_metadata(docs_root))
    errors.extend(
        validate_api_reference(
            docs_root / "api" / "api.md",
            import_objects=not static_only,
        )
    )
    errors.extend(validate_python_examples(docs_root))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    parser.add_argument("--static-only", action="store_true")
    args = parser.parse_args(argv)
    errors = run_checks(args.docs_root, static_only=args.static_only)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Documentation checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
