"""Sphinx configuration for the ray-doris documentation."""

from __future__ import annotations

import sys
from pathlib import Path

import tomllib

SOURCE_DIR = Path(__file__).resolve().parent
DOC_DIR = SOURCE_DIR.parent
REPOSITORY_ROOT = DOC_DIR.parent

sys.path.insert(0, str(REPOSITORY_ROOT / "src"))

project_metadata = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
    "project"
]

project = "ray-doris"
author = "ray-doris contributors"
copyright = "2026, ray-doris contributors"
release = str(project_metadata["version"])
version = release
language = "en"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_design",
    "sphinxcontrib.spelling",
]

source_suffix = {".md": "markdown"}
root_doc = "index"
exclude_patterns = ["_build", ".DS_Store", "Thumbs.db"]
templates_path = ["_templates"]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
]
myst_heading_anchors = 3

autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}
autodoc_preserve_defaults = True
autoclass_content = "class"
autodoc_typehints_format = "short"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pyarrow": ("https://arrow.apache.org/docs", None),
    "ray": ("https://docs.ray.io/en/latest", None),
}
intersphinx_timeout = 10

nitpicky = True
nitpick_ignore_regex = [
    ("py:class", r"(?:ray|pyarrow|pymysql)(?:\..*)?"),
    ("py:obj", r"(?:ray|pyarrow|pymysql)(?:\..*)?"),
]

html_theme = "pydata_sphinx_theme"
html_title = f"ray-doris {version}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_theme_options = {
    "use_edit_page_button": True,
    "navigation_with_keys": True,
    "navigation_depth": 4,
    "show_toc_level": 2,
    "navbar_end": ["theme-switcher", "navbar-icon-links"],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/jiangxt2/ray-doris",
            "icon": "fa-brands fa-github",
        }
    ],
    "secondary_sidebar_items": ["page-toc", "edit-this-page"],
}
# Keep the complete Ray-style root navigation expanded on every page, including
# entries hidden from the landing page body by the root toctree.
html_sidebars = {"**": ["main-sidebar"]}
html_context = {
    "github_user": "jiangxt2",
    "github_repo": "ray-doris",
    "github_version": "master",
    "doc_path": "doc/source",
}

linkcheck_anchors = False
linkcheck_retries = 2
linkcheck_timeout = 10
linkcheck_ignore = [
    r"http://127\.0\.0\.1(?::\d+)?(?:/.*)?",
    r"http://localhost(?::\d+)?(?:/.*)?",
]

spelling_lang = "en_US"
spelling_word_list_filename = "spelling_wordlist.txt"
spelling_show_suggestions = False
