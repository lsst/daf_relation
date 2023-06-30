"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "daf_relation"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

nitpick_ignore_regex = [
    ("py:obj", r"sqlalchemy\..*"),
]
nitpick_ignore = [
    ("py:obj", "match"),
    ("py:obj", "case"),
]
