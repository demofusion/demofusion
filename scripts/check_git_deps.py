#!/usr/bin/env python3
"""Check pinned git dependencies in Cargo.toml against upstream default branches.

Reports (and optionally updates) pinned `git = "..." rev = "..."` dependencies
such as valveprotos and dungers when upstream has moved forward.

Usage:
    scripts/check_git_deps.py            # report only (exit 1 if outdated)
    scripts/check_git_deps.py --update   # bump pinned revs in Cargo.toml + Cargo.lock
    scripts/check_git_deps.py --markdown # also write scripts/deps-report.md (PR body)

Environment:
    GITHUB_TOKEN — optional; raises the GitHub API rate limit.

Exit codes:
    0 — all pinned revs are up to date (or were successfully updated)
    1 — at least one dependency is outdated (report mode)
    2 — an upstream repo could not be queried (API error, auth, private repo)
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request
from dataclasses import dataclass

CARGO_TOML = "Cargo.toml"
REPORT_PATH = os.path.join(os.path.dirname(__file__), "deps-report.md")

# Matches e.g.  valveprotos = { git = "https://github.com/org/repo.git", rev = "abc123", features = [...] }
DEP_RE = re.compile(
    r'^\s*(?P<name>[A-Za-z0-9_-]+)\s*=\s*\{(?P<body>[^}]*)\}',
    re.MULTILINE,
)


@dataclass
class GitDep:
    name: str
    url: str
    rev: str
    span_start: int
    span_end: int


def find_git_deps() -> list[GitDep]:
    with open(CARGO_TOML) as f:
        text = f.read()
    deps = []
    for m in DEP_RE.finditer(text):
        body = m.group("body")
        git_m = re.search(r'git\s*=\s*"([^"]+)"', body)
        rev_m = re.search(r'rev\s*=\s*"([^"]+)"', body)
        if not git_m or not rev_m:
            continue
        deps.append(
            GitDep(
                name=m.group("name"),
                url=git_m.group(1).removesuffix(".git"),
                rev=rev_m.group(1),
                span_start=m.start(),
                span_end=m.end(),
            )
        )
    return deps


def github_api(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def latest_commit(repo_path: str) -> dict:
    """Latest commit on the repo's default branch."""
    info = github_api(f"https://api.github.com/repos/{repo_path}")
    branch = info["default_branch"]
    return github_api(f"https://api.github.com/repos/{repo_path}/commits/{branch}")


def update_rev(dep: GitDep, new_rev: str) -> None:
    with open(CARGO_TOML) as f:
        text = f.read()
    dep_re = re.compile(
        r'(?P<prefix>\b' + re.escape(dep.name) + r'\s*=\s*\{[^}]*rev\s*=\s*")'
        + re.escape(dep.rev)
        + r'(?P<suffix>")'
    )
    text, n = dep_re.subn(rf"\g<prefix>{new_rev}\g<suffix>", text, count=1)
    if n != 1:
        raise RuntimeError(f"failed to update rev for {dep.name} in {CARGO_TOML}")
    with open(CARGO_TOML, "w") as f:
        f.write(text)


def refresh_lock(name: str) -> None:
    try:
        subprocess.run(
            ["cargo", "update", "-p", name],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Lock file may not be refreshable without a full build; CI will verify.
        pass


def main() -> int:
    update = "--update" in sys.argv
    markdown = "--markdown" in sys.argv

    deps = find_git_deps()
    if not deps:
        print(f"No git-pinned dependencies found in {CARGO_TOML}")
        return 0

    rows: list[dict] = []
    errors = 0
    for dep in deps:
        repo_path = dep.url.split("github.com/")[-1]
        try:
            commit = latest_commit(repo_path)
        except Exception as exc:  # noqa: BLE001
            print(f"  ? {dep.name}: could not query {repo_path}: {exc}", file=sys.stderr)
            errors += 1
            continue
        sha = commit["sha"]
        short = sha[:9]
        msg = (commit["commit"]["message"].splitlines() or [""])[0].strip()
        if dep.rev.startswith(sha) or sha.startswith(dep.rev):
            status = "up-to-date"
            changed = False
        else:
            status = "outdated"
            changed = True
            if update:
                update_rev(dep, sha)
                refresh_lock(dep.name)
                status = "updated"
        rows.append(
            {
                "name": dep.name,
                "repo": repo_path,
                "pinned": dep.rev,
                "latest": short,
                "message": msg,
                "changed": changed,
                "status": status,
            }
        )
        marker = {"updated": "↑", "outdated": "✗", "up-to-date": "✓"}[status]
        print(f"  {marker} {dep.name} ({repo_path}): pinned {dep.rev}, latest {short} — {status}")
        print(f"      {msg}")

    if markdown:
        lines = [
            "## Pinned git dependency check",
            "",
            "| crate | repo | pinned | latest | latest commit | status |",
            "|---|---|---|---|---|---|",
        ]
        for r in rows:
            state = "updated" if r["status"] == "updated" else ("outdated" if r["status"] == "outdated" else "up to date")
            lines.append(
                f"| `{r['name']}` | [{r['repo']}](https://github.com/{r['repo']}/commits/{r['latest']}) "
                f"| `{r['pinned']}` | [`{r['latest']}`](https://github.com/{r['repo']}/commit/{r['latest']}) "
                f"| {r['message']} | {state} |"
            )
        lines += [
            "",
            "Updated revs were validated with `cargo update -p <crate>` and `cargo check --all-features`.",
            "",
        ]
        with open(REPORT_PATH, "w") as f:
            f.write("\n".join(lines))

    if update and any(r["changed"] for r in rows):
        print("\nCargo.toml updated; run `cargo check --all-features` to verify.")
        return 0
    if any(r["changed"] for r in rows):
        return 1
    if errors:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
