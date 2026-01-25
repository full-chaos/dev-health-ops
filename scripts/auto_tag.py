import os
import json
import subprocess
import re
import argparse


def get_latest_tag():
    try:
        # Get the latest reachable tag
        tag = (
            subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"])
            .decode()
            .strip()
        )
        return tag
    except subprocess.CalledProcessError:
        return None


def get_package_json_version(path):
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    return data.get("version")


def parse_semver(version_str):
    # Returns (major, minor, patch, prerelease_str, prerelease_num)
    # Supported formats: X.Y.Z, X.Y.Z-beta.N, X.Y.Z-rc.N
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z]+)[\.-]?(\d+))?$", version_str)
    if match:
        major, minor, patch, pre_type, pre_num = match.groups()
        return (
            int(major),
            int(minor),
            int(patch),
            pre_type,
            int(pre_num) if pre_num else None,
        )
    return None


def compare_versions(ver1, ver2):
    # Compare (major, minor, patch)
    if ver1[:3] > ver2[:3]:
        return 1
    if ver1[:3] < ver2[:3]:
        return -1

    # Compare pre-release
    # Note: semver says pre-release < stable.
    # My tuple: (..., pre_type, pre_num). If pre_type is None, it's stable.
    # So if ver1 has pre_type and ver2 doesn't, ver1 < ver2.

    v1_pre_type = ver1[3]
    v2_pre_type = ver2[3]

    if v1_pre_type is None and v2_pre_type is not None:
        return 1
    if v1_pre_type is not None and v2_pre_type is None:
        return -1
    if v1_pre_type is None and v2_pre_type is None:
        return 0

    # Both are pre-release. Compare type (alpha < beta < rc)?
    # Let's assume lexical comparison for simplicity.

    if v1_pre_type != v2_pre_type:
        return 1 if v1_pre_type > v2_pre_type else -1

    v1_num = ver1[4]
    v2_num = ver2[4]

    if v1_num > v2_num:
        return 1
    if v1_num < v2_num:
        return -1
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--package-json", default="grafana/plugins/dev-health-panels/package.json"
    )
    parser.add_argument("--pre-release-suffix", default="beta")
    args = parser.parse_args()

    latest_tag = get_latest_tag()
    pkg_ver_str = get_package_json_version(args.package_json)

    print(f"Latest tag: {latest_tag}")
    print(f"Package.json version: {pkg_ver_str}")

    latest_tag_clean = latest_tag.lstrip("v") if latest_tag else "0.0.0"
    latest_parsed = parse_semver(latest_tag_clean)

    if not latest_parsed:
        print(f"Error: Could not parse latest tag {latest_tag}")
        # Fallback to initial if fails?
        latest_parsed = (0, 0, 0, None, None)

    next_ver_str = None

    # Check package.json
    if pkg_ver_str:
        pkg_parsed = parse_semver(pkg_ver_str)
        if pkg_parsed:
            if compare_versions(pkg_parsed, latest_parsed) > 0:
                next_ver_str = pkg_ver_str
                print(f"Using version from package.json: {next_ver_str}")

    if not next_ver_str:
        # Increment
        major, minor, patch, pre_type, pre_num = latest_parsed

        if pre_type:
            # Increment pre-release
            next_ver_str = f"{major}.{minor}.{patch}-{pre_type}.{pre_num + 1}"
        else:
            # Start pre-release
            # Default to patch bump for pre-release of next version
            next_ver_str = f"{major}.{minor}.{patch + 1}-{args.pre_release_suffix}.1"

    next_tag = f"v{next_ver_str}"
    print(f"Next tag determined: {next_tag}")

    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"new_tag={next_tag}\n")


if __name__ == "__main__":
    main()
