import datetime
import json
import os
import subprocess
from pathlib import Path

import git


def is_main():
    repo = git.Repo(Path(__file__), search_parent_directories=True)
    current_commit = repo.head.object
    last_100_main_commits = list(
        repo.iter_commits("origin/main", max_count=100)
    )
    is_main = current_commit in last_100_main_commits
    return is_main


def get_version(tag: str):
    try:
        repo = git.Repo(Path(__file__), search_parent_directories=True)
        commit_hexsha = repo.head.object.hexsha
        matched_tag_list = list(
            filter(
                lambda tag_item: tag_item.object.hexsha == commit_hexsha
                and tag in tag_item.name,
                repo.tags,
            )
        )
        current_tag = str(matched_tag_list[0])
        current_version = current_tag.split("/")[-1]

        return current_version
    except Exception as e:
        raise Exception(
            f"Could not find semantic version from tag: Exception - {e}"
        )


def get_tag_creation_time():
    try:
        repo = git.Repo(Path(__file__), search_parent_directories=True)
        hexsha_repo = repo.head.object.hexsha

        tag_to_find_created_date = list(
            filter(lambda x: x.object.hexsha == hexsha_repo, repo.tags)
        )
        creation_time = tag_to_find_created_date[0].object.committed_datetime
        repo.__del__()
        return creation_time.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        return datetime.datetime.now("%Y-%m-%dT%H:%M:%S")


def get_persisted_version_from_config(path_to_config: str):
    with open(path_to_config) as file:
        json_of_file = json.load(file)
    try:
        version_number = int(json_of_file["sem_version"].replace(".", ""))
        isinstance(version_number, int)
    except Exception as e_info:
        raise Exception(
            f"sem_version has not been defined correctly in "
            f"config.json. See Exception - {e_info}"
        )

    return json_of_file["sem_version"]


def get_url_safe_version(version: str):
    return version.replace(".", "-")


def update_config_version(path_to_config: str, tag: str):
    if os.path.exists(path_to_config):
        with open(path_to_config) as file:
            json_of_file = json.load(file)
            json_of_file["sem_version"] = get_version(tag)

        with open(path_to_config, "w+") as file:
            json.dump(json_of_file, file)
    else:
        raise Exception("File does not exist")


def is_current_branch_main():
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], stdout=subprocess.PIPE
    )
    current_branch = result.stdout.decode("utf-8").strip()
    return current_branch == "main"


def get_diff_between_last_two_merges():
    # Get the commit hashes of the last two merges in the main branch
    merge1 = (
        subprocess.run(
            [
                "git",
                "log",
                "main",
                "--merges",
                "-n",
                "1",
                "--pretty=format:%H",
            ],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )
    merge2 = (
        subprocess.run(
            [
                "git",
                "log",
                "main",
                "--merges",
                "-n",
                "1",
                "--skip",
                "1",
                "--pretty=format:%H",
            ],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )

    # Get the commit hashes of the last commits before each merge
    commit1 = (
        subprocess.run(
            ["git", "rev-list", f"{merge1}^1", "-n", "1"],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )
    commit2 = (
        subprocess.run(
            ["git", "rev-list", f"{merge2}^1", "-n", "1"],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )

    # Get the diff between the two commits
    diff = subprocess.run(
        ["git", "diff", commit1, commit2], stdout=subprocess.PIPE
    ).stdout.decode("utf-8")

    return diff
