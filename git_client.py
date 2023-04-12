import subprocess


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
