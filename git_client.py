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
    # Get the diff between the two commits
    diff = subprocess.run(
        ["git", "diff", "--name-only", f"{merge1}..{merge2}" ], stdout=subprocess.PIPE
    ).stdout.decode("utf-8")
    return diff


print(get_diff_between_last_two_merges())