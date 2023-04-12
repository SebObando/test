import subprocess


def is_current_branch_main():
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], stdout=subprocess.PIPE
    )
    current_branch = result.stdout.decode("utf-8").strip()
    return current_branch == "main"


def get_diff_between_last_two_merges():
    last_merge = (
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
    penultimate_merge = (
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
    file_paths_differences = subprocess.run(
        ["git", "diff", "--name-only", f"{last_merge}..{penultimate_merge}" ], stdout=subprocess.PIPE
    ).stdout.decode("utf-8")

    return file_paths_differences


print(get_diff_between_last_two_merges())