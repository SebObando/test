import subprocess
import  git_client

def set_index_defintion_changed_variable(file_name: str) -> None:

    is_current_branch_main = git_client.is_current_branch_main()

    if is_current_branch_main:
        command = "git diff HEAD^"
        output = git_client.get_diff_between_last_two_merges()
    else:
        command = "git diff --name-only origin/main"
        output = subprocess.check_output(command.split()).decode()

    if file_name in output:
        is_index_changed = "true"
    else:
        is_index_changed = "false"
    
    return is_index_changed