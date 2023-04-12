"""
This module is modeled after Databricks's dbx package. It is primarily used to
create, update, and deploy data platform infrastructure.
"""
import glob
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import List

import click

from client import (
    const,
    databricks_client,
    file_system,
    git_client,
    logs,
    search_client,
)
from client.azure_vault_client import AzureVaultClient
from client.databricks_client import jobs, jobs_validations

logger = logs.logger.get_logger(__name__)


def get_email_list():
    _azure_vault_client = AzureVaultClient()
    email_list = _azure_vault_client.email_notification_list
    logger.info(f"Env E-Mail: {email_list}")
    split_list = email_list.replace(" ", "").split(",")
    return split_list


def get_filtered_pool_configs(
    path_to_config: str, filter_status: str
) -> List[dict]:
    abs_path_to_pool_config = file_system.get_abs_path_to_code(path_to_config)
    with open(abs_path_to_pool_config, "r") as file:
        pool_config_dict = json.load(file)

    filtered_configs = databricks_client.pools.filter_by_value(
        pool_config_dict, "status", filter_status
    )
    return filtered_configs


def get_current_wheel_path(path_to_project_code: str) -> str:
    search_folder = f"bazel-bin/{path_to_project_code}"
    search_path = file_system.get_abs_path_to_code(search_folder)
    all_wheels = glob.glob(f"{search_path}/*.whl")
    return all_wheels[-1]


def get_new_wheel_name(current_wheel_name: str, tag: str) -> str:
    tag_components = tag.split("/")

    version = tag_components[-1]
    if "dev" in tag_components:
        version = f"{version}.dev"

    wheel_components = current_wheel_name.split("-")
    wheel_components[1] = version
    new_wheel_name = "-".join(wheel_components)
    return new_wheel_name


@click.group(help="ACS Functionality with Indexes")
def index_operations():
    pass


@index_operations.command()
@click.option(
    "-ntc", "--number-to-keep", type=int, default=2, show_default=True
)
@click.option("--index", "-i", multiple=True)
def clean_indexes(number_to_keep, index):
    index_list = "\n".join(index).split()
    search_client.index.clean_all_indexes(number_to_keep, index_list)


@click.group()
def deployment():
    pass


@deployment.command()
def validate_all_job_config():
    root_home = file_system.get_root_directory()
    root_path = Path(root_home).joinpath("./src/projects")
    end_path = "/config/jobs/job.json"

    for root, dirs, files in os.walk(root_path, topdown=False):
        for file_name in files:
            full_path = os.path.join(root, file_name)
            if (
                full_path.endswith(end_path)
                and "curated_proj" not in full_path
            ):
                logger.info(f"Full path attempt is {full_path}")
                jobs_validations.validate_multiple_jobs_config(full_path)


@deployment.command()
@click.option("-p", "--path-to-job-config", type=click.Path())
def validate_specific_job_config(path_to_job_config: str):
    root_home = file_system.get_root_directory()
    path_to_job_json = Path(root_home).joinpath(path_to_job_config)
    jobs_validations.validate_multiple_jobs_config(path_to_job_json)


@deployment.command()
def validate_if_main_branch():
    _azure_vault_client = AzureVaultClient()
    env = _azure_vault_client.environment
    if env == const.ConfigType.prod:
        if not git_client.is_main():
            raise Exception(
                "This tag is from a feature branch. Only main can be deployed to prod"
            )
    else:
        logger.info("No checks to run")


@deployment.command()
@click.option("-p", "--path-to-project-code", type=click.Path())
@click.option("-b", "--build-version", type=click.STRING)
def update_job_configs(path_to_project_code: str, build_version: str):
    _azure_vault_client = AzureVaultClient()
    env = _azure_vault_client.environment
    absolute_path_to_job_config = file_system.get_abs_path_to_job_config(
        path_to_project_code
    )

    with open(absolute_path_to_job_config) as file:
        job_config = json.load(file)

        for job in job_config["default"]["jobs"]:
            job = databricks_client.jobs.update_settings(
                job, env, build_version
            )

    with open(absolute_path_to_job_config, "w+") as file:
        json.dump(job_config, file)


@deployment.command()
@click.option("-c", "--path-to-config", type=click.Path())
@click.option("-t", "--tag", type=click.STRING)
def update_cluster_vars(path_to_config: str, tag: str):
    absolute_path_to_cluster_config = file_system.get_abs_path_to_code(
        path_to_config
    )
    with open(absolute_path_to_cluster_config, "r") as file:
        cluster_configs = json.load(file)
        for cluster in cluster_configs["allpurpose_clusters"]:
            cluster = databricks_client.clusters.update_settings(cluster, tag)

    with open(absolute_path_to_cluster_config, "w+") as file:
        json.dump(cluster_configs, file)


@deployment.command()
@click.option(
    "-p", "--path-to-project-code", multiple=False, type=click.Path()
)
@click.option("-t", "--tag", type=click.STRING)
def add_wheel_to_dist(path_to_project_code: str, tag: str):
    current_wheel_path = get_current_wheel_path(path_to_project_code)
    abs_path_to_dist = file_system.get_abs_path_to_dist(path_to_project_code)

    new_wheel_name = get_new_wheel_name(
        os.path.basename(current_wheel_path), tag
    )

    destination_wheel_path = f"{abs_path_to_dist}/{new_wheel_name}"
    os.makedirs(os.path.dirname(destination_wheel_path), exist_ok=True)
    shutil.copy2(current_wheel_path, destination_wheel_path)


# region Databricks
# TODO: move to a new databricks_cli
@click.group()
def databricks():
    pass


@databricks.command()
@click.option("-c", "--path-to-config", multiple=True, type=click.Path())
def process_pool_config(path_to_config: str):
    abs_path_to_pool_config = file_system.get_abs_path_to_code(
        path_to_config[0]
    )
    with open(abs_path_to_pool_config, "r") as file:
        pool_config_dict = json.load(file)
    processed_configs = databricks_client.pools.classify_pools(
        pool_config_dict["pools"]
    )
    with open(abs_path_to_pool_config, "w") as file:
        json.dump(processed_configs, file)


@databricks.command()
@click.option("-c", "--path-to-config", multiple=True, type=click.Path())
def create_pools(path_to_config: str):
    new_pool_configs = get_filtered_pool_configs(path_to_config[0], "create")
    if not new_pool_configs:
        logger.info("No new pools to create. Skipping")
        return
    databricks_client.pools.create_pools(new_pool_configs)


@databricks.command()
@click.option("-c", "--path-to-config", multiple=True, type=click.Path())
def update_pools(path_to_config: str):
    update_pool_configs = get_filtered_pool_configs(
        path_to_config[0], "update"
    )
    if not update_pool_configs:
        logger.info("No pools to update. Skipping")
        return
    databricks_client.pools.update_pools(update_pool_configs)


@databricks.command()
@click.option("-c", "--path-to-config", multiple=True, type=click.Path())
def rebuild_pools(path_to_config: str):
    rebuild_pool_configs = get_filtered_pool_configs(
        path_to_config[0], "rebuild"
    )
    if not rebuild_pool_configs:
        logger.info("No pools to rebuild. Skipping")
        return
    databricks_client.pools.rebuild_pools(rebuild_pool_configs)


@databricks.command()
@click.option("-p", "--path-to-project-code", type=click.Path())
@click.option("-t", "--tag", type=click.Path())
def update_cluster_libraries(path_to_project_code: str, tag: str):
    if "dev" in tag:
        logger.info("Pushing from dev tag...Skipping cluster update")
        return

    abs_path_to_dist = file_system.get_abs_path_to_dist(path_to_project_code)
    wheels = glob.glob(f"{abs_path_to_dist.joinpath('./*.whl')}")
    new_wheel_name = os.path.basename(wheels[-1])
    dist_name = new_wheel_name.split("-")[0]
    logger.info(f"Adding wheel {new_wheel_name} to interactive clusters")

    databricks_client.clusters.uninstall_libraries(dist_name)
    databricks_client.clusters.install_wheel(new_wheel_name)


@databricks.command()
@click.option("-p", "--path-to-project-code", type=click.Path())
def upload_library_to_dbfs(path_to_project_code: str):
    absolute_path_to_dist = file_system.get_abs_path_to_dist(
        path_to_project_code
    )
    wheels = glob.glob(f"{absolute_path_to_dist.joinpath('./*.whl')}")
    databricks_client.dbfs.upload_wheel(wheels)


@databricks.command()
@click.option("-c", "--path-to-config", type=click.Path())
def update_clusters(path_to_config: str):
    absolute_path_to_cluster_config = file_system.get_abs_path_to_code(
        path_to_config
    )
    with open(absolute_path_to_cluster_config, "r") as file:
        cluster_configs = json.load(file)
        update_clusters = [
            c
            for c in cluster_configs["allpurpose_clusters"]
            if c["status"] == "update"
        ]
        for cluster in update_clusters:
            databricks_client.clusters.update(cluster["deploy_config"])


@databricks.command()
@click.option("-c", "--path-to-config", type=click.Path())
def create_clusters(path_to_config: str):
    absolute_path_to_cluster_config = file_system.get_abs_path_to_code(
        path_to_config
    )
    with open(absolute_path_to_cluster_config, "r") as file:
        cluster_configs = json.load(file)
        create_clusters = [
            c
            for c in cluster_configs["allpurpose_clusters"]
            if c["status"] == "create"
        ]
        for cluster in create_clusters:
            databricks_client.clusters.create(cluster["deploy_config"])


@databricks.command()
def restart_clusters():
    databricks_client.clusters.restart()


@databricks.command()
@click.option("-p", "--path", multiple=True, type=click.Path())
def wait_for_jobs_to_complete(path: str):
    databricks_client.jobs.wait_for_jobs_to_complete(path[0])


@databricks.command()
@click.option("-p", "--path", multiple=True, type=click.Path())
def add_job_schedules(path: str):
    databricks_client.jobs.add_schedules(path[0])


@databricks.command()
@click.option("-p", "--path", multiple=True, type=click.Path())
def remove_job_schedules(path: str):
    databricks_client.jobs.remove_schedules(path[0])


@deployment.command()
@click.option("-p", "--path-to-project-code", type=click.Path())
@click.option("--job-name", type=str)
def run_single_job(path_to_project_code, job_name):
    """
    the deployment.command annotation generates conflicts with the
    execution_run_now and the run_jobs_in_parallel, thus in order to have
    a single job run now execution this run_single_job was created
    """
    jobs.cancel_running_job(job_name)
    run_id = jobs.execute_run_now(path_to_project_code, job_name)
    click.echo(
        f"##vso[task.setvariable variable=run_ids;isOutput=true]{str(run_id)}"
    )


@deployment.command()
@click.option("-p", "--path-to-project-code", type=click.Path())
def run_multiple_jobs(path_to_project_code: str) -> list:
    click.echo("path_to_project_code: %s" % path_to_project_code)
    jobs_parameters = jobs.get_jobs_parameters(path_to_project_code)
    job_names = jobs_parameters.keys()

    run_ids = []
    for job_name in job_names:
        jobs.cancel_running_job(job_name)
        run_id = jobs.execute_run_now(path_to_project_code, job_name)
        run_ids.append(run_id)

    string_run_ids = ",".join(str(run_id) for run_id in run_ids)
    click.echo(
        f"##vso[task.setvariable variable=run_ids;isOutput=true]{string_run_ids}"
    )


@deployment.command()
def get_monitor_authorities() -> str:
    _azure_vault_client = AzureVaultClient()
    azure_functions_url = _azure_vault_client.monitor_jobs_azure_functions_url
    ado_token = _azure_vault_client.azure_dev_ops_token
    databricks_base_uri = jobs.databricks_base_uri
    databricks_token = jobs.databricks_token
    click.echo(
        f"##vso[task.setvariable variable=azure_functions_url;isOutput=true]{azure_functions_url}"
    )
    click.echo(
        f"##vso[task.setvariable variable=ado_token;isOutput=true]{ado_token}"
    )
    click.echo(
        f"##vso[task.setvariable variable=databricks_base_uri;isOutput=true]{databricks_base_uri}"
    )
    click.echo(
        f"##vso[task.setvariable variable=databricks_token;isOutput=true]{databricks_token}"
    )


@deployment.command()
@click.option("-f", "--file-name", type=str)
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
    logger.info(f"is_index_changed: {is_index_changed}")
    click.echo(
        f"##vso[task.setvariable variable=is_index_changed]{is_index_changed}"
    )
    click.echo(
        f"##vso[task.setvariable variable=is_index_changed;isOutput=true]{is_index_changed}"
    )


# endregion
@click.group()
@click.version_option()
def cli():
    pass


cli.add_command(index_operations)
cli.add_command(deployment)
cli.add_command(databricks)


if __name__ == "__main__":
    cli()
