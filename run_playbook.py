#!/bin/env python

from common.methods import set_progress

from connectors.ansible.models import AnsibleConf, AnsibleGroup
from functools import reduce


def run(job, logger=None, **kwargs):
    """
    Run the provided playbook on an Ansible management server.
    If servers are attached to the job, this will iterate over each server and
    run the playbook on the management server associated with the connector for
    the server's env. The playbook will be limited to the individual server, as
    it might need to be run on different management servers if the servers
    exist in different environments with different Ansible connectors.

    If no servers are attached to the job, then an AnsibleConf must be provided
    as a kwarg. In that case, the playbook can be targeted to a specific inventory
    group. If no group is provided, it will be run against all servers
    in that AnsibleConf's inventory.
    """

    # Fetch playbook and timeout from kwargs, but default to action inputs.
    # This allows this action to be called both as .run_as_job() and as a server action.
    playbook_path = kwargs.get("playbook_path", "{{playbook_path}}")
    timeout_as_string = kwargs.get("timeout", "{{script_timeout}}")
    if timeout_as_string == "":
        # there is no timeout defined, default to 120 seconds
        timeout_as_string = "120"

    servers_to_run_on = job.server_set.all()

    if not servers_to_run_on:
        conf_id = kwargs.get("ansibleconf_id", None)
        conf = AnsibleConf.objects.get(id=conf_id)
        if conf is None:
            return (
                "FAILURE",
                "Can't run this action without an Ansible configuration manager!",
                "",
            )

        limit = kwargs.get("limit", None)
        set_progress(
            "Running playbook '{}' on '{}' servers".format(playbook_path, limit)
        )
        output = conf.run_playbook(
            playbook_path, limit=limit, timeout=int(timeout_as_string)
        )
        set_progress(output)

    for server in servers_to_run_on:
        try:
            conf = server.environment.get_connector_confs()[0].cast()
        except IndexError:
            return (
                "FAILURE",
                "No Ansible configuration manager found for this server.",
                "",
            )

        set_progress(
            "Running playbook '{}'' on server '{}'".format(
                playbook_path, server.hostname
            )
        )
        output = conf.run_playbook(
            playbook_path,
            limit=server.ip,
            timeout=int(timeout_as_string),
            server=server,
        )
        set_progress(output)

    return "", "", ""


def get_playbooks_for_server(server):
    """
    Return a set of Ansible playbooks that should be available to the given server.
    """
    inventory_groups = AnsibleGroup.objects.filter(
        cb_application__in=server.applications.all()
    )
    available_playbooks = set()
    for inventory_group in inventory_groups:
        [
            available_playbooks.add(playbook)
            for playbook in inventory_group.available_playbooks.all()
        ]
    return available_playbooks


def generate_options_for_playbook_path(
    server=None, servers=None, inventory_group=None, **kwargs
):
    """
    Returns a list of playbook path choices, which are used as options for choosing which
    playbook to execute.
    """
    available_playbooks = set()
    if servers:
        # If multiple servers have been provided, return a list of playbooks common to all of them.
        available_playbook_sets = [get_playbooks_for_server(svr) for svr in servers]
        available_playbooks = reduce(set.intersection, available_playbook_sets)
    elif server:
        available_playbooks = get_playbooks_for_server(server)
    else:
        # Neither servers nor server were provided, so if an inventory_group was provided
        if inventory_group:
            available_playbooks = inventory_group.available_playbooks.all()

    if len(available_playbooks) == 0:
        # Perhaps this should raise an error instead, as the form might still be submittable.
        return [("---", "No playbooks available for the selected server(s)")]

    return [(playbook.path, playbook.name) for playbook in available_playbooks]
