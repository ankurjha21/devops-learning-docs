#!/bin/env python

from common.methods import set_progress

from connectors.ansible.models import AnsibleConf


def run(job, logger=None, **kwargs):
    """
    Run the provided command on an Ansible management server.
    If servers are attached to the job, this will iterate over each server and
    run the command on the management server associated with the connector for
    the server's env. The command will be limited to the individual server, as
    it might need to be run on different management servers if the servers
    exist in different environments with different Ansible connectors.

    If no servers are attached to the job, then an AnsibleConf must be provided
    as a kwarg. In that case, the command can be targeted to a specific inventory
    group. If no group is provided, it will be run against 'all' servers
    in that AnsibleConf's inventory.
    """

    # Fetch the command, args and timeout from kwargs, but default to action inputs.
    # This allows this action to be called both as .run_as_job() and as a server action.
    module = kwargs.get("module", "{{module}}")
    module_args = kwargs.get("module_arguments", "{{module_arguments}}")
    timeout_as_string = kwargs.get("timeout", "{{script_timeout}}")
    if timeout_as_string == "":
        # no timeout given, default to 120 seconds
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

        target = kwargs.get("inventory_group", "all")
        set_progress("Running command '{}' on '{}' servers".format(module, target))
        output = conf.run_adhoc_command(
            module,
            target=target,
            module_args=module_args,
            timeout=int(timeout_as_string),
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
            "Running command '{}'' on server '{}'".format(module, server.hostname)
        )
        output = conf.run_adhoc_command(
            module,
            target=server.hostname,
            module_args=module_args,
            timeout=int(timeout_as_string),
            server=server,
        )
        set_progress(output)

    return "", "", ""
