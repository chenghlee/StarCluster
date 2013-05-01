"""
StarCluster plugin to configure a cluster using the PBS/Torque
cluster scheduling/management system.
See: http://www.adaptivecomputing.com/products/open-source/torque/
"""
import starcluster
from paramiko import SFTPError
from starcluster import clustersetup
from starcluster.logger import log
from starcluster.utils import print_timing
from starcluster.exception import SSHError


class TorquePlugin(clustersetup.DefaultClusterSetup):
    """
    This plugin installs PBS/Torque as the cluster resource manager/scheduler.
    (http://www.adaptivecomputing.com/products/open-source/torque/)
    """

    # Packages needed by the PBS/Torque controller
    torque_master_packages = ["torque-server",
                              "torque-scheduler",
                              "torque-client"]

    # Packages needed by a PBS/Torque compute node
    torque_client_packages = ["torque-mom"]

    # Log file for packages installed by this package
    packages_installed_file = "/root/.torque-apt-log"

    # Script provided by the torque installer for basic set up
    torque_init_script = "/usr/share/doc/torque-common/torque.setup"

    # File that contains PBS/Torque controller's hostname
    torque_server_file = "/var/spool/torque/server_name"

    def __init__(self, compute_on_master=True, torque_admin="root"):
        """
        Constructor for the PBS/Torque plugin.

        compute_on_master: True if master should also be a compute node.
        torque_admins: User to make the Torque administrator
        """
        self.compute_on_master = str(compute_on_master).lower() == "true"
        self.torque_admin = torque_admin
        super(self.__class__, self).__init__()

    @print_timing("Installing and configuring PBS/Torque")
    def run(self, nodes, master, user, user_shell, volumes):
        """
        Run PBS/Torque plugin by first configuring the PBS/Torque controller
        (on the master node), then configuring the compute nodes.
        """
        # Configure the PBS/Torque controller
        self._configure_torque_master(master, nodes)

        # Configure each PBS/Torque compute node
        wait_count = 0
        compute_nodes = filter(lambda n: n.alias != master.alias, nodes)
        if self.compute_on_master:
            compute_nodes += master
        for node in compute_nodes:
            wait_count += 1
            self.pool.simple_job(self._configure_torque_node, args=(master, node))
        self.pool.wait(numtasks=wait_count)
        self.pool.shutdown()

        log.info("Your PBS/Torque cluster is now configured.")

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        """
        Method called when adding a node to the cluster
        """
        self._configure_torque_node(master, node)
        return

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        """
        Method called when removing a node from the cluster
        """
        self._deconfigure_torque_node(master, node)
        return

    def _configure_torque_master(self, master, nodes):
        """
        Configure PBS/Torque controller node by:
            1. Uninstalling SGE
            2. Installing PBS/Torque controller packages
            3. Configuring PBS/Torque controller
        """
        log.info("Configuring PBS/Torque controller on " + master.alias)

        # Uninstall SGE
        self._uninstall_sge_master(master)

        # Install Torque packages
        master.apt_command("update")
        for package in self.torque_master_packages:
            log.info("master: installing package " + package)
            master.apt_install(package)

        # Configure the controller
        config_commands = ["service torque-scheduler stop",
                           "service torque-server stop",
                           "echo '%s' > '%s'" % (master.alias,
                                                 self.torque_server_file),
                           "echo 'y' | sh %s %s" % (self.torque_init_script,
                                                    self.torque_admin),
                           "qterm",
                           "service torque-server start",
                           "service torque-scheduler start"]
        for cmd in config_commands:
            log.info("master: executing command: " + cmd)
            master.ssh.execute(cmd, raise_on_failure=True)

        return

    def _configure_torque_node(self, master, node):
        """
        Configure PBS/Torque compute node by:
            1. Uninstalling SGE
            2. Installing PBS/Torque execution packages
            3. Configuring PBS/Torque client
            4. Restarting PBS/Torque services
        """
        log.info("Configuring PBS/Torque node " + node.alias)

        # Uninstall SGE
        self._uninstall_sge_worker(node)

        # Install Torque packages
        node.apt_command("update")
        for package in self.torque_client_packages:
            log.info("node: installing package " + package)
            node.apt_install(package)

        # Register node with master
        cmd = "qmgr -c 'create node %s'" % node.alias
        log.info("master: executing command: " + cmd)
        master.ssh.execute(cmd, raise_on_failure=True)

        # Configure the execution node
        # NOTE: The sleep before starting torque-mom is needed; otherwise,
        #       it doesn't always pick up the new contents of "server_name",
        #       leading to a failed start up and requiring the admin to
        #       manually SSH in to the node to start the service.
        config_commands = ["service torque-mom stop",
                           "echo '%s' > '%s'" % (master.alias,
                                                 self.torque_server_file),
                           "sleep 1; service torque-mom start"]
        for cmd in config_commands:
            log.info("node: executing command: " + cmd)
            node.ssh.execute(cmd, raise_on_failure=True)

        return

    def _deconfigure_torque_node(self, master, node):
        cmd = "qmgr -c 'delete node %s'" % node.alias
        log.info("master: executing command: " + cmd)
        master.ssh.execute(cmd, raise_on_failure=True)
        return

    def _uninstall_sge_master(self, master):
        """
        Uninstall SGE from a master node.
        """
        log.info("Removing SGE from " + master.alias)
        master.ssh.execute(
                'cd /opt/sge6/; echo y | /opt/sge6/inst_sge -ux all',
                ignore_exit_status=True)
        return

    def _uninstall_sge_worker(self, node):
        """
        Uninstall SGE from a worker node.
        """
        log.info("Removing SGE from " + node.alias)
        if not node.ssh.isdir('/opt/sge6'):
            log.info('SGE already uninstalled.')
        else:
            node.ssh.execute('/opt/sge6/inst_sge -ux',
                             ignore_exit_status=True)
        return
