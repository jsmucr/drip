#!/usr/bin/env python
# -*- coding: utf-8 -*-

__license__ = "EPL Eclipse Public License"
__docformat__ = 'reStructuredText'

import hashlib
import logging
import io
import json
import os
import pathlib
import platform
import subprocess
import sys
import typing

import psutil


class CJavaPrefork(object):
    DRIP_JAR = "drip.jar"
    DRIP_MAIN = "org.flatland.drip.Main"
    # application.name is just useful in case you need to quickly identify stale processes with jps
    DEFAULT_OPTIONS = ("-Djava.awt.headless=true", "-Dapplication.name=CJavaPrefork")
    # Default number of JVM instances to be forked
    DEFAULT_INSTANCES_NR = 2

    STATUS_FILE = "preforkj_status.json"

    CLASSPATH_SEPARATOR = ';' if platform.system() == 'Windows' else ":"

    # Child process status. We don't use enum as it would require a custom JSON serializer
    STATUS_IDLE = 0
    STATUS_COMMAND_SENT = 1

    def __init__(self, working_dir: str, drip_jar_path: typing.Optional[str] = None, java_path: str = "java"):
        """
        Initialize the Java preforker object.

        :param working_dir: where to create control FIFOs and child stdout/stderr
        :param drip_jar_path: full path to drip JAR. If not supplied will look in working_dir
        :param java_path: full path to java interpreter
        """
        super().__init__()

        self.logger = logging.getLogger("java_prefork")
        # Full path of java interpreter, since we don't rely on shell to look into PATH
        self._java = java_path

        self._working_dir = pathlib.Path(working_dir)
        try:
            self._working_dir.mkdir()
        except FileExistsError:
            pass

        if drip_jar_path:
            self._drip_jar_path = str(pathlib.Path(drip_jar_path).absolute())
        else:
            self._drip_jar_path = str((self._working_dir / self.DRIP_JAR).absolute())

        # All currently configured groups
        # key = group name
        # value = classpath for all the JVM instances belonging to the group
        self._groups = {}  # type: typing.Dict[str, str]
        try:
            self._status_fd = open(self.STATUS_FILE, "r+")
            # Current subprocesses status, indexed by classpath. Setting the classpath at runtime is
            # too much an hassle
            # {
            #   "group_name":
            #       [
            #           {
            #               "pid": 12345,
            #               "status": 0 - Idle | 1 - Command sent  # Child process status
            #               "unique_id": "group name sha1.process index"
            #               "control": "subprocess control fifo path"
            #               "commandline": "something"
            #               "classpath": "classpath_X"  # The same for all the JVM instances in the same group
            #           },
            #           ...
            #       ],
            #   ...
            # }
            self._status = json.load(self._status_fd) \
                # type: typing.Dict[str, typing.List[typing.Dict[str, typing.Union[str, int]]]]
            for group_name, processes in self._status.items():
                if not processes:
                    continue
                if group_name not in self._groups:
                    self._groups[group_name] = processes[0]["classpath"]

        except (FileNotFoundError, json.JSONDecodeError):
            self._status_fd = open(self.STATUS_FILE, "w+")
            self._status = {}  # type: typing.Dict[str, typing.List[typing.Dict[str, typing.Union[str, int]]]]

        self.update_status()

    def __str__(self) -> str:
        stream = io.StringIO()
        for group_name, processes in self._status.items():
            if processes:
                stream.write("-- GROUP '{}' - classpath '{}'\n".format(group_name, processes[0]["classpath"]))
                for process_info in processes:
                    stream.write(" | PID {} - status {} - UID {}\n".format(
                        process_info["pid"],
                        "IDLE" if process_info["status"] == self.STATUS_IDLE else "COMMAND_SENT",
                        process_info["unique_id"],
                    ))
        return stream.getvalue()

    def prefork_jvm(self, group_name: str, classpath: typing.Optional[str] = None, n_instances: int = DEFAULT_INSTANCES_NR, *args) -> None:
        """ Make sure that `n_instances` JVM instances are available in `group_name` group. Fork the missing instances
            with `classpath' classpath (if any).

        :param group_name: JVM group
        :param classpath: classpath of the newly forked JVM
        :param n_instances: total number of JVM instances we want in this group
        :param args: optional args to the newly forked JVMs
        """
        idle_count = n_instances
        # Extend classpath with drip JAR
        if classpath:
            # New classpath supplied, overwrite existing
            extended_classpath = "{}{}{}".format(classpath, self.CLASSPATH_SEPARATOR, str(self._drip_jar_path))
        elif group_name in self._groups:
            # Existing group, no classpath supplied, use existing
            extended_classpath = self._groups[group_name]
        else:
            # No classpath supplied
            extended_classpath = str(self._drip_jar_path)
        self._groups[group_name] = extended_classpath

        try:
            existing = self._status[group_name]
        except KeyError:
            pass
        else:
            for process_info in existing:
                if process_info["status"] == self.STATUS_IDLE:
                    idle_count -= 1
        finally:
            self.logger.info("Preforking %d instances with classpath '%s'", idle_count, extended_classpath)
            while idle_count > 0:
                self._run_jvm(group_name, extended_classpath, *args)
                idle_count -= 1
            self._status_fd.seek(0, 0)
            json.dump(self._status, self._status_fd)

    def _run_jvm(self, group_name: str, classpath: str, *args) -> None:
        processes = self._status.setdefault(group_name, [])
        group_name_hash = hashlib.sha256()
        group_name_hash.update(group_name.encode('utf8'))
        process_position = len(processes)
        unique_id = "{}.{}".format(group_name_hash.hexdigest(), process_position)
        fifo_name = "control.{}".format(unique_id)
        [pathlib.Path(item).unlink(True) for item in
         (fifo_name, "stdout.{}".format(unique_id), "stderr.{}".format(unique_id))]

        os.mkfifo(fifo_name)

        commandline = (
            self._java, *self.DEFAULT_OPTIONS, *args, "-classpath", classpath,
            self.DRIP_MAIN, unique_id, str(self._working_dir.absolute())
        )

        # We don't use the shell so we can get the spawned process PID
        pipe = subprocess.Popen(commandline, shell=False, start_new_session=True)
        if pipe.poll() is None:
            process_info = {
                "pid": pipe.pid,
                "status": self.STATUS_IDLE,
                "unique_id": unique_id,
                "control": fifo_name,
                "commandline": " ".join(commandline),
                "classpath": classpath
            }
            processes.append(process_info)
            self.logger.debug("Created subprocess in position %d: %s", process_position, process_info)
        else:
            raise ChildProcessError(
                "Process '{}' exited unexpectedly with return code {}".format(commandline, pipe.returncode))

    def exec_(
            self,
            group: str,
            main_class: str,
            target_program_args: typing.Sequence[str] = None,
            target_system_properties: typing.Sequence[str] = None,
            target_environment: typing.Dict[str, str] = None
    ) -> int:
        """ Execute a 'main_class' within any of the available JVMs using classpath 'with_classpath'

        :param group: Select a JVM from specified group
        :param main_class: target program main class to be run
        :param target_program_args: target program command line arguments
        :param target_system_properties: target JVM properties to be set
        :param target_environment: target JVM environment to be merged in
        :return: selected JVM PID
        """
        try:
            processes = self._status[group]
        except KeyError:
            raise ProcessLookupError("No preforked process available in group '{}'".format(group))

        for process_info in processes:
            if process_info["status"] == self.STATUS_IDLE:
                with open(process_info["control"], "w") as fd:
                    fd.write("{}:{},".format(len(main_class), main_class))  # Target program main class

                    program_args = "\u0000".join(target_program_args) if target_program_args else ""
                    fd.write(
                        "{}:{},".format(len(program_args), program_args))  # Target program args, delimited by \u0000

                    system_properties = "\u0000".join(target_system_properties) if target_system_properties else ""
                    fd.write("{}:{},".format(len(system_properties),
                                             system_properties))  # System properties to be set before starting target program

                    environment = "\u0000".join(
                        ["=".join((k, v)) for k, v in target_environment.items()]) if target_environment else ""
                    fd.write("{}:{},".format(len(environment), environment))  # Target program environment

                process_info["status"] = self.STATUS_COMMAND_SENT
                self._status_fd.seek(0, 0)
                json.dump(self._status, self._status_fd)
                self.logger.info(
                    "PID %d executes class %s(%s) with classpath '%s' system properties %s, environment %s",
                    process_info["pid"], main_class, repr(target_program_args), process_info["classpath"],
                    repr(system_properties), repr(environment)
                )
                return process_info["pid"]

        raise ProcessLookupError("No preforked idle process available in group '{}'".format(group))

    def update_status(self) -> None:
        """ Check if the processes saved in status are still running and update status.
            If not running remove control fifo and stdout/stderr
        """
        for group, processes in self._status.items():
            to_reap = []
            for process_info in processes:
                if not psutil.pid_exists(process_info["pid"]):
                    to_reap.append(process_info)
                    fifo_name = process_info["control"]
                    unique_id = process_info["unique_id"]
                    [pathlib.Path(item).unlink(True) for item in
                     (fifo_name, "stdout.{}".format(unique_id), "stderr.{}".format(unique_id))]
                    self.logger.info("PID %d does not exist, removing from group '%s' pool", process_info['pid'],
                                     group)

            [processes.remove(item) for item in to_reap]

        self._status_fd.seek(0, 0)
        json.dump(self._status, self._status_fd)

    # Properties --------------------------------------------------------------
    @property
    def java_path(self) -> str:
        return self._java

    @java_path.setter
    def java_path(self, full_java_path: str):
        self._java = full_java_path
    # ~Properties -------------------------------------------------------------


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    # logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    # parser = argparse.ArgumentParser(prog="drip")
    # parser.add_argument("command", choices=("ps", "version", "kill"), nargs="?", default=None)
    # drip_command, java_args = parser.parse_known_args()

    # drip = CJavaPrefork(".")
    # drip.java_path = "/opt/JRE1.8.0/bin/java"
    # drip.prefork_jvm("jasperstarter", "/home/corbelli/compile/jasperstarter-3.5/lib/jasperstarter.jar")
    # drip.prefork_jvm("test")
    # print(drip)
    # drip.exec_(
    #     "jasperstarter",
    #     "de.cenote.jasperstarter.App",
    #     ("-v", "@jasper.conf",)
    # )
    # print(drip)