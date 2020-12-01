#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

    drip = CJavaPrefork(".")
    drip.java_path = r"c:\Program Files (x86)\Java\java-se-8u41-ri\bin\java.exe"
    drip.prefork_jvm("jasperstarter", r"c:\Program Files (x86)\jasperstarter\lib\jasperstarter.jar", 2)
    print(drip)
    drip.exec_(
         "jasperstarter",
         "de.cenote.jasperstarter.App",
         ("-V", )
    )
"""

__license__ = "EPL Eclipse Public License"
__docformat__ = 'reStructuredText'

import hashlib
import logging
import io
import json
import os
import pathlib
import subprocess
import sys
import time
import typing

import psutil

_is_win = sys.platform.startswith('win')
_is_linux = sys.platform.startswith('linux')

if _is_win:
    import win32api
    import win32file
    import win32pipe


class CJavaPrefork(object):
    LOGGER_NAME = "java_prefork"

    DRIP_JAR = "drip.jar"
    DRIP_MAIN = "org.flatland.drip.Main"
    # application.name is just useful in case you need to quickly identify stale processes with jps
    DEFAULT_OPTIONS = ("-Djava.awt.headless=true", "-Dapplication.name=CJavaPrefork")
    # Default number of JVM instances to be forked
    DEFAULT_INSTANCES_NR = 2

    STATUS_FILE = "preforkj_status.json"

    CLASSPATH_SEPARATOR = os.pathsep

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

        self.logger = logging.getLogger(self.LOGGER_NAME)
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

        if _is_win:
            # Our side of the named pipe indexed by child process unique ID
            self._control_handles = {}  # type: typing.Dict[str, int]
            self._mkfifoname = self._mkfifoname_windows
            self._mkfifo = self._mkfifo_windows
            self._unlinkfifo = self._unlinkfifo_windows
        elif _is_linux:
            self._mkfifoname = self._mkfifoname_unix
            self._mkfifo = self._mkfifo_unix
            self._unlinkfifo = self._unlinkfifo_unix
        else:
            raise NotImplementedError("Platform '{}' is currently not supported".format(sys.platform))

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


    def _mkfifoname_unix(self, unique_id: str) -> str:
        return "control.{}".format(unique_id)

    def _mkfifoname_windows(self, unique_id: str) -> str:
        return "\\\\.\\pipe\\preforkj.control.{}".format(unique_id)

    def _mkfifo_unix(self, unique_id: str) -> str:
        fifo_name = self._mkfifoname(unique_id)
        os.mkfifo(self._working_dir / fifo_name)
        return fifo_name

    def _mkfifo_windows(self, unique_id: str) -> str:
        fifo_name = self._mkfifoname(unique_id)
        # "The pipe exists as long as a server or client process has an open handle to the pipe."
        # https://docs.microsoft.com/en-us/windows/win32/api/namedpipeapi/nf-namedpipeapi-disconnectnamedpipe
        # May raise
        handle = win32pipe.CreateNamedPipe(
            fifo_name,
            win32pipe.PIPE_ACCESS_OUTBOUND,
            win32pipe.PIPE_TYPE_BYTE|win32pipe.PIPE_WAIT,
            1,  # Max instances
            256, 256,  # Buffering
            0,
            None
        )
        self._control_handles[unique_id] = handle

        return fifo_name

    def _unlinkfifo_unix(self, unique_id: str):
        fifo_name = self._mkfifoname(unique_id)
        try:
            pathlib.Path(self._working_dir / fifo_name).unlink()
        except OSError:
            pass

    def _unlinkfifo_windows(self, unique_id: str):
        try:
            # We don't have a file but we can disconnect our end of the named pipe
            win32pipe.DisconnectNamedPipe(self._control_handles[unique_id])
        except KeyError:
            pass

    def _waitfifo(self, unique_id: str):
        # Windows only, have to wait for clients to connect
        if _is_win:
            if 0 != win32pipe.ConnectNamedPipe(self._control_handles[unique_id], None):
                raise IOError("PIPE connection failed - {}".format(win32api.GetLastError()))

    def _writefifo(self, process_info, data: typing.Iterable[str]):
        encoded = (("{}:{},".format(len(line), line)).encode('utf8') for line in data)
        if _is_linux:
            with open(process_info["control"], "wb") as fd:
                for line in encoded:
                    self.logger.debug("_writefifo '%s'", line)
                    fd.write(line)
        elif _is_win:
            unique_id = process_info["unique_id"]
            handle = self._control_handles[unique_id]
            for line in data:
                self.logger.debug("_writefifo '%s'", line)
                win32file.WriteFile(handle, line)


    def prefork_jvm(
        self,
        group_name: str,
        classpath: typing.Optional[str] = None,
        n_instances: int = DEFAULT_INSTANCES_NR,
        shutdown_timeout_m: int = 0,
        *args
    ) -> None:
        """ Make sure that `n_instances` JVM instances are available in `group_name` group. Fork the missing instances
            with `classpath' classpath (if any).

        :param group_name: JVM group
        :param classpath: classpath of the newly forked JVM
        :param n_instances: total number of JVM instances we want in this group
        :param shutdown_timeout_m: exit the child process within this timeout in minutes.
            0 (the default) disables the timeout.
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
                self._run_jvm(group_name, extended_classpath, shutdown_timeout_m, *args)
                idle_count -= 1
            self._status_fd.seek(0, 0)
            json.dump(self._status, self._status_fd)

    def _run_jvm(self, group_name: str, classpath: str, shutdown_timeout_m: int, *args) -> None:
        processes = self._status.setdefault(group_name, [])
        group_name_hash = hashlib.sha256()
        group_name_hash.update(group_name.encode('utf8'))
        process_position = len(processes)
        unique_id = "{}.{}".format(group_name_hash.hexdigest(), process_position)
        for item in "stdout.{}".format(unique_id), "stderr.{}".format(unique_id):
            try:
                pathlib.Path(self._working_dir / item).unlink()
            except OSError:
                pass

        self._unlinkfifo(unique_id)
        fifo_name = self._mkfifo(unique_id)

        env = os.environ.copy()
        env["DRIP_SHUTDOWN"] = str(shutdown_timeout_m)
        commandline = (
            self._java, *self.DEFAULT_OPTIONS, *args, "-classpath", classpath,
            self.DRIP_MAIN, unique_id, str(self._working_dir.absolute())
        )
        kwargs = {"shell": False, "start_new_session": True, "env": env}
        if _is_win:
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        self.logger.debug("Forking process with commandline '%s' timeout %d [min]", commandline, shutdown_timeout_m)
        # We don't use the shell so we can get the spawned process PID
        pipe = subprocess.Popen(commandline, **kwargs)
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
            self._waitfifo(unique_id)
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
                program_args = "\u0000".join(target_program_args) \
                        if target_program_args else ""
                system_properties = "\u0000".join(target_system_properties) \
                        if target_system_properties else ""
                environment = "\u0000".join(["=".join((k, v)) for k, v in target_environment.items()]) \
                        if target_environment else ""
                data_lines = (
                    main_class,  # Target program main class
                    program_args,  # Target program args, delimited by \u0000
                    system_properties,  # System properties to be set before starting target program
                    environment  # Target program environment
                )
                self._writefifo(process_info, data_lines)

                process_info["status"] = self.STATUS_COMMAND_SENT
                self._status_fd.seek(0, 0)
                json.dump(self._status, self._status_fd)
                self.logger.info(
                    "PID %d UID %s executes class %s(%s) with classpath '%s' system properties %s, environment %s",
                    process_info["pid"], process_info["unique_id"], main_class, target_program_args,
                    process_info["classpath"], target_system_properties, target_environment
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

                    self._unlinkfifo(unique_id)
                    for item in "stdout.{}".format(unique_id), "stderr.{}".format(unique_id):
                        try:
                            pathlib.Path(item).unlink()
                        except OSError:
                            pass
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
