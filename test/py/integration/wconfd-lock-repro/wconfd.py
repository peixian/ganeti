#
#

# Copyright (C) 2026 the Ganeti project
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
# IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""Load a built Ganeti checkout and run a temporary ganeti-wconfd."""
import json
import os
import shutil
import subprocess
import sys
import time
import types

HOST = "node1.example.com"
LOCKING_RPCS = ("updateDisk", "updateInstance", "lockConfig")
_DAEMONS = ("MASTERD", "CONFD", "RAPI", "LUXID", "WCONFD", "KVMD", "METAD",
            "MOND", "NODED")


def load_ganeti_from_build(build_dir, workdir):
  """Import a built checkout with a temporary virtual-cluster root."""
  build_lib = os.path.join(os.path.abspath(build_dir), "lib")
  for filename in ("_constants.py", "_vcsversion.py", "opcodes.py",
                   "_generated_rpc.py", "rpc/stub/wconfd.py"):
    if not os.path.isfile(os.path.join(build_lib, filename)):
      raise RuntimeError(
        "missing generated file %s; configure and build the source tree first"
        % filename)

  root = os.path.join(workdir, HOST)
  os.environ["GANETI_ROOTDIR"] = root
  os.environ["GANETI_HOSTNAME"] = HOST

  pkg_dir = os.path.join(workdir, "py")
  os.mkdir(pkg_dir)
  os.symlink(build_lib, os.path.join(pkg_dir, "ganeti"))
  sys.path.insert(0, pkg_dir)
  sys.dont_write_bytecode = True

  # Ganeti computes paths on import, so import it after setting the environment.
  from ganeti import config, constants, netutils, objects, pathutils
  from ganeti import serializer, utils
  from ganeti.cmdlib.instance_storage import TLReplaceDisks
  from ganeti.rpc import client as rpc_client
  from ganeti.utils import livelock

  netutils.GetHostname = lambda name=None, family=None: types.SimpleNamespace(
    name=name or HOST, ip="127.0.0.1",
  )

  # Measure time between the first poll and the start of a successful poll.
  # This includes RPC time for unsuccessful polls, but not the final write.
  orig_invoke = rpc_client.AbstractStubClient._GenericInvoke

  def timed_invoke(self, method, *args):
    now = time.monotonic()
    if method in LOCKING_RPCS and getattr(self, "_wait_start", None) is None:
      self._wait_start = now
    result = orig_invoke(self, method, *args)
    if method in LOCKING_RPCS:
      if result is not None:
        wait = now - self._wait_start
        self.max_lock_wait = max(getattr(self, "max_lock_wait", 0.0), wait)
        self._wait_start = None
      else:
        wait = time.monotonic() - self._wait_start
        self.max_lock_wait = max(getattr(self, "max_lock_wait", 0.0), wait)
    return result

  rpc_client.AbstractStubClient._GenericInvoke = timed_invoke
  return types.SimpleNamespace(
    config=config, constants=constants, objects=objects, serializer=serializer,
    utils=utils, livelock=livelock, pathutils=pathutils,
    root=root,
    replace_disks=TLReplaceDisks._UpdateDisksSecondary,
  )


class Daemon:
  """Run WConfd in a private user/mount namespace and virtual-cluster root."""

  def __init__(self, wconfd_bin, ganeti, config_dict):
    self.wconfd_bin = os.path.abspath(wconfd_bin)
    self.ganeti = ganeti
    self.root = ganeti.root
    self.config_dict = config_dict
    self.proc = None

  def __enter__(self):
    paths = self.ganeti.pathutils
    constants = self.ganeti.constants
    shutil.rmtree(self.root, ignore_errors=True)
    for path in (paths.DATA_DIR, paths.SOCKET_DIR, paths.LIVELOCK_DIR,
                 paths.LOG_DIR):
      os.makedirs(path)
    with open(paths.CLUSTER_CONF_FILE, "w") as config_file:
      json.dump(self.config_dict, config_file, separators=(",", ":"))
    open(paths.NODED_CERT_FILE, "w").close()

    # Map the configured daemon accounts to namespace root without host changes.
    users = {getattr(constants, daemon + "_USER") for daemon in _DAEMONS}
    groups = {getattr(constants, daemon + "_GROUP") for daemon in _DAEMONS}
    groups.update((constants.DAEMONS_GROUP, constants.ADMIN_GROUP))
    passwd_path = os.path.join(self.root, "passwd")
    group_path = os.path.join(self.root, "group")
    with open(passwd_path, "w") as passwd_file:
      for user in sorted(users | {"root"}):
        passwd_file.write("%s:x:0:0::/:/bin/sh\n" % user)
    with open(group_path, "w") as group_file:
      for group in sorted(groups | {"root"}):
        group_file.write("%s:x:0:\n" % group)

    env = dict(os.environ, GANETI_ROOTDIR=self.root, GANETI_HOSTNAME=HOST)
    log_path = os.path.join(paths.LOG_DIR, "wconfd.log")
    cmd = (
      'mount --bind "$1" /etc/passwd && mount --bind "$2" /etc/group && '
      'exec "$3" -f --no-user-checks --no-voting --yes-do-it --force-node'
    )
    with open(log_path, "w") as log:
      self.proc = subprocess.Popen(
        ["unshare", "-r", "-m", "sh", "-c", cmd, "_", passwd_path, group_path,
         self.wconfd_bin],
        env=env, stdout=log, stderr=subprocess.STDOUT,
      )
    try:
      from ganeti import wconfd as wc
      for _ in range(600):
        if self.proc.poll() is not None:
          with open(log_path) as log:
            raise RuntimeError("ganeti-wconfd exited:\n" + log.read())
        if os.path.exists(paths.WCONFD_SOCKET):
          try:
            wc.Client().Echo("ready")
            return self
          except Exception:
            pass
        time.sleep(0.1)
      with open(log_path) as log:
        raise RuntimeError("ganeti-wconfd timed out:\n" + log.read())
    except Exception:
      self.__exit__(None, None, None)
      raise

  def __exit__(self, *exc_info):
    if self.proc is not None:
      self.proc.terminate()
      try:
        self.proc.wait(timeout=10)
      except subprocess.TimeoutExpired:
        self.proc.kill()
        self.proc.wait()

  def read_config(self):
    from ganeti import wconfd as wc
    return wc.Client().ReadConfig()

  def cpu_seconds(self):
    """User + system CPU seconds used by WConfd (/proc/<pid>/stat)."""
    with open("/proc/%d/stat" % self.proc.pid) as stat_file:
      fields = stat_file.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
