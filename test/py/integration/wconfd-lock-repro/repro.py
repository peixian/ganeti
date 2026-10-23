#!/usr/bin/python3
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

"""Compare DRBD secondary-node config updates under WConfd lock contention.

The before function preserves the old config-update loop. The current run
calls TLReplaceDisks._UpdateDisksSecondary from this checkout. Both use
Ganeti's real ConfigWriter and a temporary ganeti-wconfd, without performing
a full replace-disks operation.

The before loop saves logical_id, then calls SetDiskNodes to update nodes.
SetDiskNodes takes the exclusive config lock and transfers the whole config.
A timeout between these writes can leave the two fields naming different
secondary nodes. The fix saves both fields with one disk Update.

All cluster data comes from Ganeti's public test fixture and synthetic copies.
See README.md for build requirements, usage, and interpretation.
"""
import argparse
import copy
import json
import multiprocessing
import os
import shutil
import tempfile
import time
import types
import wconfd

MP = multiprocessing.get_context("fork")
OLD_NODES = ["node1", "node3"]
NEW_NODES = ["node1", "node2"]
GANETI = None


def replace_disks_before(self, iv_names, feedback_fn):
  """The update loop before the fix."""
  new_nodes = [self.instance.primary_node, self.new_node_uuid]
  for disk, _, new_logical_id in iv_names.values():
    disk.logical_id = new_logical_id
    # Saves logical_id and releases the config lock; disk.nodes still names
    # the old secondary until SetDiskNodes finishes.
    self.cfg.Update(disk, feedback_fn)
    # Re-locks WConfd exclusively and round-trips the full cluster config.
    # If waiting for this lock times out after 30 s, the disk is left split.
    self.cfg.SetDiskNodes(disk.uuid, new_nodes)
  self.cfg.Update(self.instance, feedback_fn)


def make_config(num_instances=1950):
  """Clone instance1 from test/data/cluster_config_2.16.json into N DRBD VMs."""
  seed = os.path.join(os.path.dirname(__file__),
                      "../../../data/cluster_config_2.16.json")
  with open(seed) as f:
    raw = json.load(f)

  # Upgrade the 2.16 fixture to the current config version using Ganeti's own
  # ConfigData.UpgradeConfig(), and name the 3 nodes "node1".."node3".
  raw["version"] = GANETI.constants.CONFIG_VERSION
  cfg_obj = GANETI.objects.ConfigData.FromDict(raw)
  cfg_obj.UpgradeConfig()
  cfg = json.loads(GANETI.serializer.DumpJson(
    cfg_obj.ToDict(_with_private=True),
    private_encoder=GANETI.serializer.EncodeWithPrivateFields,
  ))
  cfg["cluster"].update(master_node="node1", tcpudp_port_pool=[])
  node_tmpl = next(iter(cfg["nodes"].values()))
  cfg["nodes"] = {}
  for idx, name in enumerate(["node1", "node2", "node3"], 1):
    node = copy.deepcopy(node_tmpl)
    node.update(uuid=name, name="%s.example.com" % name,
                primary_ip="127.0.0.%d" % idx, secondary_ip="127.0.1.%d" % idx,
                offline=False)
    cfg["nodes"][name] = node

  inst_tmpl = next(i for i in cfg["instances"].values()
                   if i["name"] == "instance1.example.com")
  disk_tmpl = cfg["disks"][inst_tmpl["disks"][0]]
  secret = raw["disks"][inst_tmpl["disks"][0]]["logical_id"][5]

  cfg["instances"], cfg["disks"] = {}, {}
  for i in range(num_instances):
    inst = copy.deepcopy(inst_tmpl)
    inst.update(uuid="inst-%d" % i, name="inst-%d.example.com" % i,
                primary_node="node1", disks=[])
    inst["nics"][0].update(
      uuid="nic-%d" % i, mac="aa:00:00:%02x:%02x:%02x" % (0, i // 256, i % 256)
    )
    for d in range(3):
      minor = 3 * i + d
      disk = copy.deepcopy(disk_tmpl)
      disk.update(
        uuid="disk-%d-%d" % (i, d), iv_name="disk/%d" % d,
        nodes=list(OLD_NODES),
        logical_id=["node1", "node3", 11000 + minor, minor, minor, secret],
      )
      for c_idx, child in enumerate(disk["children"]):
        child.update(uuid="child-%d-%d-%d" % (i, d, c_idx),
                     nodes=list(OLD_NODES),
                     logical_id=["xenvg", "lv-%d-%d-%d" % (i, d, c_idx)])
      cfg["disks"][disk["uuid"]] = disk
      inst["disks"].append(disk["uuid"])
    cfg["instances"][inst["uuid"]] = inst
  return cfg


def vm_disks(cfg, inst_idx):
  return [cfg["disks"]["disk-%d-%d" % (inst_idx, d)] for d in range(3)]


def is_split_vm(cfg, inst_idx):
  """True if a VM's disks don't all have the same logical_id[:2] and nodes."""
  node_pairs = set()
  for disk in vm_disks(cfg, inst_idx):
    node_pairs.add(tuple(disk["logical_id"][:2]))
    node_pairs.add(tuple(disk["nodes"]))
  return len(node_pairs) > 1


def format_split_vm(cfg, inst_idx):
  lines = ["  Example split VM in WConfd's config (inst-%d):" % inst_idx]
  for disk in vm_disks(cfg, inst_idx):
    lid, nodes = disk["logical_id"][:2], disk["nodes"]
    if lid != nodes:
      suffix = "  <-- disagrees!"
    elif nodes != NEW_NODES:
      suffix = "  <-- still on node3!"
    else:
      suffix = ""
    lines.append("    %s: logical_id[:2]=%s  nodes=%s%s" % (
      disk["uuid"], lid, nodes, suffix))
  return "\n".join(lines)


def _run_worker(args):
  idx, update_fn = args
  name = "job-%d" % idx
  inst_uuid = "inst-%d" % idx
  cfg = GANETI.config.GetConfig(name, GANETI.livelock.LiveLock(name))
  inst = cfg.GetInstanceInfo(inst_uuid)
  iv_names = {}
  for d_idx, dev in enumerate(cfg.GetInstanceDisks(inst_uuid)):
    new_logical_id = tuple(NEW_NODES) + tuple(dev.logical_id[2:])
    iv_names[d_idx] = (dev, dev.children, new_logical_id)

  tasklet = types.SimpleNamespace(
    lu=types.SimpleNamespace(LogInfo=lambda *a, **kw: None),
    cfg=cfg, instance=inst, new_node_uuid="node2",
  )
  ok = True
  try:
    update_fn(tasklet, iv_names, lambda *_: None)
  except GANETI.utils.RetryTimeout:
    ok = False
  return ok, getattr(cfg._wconfd, "max_lock_wait", 0.0)


def run_batch(wconfd_bin, cfg_dict, writers, update_fn):
  """Run concurrent config-update loops through ConfigWriter."""
  with wconfd.Daemon(wconfd_bin, GANETI, cfg_dict) as daemon:
    t0, cpu0 = time.monotonic(), daemon.cpu_seconds()
    with MP.Pool(writers) as pool:
      outcomes = pool.map(
        _run_worker, [(i, update_fn) for i in range(writers)]
      )
    wall, cpu = time.monotonic() - t0, daemon.cpu_seconds() - cpu0
    final_cfg = daemon.read_config()

  split_vms = [i for i in range(writers) if is_split_vm(final_cfg, i)]
  example = format_split_vm(final_cfg, split_vms[0]) if split_vms else None
  failed = sum(not ok for ok, _ in outcomes)
  max_wait = max(w for _, w in outcomes)
  return failed, len(split_vms), max_wait, wall, cpu, example


def check_equivalence(wconfd_bin):
  """Compare config content on one VM, ignoring timestamps and write count."""
  def run_single(update_fn):
    cfg_dict = make_config(num_instances=1)
    with wconfd.Daemon(wconfd_bin, GANETI, cfg_dict) as daemon:
      ok, _ = _run_worker((0, update_fn))
      assert ok, "single-instance config update timed out"
      return daemon.read_config()

  before = run_single(replace_disks_before)
  current = run_single(GANETI.replace_disks)
  for cfg in (before, current):
    cfg["cluster"]["mtime"] = 0
    for inst in cfg["instances"].values():
      inst["mtime"] = 0
    for disk in cfg["disks"].values():
      disk["mtime"] = 0
  # Dropping 3 SetDiskNodes calls saves 3 full-config writes (serial_no - 3).
  assert current["serial_no"] == before["serial_no"] - 3
  for key in ("disks", "instances", "nodes", "cluster"):
    assert before[key] == current[key], key


def run_table(wconfd_bin, cfg_dict, writers_list, label, update_fn):
  print("%-10s %7s %6s %5s %10s %8s %11s %9s" % (
    "mode", "writers", "failed", "split", "max_wait", "wall", "wconfd_cpu",
    "1core_%"))
  example_split = None
  for writers in writers_list:
    failed, split, max_wait, wall, cpu, example = run_batch(
      wconfd_bin, cfg_dict, writers, update_fn,
    )
    print("%-10s %7d %6d %5d %9.1fs %7.1fs %10.1fs %8.1f%%" % (
      label, writers, failed, split, max_wait, wall, cpu,
      100.0 * cpu / wall), flush=True)
    if example and example_split is None:
      example_split = example
  if example_split:
    print("\n" + example_split)


def main():
  global GANETI
  parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
  parser.add_argument("--build-dir", required=True,
                      help="configured and built Ganeti directory")
  parser.add_argument("--wconfd", required=True,
                      help="matching ganeti-wconfd binary")
  parser.add_argument("--instances", type=int, default=1950,
                      help="3-disk DRBD instances in config (1950 ~= 5.0 MB)")
  parser.add_argument("--writers", type=int, nargs="+", default=[1, 4, 8],
                      help="concurrent config-update loops to run")
  args = parser.parse_args()
  if args.instances < 1 or any(n < 1 for n in args.writers):
    parser.error("--instances and --writers must be positive")
  if max(args.writers) > args.instances:
    parser.error("--instances must be at least the largest --writers value")

  # Unix-domain socket paths must fit within the kernel's short length limit.
  workdir = tempfile.mkdtemp(prefix="wconfd-repro-", dir="/tmp")
  try:
    GANETI = wconfd.load_ganeti_from_build(args.build_dir, workdir)
    cfg = make_config(args.instances)
    mb = len(json.dumps(cfg, separators=(",", ":"))) / 1e6
    print("1. Built %.2f MB config (%d 3-disk DRBD VMs on [node1, node3])" % (
      mb, args.instances))

    print("\n2. Before: disk fields saved in separate calls:")
    run_table(args.wconfd, cfg, args.writers, "before", replace_disks_before)

    print("\n3. Current source: TLReplaceDisks._UpdateDisksSecondary:")
    check_equivalence(args.wconfd)
    print("   1-VM config equivalence check: PASS")
    run_table(args.wconfd, cfg, args.writers, "current", GANETI.replace_disks)
  finally:
    shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
  main()
