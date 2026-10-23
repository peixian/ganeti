# WConfd config-lock reproducer

This harness compares the old two-step DRBD secondary-node update loop with
`TLReplaceDisks._UpdateDisksSecondary` from the current checkout. Both paths
run against the checkout's real `ConfigWriter` and a temporary
`ganeti-wconfd`, using synthetic instances expanded from Ganeti's public
`test/data/cluster_config_2.16.json` fixture.

## Build and run

Install Ganeti's build dependencies described in `INSTALL`, including Cabal
2.4 or newer as required by `ganeti.cabal`. Python 3.6 also needs the
`dataclasses` backport; Python 3.7 and newer include it. You also need Linux
with `unshare`, `mount`, and unprivileged user and mount namespaces enabled.
From a Git checkout, run:

```sh
./autogen.sh
./configure
make wconfd-lock-repro
```

The Make target builds `exe/ganeti-wconfd` and the generated Python modules
using the checkout's normal Autotools, Make, and Cabal setup, then runs the
reproducer against them. No Ganeti installation or cluster setup is needed.

For a quick check, run:

```sh
make wconfd-lock-repro WCONFD_REPRO_ARGS='--instances 1 --writers 1'
```

The default workload expands the fixture to 1,950 instances with three DRBD
disks each and runs 1, 4, and 8 concurrent writers. To override those counts,
pass `--instances` and `--writers` through `WCONFD_REPRO_ARGS`. Each writer
updates the three disks of one instance, so `--instances` must be at least
the largest writer count.

Run the target as an ordinary user. The helper creates a short temporary
working directory under `/tmp` so the Unix-domain socket path fits within the
kernel limit, and bind-mounts temporary `passwd` and `group` files over
`/etc/passwd` and `/etc/group` inside the private mount namespace without
modifying host account files. The script runs only the config-update step and
does not run a full `replace-disks` job or create VMs or block devices.

## Reading the output

- `before` runs a copy of the old loop, which writes `logical_id` with
  `cfg.Update` before calling `cfg.SetDiskNodes`.

- `current` runs `TLReplaceDisks._UpdateDisksSecondary` from the checkout.

- `failed` counts workers whose update raised `RetryTimeout`.

- `split` counts instances whose disk node pairs disagree within a disk or
  across their three disks in WConfd's `ReadConfig` output. The script prints
  an example when it finds one.

- `max_wait` measures the longest time a worker waited for a config-locking
  RPC, from the first lock poll to the start of a successful poll, or to the
  end of the final unsuccessful poll on timeout. It includes failed poll RPCs
  and retry sleeps, and excludes the duration of the final successful RPC.

- `wall` is elapsed batch time. `wconfd_cpu` is WConfd's user plus system CPU
  time, and `1core_%` expresses that CPU time as a percentage of `wall` on one
  core.

A single-instance check compares disk, instance, node, and cluster records,
ignoring modification timestamps. It also checks that the current method
advances the config `serial_no` three fewer times, one for each removed
`SetDiskNodes` call.

This target is a manual stress reproducer and is not collected by pytest or
default CI runs. Whether workers hit `RetryTimeout` depends on machine speed
and scheduling, so timeout counts are observations rather than a deterministic
test assertion. Failed batches stop early and leave work incomplete, so their
wall times do not measure a speedup for equal completed work. The fix writes
both node fields together for each disk, though an instance's disks are still
updated in separate RPCs.

AI assisted with this reproducer and its adaptation.
