> Your prayer, answered.
>
> — <cite>Zeus</cite>

# zeus

**zeus** is a tool for performing backups onto another zfs pool which is
physically connected to the machine.

# Quick Start

0. Get **zeus**:  
  `go get github.com/reconquest/zeus/cmd/...`.

1. Create pool named `zbackup`.

2. Mark dataset which you want to backup:  
   `zfs zeus:backup=on <your-dataset>`.

2. Run **zeus** like this:  
   `sudo zeusd backup`.

# Encryption

It is possible to use **zeus** with encrypted filesystems.

**Currently**, only pools with root filesystem encrypted are supported.

To use **zeus** with encryption:

1. Create pool with encrypted root filesystem:  
  `sudo zpool create zbackup -O encryption=on -O keyformat=passphrase -m none <vdev-configuration>`

2. Provide executable named `zfs-encryption-key` in your path which will accept
   single argument with your backup pool name (e.g. `zbackup`) and will output
   encryption key to stdout.

   Example:

   ```bash
   #!/bin/bash
   sudo -u operator sh -c "carcosa -p ~/.secrets/my -cG zfs/$1"
   ```

# Note on holds

**zeus** will enforce additional protection for lately made snapshots by using
`zfs hold` with `zeus` tag to avoid accidental deletion which will make further
incremental backups impossible.

If you need to delete those snapshots, you always can release this lock by using:  
`zfs release zeus <snapshot>`.

# Note on backup filesystem names

By default **zeus** will receive source snapshots into paths like this:  
`zbackup/<hostname>/guid:<guid>/<your-backuped-filesystem>`

* `<hostname>` part can be configured via config file.
* `guid:<guid>` is namespace based on source snapshot GUID which is unique and
  is required to avoid conflicts when you backup parts of the same hierarchy,
  like `z/home/username/data` and `z/home/username`.

# Configuration

**zeus** has two ways of configuration:

* via config file
    * `/etc/zeus/zeusd.conf` if `zeusd` is run by root,
    * `~/.config/zeus/zeusd.conf` if `zeusd` is run by user.
* via `zfs` properties, namespaced with `zeus:`.

Config file is used mostly for static configuration and meant to be changed
just once for initial configuration.

`zfs` properties are meant to be used for specifying which datasets you want to
backup and fine-tune other backup process parameters.

## Config

Config file is written in TOML.

If you want to use different backup pool name you need to change it in config
file.

Check out `zeus.conf.example` for structure of configuration file and options
description.

## `zfs` properties

All **zeus**-related properties are prefixed with `zeus:`.

Following `zfs` properties are supported:

* `zeus:backup`:
    * `on` — enable backup on given filesystem,
    * `off` — disable backup on given filesystem.

* `zeus:housekeeping` (default: `by-count`): specifying which housekeeping
  policy to apply after backup. Housekeeping is process of cleaning up old
  snapshots. **zeus** will attempt to clean up only snapshots managed by
  **zeus**.
    * `none` — do not apply any housekeeping.
    * `by-count` — clean up snapshots when their amount exceeds specified
      numbers. See next for more parameters for `by-count` housekeeping policy.

* `zeus:housekeeping:by-count:keep-on-source` (default: `1`): specifies how
  many snapshots keep on given filesystem. At least one snapshot required for
  incremental backups to work.

* `zeus:housekeeping:by-count:keep-on-target` (default: `10`): specifies how
  many snapshots keep on target backup pool. At least one snapshot required for
  incremental backups to work.


# Testing

It is possible to test how **zeus** works without real backup pool.

1. Create test file-based temporary pool by using:
   ```
   truncate --size 10G test.img
   sudo zpool create zbackup `pwd`/test.img -f -m none
   ```

2. Set `zeus:backup=on` on any of yours filesystems.

3. Run `zeusd backup --no-export`.
