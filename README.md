> Your prayer, answered.
>
> — <cite>Zeus</cite>

# zeus

**zeus** is a tool for performing backups onto another zfs pool which is
physically connected to the machine.

# Usage

## Without configuration

**zeus** expects to be able to import `zbackup` pool which can be used for
backup purposes.

- [ ] __TODO__ add note on how to create encrypted pool

Set property `zeus:backup` to `on` on the datasets which you want to backup,
like this:

```bash
sudo zfs set zeus:backup=on z/home
```

Then, run `zeusd` to create a backup for all datasets marked for backup:

```bash
zeusd backup
```

- [ ] __TODO__ write about using systemd timers for now till interval backup feature is implemented

## With configuration

- [ ] __TODO__ write about config file
