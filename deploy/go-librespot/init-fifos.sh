#!/bin/sh
# One-shot initializer for the lavalink-go-librespot compose stack.
#
#  1. Creates /spdirect/spdirect-<n>.fifo per configured backend (mkfifo),
#     group-owned by $PGID with mode 0660 — group-writable, NEVER
#     world-writable. The go-librespot daemon (runs with gid $PGID) writes,
#     the Lavalink plugin JVM (lavalink image user, gid 322 by default) reads.
#  2. Seeds each per-daemon /config volume with the matching config template
#     (config.yml) and chowns the volume to $PUID:$PGID with mode 0700, so the
#     unprivileged daemon can write state.json / credentials.json / lockfile
#     (cmd/daemon/main.go keeps those in the config dir).
#
# POSIX sh (busybox-compatible), idempotent: safe to re-run.
# Exit code 0 = everything is in place.

set -eu

FIFO_DIR="${FIFO_DIR:-/spdirect}"
FIFO_NAMES="${FIFO_NAMES:-spdirect-1.fifo spdirect-2.fifo}"
PUID="${PUID:-1000}"
PGID="${PGID:-322}"
CONFIG_TEMPLATE_DIR="${CONFIG_TEMPLATE_DIR:-/config-template}"
# Space-separated "volume-mount-path=template-file" pairs.
CONFIG_PAIRS="${CONFIG_PAIRS:-config-1=config.yml config-2=config.2.yml}"

[ -d "$FIFO_DIR" ] || { echo "init: fatal: FIFO dir '$FIFO_DIR' is not mounted" >&2; exit 1; }
[ -d "$CONFIG_TEMPLATE_DIR" ] || { echo "init: fatal: template dir '$CONFIG_TEMPLATE_DIR' is not mounted" >&2; exit 1; }

# ---------------------------------------------------------------- FIFOs ---
for name in $FIFO_NAMES; do
    fifo="$FIFO_DIR/$name"
    if [ ! -p "$fifo" ]; then
        mkfifo "$fifo"
        echo "init: created FIFO $fifo"
    else
        echo "init: FIFO $fifo already exists"
    fi
    chgrp "$PGID" "$fifo"
    chmod 0660 "$fifo"
    echo "init: $fifo owned root:$PGID mode 0660 (group-writable, not world-writable)"
done

# ------------------------------------------------- daemon config volumes ---
for pair in $CONFIG_PAIRS; do
    config_dir="${pair%%=*}"
    template="${pair#*=}"
    [ -n "$config_dir" ] || { echo "init: fatal: empty config dir in pair '$pair'" >&2; exit 1; }
    [ -n "$template" ] || { echo "init: fatal: empty template in pair '$pair'" >&2; exit 1; }

    [ -d "$config_dir" ] || { echo "init: fatal: config dir '$config_dir' is not mounted" >&2; exit 1; }
    [ -f "$CONFIG_TEMPLATE_DIR/$template" ] || { echo "init: fatal: template '$CONFIG_TEMPLATE_DIR/$template' not found" >&2; exit 1; }

    if [ ! -f "$config_dir/config.yml" ]; then
        cp "$CONFIG_TEMPLATE_DIR/$template" "$config_dir/config.yml"
        echo "init: seeded $config_dir/config.yml from $template"
    else
        echo "init: $config_dir/config.yml already present (kept)"
    fi

    # state.json / credentials.json / lockfile must be writable by the
    # unprivileged daemon; nobody else should read the stored session.
    chown -R "$PUID:$PGID" "$config_dir"
    chmod 0700 "$config_dir"
    echo "init: config dir $config_dir owned $PUID:$PGID mode 0700"
done

echo "init: done"
