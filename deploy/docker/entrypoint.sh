#!/bin/sh
set -eu

if [ "$(id -u)" = "0" ]; then
    mkdir -p /var/lib/nodus/data /var/lib/nodus/backups /var/log/nodus
    chown -R nodus:nodus /var/lib/nodus /var/log/nodus
    exec gosu nodus "$@"
fi

exec "$@"
