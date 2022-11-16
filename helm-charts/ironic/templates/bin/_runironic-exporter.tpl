#!/usr/bin/bash

. /bin/configure-ironic.sh

FLASK_RUN_HOST=${FLASK_RUN_HOST:-"0.0.0.0"}
FLASK_RUN_PORT=${FLASK_RUN_PORT:-"9608"}

export IRONIC_CONFIG="/etc/ironic/ironic.conf"

exec gunicorn -b ${FLASK_RUN_HOST}:${FLASK_RUN_PORT} -w 4 \
    ironic_prometheus_exporter.app.wsgi:application
