#!/bin/bash

1 0 * * * cd /home/goncalo/GreenDIGIT-WP6-CIM-KPI-Pipeline/ && { echo "===== Run started at $(date '+\%Y-\%m-\%d \%H:\%M:\%S') ====="; ./scripts/start_cluster.sh; } >> /home/goncalo/GreenDIGIT-AuthServer/cronjob_auth/logs/start_cluster.log 2>&1
