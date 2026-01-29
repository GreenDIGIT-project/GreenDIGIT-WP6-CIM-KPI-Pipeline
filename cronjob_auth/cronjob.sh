#!/bin/bash

# For testing, comment out and run the script
# RUN_SCRIPT=$(
#      cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline && {
#           export BASE_PATH="/home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline"
#           export LOG_PATH="$BASE_PATH/cronjob_auth/logs/restart_cluster.log"

#           if [ ! -f "$LOG_PATH" ]; then
#                mkdir -p "$(dirname "$LOG_PATH")"
#                touch "$LOG_PATH"
#           fi
#           echo "===== Run started at $(date '+\%Y-\%m-\%d \%H:\%M:\%S') =====";
#           ./scripts/restart_cluster.sh;
#      } >> $LOG_PATH 2>&1
# )

# RUN_SCRIPT

# THE ACTUAL ONE-LINER COMMAND FOR THE CRONTAB
# This is what needs to be pasted in the crontab -e
# Yes, I know. Annoying one-liner. What can we do. :)
# 1 0 * * * cd /home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline && BASE_PATH=/home/ubuntu/GreenDIGIT-WP6-CIM-KPI-Pipeline && LOG_PATH=$BASE_PATH/cronjob_auth/logs/restart_cluster.log && mkdir -p "$(dirname "$LOG_PATH")" && touch "$LOG_PATH" && echo "===== Run started at $(date '+\%Y-\%m-\%d \%H:\%M:\%S') =====" >> "$LOG_PATH" && ./scripts/restart_cluster.sh >> "$LOG_PATH" 2>&1

