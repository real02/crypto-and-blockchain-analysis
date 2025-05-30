#!/bin/bash

# Run this script to set up the cron jobs for refreshing SingleStore data
# This will add cron jobs for each procedure with appropriate scheduling intervals

# Set up log directory
mkdir -p /home/ubuntu/arbitrage-service/logs

# Create a temporary file
TEMP_CRON=$(mktemp)

# Get existing crontab
crontab -l >$TEMP_CRON 2>/dev/null || true

# Add our cron jobs
cat >>$TEMP_CRON <<EOL
# Crypto arbitrage analysis scheduled procedures
# Run network_status refresh every 30 seconds
* * * * * /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_network_status
* * * * * sleep 30 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_network_status

# Run price_comparison refresh every 10 seconds
* * * * * /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison
* * * * * sleep 10 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison
* * * * * sleep 20 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison
* * * * * sleep 30 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison
* * * * * sleep 40 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison
* * * * * sleep 50 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_price_comparison

# Run arbitrage detection every 10 seconds
* * * * * /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities
* * * * * sleep 10 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities
* * * * * sleep 20 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities
* * * * * sleep 30 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities
* * * * * sleep 40 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities
* * * * * sleep 50 && /home/ubuntu/arbitrage-service/scripts/run_procedures.sh detect_arbitrage_opportunities

# Run transaction stats history refresh every 15 minutes
*/15 * * * * /home/ubuntu/arbitrage-service/scripts/run_procedures.sh refresh_tx_stats_history
EOL

# Install new crontab
crontab $TEMP_CRON

# Remove temp file
rm $TEMP_CRON

echo "Cron jobs installed successfully!"
echo "To view the current crontab, run: crontab -l"
echo "To edit the crontab, run: crontab -e"
