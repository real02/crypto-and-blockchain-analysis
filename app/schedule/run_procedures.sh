# Load environment variables
source /home/ubuntu/arbitrage-service/.env

# SingleStore connection details - get from environment
SINGLESTORE_HOST=${SINGLESTORE_HOST:-localhost}
SINGLESTORE_PORT=${SINGLESTORE_PORT:-3306}
SINGLESTORE_USER=${SINGLESTORE_USER:-root}
SINGLESTORE_PASSWORD=${SINGLESTORE_PASSWORD:-""}
SINGLESTORE_DB=${SINGLESTORE_DB:-crypto_arbitrage}

# Define procedure to execute - passed as argument
PROCEDURE=$1

# Check if procedure was provided
if [ -z "$PROCEDURE" ]; then
  echo "Error: No procedure specified"
  echo "Usage: $0 <procedure_name>"
  exit 1
fi

# Execute the procedure
mysql -h $SINGLESTORE_HOST -P $SINGLESTORE_PORT -u $SINGLESTORE_USER -p$SINGLESTORE_PASSWORD $SINGLESTORE_DB -e "CALL $PROCEDURE();"

# Log execution
echo "$(date) - Executed procedure: $PROCEDURE" >>/home/ubuntu/arbitrage-service/logs/procedures.log

# Exit with the mysql command's exit code
exit $?
