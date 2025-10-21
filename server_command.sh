#!/bin/bash
# Usage: server_command.sh {server_key} {command}

# Exit if fewer than two arguments are provided
if [ $# -lt 2 ]; then
  echo "Usage: $0 {server_key} {command}"
  exit 1
fi

SERVER_KEY=$1
COMMAND=$2

# Convert command to lowercase (e.g., START -> start)
COMMAND=$(echo "$COMMAND" | tr '[:upper:]' '[:lower:]')

SCRIPT="server_${COMMAND}.py"

# Check if the corresponding Python file exists
if [ ! -f "$SCRIPT" ]; then
  echo "Error: Script '$SCRIPT' not found."
  exit 2
fi

# Run the Python script with the server key
python "$SCRIPT" "$SERVER_KEY"
