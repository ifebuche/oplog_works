#!/bin/bash
# Run using : bash code_check.sh

echo "Formatting scripts in MI-ETL dir"
black MI_ETL/

# echo "Type Checking scripts in MI-ETL dir"
# mypy MI_ETL/