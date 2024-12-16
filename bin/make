#!/bin/bash
# Script to create adap_v2.x python virtual environment and activate it

# Check if user is oper
if [[ "$USER" != "oper" ]]; then
    echo 'Only oper can execute this script'
    exit 1
fi
#Test if uv is available
if ! [ -x "$(command -v uv)" ]; then
    echo "Installing uv for 'oper' user"
    curl -LsSf https://astral.sh/uv/install.sh | sh
else
    echo "Updating uv"
    uv self update
fi
# Create python virtual environment
uv venv venv
source venv/bin/activate
python -V
deactivate
