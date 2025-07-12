#!/bin/bash
set -e

# Install Python dependencies
pip3 install -r requirements.txt

# Run the PySpark batch job
python aggregator/main.py

