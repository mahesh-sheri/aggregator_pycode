#!/bin/bash
set -e

# Install Python dependencies
pip install -r requirements.txt

# Run the PySpark batch job
python aggregator/main.py

