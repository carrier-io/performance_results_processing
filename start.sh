#!/bin/bash

echo "Started"
python -V

echo "Start results_collector"
python results_collector.py

echo "Start R"
Rscript results_processor.R

echo "Start reporter"
python reporter.py