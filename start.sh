#!/bin/bash
echo "Started"
echo "Start python"
python3 /results_collector.py
echo "Start R"
Rscript /results_processor.R
echo "Start reporter"
python3 /reporter.py
