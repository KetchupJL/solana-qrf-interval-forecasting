#!/usr/bin/env bash

# Project scaffold for solana-qrf-interval-forecasting

# Create directories
 dirs=(
   data/raw
   data/processed
   notebooks
   src/data
   src/features
   src/models
   src/utils
   results/figures
   results/tables
   docs
   tests
 )

for dir in "${dirs[@]}"; do
  mkdir -p "$dir"
done

# Add placeholder .gitkeep to track empty directories
find . -type d -empty -exec touch {}/.gitkeep \;

# Create essential files
touch .gitignore requirements.txt LICENSE .env.example ci.yml

echo "Project structure has been created."
