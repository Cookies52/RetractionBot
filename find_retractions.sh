#!/bin/sh

cd "$HOME/RetractionBot" || exit 1
venv/bin/python -m src.RetractionBot.find_retractions
