#!/bin/sh
set -e

cd "${HOME}/RetractionBot" || exit 1

venv/bin/python -m src.RetractionBot.find_retractions
venv/bin/python -m src.RetractionBot.retraction_bot
