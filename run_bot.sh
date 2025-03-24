#/bin/sh

cd $HOME/RetractionBot

venv/bin/python -m src.RetractionBot.find_retractions
venv/bin/python -m src.RetractionBot.retraction_bot