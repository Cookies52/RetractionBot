import logging
from pathlib import Path

import yaml


def load_bot_settings():
    """Read bot_settings.yml from the job's working directory."""
    with open("bot_settings.yml", encoding="utf-8") as settings:
        return yaml.safe_load(settings)


def configure_logging(filename):
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
        handlers=[
            logging.FileHandler(Path(__file__).with_name(filename), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
