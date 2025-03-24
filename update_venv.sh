/bin/bash

# use bash strict mode
set -euo pipefail

cd RetractionBot

python3 -m venv venv

# activate it
source venv/bin/activate

# upgrade pip inside the venv and add support for the wheel package format
pip install -U pkg-config
pip install -U pip wheel
pip install -r requirements.txt
