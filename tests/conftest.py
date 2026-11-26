import os

# Importing pywikibot looks for a user-config.py. The tests have no wiki
# account and never create a Site, so tell pywikibot not to look.
os.environ.setdefault("PYWIKIBOT_NO_USER_CONFIG", "2")
