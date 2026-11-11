# RetractionBot

RetractionBot flags Wikipedia citations using Crossref's Retraction Watch
data. New and changed notices are searched first. The bot revisits older
papers in a rolling scan. See the
[English Wikipedia bot account](https://en.wikipedia.org/wiki/User:RetractionBot).

## Set up the bot

1. Create a Python virtual environment and install the runtime dependencies.
   `python3 -m venv venv` then `venv/bin/python -m pip install -r requirements.txt`.
2. Configure Pywikibot for the account and wikis this job is authorized to edit.
3. Set the database host and name in `bot_settings.yml`. Put the MariaDB
   credentials in `~/replica.my.cnf`. The refresh needs permission to read,
   create, insert, rename and drop the snapshot tables.
4. For a new database, load `schema.sql` with the MariaDB client. For an existing
   database, add the lookup indexes listed in `schema.sql` if needed and run the
   scripts in `migrations/` in filename order before starting the updated code.
   Refresh the data after applying the notice-date migration.

The `template_name_map`, `template_field_names`, `citation_templates` and
`summary_map` settings define each wiki's tagging behavior. The site-specific
`User:RetractionBot/run` page must contain `yes` for edits to proceed. Change
it to `no` to stop edits on that wiki. The job refreshes it periodically and
before saving a changed page.

## Schedule the bot

Run from the repository directory. Refresh the feed before scanning the wikis.

```sh
venv/bin/python -m src.RetractionBot.find_retractions &&
venv/bin/python -m src.RetractionBot.retraction_bot
```

Schedule this sequence through the job runner or cron for the deployment.
The default `scan.limit_per_wiki` is 500 identifiers per invocation. Allow
at least five seconds per identifier plus API time. Adjust the cadence and
budget for the enabled wikis and approved edit rate. Store `scan.state_file`
on persistent local storage. Its default is
`~/.local/state/retractionbot/scan.sqlite3`. A fresh state file begins a new
scan, with the newest dated notices first.

## Test

Install the dependencies, including `mwparserfromhell` and `pytest`, in the
virtual environment. Run the tests without wiki or MariaDB credentials.

```sh
PYWIKIBOT_NO_USER_CONFIG=2 venv/bin/python -m pytest
```

The idea for this bot came from WikiCite 2018 and
[on-wiki discussions about retractions](https://en.wikipedia.org/wiki/Wikipedia_talk:WikiProject_Medicine/Archive_118#Med_article_retractions).
