\# Medical Telegram Warehouse (Interim)



End-to-end ELT pipeline for Ethiopian medical/pharma Telegram channels.

This interim submission covers:



\- \*\*Task 1\*\*: Telegram scraping + raw data lake (JSON + images + logs)

\- \*\*Task 2\*\*: Load raw data into PostgreSQL + dbt transformations (staging + star schema marts) + tests + dbt docs



\## Project Structure



medical-telegram-warehouse/

├── src/

│ ├── scraper.py

│ ├── datalake.py

│ └── load\_raw\_to\_postgres.py

├── data/

│ └── raw/

│ ├── telegram\_messages/YYYY-MM-DD/<channel>.json

│ └── images/<channel>/<message\_id>.jpg

├── logs/

├── scripts/

│ └── create\_raw\_tables.sql

├── medical\_warehouse/ # dbt project

│ ├── dbt\_project.yml

│ ├── profiles.yml # uses env vars

│ ├── packages.yml

│ ├── models/

│ │ ├── staging/stg\_telegram\_messages.sql

│ │ └── marts/

│ │ ├── dim\_channels.sql

│ │ ├── dim\_dates.sql

│ │ └── fct\_messages.sql

│ └── tests/assert\_no\_future\_messages.sql

├── docker-compose.yml

├── requirements.txt

└── README.md



> NOTE: `.env` is required locally but \*\*must not be committed\*\*.



---



\## Task 1: Scrape Telegram + Build Raw Data Lake



\### Channels

\- https://t.me/lobelia4cosmetics

\- https://t.me/tikvahpharma



\### Output Layout

\- Raw JSON:

&nbsp; - `data/raw/telegram\_messages/YYYY-MM-DD/<channel>.json`

&nbsp; - `data/raw/telegram\_messages/YYYY-MM-DD/\_manifest.json`

\- Images:

&nbsp; - `data/raw/images/<channel>/<message\_id>.jpg`

\- Logs:

&nbsp; - `logs/scrape\_YYYY-MM-DD.log`



\### Run Scraper (Windows PowerShell)

```powershell

python src\\scraper.py --path data --channels https://t.me/lobelia4cosmetics https://t.me/tikvahpharma --limit 300 --message-delay 0.7

Task 2: Load into PostgreSQL + dbt Transformations

Start PostgreSQL (Docker)



Windows note: local PostgreSQL may already use port 5432, so this project maps Postgres to 5433.



docker compose up -d

docker ps



Create Raw Table

Get-Content scripts\\create\_raw\_tables.sql | docker exec -i med\_postgres psql -U med\_user -d med\_warehouse



Load Raw JSON to Postgres

python src\\load\_raw\_to\_postgres.py



Validate Load

docker exec -it med\_postgres psql -U med\_user -d med\_warehouse -c "SELECT channel\_name, COUNT(\*) FROM raw.telegram\_messages GROUP BY 1 ORDER BY 2 DESC;"



dbt Models (Transform)

Staging



stg\_telegram\_messages: cleans and standardizes raw messages (types, naming, derived fields like message\_length, has\_image)



Star Schema (Marts)



dim\_channels: channel dimension (surrogate key + summary stats)



dim\_dates: date dimension generated from message timestamps



fct\_messages: message fact table (joins to dim\_channels + dim\_dates)



Run dbt

cd medical\_warehouse

dbt deps --profiles-dir .

dbt run --profiles-dir .

dbt test --profiles-dir .



Generate dbt Docs

dbt docs generate --profiles-dir .

dbt docs serve --profiles-dir .



Data Quality Tests



schema tests: not\_null, unique, and FK relationship tests



custom test: assert\_no\_future\_messages.sql (ensures no message timestamps are in the future)



Environment Variables



Create a .env file in the repo root:



TELEGRAM\_API\_ID=...

TELEGRAM\_API\_HASH=...

TELEGRAM\_SESSION=telegram\_session



DB\_HOST=127.0.0.1

DB\_PORT=5433

DB\_NAME=med\_warehouse

DB\_USER=med\_user

DB\_PASSWORD=med\_password



