# trendr
Database project for CSE 412


## Setup

Run this bash script to create the database cluster. Commands
will have to be modified slightly for MacOS or Windows:
```bash
chmod +x initialization/postgres_setup.sh
./initialization/postgres_setup.sh
```

Create tables in the database and populate them with data
from the [Kaggle dataset](https://www.kaggle.com/datasets/sachinkanchan92/reddit-top-posts-50-subreddit-analysis-2011-2024?resource=download):

```bash
# Pip install
pip install -r requirements.txt
# Conda install
conda install --yes --file requirements.txt

python initialization/postgres_import.py
```

To access the DB manually:
```bash
psql -d trendr
```

VS Code extension for managing Postgres connection through the IDE:

https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools
