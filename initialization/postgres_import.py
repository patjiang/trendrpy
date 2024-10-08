import psycopg2
from psycopg2 import sql
from pathlib import Path
import nltk
from tqdm import tqdm

nltk.download("stopwords")
from nltk.corpus import stopwords
import re

DB_NAME = "trendr"
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "8889"

# Relative to the project root
DATA_PATH = "data"

# Filter out non-keywords like "the"
STOP_WORDS = set(stopwords.words("english"))

create_tmp_table = """
CREATE TABLE IF NOT EXISTS tmp (
    id TEXT PRIMARY KEY,
    title TEXT,
    score INT,
    upvote_ratio FLOAT,
    num_comments INT,
    created_utc TIMESTAMP,
    subreddit TEXT,
    subscribers INT,
    permalink TEXT,
    url TEXT,
    domain TEXT,
    num_awards INT,
    num_crossposts INT,
    crosspost_subreddits TEXT,
    post_type TEXT,
    is_nsfw BOOLEAN,
    is_bot BOOLEAN,
    is_megathread BOOLEAN,
    body TEXT
);
"""

assume_null_bools_false = """
UPDATE tmp
SET is_nsfw = COALESCE(is_nsfw, FALSE),
    is_bot = COALESCE(is_bot, FALSE),
    is_megathread = COALESCE(is_megathread, FALSE);
"""

create_subreddit_table = """
CREATE TABLE IF NOT EXISTS subreddit (
    s_name TEXT PRIMARY KEY,
    num_subscribers INT
);
"""

create_post_table = """
CREATE TABLE IF NOT EXISTS post (
    p_post_id SERIAL PRIMARY KEY,
    title TEXT,
    score INT,
    upvote_ratio FLOAT,
    num_comments INT,
    created_utc TIMESTAMP,
    subreddit TEXT REFERENCES subreddit(s_name),
    permalink TEXT,
    url TEXT,
    domain TEXT,
    num_awards INT,
    num_crossposts INT,
    crosspost_subreddits TEXT,
    post_type TEXT,
    is_nsfw BOOLEAN,
    is_bot BOOLEAN,
    is_megathread BOOLEAN,
    body TEXT
);
"""

create_keyword_table = """
CREATE TABLE IF NOT EXISTS keyword (k_word TEXT PRIMARY KEY);
"""

create_post_keyword_table = """
CREATE TABLE IF NOT EXISTS post_keyword (
    pk_post_id INT REFERENCES post(p_post_id) ON DELETE CASCADE,
    pk_word TEXT REFERENCES keyword(k_word) ON DELETE CASCADE,
    PRIMARY KEY (pk_post_id, pk_word)
);
"""

copy_data_into_tmp = """
COPY tmp 
FROM STDIN WITH CSV HEADER DELIMITER ',';
"""

insert_posts = """
INSERT INTO post (
        title,
        score,
        upvote_ratio,
        num_comments,
        created_utc,
        subreddit,
        permalink,
        url,
        domain,
        num_awards,
        num_crossposts,
        crosspost_subreddits,
        post_type,
        is_nsfw,
        is_bot,
        is_megathread,
        body
    )
SELECT title,
    score,
    upvote_ratio,
    num_comments,
    created_utc,
    subreddit,
    permalink,
    url,
    domain,
    num_awards,
    num_crossposts,
    crosspost_subreddits,
    post_type,
    is_nsfw,
    is_bot,
    is_megathread,
    body
    FROM tmp;
"""

insert_subreddits = """
INSERT INTO subreddit (s_name, num_subscribers)
SELECT DISTINCT subreddit,
    subscribers
FROM tmp ON CONFLICT (s_name) DO NOTHING;
"""

clear_tables = """
DELETE FROM tmp;
DELETE FROM post;
DELETE FROM subreddit;
DELETE FROM keyword;
DELETE FROM post_keyword;
"""

delete_tmp = """
DROP TABLE IF EXISTS tmp;
"""


def get_filenames(directory_path, exclude):
    filenames = [
        f.name
        for f in directory_path.iterdir()
        if f.is_file() and f.name != exclude
    ]
    return filenames


def extract_keywords(body):
    # Convert body text to lowercase and remove non-alphabetical characters
    words = re.findall(r"\b[a-z]+\b", body.lower())
    # Filter out stop words
    filtered_words = [word for word in words if word not in STOP_WORDS]
    return set(filtered_words)


def insert_keywords_and_associations(cursor):
    cursor.execute("SELECT p_post_id, body FROM post WHERE body IS NOT NULL")
    posts = cursor.fetchall()

    # Cache
    existing_keywords = set()
    new_keywords = set()
    post_keyword_associations = []

    for post_id, body in tqdm(
        posts, desc="Processing posts for keywords", unit="post"
    ):
        keywords = extract_keywords(body)

        for keyword in keywords:
            if keyword not in existing_keywords:
                new_keywords.add(keyword)
                existing_keywords.add(keyword)

            # Add the post-keyword association
            post_keyword_associations.append((post_id, keyword))

    print(f"Inserting {len(new_keywords)} keywords...")
    # Batch insert new keywords
    if new_keywords:
        cursor.executemany(
            """
            INSERT INTO keyword (k_word)
            VALUES (%s)
            ON CONFLICT (k_word) DO NOTHING;
            """,
            [(kw,) for kw in new_keywords],
        )

    print(
        f"Inserting {len(post_keyword_associations)} post-keyword associations..."
    )
    if post_keyword_associations:
        cursor.executemany(
            """
            INSERT INTO post_keyword (pk_post_id, pk_word)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """,
            post_keyword_associations,
        )


def execute_sql_commands():
    try:
        filenames = get_filenames(Path(DATA_PATH), "50_subreddits_list.csv")

        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        connection.autocommit = True

        cursor = connection.cursor()
        cursor.execute(create_tmp_table)

        cursor.execute(create_subreddit_table)
        cursor.execute(create_post_table)
        cursor.execute(create_keyword_table)
        cursor.execute(create_post_keyword_table)

        # Delete everything before reimporting data to make this
        # script idempotent
        cursor.execute(clear_tables)

        print("Copying data from CSV files into tmp...")
        for fname in filenames:
            with open(f"{DATA_PATH}/{fname}", "r") as f:
                cursor.copy_expert(sql.SQL(copy_data_into_tmp), f)

        # I don't wanna deal with null values later
        cursor.execute(assume_null_bools_false)

        print("Inserting data into subreddit table...")
        cursor.execute(insert_subreddits)

        print("Inserting data into posts table...")
        cursor.execute(insert_posts)

        insert_keywords_and_associations(cursor)

        print("Dropping the temporary table...")
        cursor.execute(delete_tmp)

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()


if __name__ == "__main__":
    execute_sql_commands()
