import psycopg2

DATABASE_NAME = "sparkifydb"

SONG_PLAY_TABLE_NAME = "song_plays"
USER_TABLE_NAME = "dim_user"
SONG_TABLE_NAME = "dim_song"
ARTIST_TABLE_NAME = "dim_artist"
TIME_TABLE_NAME = "dim_time"


def get_connection(db_name=None):
    import os

    # Get environment variables
    db_user = os.getenv('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_host = os.environ.get('DB_HOST')

    if db_name is None:
        db_name = os.environ.get('DB_NAME')

    # connect to default database
    conn = psycopg2.connect(f"host={db_host} dbname={db_name} user={db_user} password={db_password}")
    conn.set_session(autocommit=True)
    return conn


