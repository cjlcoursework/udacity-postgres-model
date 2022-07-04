import unittest

import psycopg2

conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user=postgres password=chinois1")
conn.set_session(autocommit=True)
cur = conn.cursor()


class MyTestCase(unittest.TestCase):

    def head_table(self, table_name : str, limit: int):
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        results = cur.fetchall()
        self.assertEqual(len(results), limit)  # add assertion here

    def test_basic_songplay(self):
        self.head_table("song_plays", 15)

    def test_basic_users(self):
        self.head_table("dim_user", 5)

    def test_basic_artists(self):
        self.head_table("dim_artist", 5)

    def test_basic_time(self):
        self.head_table("dim_time", 5)

    def test_basic_songs(self):
        self.head_table("dim_song", 5)

    def test_join_songplay(self):
        cur.execute("SELECT * FROM song_plays where song_id is not null LIMIT 50")
        results = cur.fetchall()
        self.assertEqual(len(results), 1)  # add assertion here


if __name__ == '__main__':
    unittest.main()
    conn.close()
    cur.close()
