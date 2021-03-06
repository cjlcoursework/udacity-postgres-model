import unittest

import psycopg2

conn = psycopg2.connect(f"host=127.0.0.1 dbname=sparkifydb user=postgres password=chinois1")
conn.set_session(autocommit=True)
cur = conn.cursor()


class MyTestCase(unittest.TestCase):

    def head_table(self, table_name: str, limit: int):
        """
        This is the common select/assert utility used by other tests.
        It runs a select statement against the table_name sent in
        and asserts that th row count is the same as the limit passed in

        Parameters
        ----------
        table_name : the name of the table to query
        limit : how many records to pull down

        Returns
        -------
        pass/fail
        """
        cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        results = cur.fetchall()
        self.assertEqual(len(results), limit)  # add assertion here

    def test_basic_songplay(self):
        """
        select from the song_play table
        """
        self.head_table("songplays", 15)

    def test_basic_users(self):
        """
        select from the dim_user table
        """
        self.head_table("dim_user", 5)

    def test_basic_artists(self):
        """
        select from the dim_artist table
        """
        self.head_table("dim_artist", 5)

    def test_basic_time(self):
        """
        select from the dim_time table
        """
        self.head_table("dim_time", 5)

    def test_basic_songs(self):
        """
        select from the dim_song table
        """
        self.head_table("dim_song", 5)

    def test_find_hits(self):
        """
        select from the dim_song table
        """
        cur.execute("SELECT * FROM songplays where song_id is not null LIMIT 50")
        results = cur.fetchall()
        self.assertEqual(len(results), 1)  # add assertion here

    def test_kitchen_sink(self):
        """
        join everything together and look for the ONE record that has all primary and foreign keys matching
        """
        cur.execute(f"""
        select sp.start_time, sp.location, ds.title, da.name, dt.year, dt.month, dt.day
        from songplays sp
        join dim_song ds on sp.song_id = ds.song_id
        join dim_artist da on sp.artist_id = da.artist_id
        join dim_time dt on sp.start_time = dt.start_time
        where sp.song_id is not null    
        """)

        results = cur.fetchall()
        self.assertEqual(len(results), 1)
        # add assertion here


if __name__ == '__main__':
    unittest.main()
    conn.close()
    cur.close()
