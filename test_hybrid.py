import unittest
import json
from state import (
    parse_title_year,
    parse_series,
    fmt_size,
    movie_id,
    quality,
    source,
    ctype,
    normalize,
    flex_match,
    parse_show_title,
    show_id
)
from downloader import DownloadMap

class TestStateHelpers(unittest.TestCase):
    def test_parse_title_year(self):
        cases = [
            ("The.Matrix.1999.1080p.Bluray.x264.mkv", ("The Matrix", "1999")),
            ("Inception 2010 2160p HEVC.mkv", ("Inception", "2010")),
            ("NoYearMovie.1080p.webrip.mp4", ("Noyearmovie", "")),
            ("Random.Title.With.Numbers.1234.mp4", ("Random Title With Numbers 1234", "")),
        ]
        for fn, expected in cases:
            with self.subTest(fn=fn):
                self.assertEqual(parse_title_year(fn), expected)

    def test_parse_series(self):
        self.assertEqual(parse_series("Breaking.Bad.S05E14.Ozymandias.1080p.mkv"), {"season": 5, "episode": 14})
        self.assertEqual(parse_series("Friends Season 2 Episode 10.mp4"), {"season": 2, "episode": 10})
        self.assertIsNone(parse_series("The Matrix 1999.mkv"))

    def test_parse_show_title(self):
        self.assertEqual(parse_show_title("Game.of.Thrones.S01E01.1080p.mkv"), "Game Of Thrones")
        self.assertEqual(parse_show_title("Chernobyl.Season.1.Episode.01.720p.mkv"), "Chernobyl")
        self.assertEqual(parse_show_title("Breaking.Bad.S05E12.Webrip.mkv"), "Breaking Bad")

    def test_show_id(self):
        self.assertEqual(show_id("Game.of.Thrones.S01E01.1080p.mkv"), "game_of_thrones")
        self.assertEqual(show_id("Breaking.Bad.S05E12.Webrip.mkv"), "breaking_bad")

    def test_fmt_size(self):
        self.assertEqual(fmt_size(500), "500.0 B")
        self.assertEqual(fmt_size(1536), "1.5 KB")
        self.assertEqual(fmt_size(1024 * 1024 * 2.5), "2.5 MB")
        self.assertEqual(fmt_size(1024 * 1024 * 1024 * 3.75), "3.8 GB")

    def test_movie_id(self):
        self.assertEqual(movie_id("The-Matrix (1999).mkv"), "the_matrix__1999__mkv")

    def test_quality(self):
        self.assertEqual(quality("Movie.2160p.mkv"), "2160P")
        self.assertEqual(quality("Movie.1080p.mkv"), "1080P")
        self.assertEqual(quality("Movie.720p.mkv"), "720P")
        self.assertEqual(quality("Movie.Webrip.mkv"), "Unknown")

    def test_source(self):
        self.assertEqual(source("Movie.Bluray.mkv"), "BLURAY")
        self.assertEqual(source("Movie.Webrip.mkv"), "WEBRIP")
        self.assertEqual(source("Movie.Unknown.mkv"), "")

    def test_ctype(self):
        self.assertEqual(ctype("Movie.mkv"), "video/x-matroska")
        self.assertEqual(ctype("Movie.webm"), "video/webm")
        self.assertEqual(ctype("Movie.mp4"), "video/mp4")

    def test_normalize(self):
        self.assertEqual(normalize("The.Matrix-1999+Remux"), "the matrix 1999 remux")

    def test_flex_match(self):
        self.assertTrue(flex_match("The Matrix", "The.Matrix.1999.1080p.mkv"))
        self.assertTrue(flex_match("Matrix", "The.Matrix.1999.1080p.mkv"))
        self.assertFalse(flex_match("Inception", "The.Matrix.1999.1080p.mkv"))


class TestDownloadMap(unittest.TestCase):
    def test_empty_map(self):
        dm = DownloadMap()
        self.assertEqual(dm.total_bytes(), 0)
        self.assertFalse(dm.has_range(0, 100))
        self.assertEqual(dm.covered_prefix(0), 0)

    def test_add_and_merge(self):
        dm = DownloadMap()
        dm.add(10, 20)
        self.assertEqual(dm._ivs, [[10, 20]])
        self.assertEqual(dm.total_bytes(), 11)

        # Non-overlapping after
        dm.add(30, 40)
        self.assertEqual(dm._ivs, [[10, 20], [30, 40]])

        # Overlapping / merging
        dm.add(21, 29)
        self.assertEqual(dm._ivs, [[10, 40]])

        # Overlapping multiple
        dm = DownloadMap([[10, 20], [30, 40], [50, 60]])
        dm.add(15, 35)
        self.assertEqual(dm._ivs, [[10, 40], [50, 60]])

    def test_has_range(self):
        dm = DownloadMap([[10, 20], [30, 40]])
        self.assertTrue(dm.has_range(12, 18))
        self.assertTrue(dm.has_range(10, 20))
        self.assertFalse(dm.has_range(5, 15))
        self.assertFalse(dm.has_range(15, 25))
        self.assertFalse(dm.has_range(25, 28))

    def test_covered_prefix(self):
        dm = DownloadMap([[10, 20], [21, 30], [35, 40]])
        self.assertEqual(dm.covered_prefix(10), 21) # 10 to 30 inclusive (length 21)
        self.assertEqual(dm.covered_prefix(15), 16) # 15 to 30 inclusive (length 16)
        self.assertEqual(dm.covered_prefix(32), 0)
        self.assertEqual(dm.covered_prefix(35), 6)  # 35 to 40 inclusive (length 6)

    def test_serialization(self):
        dm1 = DownloadMap([[10, 20], [30, 40]])
        js = dm1.to_json()
        dm2 = DownloadMap.from_json(js)
        self.assertEqual(dm2._ivs, [[10, 20], [30, 40]])


if __name__ == "__main__":
    unittest.main()
