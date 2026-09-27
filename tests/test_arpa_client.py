"""
Unit tests for ArpaSocrataClient.
Tests throttling backoff, query formatting, and sensor chunking.
"""

import json
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

from src.arpa_client import ArpaSocrataClient, DATASET_STATIONS, DATASET_MEASUREMENTS


class TestArpaSocrataClient(unittest.TestCase):

    def setUp(self):
        self.client = ArpaSocrataClient(
            base_url="https://test.dati.lombardia.it/resource",
            max_retries=3,
            base_delay=0.01,
            polite_delay=0.0,
        )

    @patch("urllib.request.urlopen")
    def test_get_stations_formatting(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([
            {"idsensore": "1001", "nomestazione": "Bergamo Meucci", "provincia": "BG"}
        ]).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        stations = self.client.get_stations(province="BG")

        self.assertEqual(len(stations), 1)
        self.assertEqual(stations[0]["nomestazione"], "Bergamo Meucci")

        # Verify request URL parameters
        args, _ = mock_urlopen.call_args
        request = args[0]
        self.assertIn(DATASET_STATIONS, request.full_url)
        self.assertIn("provincia", request.full_url)
        self.assertIn("BG", request.full_url)

    @patch("urllib.request.urlopen")
    def test_throttling_exponential_backoff(self, mock_urlopen):
        # Simulate two 429 Too Many Requests errors followed by a 200 OK success
        err_429 = urllib.error.HTTPError(
            url="https://test.url",
            code=429,
            msg="Too Many Requests",
            hdrs={},
            fp=None,
        )
        mock_success = MagicMock()
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([{"idsensore": "1", "valore": "25.4"}]).encode("utf-8")
        mock_success.__enter__.return_value = mock_response

        mock_urlopen.side_effect = [
            err_429,
            err_429,
            mock_success,
        ]

        # Execute
        result = self.client._execute_request("https://test.url")

        # Verify it retried twice and succeeded on 3rd attempt
        self.assertEqual(len(result), 1)
        self.assertEqual(mock_urlopen.call_count, 3)

    @patch("urllib.request.urlopen")
    def test_iter_measurements_sensor_chunking(self, mock_urlopen):
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps([
            {"idsensore": "1", "data": "2024-01-01T00:00:00", "valore": "30"}
        ]).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # 5 sensors with chunk size of 2 -> 3 chunks
        sensor_ids = [101, 102, 103, 104, 105]
        batches = list(self.client.iter_measurements_for_sensors(
            sensor_ids=sensor_ids,
            chunk_size_sensors=2,
            page_size=50000,
        ))

        self.assertEqual(len(batches), 3)
        self.assertEqual(mock_urlopen.call_count, 3)


if __name__ == "__main__":
    unittest.main()
