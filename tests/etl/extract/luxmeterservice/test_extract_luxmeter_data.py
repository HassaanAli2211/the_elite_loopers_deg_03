from unittest import TestCase
from unittest.mock import Mock, patch


from the_elite_loopers_deg_03.etl.extract.luxmeterservice import luxmeter_service

room_ids = ["kitchen", "bedroom", "bathroom", "living_room"]


@patch("the_elite_loopers_deg_03.etl.extract.luxmeterservice.luxmeter_service.requests.get")
class TestExample(TestCase):
    def test_get_lux_data_gets_one_measurement_logs_response(self, mock_request_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            "test_key": "test_value",
            "measurements": ["test_measurement"],
        }
        mock_request_get.return_value = mock_response
        with self.assertLogs() as captured:
            luxmeter_service.lux_data()
        self._assert_logged_messages_and_len(
            records=captured.records,
            expected_logged_response={
                "test_key": "test_value",
                "measurements": "test_measurement",
            },
        )

    def test_get_lux_data_two_msrmnts_logs_last_msrmnt(self, mock_request_get):
        mock_response = Mock()
        mock_response.json.return_value = {
            "test_key": "test_value",
            "measurements": [
                "test_measurement1",
                "test_measurement2",
            ],
        }
        mock_request_get.return_value = mock_response
        with self.assertLogs() as captured:
            luxmeter_service.lux_data()
        self._assert_logged_messages_and_len(
            records=captured.records,
            expected_logged_response={
                "test_key": "test_value",
                "measurements": "test_measurement2",
            },
        )

    def _assert_logged_messages_and_len(self, records, expected_logged_response):
        self.assertEqual(len(records), len(room_ids))
        for room, log_message in zip(room_ids, records):
            self.assertEqual(
                log_message.getMessage(),
                f"Received Luxmeter data for {room}:{expected_logged_response}",
            )