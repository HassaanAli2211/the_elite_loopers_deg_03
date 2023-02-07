from unittest import TestCase

from fastapi.testclient import TestClient

from etl.extract.moisturecarbonservice.extract_moisture_carbon_data import app

client = TestClient(app)


class TestExample(TestCase):
    def test_moisturemate_data_response_200_and_logs(self):
        with self.assertLogs() as captured:
            response = client.post(
                "/api/moisturemate",
                json={"test_key": "test_value"},
            )
        assert response.status_code == 200
        self.assertEqual(len(captured.records), 1)
        self.assertEqual(
            captured.records[0].getMessage(),
            "MoistureMate: {'test_key': 'test_value'}",
        )

    def test_carbonsense_data_response_200_and_logs(self):
        with self.assertLogs() as captured:
            response = client.post(
                "/api/carbonsense",
                json={"test_key": "test_value"},
            )
        assert response.status_code == 200
        self.assertEqual(len(captured.records), 1)
        self.assertEqual(
            captured.records[0].getMessage(),
            "carbonsense: {'test_key': 'test_value'}",
        )

    def test_invalid_endpoint_response_404():
    	response = client.post("/invalid_endpoint", json={"test_key": "test_value"})
    	assert response.status_code == 404

