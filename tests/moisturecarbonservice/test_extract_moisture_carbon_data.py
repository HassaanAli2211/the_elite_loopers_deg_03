from fastapi.testclient import TestClient

from etl.extract.moisturecarbonservice.extract_moisture_carbon_data import app

client = TestClient(app)


def test_collect_moisture_data_response_200():
    response = client.post(
        "/api/moisturemate",
        json={"test_key": "test_value"},
    )
    assert response.status_code == 200


def test_collect_moisture_data_response_200():
    response = client.post(
        "/api/carbonsense",
        json={"test_key": "test_value"},
    )
    assert response.status_code == 200


def test_invalid_endpoint_response_404():
    response = client.post("/invalid_endpoint", json={"test_key": "test_value"})
    assert response.status_code == 404
