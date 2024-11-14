
# User Access Management Service

A simple service to manage user access based on event-driven rules.

## Project Structure

```text
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── app.py
└── event_sender
    ├── Dockerfile
    ├── requirements.txt
    └── send_events.py
```

## Getting Started

1. Build and start the services:

   ```bash
   docker-compose up --build
   ```

2. Access `user_access_service` at [http://localhost:5000](http://localhost:5000).

3. Modify `event_sender/send_events.py` to customize event data as needed.

    - This is for testing purposes only and to demonstrates and example of an external service sending events.  Feel free to modify in any way you choose.

4. Implement your code in `app.py`
    - Note: `app.py` is just the entrypoint.  Feel free to break of the code into multiple files as you see fit.

## Running Tests

In a virtual environment install the dependencies and test dependences
```bash
python -m pip install -r requirements.txt
python -m pip install -r requirements-test.txt
```

Run the tests

```bash
python -m pytest tests/
```

To run load tests start the service and start locust UI

```bash
docker compose up user_access_service --build

cd load_testing
python -m locust
```

In the UI you can select the host (http://localhost:5001 with the docker setup). You can additionally set the number of "users".
The load test is configured so that roughly you will get one request per second from each user.
The load requests are half event posts and half access queries.

Here is an image from a load test with 2000 concurrent requests, run on my laptop with 11th Gen Intel I5 processors.

![Load Test][assets/load_test.png]

## Endpoints

- `**POST /event**:` Receives events.
- `**GET /canmessage**`: Checks if the user can send messages.
- `**GET /canpurchase**`: Checks if the user can make purchases.

