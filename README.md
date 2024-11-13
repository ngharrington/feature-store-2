
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

## Endpoints

- `**POST /event**:` Receives events.
- `**GET /canmessage**`: Checks if the user can send messages.
- `**GET /canpurchase**`: Checks if the user can make purchases.

