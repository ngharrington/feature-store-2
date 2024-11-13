from flask import Flask, jsonify, request

# you do not have to use Flask, you can use any other framework like FastAPI, Flask, etc.. or nothing at all
app = Flask(__name__)

# Event endpoint to receive events from the external system
@app.route('/event', methods=['POST'])
def handle_event():

    # Get the request data
    event_data = request.get_json()

    # Print the request body
    app.logger.info(f"Received event data: {event_data}")

    # Return a success response
    return jsonify({"status": "success", "message": "Event received"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
