from flask import Flask, jsonify
import threading

app = Flask(__name__)

counter = 0
counter_lock = threading.Lock()


@app.route("/inc", methods=["GET"])
def inc():
    global counter
    with counter_lock:
        counter += 1
        value = counter

    return jsonify({"count": value})


@app.route("/count", methods=["GET"])
def get_count():

    with counter_lock:
        value = counter
    return jsonify({"count": value})


if __name__ == "__main__":
    # threaded=True — багатопоточність
    app.run(host="0.0.0.0", port=8080, threaded=True)
