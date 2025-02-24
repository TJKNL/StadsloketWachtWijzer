from flask import Flask, jsonify, render_template
from wait_time_data import WaitTimeLib, create_database
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

@app.route('/', methods=['GET'])
def index():
    create_database(db_config)
    wait_time_data = WaitTimeLib(db_config)
    mean_waits = wait_time_data.get_mean_wait_times()
    best_loket = min(mean_waits, key=lambda x: x[2]) if mean_waits else None
    wait_time_data.close()
    return render_template('index.html', mean_waits=mean_waits, best_loket=best_loket)

@app.route('/mean_wait_times', methods=['GET'])
def mean_wait_times():
    create_database(db_config)
    wait_time_data = WaitTimeLib(db_config)
    mean_wait = wait_time_data.get_mean_wait_times()
    wait_time_data.close()
    return jsonify(mean_wait)

if __name__ == '__main__':
    app.run(debug=True)
