from flask import Flask, jsonify
from wait_time_data import WaitTimeData

app = Flask(__name__)

db_config = {
    'host': 'localhost',
    'user': 'yourusername',
    'password': 'yourpassword',
    'database': 'wachttijden'
}

@app.route('/mean_wait_times', methods=['GET'])
def mean_wait_times():
    wait_time_data = WaitTimeData(db_config)
    mean_wait_times = wait_time_data.get_mean_wait_times()
    wait_time_data.close()
    return jsonify(mean_wait_times)

if __name__ == '__main__':
    app.run(debug=True)
