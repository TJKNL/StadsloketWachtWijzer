from wait_time_data import WaitTimeData

db_config = {
    'host': 'localhost',
    'user': 'yourusername',
    'password': 'yourpassword',
    'database': 'wachttijden'
}

wait_time_data = WaitTimeData(db_config)
data = wait_time_data.fetch_data()
wait_time_data.store_data(data)
wait_time_data.close()
