#%%
from wait_time_data import create_database, WaitTimeLib

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}


create_database(db_config)
#%%
wait_time_data = WaitTimeLib(db_config)

#%%
wait_time_data.create_loket_names_table()
wait_time_data.store_data(wait_time_data.fetch_data())
wait_time_data.fetch_loket_names()
wait_time_data.close

#%%
wait_time_data.get_mean_wait_times()

#%%
results = wait_time_data.get_raw_data()
print(results)

# %%
wait_time_data.close()
# %%
