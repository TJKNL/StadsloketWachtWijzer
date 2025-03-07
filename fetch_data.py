#%%
import os
from wait_time_data import create_database, WaitTimeLib

# Use connection string
db_url = os.getenv('DATABASE_URL')

create_database(db_url)
#%%
# Initialize with connection string
wait_time_data = WaitTimeLib(db_url)

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
