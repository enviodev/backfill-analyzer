import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:testing@localhost:5433/envio-dev')

query = 'SELECT "db_write_timestamp" FROM "raw_events"'
df = pd.read_sql_query(query, engine)

# Assuming its a ordered result is returned we could just take the first and last entry from the dataframe
min_timestamp = df['db_write_timestamp'].min()
max_timestamp = df['db_write_timestamp'].max()
time_diff = max_timestamp - min_timestamp

total_events = df.shape[0]
total_seconds = time_diff.total_seconds()
average_events_per_second = total_events / total_seconds

# Print the descriptive statistics, time difference, and average events per second
print(df.describe())  
print(f"Time difference between smallest and greatest timestamp: {time_diff}")
print(f"Average raw events processed per second: {average_events_per_second:.2f}")


plt.figure(figsize=(10, 6)) 
sns.histplot(df['db_write_timestamp'], kde=True, bins=80) # bins control the number of bars
plt.title('Distribution of Timestamp Entries Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Density')

plt.show()