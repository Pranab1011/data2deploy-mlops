from etl.etl_utils import *

end_timestamp = "2024-12-30T00:00:00.000"

df = fetch_multiple_days(end_timestamp, 1)
print(df.columns)
