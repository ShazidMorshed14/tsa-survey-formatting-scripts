import pandas as pd

# ------------------------------
# Step 0: CSV file path
# ------------------------------
csv_file = "market_visit_gt.csv"

# ------------------------------
# Step 1: Chunk settings
# ------------------------------
chunksize = 10000  # adjust based on your RAM
cols_needed = [
    'tsa_call_uid', 'retailer_code','retailer_name', 
    'channel', 'sub_channel', 'territory', 'route', 'section', 'call_type',
    'call_start_time', 'call_end_time', 'retailer_status',
    'geo_verified', 'internet_connection_validity', 
    'geo_verified_image_url', 'within_meters', 'latitude', 'longitude',
    'call_date', 
    'tsa_name', 'tsa_code', 'question_id', 'question', 'response'
]

# ------------------------------
# Step 2: Read CSV in chunks
# ------------------------------
chunks = []
for chunk in pd.read_csv(csv_file, usecols=cols_needed, chunksize=chunksize):
    chunks.append(chunk)
    print(f"Loaded chunk with {len(chunk)} rows")

# Combine all chunks
df = pd.concat(chunks, ignore_index=True)
print(f"Total rows in DataFrame: {len(df)}")

# ------------------------------
# Step 3: Sort by call and question
# ------------------------------
df = df.sort_values(by=['tsa_call_uid', 'question_id'])

# ------------------------------
# Step 4: Pivot survey responses
# ------------------------------
index_cols = [
    'tsa_call_uid', 'retailer_code','retailer_name', 
    'channel', 'sub_channel', 'territory', 'route', 'section', 'call_type',
    'call_start_time', 'call_end_time', 'retailer_status',
    'geo_verified', 'internet_connection_validity', 
    'geo_verified_image_url', 'within_meters', 'latitude', 'longitude',
    'call_date', 
    'tsa_name', 'tsa_code'
]

# Custom function to join multiple responses
def join_responses(x):
    # Convert to string, remove brackets/quotes if present, join by comma
    return ','.join(str(i).strip("[]'\" ") for i in x if pd.notnull(i))

survey_df = df.pivot_table(
    index=index_cols,
    columns='question',
    values='response',
    aggfunc=join_responses
).reset_index()

# Flatten columns
survey_df.columns.name = None
survey_df.columns = [str(col) for col in survey_df.columns]

# ------------------------------
# Step 5: Order question columns by question_id
# ------------------------------
question_order = df[['question_id', 'question']].drop_duplicates().sort_values('question_id')
question_cols_sorted = question_order['question'].tolist()
survey_df = survey_df[index_cols + question_cols_sorted]

# Step 1: Convert to datetime
survey_df['call_start_time'] = pd.to_datetime(survey_df['call_start_time'], format='%Y-%m-%d %I:%M %p')

# Step 2: Sort by this column in ascending order
survey_df = survey_df.sort_values(by='call_start_time', ascending=True)

# Format as string with AM/PM
survey_df['call_start_time'] = survey_df['call_start_time'].dt.strftime('%Y-%m-%d %I:%M %p')


# ------------------------------
# Step 6: Preview
# ------------------------------
print("First 5 rows of formatted survey responses:")
print(survey_df.head())

# ------------------------------
# Step 7: Save to Excel
# ------------------------------
survey_df.to_excel("market_visit_gt_sept_25_formatted.xlsx", index=False)
print("Formatted survey responses saved to 'survey_responses_formatted.xlsx'")
