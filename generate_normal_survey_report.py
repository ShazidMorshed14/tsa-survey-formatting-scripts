import pandas as pd
import ast

# ------------------------------
# Step 1: CSV file path
# ------------------------------
csv_file = "./Dumps/retailer_questionaries_survey_oct.csv"
output_file_name="report_retailer_questionaries_survey_oct_till_15_Oct.xlsx"

# ------------------------------
# Step 2: Columns to use
# ------------------------------
cols_needed = [
    'retailer_code','partner_code','retailer_name','channel','sub_channel',
    'territory','route','section','retailer_status','latitude','longitude',
    'submit_time','tsa_name','tsa_code','question_id','question','response'
]

# ------------------------------
# Step 3: Read CSV in chunks (for large files)
# ------------------------------
chunksize = 10000
chunks = []

for chunk in pd.read_csv(csv_file, usecols=cols_needed, chunksize=chunksize):
    chunks.append(chunk)
    print(f"Loaded chunk with {len(chunk)} rows")

df = pd.concat(chunks, ignore_index=True)
print(f"✅ Total rows loaded: {len(df)}")

# ------------------------------
# Step 4: Pivot the data
# ------------------------------
index_cols = [
    'retailer_code','partner_code','retailer_name','channel','sub_channel',
    'territory','route','section','retailer_status','latitude','longitude',
    'submit_time','tsa_name','tsa_code'
]

# Function to join multiple responses with comma


def join_responses(x):
    cleaned = []
    for i in x:
        if pd.isnull(i):
            continue
        try:
            # Try to convert string representation of list to actual list
            val = ast.literal_eval(i)
            if isinstance(val, list):
                cleaned.extend([str(v).strip() for v in val if v is not None])
            else:
                cleaned.append(str(val).strip())
        except:
            # Not a list, just strip
            cleaned.append(str(i).strip())
    return ', '.join(cleaned)

pivot_df = df.pivot_table(
    index=index_cols,
    columns='question',
    values='response',
    aggfunc=join_responses
).reset_index()

pivot_df.columns.name = None
pivot_df.columns = [str(c) for c in pivot_df.columns]

# ------------------------------
# Step 5: Format submit_time
# ------------------------------
pivot_df['submit_time'] = pd.to_datetime(
    pivot_df['submit_time'], errors='coerce'
).dt.strftime('%Y-%m-%d %I:%M:%S %p')

# ------------------------------
# Step 6: Sort question columns by question_id
# ------------------------------
# Fixed columns remain at the front
fixed_cols = index_cols

# Map question -> question_id and sort
question_order = df[['question_id','question']].drop_duplicates().sort_values('question_id')
question_cols_sorted = question_order['question'].tolist()

# Reorder pivot_df columns
pivot_df = pivot_df[fixed_cols + question_cols_sorted]

# ------------------------------
# Step 7: Sort rows by submit_time ascending
# ------------------------------
pivot_df['submit_time_dt'] = pd.to_datetime(pivot_df['submit_time'], format='%Y-%m-%d %I:%M:%S %p', errors='coerce')
pivot_df = pivot_df.sort_values('submit_time_dt').drop(columns=['submit_time_dt'])

# ------------------------------
# Step 8: Export to Excel
# ------------------------------

pivot_df.to_excel(output_file_name, index=False)

print(f"✅ Formatted survey responses saved to '{output_file_name}'")
