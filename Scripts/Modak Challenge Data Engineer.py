
######## Modak Challenge ###########

# Date        Version        Who              What
# 2024-02-11     1           Bianca Lacerda   Modak Challenge

#########################################################################################################################################################
#If you have any questions, you can find the documentation at https://
#When a new code block starts, it has the same name comented and centralized from the documentation: Ex.: ########################### Code Implementation ##############################
#it means that that block corresponds to the "Code Implementation" section of the documentation.
#########################################################################################################################################################



#Imports
import os #For interacting with the operating system, such as accessing file directories
import json #For reading data from the JSON file containing the events.
import pandas as pd #For data manipulation and analysis, converting JSON/CSV data into DataFrames.
from datetime import datetime, timedelta #For manipulating dates and time intervals.

def load_initial_file(file_path, file_type="json"):
    """
    Loads a JSON or CSV file into a Pandas DataFrame.
    
    Args:
        file_path (str): Full path to the file.
        file_type (str): Type of file: 'json' or 'csv'.
        
    Returns:
        pd.DataFrame: DataFrame loaded from the file.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None
    
    try:
        if file_type == "json":
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.json_normalize(data)
        elif file_type == "csv":
            df = pd.read_csv(file_path)
        else:
            print(f"Unknown file type: {file_type}")
            return None
        
        print(f"\nDataFrame from {file_type.upper()} ({file_path}):")
        print(df.head())
        return df
        
    except Exception as e:
        print(f"Error loading file {file_path}: {e}")
        return None
    

    
def categorize_discrepancy(row):

    """
    Categorizes discrepancies based on the difference between two timestamps, `timestamp_diff`.
    - If the absolute value of `timestamp_diff` is less than 1 day, classify as 'backend logic issues'.
    - Otherwise, classify as 'timestamp delay issue'.
    
    Parameters:
    row (pandas.Series): A row of the DataFrame containing the timestamp_diff value.
    
    Returns:
    str: The category of the discrepancy ('backend logic issues' or 'timestamp delay issue').
    """
        
    # If the timestamp_diff is less than 1 day, categorize as 'backend logic issues'
    if abs(row['timestamp_diff']) < pd.Timedelta(days=1):
        return 'backend logic issues'
    else:
        return 'timestamp delay issue'
    

def calculate_next_ocurrence(start_date, frequency_type, target_day):
    """
    The function uses these inputs to determine the next occurrence of the specified frequency and day,
    iterating until the calculated date reaches or exceeds a predefined limit (December 3, 2024).
    
    Args:
        start_date (str, datetime, or pd.Timestamp): The initial date from which calculations will begin.
        frequency_type (str): The frequency type for the calculation: 'weekly', 'biweekly', 'daily', or 'monthly'.
        target_day (str): The target day for the calculation. For 'weekly' and 'biweekly', provide the day of the week (e.g., 'monday', 'sunday').
                           For 'monthly', use 'first_day' or 'fifteenth_day'.

    Returns:
        str: The last calculated date in the format 'dd', or None if an invalid input is encountered.
    """

    # If start_date is NaT or null, return None to ignore
    if pd.isna(start_date) or start_date == "NaT":
        return None  

    # If it's already a datetime, do nothing
    if isinstance(start_date, datetime):
        pass
    # If it's a Timestamp, convert to datetime
    elif isinstance(start_date, pd.Timestamp):
        start_date = start_date.to_pydatetime()
    else:
        try:
            # Remove milliseconds if present
            start_date = start_date.split('.')[0]  
            # Try to convert to datetime with time
            start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # If it fails, try the format without time
            start_date = datetime.strptime(start_date, "%d/%m/%Y")

    # Define the limit date as December 3, 2024
    limit_date = datetime(2024, 12, 3)
    result_date = None

    while True:
        if frequency_type == "weekly" or frequency_type == "biweekly":
            if frequency_type == 'biweekly':
                weekly_increment = 2
            elif frequency_type == 'weekly':
                weekly_increment = 1

            week_days_map = {
                "sunday": 6,
                "monday": 0,
                "tuesday": 1,
                "wednesday": 2,
                "thursday": 3,
                "friday": 4,
                "saturday": 5
            }

            if target_day not in week_days_map:
                return "Invalid day for weekly or biweekly"

            day_of_week = week_days_map[target_day]

            # Calculate the next desired day in the week
            days_until_next = (day_of_week - start_date.weekday()) % 7
            if days_until_next == 0:
                days_until_next = 7 * weekly_increment
            else:
                days_until_next += 7 * (weekly_increment - 1)

            result_date = start_date + timedelta(days=days_until_next)

        elif frequency_type == "daily":
            # For daily, just add one day
            result_date = start_date + timedelta(days=1)

        elif frequency_type == "monthly":
            # If frequency is monthly, adjust based on the day
            if target_day == "first_day":
                # If it's "first day", go to the first day of the next month
                if start_date.month == 12:
                    result_date = datetime(start_date.year + 1, 1, 1)
                else:
                    result_date = datetime(start_date.year, start_date.month + 1, 1)

            elif target_day == "fifteenth_day":
                # If it's "fifteenth day", check the current date
                if start_date.day < 15:
                    # If before or on the 15th, go to the 15th of the same month
                    result_date = datetime(start_date.year, start_date.month, 15)
                else:
                    # If after the 15th, go to the 15th of the next month
                    if start_date.month == 12:
                        result_date = datetime(start_date.year + 1, 1, 15)
                    else:
                        result_date = datetime(start_date.year, start_date.month + 1, 15)
            else:
                return "Invalid day for monthly"

        else:
            return "Invalid frequency type"

        # Update the start_date for the next calculation
        start_date = result_date

        # Check if the limit date has been reached
        if result_date > limit_date:
            break

    # If the last date was on or before the limit, calculate once more
    if result_date <= limit_date:
        if frequency_type == "weekly" or frequency_type == "biweekly":
            result_date += timedelta(weeks=weekly_increment)
        elif frequency_type == "daily":
            result_date += timedelta(days=1)
        elif frequency_type == "monthly":
            if target_day == "first_day":
                if result_date.month == 12:
                    result_date = datetime(result_date.year + 1, 1, 1)
                else:
                    result_date = datetime(result_date.year, result_date.month + 1, 1)
            elif target_day == "fifteenth_day":
                if result_date.month == 12:
                    result_date = datetime(result_date.year + 1, 1, 15)
                else:
                    result_date = datetime(result_date.year, result_date.month + 1, 15)

    return result_date.strftime("%d")



def calculate_incremented_date(start_date, frequency_type, day):
    """
    Calculates the next exact occurrence based on the provided start date, frequency type, and target day.
    This function does not use loops. It directly calculates the next valid date based on the given frequency
    and target day.
    
    Args:
        start_date (datetime or str): The initial date from which to calculate the next occurrence.
        frequency_type (str): The frequency type, which can be 'weekly', 'biweekly', 'daily', or 'monthly'.
        day (str): The target day to calculate the next occurrence. For 'weekly' or 'biweekly', this is a day of the week (e.g., 'monday'). 
                   For 'monthly', it can be 'first_day' or 'fifteenth_day'.
    
    Returns:
        str: The day of the month for the next occurrence in the format 'dd'.
        If the calculation fails or the frequency type is invalid, returns an error message.
    """

    # If start_date is NaT or null, return None to ignore
    if pd.isna(start_date) or start_date == "NaT":
        return None  

    # If already a datetime, do nothing
    if isinstance(start_date, datetime):
        pass
    # If it's a Timestamp, convert to datetime
    elif isinstance(start_date, pd.Timestamp):
        start_date = start_date.to_pydatetime()
    else:
        try:
            # Remove milliseconds if any
            start_date = start_date.split('.')[0]  
            # Try to convert to the format with time
            start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # If it fails, try the format without time
            start_date = datetime.strptime(start_date, "%d/%m/%Y")

    result_date = None

    # Handle the frequency calculation
    if frequency_type == "weekly" or frequency_type == "biweekly":
        weekly_increment = 1 if frequency_type == "weekly" else 2
        
        week_days = {
            "sunday": 6,
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5
        }

        if day not in week_days:
            return "Invalid day for weekly or biweekly"

        day_of_week = week_days[day]

        # Calculate the next desired day in the week
        days_until_next = (day_of_week - start_date.weekday()) % 7
        if days_until_next == 0:
            days_until_next = 7 * weekly_increment
        else:
            days_until_next += 7 * (weekly_increment - 1)

        result_date = start_date + timedelta(days=days_until_next)
    
    elif frequency_type == "daily":
        # For daily, just add one day
        result_date = start_date + timedelta(days=1)

    elif frequency_type == "monthly":
        # If the frequency is monthly, adjust based on the day
        if day == "first_day":
            # If "first day", go to the first day of the next month
            if start_date.month == 12:
                result_date = datetime(start_date.year + 1, 1, 1)
            else:
                result_date = datetime(start_date.year, start_date.month + 1, 1)
        
        elif day == "fifteenth_day":
            # If "fifteenth day", check the current date
            if start_date.day < 15:
                # If before or on the 15th, go to the 15th of the same month
                result_date = datetime(start_date.year, start_date.month, 15)
            else:
                # If after the 15th, go to the 15th of the next month
                if start_date.month == 12:
                    result_date = datetime(start_date.year + 1, 1, 15)
                else:
                    result_date = datetime(start_date.year, start_date.month + 1, 15)
        else:
            return "Invalid day for monthly"

    else:
        return "Invalid frequency type"

    # Return the date formatted, if it was correctly calculated
    if result_date is not None:
        return result_date.strftime("%d")
    else:
        return "Error in date calculation"
    


############################################################ Code Implementation #################################################################################

######################################## Load files and do type conversions to increase efficiency ###########################################################


# Get the directory where the script is located
file_path = os.path.dirname(os.path.abspath(__file__))

# Build paths for the files
allowance_events_path = os.path.join(file_path, "allowance_events.json")
allowance_backend_path = os.path.join(file_path, "allowance_backend_table.csv")
payment_schedule_path = os.path.join(file_path, "payment_schedule_backend_table.csv")


# Load files using the generic function
allowance_events_df = load_initial_file(allowance_events_path, "json")
allowance_backend_df = load_initial_file(allowance_backend_path, "csv")
payment_schedule_df = load_initial_file(payment_schedule_path, "csv")


# Convert data types for allowance_events_df to ensure correct data format
df_events = allowance_events_df.astype({
    'event.name': 'category',
    'user.id': 'string',
    'allowance.amount': 'float',
    'allowance.scheduled.frequency': 'category',
    'allowance.scheduled.day': 'string'
})

# Convert 'event.timestamp' to datetime format
df_events['event.timestamp'] = pd.to_datetime(allowance_events_df['event.timestamp'], format='%Y-%m-%d %H:%M:%S')

#Print to check if the data was correctly hadled
print(df_events.dtypes)
print(df_events.head())



# Convert data types for allowance_backend_df to ensure correct data format
df_backend = allowance_backend_df.astype({
    'uuid': 'string',
    'creation_date': 'string',
    'frequency': 'category',
    'day': 'string',
    'next_payment_day': 'int',
    'status': 'category'
})

# Convert 'updated_at' to datetime format and remove timezone information
df_backend['updated_at'] = pd.to_datetime(allowance_backend_df['updated_at'], errors='coerce').dt.tz_localize(None)

# Check where conversion failed (values as 'NaT')
print(df_backend[df_backend['updated_at'].isna()])

# Convert Unix timestamp values to datetime and remove timezone as well
df_backend['updated_at'] = df_backend['updated_at'].fillna(
    pd.to_datetime(allowance_backend_df['updated_at'], unit='s', errors='coerce').dt.tz_localize(None)
)

# Verify the data types in the backend DataFrame and display the first few rows
print(df_backend.dtypes)
print(df_backend.head())


# Convert data types for payment_schedule_df to ensure correct data format
df_payment = payment_schedule_df.astype({
    'user_id': 'string',
    'payment_date': 'int'
})

# Verify the data types in the backend DataFrame and display the first few rows
print(df_payment.dtypes)
print(df_payment.head())






##################################################### Analysis of Disabled and Duplicate Users in Events and Backend Tables ##################################################


#Count users with 'disabled' status in backend table
disabled_users_backend = df_backend[df_backend["status"] == "disabled"]
num_disabled_users_backend = disabled_users_backend.shape[0]

# Display the number of users with 'disabled' status
print(f'Number of users with "disabled" status in the allowance_backend_table: {num_disabled_users_backend}')

# Calculate the percentage of disabled users
total_users_backend = df_backend.shape[0]
percentage_disabled_users = (num_disabled_users_backend / total_users_backend) * 100

# Step 2: Print the users with 'disabled' status in allowance_backend_table
print("List of disabled users in the allowance_backend_table:")
print(disabled_users_backend)



# Step 2: Remove users with 'disabled' status from the dataframe
df_backend_enabled_users = df_backend[df_backend["status"] != "disabled"].reset_index(drop=True)

# Display the dataframe without the disabled users
print(df_backend_enabled_users.head())


# Merge the df_backend and df_events tables
df_merged = pd.merge(df_backend, df_events, left_on='uuid', right_on='user.id', how='inner')

# Filter users with 'disabled' status in the df_merged table
disabled_users_events = df_merged[df_merged["status"] == "disabled"]

# Count the number of disabled users present in the events table
num_disabled_users_events = disabled_users_events["user.id"].nunique()

# Display the number of disabled users present in the events table
print(f'Number of disabled users present in the events table: {num_disabled_users_events}')



#Get the user IDs of disabled users present in the events table
disabled_user_ids_in_events = disabled_users_events["user.id"].unique()

#Remove these users from the df_events table and reset the index of the resulting dataframe to ensure a clean, sequential index after filtering
df_events_cleaned = df_events[~df_events["user.id"].isin(disabled_user_ids_in_events)].reset_index(drop=True)

# Checking number of users before removal
num_users_before = len(df_events)

# Checking number of users after removing the disabled ones
num_users_after = len(df_events_cleaned)

#Calculationg number of users removed
num_users_removed = num_users_before - num_users_after

# Display the number of removed users
# The number of users removed in the df_events table is higher because a single user can be associated with multiple events.
print(f"Number of users removed: {num_users_removed}")

# Display the cleaned dataframe
print(f"The cleaned dataframe after removing disabled users is:")
print(df_events_cleaned.head())



################################################### Comparative Analysis: Event-Based Payment Dates vs. Backend System Dates ##############################################



#Adicionando a coluna 'next_expected_payment_date' no DataFrame df_events_cleaned
df_events_cleaned["next_expected_payment_date"] = df_events_cleaned.apply(
    lambda row: calculate_next_ocurrence(
        row["event.timestamp"], 
        row["allowance.scheduled.frequency"], 
        row["allowance.scheduled.day"]
    ),
    axis=1
)

# Exibindo o resultado para verificar a coluna correta
print(df_events_cleaned[["user.id", "event.timestamp", "next_expected_payment_date"]])

##### Filtering to keep only the latest entry per user #####

# Sort by user.id and event.timestamp to ensure the latest event for each user is at the top
df_events_cleaned = df_events_cleaned.sort_values(by=["user.id", "event.timestamp"], ascending=[True, False])

# Remove duplicates, keeping only the latest event for each user
df_events_cleaned = df_events_cleaned.drop_duplicates(subset="user.id", keep="first").reset_index(drop=True)

# Display the filtered dataframe
print("Filtered allowance_ events dataFrame with the latest event per user:")
print(df_events_cleaned.head())


# Merging df_events_cleaned with df_backend
df_events_merged = pd.merge(
    df_events_cleaned,
    df_backend,
    left_on='user.id',    # Key from df_events_cleaned
    right_on='uuid',      # Key from df_backend
    how='left'            # To keep all events, even those without a match in the backend
)

# Displaying the final result with the relevant columns
print("Filtered allowance_ events dataFrame with the latest event per user:")
print(df_events_merged.head())


df_events_merged['next_payment_day'] = df_events_merged['next_payment_day'].fillna(0).astype(int).astype(str).str.zfill(2)

# Displaying results after conversion
print("Displaying dataframe after conversion:")
print(df_events_merged.head())


# Creating new column with comparison results
df_events_merged['is_next_payment_day_correct'] = (
    df_events_merged['next_expected_payment_date'] == df_events_merged['next_payment_day']   
)

#Displaying results for first visual analysis
print(df_events_merged.head(30))


# Convertendo as colunas para datetime, caso ainda não estejam
df_events_merged['event.timestamp'] = pd.to_datetime(df_events_merged['event.timestamp'])
df_events_merged['updated_at'] = pd.to_datetime(df_events_merged['updated_at'])

# Calculando a diferença de timestamp entre as tabelas de backend e eventos
df_events_merged['timestamp_diff'] = df_events_merged['event.timestamp'] - df_events_merged['updated_at']

# Exibindo o resultado para verificar a diferença de timestamp
print(df_events_merged.head())

df_events_merged["next_payment_day_from_updated_at"] = df_events_merged.apply(
    lambda row: calculate_incremented_date(
        row["updated_at"], 
        row["frequency"], 
        row["day"]
    ),
    axis=1
)

# Displaying new column next_payment_day_from_updated_at in dataframe:
print("Displaying new column next_payment_day_from_updated_at in dataframe:")
print(df_events_merged[['user.id', 'creation_date', 'frequency', 'day', 'updated_at', 'next_payment_day']])

# Comparison between fields 'next_payment_day' from backend and new calculated field 'next_payment_day_from_updated_at'
df_events_merged['match_with_updated_at'] = (
    df_events_merged['next_payment_day'] == df_events_merged['next_payment_day_from_updated_at']
)

# Displaying new column next_payment_day_from_updated_at in dataframe:
print("Displaying new column 'match_with_updated_at' in dataframe:")
print(df_events_merged.head())





####################################### Analysis of Payment Date Discrepancies in allowance_backend_table : Identifying Patterns and Hypotheses #############################




# Filtering
filtered_rows = df_events_merged[(df_events_merged['match_with_updated_at'] == True) &
                                   (df_events_merged['is_next_payment_day_correct'] == True)]


# Calculating the number of filtered rows
count_filtered_rows = len(filtered_rows)


# Calculating the percentage relative to the total number of rows in the DataFrame
percentage = (count_filtered_rows / len(df_events_merged)) * 100


# Displaying the result
print(f"Percentage of rows where match_with_updated_at and is_next_payment_day_correct are both TRUE: {percentage:.2f}%")
print(f"Total number of records where both criteria are TRUE: {count_filtered_rows}")


# Display the rows where both conditions are TRUE
print("Rows where both match_with_updated_at and is_next_payment_day_correct are TRUE:")
print(filtered_rows.head()) #adjust here if you want to see all filtered_rows


# Removing rows where 'match_with_updated_at' is True and 'is_next_payment_day_correct' is True
df_events_adjusted = df_events_merged[~((df_events_merged['match_with_updated_at'] == True) & 
                                      (df_events_merged['is_next_payment_day_correct'] == True))]


# Displaying the cleaned DataFrame
print("Displaying dataframe without correct next_payment_day to focus on discrepancies:")
print(df_events_adjusted.head())





# Filtering the rows where match_with_payment_next_day is True and is_next_payment_day_correct is False
filtered_rows_2 = df_events_merged[(df_events_merged['match_with_updated_at'] == True) &
                                   (df_events_merged['is_next_payment_day_correct'] == False)]

# Calculating the number of filtered rows
count_filtered_rows_2 = len(filtered_rows_2)

# Calculating the percentage relative to the total number of rows in the DataFrame
percentage = (count_filtered_rows_2 / len(df_events_merged)) * 100

# Displaying the result of counting and percentage
print(f"Percentage of rows where match_with_updated_at and is_next_payment_day_correct are both TRUE: {percentage:.2f}%")
print(f"Total number of records where both criteria are TRUE: {count_filtered_rows_2}")

# Display the rows where both conditions are TRUE
print("Rows where both match_with_updated_at and is_next_payment_day_correct are TRUE:")
print(filtered_rows_2.head()) #adjust here if you want to see all filtered_rows





# Adding reason_of_discrepancy only to rows in filtered_rows_2
df_events_adjusted['reason_of_discrepancy'] = df_events_adjusted.apply(
    lambda row: categorize_discrepancy(row) if row.name in filtered_rows_2.index else '', axis=1)

# Count the number of rows for each category
category_counts = df_events_adjusted['reason_of_discrepancy'].value_counts()

# Print the counts for each category
print("Count of rows categorized by reason_of_discrepancy:")
print(category_counts)

# Print rows for each category
for category in category_counts.index:
    print(f"\nRows categorized as '{category}':")
    print(df_events_adjusted[df_events_adjusted['reason_of_discrepancy'] == category][['user.id', 'reason_of_discrepancy']].head())





# Saving the final dataframe with the new 'reason_of_discrepancy' column to a CSV file
output_file = 'discrepancies_in_payment_dates.csv'
df_events_adjusted.to_csv(output_file, index=False)

# Confirming the file is saved
print(f"\nThe final DataFrame has been saved to {output_file}")





############################################################### Payment Schedule Backend Table analysis #################################################################################################



# Checking for duplicates in the user_id column
# Counting how many times each user_id appears
user_id_counts = df_payment['user_id'].value_counts()

# Filtering only the user_ids that appear more than once
duplicate_user_ids = user_id_counts[user_id_counts > 1]

print(f"Number of duplicate user_ids: {len(duplicate_user_ids)}")
print(duplicate_user_ids)

# Removing duplicates and keeping only the first occurrence of each user_id
df_payment_unique = df_payment.drop_duplicates(subset='user_id', keep='first')

# Verifying the result
print(f"Dataframe after removing duplicates: {df_payment_unique.head()}")




# Performing the merge between df_merged and df_payment
df_final_merged = pd.merge(
    df_events_merged,  # DataFrame df_merged (already containing the backend and user_id links)
    df_payment,  # DataFrame df_payment
    left_on='user.id',  # Key from df_merged
    right_on='user_id',  # Key from df_payment
    how='left',  # Left join to keep all records from df_merged
    suffixes=('_merged', '_payment')  # Suffixes for the columns after the merge
)

# Displaying the final merge result
print(df_final_merged.head())

# Ensuring that the dates are formatted as strings with two digits and then converted to integers for comparison
df_final_merged['payment_date'] = df_final_merged['payment_date'].astype(str).str.zfill(2).astype(int)
df_final_merged['next_payment_day'] = df_final_merged['next_payment_day'].astype(str).str.zfill(2).astype(int)
df_final_merged['next_expected_payment_date'] = df_final_merged['next_expected_payment_date'].astype(str).str.zfill(2).astype(int)

# Creating a new column to categorize the payment date matches
df_final_merged['payment_date_status'] = df_final_merged.apply(
    lambda row: 'backend error - logic' if row['payment_date'] == row['next_payment_day'] and row['payment_date'] != row['next_expected_payment_date'] else
               ('backend error - timestamp' if row['payment_date'] != row['next_payment_day'] and row['payment_date'] == row['next_expected_payment_date'] else
               ('unknown error' if row['payment_date'] != row['next_payment_day'] and row['payment_date'] != row['next_expected_payment_date'] else
               'correct payment date')), axis=1)

# Displaying the result
print(df_final_merged[['user_id', 'payment_date', 'next_payment_day', 'next_expected_payment_date', 'payment_date_status']].head())

# Saving the result to a CSV file
output_file_payment_status = 'payment_table_discrepancy.csv'
df_final_merged[['user_id', 'payment_date', 'next_payment_day', 'next_expected_payment_date', 'payment_date_status']].to_csv(output_file_payment_status, index=False)