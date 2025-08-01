import json
import os
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import pandas as pd
import modules.load_data_to_s3 as ls3

# Mailchimp API:
# Reference: https://mailchimp.com/developer/marketing/api/root/
# Python SDK: https://github.com/mailchimp/mailchimp-marketing-python

# If True, will overwrite existing files with new data
overwrite_if_already_exists = True

# Retry parameters
wait_time = 5
total_wait_time = 10
month_diffs = [1, 2]  # How many months back to extract

# Load API key from .env
load_dotenv()
api_keys = {'api_key': os.getenv('MAILCHIMP_API_KEY')}

# Set up local folders for data and logs
data_dir = 'data'
campaign_data_dir = os.path.join(data_dir, 'campaign')
email_dir = os.path.join(data_dir, 'email')
log_path = os.path.join(data_dir, 'mailchimp_extract_logs.csv')

os.makedirs(data_dir, exist_ok=True)
os.makedirs(campaign_data_dir, exist_ok=True)
os.makedirs(email_dir, exist_ok=True)

# Logging setup
log_times = []
log_items = []
log_descriptions = []

log_desriptions_dict = {
    'create': 'File created',
    'delete': 'File deleted',
    'copy': 'File copied',
    'overwrite': 'File overwritten',
    'extract': 'Zip extracted',
    'get': 'API get',
    'timeout': 'Timeout - total wait time exceeded',
    'wait': 'Waiting before trying again in'
}
log_items_dict = {
    'error': 'Error',
    'api': 'API call',
    'timeout': 'Timeout',
    'wait': 'Wait'
}

# Filename prefixes and date formats
campaign_filename_base = 'campaigns'
email_filename_base = 'emailcampaign'
date_format_API = '%Y-%m-%dT%H:%M:%S+00:00'
date_format_out = '%Y%m%dT%H%M%S'
now_datetime = dt.datetime.now()

# Loop through selected months
for month_diff in month_diffs:
    # Get start/end for that month
    now_month = now_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    previous_month_start = now_month - relativedelta(months=month_diff)
    previous_month_end = (now_month - relativedelta(months=month_diff - 1)) - dt.timedelta(seconds=1)

    # Format for API
    since_time = previous_month_start.strftime(date_format_API)
    before_time = previous_month_end.strftime(date_format_API)
    print(since_time)
    print(before_time)

    # Create ID string for file naming
    date_string_id_out = previous_month_start.strftime(date_format_out) + '-' + previous_month_end.strftime(date_format_out)
    waited_time = 0

    while waited_time < total_wait_time:
        try:
            # Ping Mailchimp API to validate credentials
            client = MailchimpMarketing.Client()
            client.set_config(api_keys)
            status_response = client.ping.get()
            print(status_response, '\n')
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['api'])
            log_descriptions.append(log_desriptions_dict['get'])

            # Fetch campaigns for that month
            campaigns_list = client.campaigns.list(since_send_time=since_time, before_send_time=before_time)
            print('\ncampaigns_list extracted')

            # Save campaign list to JSON
            campaign_filename = campaign_filename_base + date_string_id_out + '.json'
            campaigns_filepath = os.path.join(campaign_data_dir, campaign_filename)
            file_exists = os.path.exists(campaigns_filepath)
            file_non_empty = file_exists and os.path.getsize(campaigns_filepath) > 0

            if not file_exists or not file_non_empty:
                action = 'create'
            elif overwrite_if_already_exists:
                action = 'overwrite'
            else:
                print('File already exists, moving on.')
                action = None

            if action:
                with open(campaigns_filepath, 'w') as file:
                    json.dump(campaigns_list, file, indent=2)
                    log_times.append(dt.datetime.now())
                    log_items.append(campaigns_filepath)
                    log_descriptions.append(log_desriptions_dict[action])

            # Extract campaign IDs from response
            campaign_ids = [campaign['id'] for campaign in campaigns_list['campaigns']]
            print('\ncampaign_ids extracted\n', campaign_ids)

            # Loop through campaign IDs and fetch email activity
            for campaign in campaign_ids:
                email_data = client.reports.get_email_activity_for_campaign(campaign)
                print('Email data loaded')
                email_filename = email_filename_base + campaign + '_' + date_string_id_out + '.json'
                email_filepath = os.path.join(email_dir, email_filename)

                file_exists = os.path.exists(email_filepath)
                file_non_empty = file_exists and os.path.getsize(email_filepath) > 0

                if not file_exists or not file_non_empty:
                    action = 'create'
                elif overwrite_if_already_exists:
                    action = 'overwrite'
                else:
                    print('File already exists, moving on.')
                    action = None

                if action:
                    with open(email_filepath, 'w') as file:
                        json.dump(email_data, file, indent=2)
                        log_times.append(dt.datetime.now())
                        log_items.append(email_filepath)
                        log_descriptions.append(log_desriptions_dict[action])

            break  # Exit retry loop if successful

        except ApiClientError as e:
            # Handle known Mailchimp errors
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(str(e) or 'No error message')
            print(f'Error {e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time += wait_time
            time.sleep(wait_time)
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

        except Exception as e:
            # Catch-all for other exceptions
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(str(e) or 'No error message')
            print(f'Error {e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time += wait_time
            time.sleep(wait_time)
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

    # If total wait exceeded, log timeout
    if waited_time >= total_wait_time:
        print(f'Timeout. Total time waited {waited_time}s.')
        log_times.append(dt.datetime.now())
        log_items.append(log_items_dict['timeout'])
        log_descriptions.append(f'{log_desriptions_dict["timeout"]} ({waited_time}s out of {wait_time}s)')

# Write logs to disk
log_df = pd.DataFrame({
    'log_time': log_times,
    'log_item': log_items,
    'log_description': log_descriptions
})

# Append to previous log file if it exists
if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
    existing_log_df = pd.read_csv(log_path)
else:
    existing_log_df = pd.DataFrame()

combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)
combined_log_df.to_csv(log_path, index=False)
print(f"Appended new logs to {log_path}")

# Upload results to S3
api_keys = {
    'Access_key_ID': os.getenv('Access_key_ID'),
    'Secret_access_key': os.getenv('Secret_access_key'),
    'AWS_BUCKET_NAME': os.getenv('AWS_BUCKET_NAME')
}

remove_local = False
filepath_bases_json = ['data/campaign', 'data/email']
s3filepath_bases_json = ['python-import/campaign', 'python-import/email']
filepath_base_log = 'data'
s3filepath_base_log = 'python-import/log'

for i, filepath_base in enumerate(filepath_bases_json):
    s3filepath_base = s3filepath_bases_json[i]
    ls3.load_files_to_s3(filepath_base, s3filepath_base, api_keys, '.json', remove_local)

ls3.load_logs_csv(filepath_base_log, s3filepath_base_log, api_keys, remove_local)
