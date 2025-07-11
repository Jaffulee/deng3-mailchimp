import json
import os
import datetime as dt
import time
from dateutil.relativedelta import relativedelta
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from dotenv import load_dotenv
import pandas as pd

# API Reference: https://mailchimp.com/developer/marketing/api/root/
# Python Library: https://github.com/mailchimp/mailchimp-marketing-python

# Set overwrite if file exists
overwrite_if_already_exists = True

# init wait times and relative month selections
wait_time = 5
total_wait_time = 10
month_diffs = [1,2]

# load .env file
load_dotenv()

# read .env file
api_keys = {'api_key' : os.getenv('MAILCHIMP_API_KEY')}

# init directories
data_dir = 'data'
campaign_data_dir = os.path.join(data_dir,'campaign')
email_dir = os.path.join(data_dir,'email')
log_path = os.path.join(data_dir,'mailchimp_extract_logs.csv')
os.makedirs(data_dir, exist_ok = True)
os.makedirs(campaign_data_dir, exist_ok = True)
os.makedirs(email_dir, exist_ok = True)

# Init logs
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
    'api' : 'API call',
    'timeout': 'Timeout',
    'wait': 'Wait'
}

# Init out paths and dates
campaign_filename_base = 'campaigns'
email_filename_base = 'emailcampaign'

date_format_API = '%Y-%m-%dT%H:%M:%S+00:00'
date_format_out = '%Y%m%dT%H%M%S'
now_datetime = dt.datetime.now()

for month_diff in month_diffs:
    # get previous month start and end
    now_month = now_datetime.replace(day=1,hour=0, minute=0, second=0, microsecond=0)
    previous_month_start = now_month - relativedelta(months=month_diff)
    previous_month_end = (now_month - relativedelta(months=month_diff-1)) - dt.timedelta(seconds=1)

    # format as string for api call parameter
    since_time = previous_month_start.strftime(date_format_API)
    before_time = previous_month_end.strftime(date_format_API)
    print(since_time)
    print(before_time)

    date_string_id_out = previous_month_start.strftime(date_format_out) +'-'+ previous_month_end.strftime(date_format_out)

    waited_time = 0

    while waited_time<total_wait_time:
        try:
            #Check response
            client = MailchimpMarketing.Client()
            client.set_config(api_keys)
            status_response = client.ping.get() # Errors if bad - secretly raise for status
            print(status_response,'\n')
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['api'])
            log_descriptions.append(log_desriptions_dict['get'])

            # Get campaigns json
            campaigns_list = client.campaigns.list(since_send_time=since_time, before_send_time = before_time)
            print('\ncampaigns_list extracted')

            campaign_filename = campaign_filename_base + date_string_id_out + '.json'
            campaigns_filepath = os.path.join(campaign_data_dir,campaign_filename)

            file_exists = os.path.exists(campaigns_filepath)
            file_non_empty = file_exists and os.path.getsize(campaigns_filepath) > 0

            # Determine what to do
            if not file_exists or not file_non_empty:
                action = 'create'
            elif overwrite_if_already_exists:
                action = 'overwrite'
            else:
                print('File already exists, moving on.')
                action = None

            # Write if needed
            if action:
                with open(campaigns_filepath, 'w') as file:
                    json.dump(campaigns_list, file, indent=2)
                    log_times.append(dt.datetime.now())
                    log_items.append(campaigns_filepath)
                    log_descriptions.append(log_desriptions_dict[action])
            # Parse IDs from json
            campaign_ids = [campaign['id'] for campaign in campaigns_list['campaigns']]
            print('\ncampaign_ids extracted\n',campaign_ids)

            for campaign in campaign_ids:
                email_data = client.reports.get_email_activity_for_campaign(campaign)
                print('Email data loaded')
                email_filename = email_filename_base + campaign +'_'+ date_string_id_out + '.json'
                email_filepath = os.path.join(email_dir,email_filename)

                file_exists = os.path.exists(email_filepath)
                file_non_empty = file_exists and os.path.getsize(email_filepath) > 0

                # Determine what to do
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

            break

        except ApiClientError as e: # for future specific error handling
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(e)
            print(f'Error {e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time+=wait_time
            time.sleep(wait_time)
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

        except Exception as e:
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['error'])
            log_descriptions.append(e)
            print(f'Error {e}')
            print(f'Trying again, waiting {wait_time}s, total time waited {waited_time}s.')
            waited_time+=wait_time
            time.sleep(wait_time)
            log_times.append(dt.datetime.now())
            log_items.append(log_items_dict['wait'])
            log_descriptions.append(f'{log_desriptions_dict["wait"]} {wait_time}s ({waited_time}s out of {wait_time}s)')

    if waited_time>= total_wait_time:
        print(f'Timout. Total time waited {waited_time}s.')
        log_times.append(dt.datetime.now())
        log_items.append(log_items_dict['timeout'])
        log_descriptions.append(f'{log_desriptions_dict["timeout"]} ({waited_time}s out of {wait_time}s)')

# Create and combine logs
log_df = pd.DataFrame({
    'log_time': log_times,
    'log_item': log_items,
    'log_description': log_descriptions
})

# Load existing logs if the file exists and is not empty
if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
    existing_log_df = pd.read_csv(log_path)
else:
    existing_log_df = pd.DataFrame()

# Combine the existing logs with the new one
combined_log_df = pd.concat([existing_log_df, log_df], ignore_index=True)

# Save the combined logs back to file
combined_log_df.to_csv(log_path,index=False)

print(f"Appended new logs to {log_path}")