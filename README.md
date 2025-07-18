# 📬 Mailchimp Extractor & S3 Uploader

This Python script extracts **campaign** and **email activity data** from the **Mailchimp Marketing API**, stores them as JSON files locally, and uploads the data to an **AWS S3 bucket**. It also maintains a **detailed log** of actions and errors, saved locally and uploaded to S3.

---

## 🚀 Features

- Extracts Mailchimp campaign metadata for the previous months
- Extracts email activity for each campaign
- Saves campaign and email data as `.json` files
- Logs API activity, errors, retries, and file actions in a CSV log
- Uploads all `.json` and log files to Amazon S3
- Supports retry and timeout logic for resilient API extraction
- Uses `.env` file to securely store API and AWS credentials

---

```
## 📁 Project Structure


```

project/
│
├── data/ # Local folder for extracted JSON data and logs
│ ├── campaign/ # Campaign-level data
│ ├── email/ # Email-level data
│ └── mailchimp_extract_logs.csv
│
├── modules/
│ └── load_data_to_s3.py # Upload functions for S3
│
├── .env # Environment variables (excluded from version control)
├── main_script.py # Main extraction and upload logic
└── README.md

```

---

🔐 .env File Example
Create a .env file in the root directory:

MAILCHIMP_API_KEY=your-mailchimp-api-key
Access_key_ID=your-aws-access-key-id
Secret_access_key=your-aws-secret-access-key
AWS_BUCKET_NAME=your-s3-bucket-name
🧪 How It Works
✅ Step 1: Extract Campaign and Email Data
For each of the last N months (customizable), the script:

Gets campaign data using client.campaigns.list(...)

Extracts campaign IDs

For each campaign ID, fetches email activity using client.reports.get_email_activity_for_campaign(...)

Saves the results as JSON files to data/campaign/ and data/email/

✅ Step 2: Logging
Logs are recorded with timestamps and actions (create, overwrite, error, wait)

Appended to mailchimp_extract_logs.csv

✅ Step 3: Upload to S3
All .json and log files are uploaded to your S3 bucket using the boto3 client

You can configure whether to delete local files after upload with remove_local = True

📝 Logging Example
Each row in mailchimp_extract_logs.csv contains:

log_time log_item log_description
2025-07-10 12:00:00 data/campaign/file1.json File created
2025-07-10 12:00:10 python-import/campaign/file1 File copied to S3
2025-07-10 12:00:15 Error TimeoutError: ...

🛠️ Customization
You can easily configure:

month_diffs = [1, 2] — number of months back to extract

overwrite_if_already_exists = True — whether to overwrite local files

remove_local = False — whether to remove local files after upload

wait_time and total_wait_time — retry configuration on API failure

```

```

```
