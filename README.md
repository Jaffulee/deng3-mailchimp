<!-- README.html -->

<p><a href="https://jaffulee.github.io/Jaffulee/">Visit my website</a></p>

<h1>deng3-mailchimp</h1>

<p><strong>Mailchimp API Extraction Pipeline</strong><br>
Extracting campaign and email engagement data from the Mailchimp Marketing API for downstream analytics.</p>

<ul>
  <li>GitHub: <a href="https://github.com/Jaffulee">https://github.com/Jaffulee</a></li>
  <li>LinkedIn: <a href="https://www.linkedin.com/in/jeffrey-brian-thompson/">https://www.linkedin.com/in/jeffrey-brian-thompson/</a></li>
  <li>Writing: <a href="https://www.thedataschool.co.uk/blog/jeffrey-brian-thompson/">https://www.thedataschool.co.uk/blog/jeffrey-brian-thompson/</a></li>
</ul>

<hr>

<h2>About</h2>

<p>
This project extracts <strong>campaign metadata</strong> and <strong>email-level engagement activity</strong> from the
<strong>Mailchimp Marketing API</strong>, writes the results as JSON files, and uploads them to <strong>AWS S3</strong>.
</p>

<p>
It forms the <strong>Bronze (raw ingestion) layer</strong> of a wider analytics pipeline, with transformations and modelling
handled downstream in dbt Cloud.
</p>

<hr>

<h2>Downstream Usage</h2>

<p>
The data produced by this project is modelled and transformed in the following dbt Cloud project:
</p>

<ul>
  <li>
    <strong>dbt analytics layer:</strong>
    <a href="https://github.com/Jaffulee/dbt-cloud-deng3">
      https://github.com/Jaffulee/dbt-cloud-deng3
    </a>
  </li>
</ul>

<p>
In dbt, the raw Mailchimp JSON data is:
</p>

<ul>
  <li>Parsed and standardised in <strong>staging (Bronze)</strong> models</li>
  <li>Flattened and enriched in <strong>intermediate (Silver)</strong> models</li>
  <li>Exposed as analytics-ready <strong>mart (Gold)</strong> models for reporting</li>
</ul>

<hr>

<h2>Features</h2>

<ul>
  <li>Extracts Mailchimp campaign metadata for configurable previous months</li>
  <li>Extracts email activity for each campaign</li>
  <li>Saves campaign and email data as JSON files</li>
  <li>Uploads extracted data and logs to Amazon S3</li>
  <li>CSV-based logging of API calls, retries, errors, and file actions</li>
  <li>Retry and timeout logic for resilient API extraction</li>
  <li>Secrets managed via <code>.env</code> file</li>
</ul>

<hr>

<h2>Project Structure</h2>

<pre>
├── extract_mailchimp_data.py      # Main extraction and upload logic
├── modules/
│   └── load_data_to_s3.py         # Shared S3 upload utilities
├── requirements.txt               # Python dependencies
├── schema_example.sql              # Example downstream schema
├── snowpipe_example.sql            # Example Snowflake ingestion pattern
└── README.md
</pre>

<hr>

<h2>How It Works</h2>

<h3>Step 1: Extract Campaign & Email Data</h3>

<p>
For each of the last <code>N</code> months (configurable), the script:
</p>

<ul>
  <li>Fetches campaign metadata using <code>client.campaigns.list(...)</code></li>
  <li>Extracts campaign IDs</li>
  <li>Fetches email activity for each campaign using
      <code>client.reports.get_email_activity_for_campaign(...)</code></li>
  <li>Writes the results as JSON files</li>
</ul>

<h3>Step 2: Logging</h3>

<p>
All actions are logged to a CSV file with timestamps and descriptions, including:
</p>

<ul>
  <li>File creation and overwrites</li>
  <li>S3 uploads</li>
  <li>Retries, waits, and API errors</li>
</ul>

<h3>Step 3: Upload to S3</h3>

<p>
All JSON files and logs are uploaded to an S3 bucket using <code>boto3</code>.
Local file retention can be configured.
</p>

<hr>

<h2>Configuration</h2>

<p>
Secrets are stored in a <code>.env</code> file (excluded from version control):
</p>

<pre>
MAILCHIMP_API_KEY=your-mailchimp-api-key
Access_key_ID=your-aws-access-key-id
Secret_access_key=your-aws-secret-access-key
AWS_BUCKET_NAME=your-s3-bucket-name
</pre>

<p>
Key configurable parameters include:
</p>

<ul>
  <li><code>month_diffs</code> — number of months back to extract</li>
  <li><code>overwrite_if_already_exists</code> — overwrite local files</li>
  <li><code>remove_local</code> — delete local files after upload</li>
  <li><code>wait_time</code> / <code>total_wait_time</code> — retry behaviour</li>
</ul>

<hr>

<h2>Notes</h2>

<ul>
  <li>This project focuses on <strong>extraction and loading only</strong>.</li>
  <li>No transformations or business logic are applied here.</li>
  <li>All modelling and analytics logic lives in the downstream dbt project.</li>
</ul>
