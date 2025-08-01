use database til_data_engineering;
use schema jt_deng3_staging;

DESC INTEGRATION JT_DENG3_AIRBYTE_SYNC_MAILCHIMP;

CREATE SCHEMA JT_DENG3_STAGING;

USE SCHEMA JT_DENG3_STAGING;

CREATE OR REPLACE FILE FORMAT jts_json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT jts_jsonl_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE;

CREATE OR REPLACE FILE FORMAT jts_parquet_format
  TYPE = 'PARQUET';
  
CREATE OR REPLACE FILE FORMAT jts_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6';

  
CREATE OR REPLACE STAGE mailchimp_AIRBYTE_SYNC_stage
  STORAGE_INTEGRATION = JT_DENG3_AIRBYTE_SYNC_MAILCHIMP
  URL = 's3://deng3-jt-mailchimp/airbyte-sync/'
  FILE_FORMAT = jts_jsonl_format;
  
LIST @mailchimp_AIRBYTE_SYNC_stage;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_campaigns (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_campaigns
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/campaigns
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_campaigns
LIMIT 100;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_email_activity (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_email_activity
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/email_activity
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_email_activity
LIMIT 100;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_list_members (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_list_members
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/list_members
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_list_members
LIMIT 100;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_lists (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_lists
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/lists
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_lists
LIMIT 100;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_reports (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_reports
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/reports
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_reports
LIMIT 100;

CREATE OR REPLACE TABLE airbyte_sync_mailchimp_data_raw_python_unsubscribes (
  json_data VARIANT
);

COPY INTO airbyte_sync_mailchimp_data_raw_python_unsubscribes
FROM @mailchimp_airbyte_sync_stage/MAILCHIMP/unsubscribes
FILE_FORMAT = (FORMAT_NAME = jts_jsonl_format);

SELECT *
FROM airbyte_sync_mailchimp_data_raw_python_unsubscribes
LIMIT 100
;



-- Create new schema for mailchimp silver and tarnished layer
create schema jt_deng3_mailchimp;
use schema jt_deng3_mailchimp;

SELECT *
FROM jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns
LIMIT 100;


SELECT *
FROM jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_email_activity
LIMIT 100;


-- Create campaign table by parsing the raw data
create or replace table campaign as (
with parse_json_cte as (
    select
        hash(r.json_data)::int as created_id,
        (parse_json(r.json_data)):"_airbyte_data" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as created_campaign_id,
    json:"archive_url"::string as campaign_archive_url,
    json:"content_type"::string as content_type,
    json:"create_time"::timestamp as create_time,
    hash(json:"delivery_status")::int as delivery_status_id,
    json:"emails_sent"::int as emails_sent,
    json:"id"::string as campaign_id,
    json:"long_archive_url"::string as campaign_long_archive_url,
    json:"needs_block_refresh"::boolean as needs_block_refresh, -- deprecated
    hash(json:"recipients")::int as recipients_id,
    hash(json:"report_summary")::int as report_summary_id,
    json:"resendable"::int as resendable,
    json:"send_time"::timestamp as send_time,
    hash(json:"settings")::int as settings_id,
    json:"status"::string as campaign_status,
    hash(json:"tracking")::int as tracking_id,
    json:"type"::string as campaign_type,
    json:"web_id"::int as web_id
from parse_json_cte as e

)
-- limit 1
;


-- Create campaign_delivery_status table by parsing the raw data
create or replace table campaign_delivery_status as (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"delivery_status"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"delivery_status" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as delivery_status_id,
    json:"can_cancel"::boolean as can_cancel,
    json:"emails_canceled"::int as emails_canceled,
    json:"emails_sent"::int as emails_sent,
    json:"enabled"::boolean as enabled,
    json:"status"::string as status

from parse_json_cte as e
group by all

)
-- limit 1
;

-- Create recipients table by parsing the raw data
create or replace table campaign_recipients as (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"recipients"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"recipients" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as recipients_id,
    json:"list_id"::string as list_id,
    json:"list_is_active"::boolean as list_is_active,
    json:"list_name"::string as list_name,
    json:"recipient_count"::int as recipient_count


from parse_json_cte as e
group by all

)
-- limit 1
;

-- Create campaign_reports table by parsing the raw data
create or replace table campaign_reports as (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"report_summary"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"report_summary" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as report_summary_id,
    json:"click_rate"::float as click_rate,
    json:"clicks"::int as clicks,
    hash(json:"ecommerce")::int as ecommerce_id,
    json:"open_rate"::float as open_rate,
    json:"opens"::int as opens,
    json:"subscriber_clicks"::int as subscriber_clicks,
    json:"unique_opens"::int as unique_opens

from parse_json_cte as e
group by all

)
-- limit 1
;

-- Create campaign_reports_ecommerce table by parsing the raw data
create or replace table campaign_reports_ecommerce as (
with parse_json_cte as (
    select
        hash(
(((parse_json(r.json_data)):"_airbyte_data"):"report_summary"):"ecommerce"
        )::int as created_id,
        (((parse_json(r.json_data)):"_airbyte_data"):"report_summary"):"ecommerce" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as ecommerce_id,
    json:"total_orders"::int as total_orders,
    json:"total_revenue"::int as total_revenue,
    json:"total_spent"::int as total_spent

from parse_json_cte as e
group by all

)
-- limit 1
;


-- Create campaign_settings table by parsing the raw data
create or replace table campaign_settings as (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"settings"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"settings" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as settings_id,
    json:"authenticate"::boolean as authenticate,
    json:"auto_footer"::boolean as auto_footer,
    json:"auto_tweet"::boolean as auto_tweet,
    json:"drag_and_drop"::boolean as drag_and_drop,
    json:"fb_comments"::boolean as fb_comments,
    json:"from_name"::string as from_name,
    json:"inline_css"::boolean as inline_css,
    json:"preview_text"::string as preview_text,
    json:"reply_to"::string as reply_to,
    json:"subject_line"::string as subject_line,
    json:"template_id"::int as template_id,
    json:"timewarp"::boolean as timewarp,
    json:"title"::string as title,
    json:"use_conversation"::boolean as use_conversation

from parse_json_cte as e
group by all

)
-- limit 1
;


-- Create campaign_tracking table by parsing the raw data
create or replace table campaign_tracking as (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"tracking"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"tracking" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as tracking_id,
    json:"ecomm360"::boolean as ecomm360,
    json:"goal_tracking"::boolean as goal_tracking,
    json:"html_clicks"::boolean as html_clicks,
    json:"opens"::boolean as opens,
    json:"text_clicks"::boolean as text_clicks

from parse_json_cte as e
group by all

)
-- limit 1
;


-- Create email_activity table by parsing the raw data
create or replace table email_activity as (
with parse_json_cte as (
    select
        hash(r.json_data)::int as created_id,
        (parse_json(r.json_data)):"_airbyte_data" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_email_activity as r
)

select
    created_id as created_email_id,
    json:"action"::string as action,
    json:"campaign_id"::string as campaign_id,
    json:"email_address"::string as email_address,
    json:"email_id"::string as email_id,
    json:"ip"::string as email_ip,
    json:"list_id"::string as list_id,
    json:"list_is_active"::boolean as list_is_active,
    json:"timestamp"::timestamp as email_timestamp,
    json:"url"::string as email_url
from parse_json_cte as e

)
-- limit 1
;

create or replace procedure update_silver_layer_mailchimp_campaign()
returns varchar
language sql
as
$$
begin

-- get new campaigns
create or replace table new_campaigns as (
    with parse_json_cte as (
        select
            hash(r.json_data)::int as created_id,
            (parse_json(r.json_data)):"_airbyte_data" as json
        from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
    )
    
    select
        created_id as created_campaign_id,
        hash(json:"delivery_status")::int as delivery_status_id,
        hash(json:"recipients")::int as recipients_id,
        hash(json:"report_summary")::int as report_summary_id,
        hash(json:"settings")::int as settings_id,
        hash(json:"tracking")::int as tracking_id,
        hash((json:"report_summary"):"ecommerce")::int as ecommerce_id
    from parse_json_cte as e
    where json:"create_time"::timestamp > (
    select max(create_time) from campaign
    )

    );


    
-- Update campaigns and the other tables using new campaigns
    insert into campaign (
    with parse_json_cte as (
        select
            hash(r.json_data)::int as created_id,
            (parse_json(r.json_data)):"_airbyte_data" as json
        from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
    )
    
    select
        created_id as created_campaign_id,
        json:"archive_url"::string as campaign_archive_url,
        json:"content_type"::string as content_type,
        json:"create_time"::timestamp as create_time,
        hash(json:"delivery_status")::int as delivery_status_id,
        json:"emails_sent"::int as emails_sent,
        json:"id"::string as campaign_id,
        json:"long_archive_url"::string as campaign_long_archive_url,
        json:"needs_block_refresh"::boolean as needs_block_refresh, -- deprecated
        hash(json:"recipients")::int as recipients_id,
        hash(json:"report_summary")::int as report_summary_id,
        json:"resendable"::int as resendable,
        json:"send_time"::timestamp as send_time,
        hash(json:"settings")::int as settings_id,
        json:"status"::string as campaign_status,
        hash(json:"tracking")::int as tracking_id,
        json:"type"::string as campaign_type,
        json:"web_id"::int as web_id
    from parse_json_cte as e
    where created_id in (
    select created_campaign_id from new_campaigns
    )

    )
;


-- update campaign_delivery_status table by parsing the raw data
insert into  campaign_delivery_status (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"delivery_status"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"delivery_status" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as delivery_status_id,
    json:"can_cancel"::boolean as can_cancel,
    json:"emails_canceled"::int as emails_canceled,
    json:"emails_sent"::int as emails_sent,
    json:"enabled"::boolean as enabled,
    json:"status"::string as status

from parse_json_cte as e
group by all
    having created_id in (
    select delivery_status_id from new_campaigns
    )

)
-- limit 1
;

--  recipients table by parsing the raw data
insert into campaign_recipients (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"recipients"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"recipients" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as recipients_id,
    json:"list_id"::string as list_id,
    json:"list_is_active"::boolean as list_is_active,
    json:"list_name"::string as list_name,
    json:"recipient_count"::int as recipient_count


from parse_json_cte as e
group by all
    having created_id in (
    select recipients_id from new_campaigns
    )

)
-- limit 1
;

-- Create campaign_reports table by parsing the raw data
insert into campaign_reports (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"report_summary"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"report_summary" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as report_summary_id,
    json:"click_rate"::float as click_rate,
    json:"clicks"::int as clicks,
    hash(json:"ecommerce")::int as ecommerce_id,
    json:"open_rate"::float as open_rate,
    json:"opens"::int as opens,
    json:"subscriber_clicks"::int as subscriber_clicks,
    json:"unique_opens"::int as unique_opens

from parse_json_cte as e
group by all
    having created_id in (
    select report_summary_id from new_campaigns
    )

)
-- limit 1
;

--  campaign_reports_ecommerce table by parsing the raw data
insert into campaign_reports_ecommerce (
with parse_json_cte as (
    select
        hash(
(((parse_json(r.json_data)):"_airbyte_data"):"report_summary"):"ecommerce"
        )::int as created_id,
        (((parse_json(r.json_data)):"_airbyte_data"):"report_summary"):"ecommerce" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as ecommerce_id,
    json:"total_orders"::int as total_orders,
    json:"total_revenue"::int as total_revenue,
    json:"total_spent"::int as total_spent

from parse_json_cte as e
group by all
    having created_id in (
    select ecommerce_id from new_campaigns
    )

)
-- limit 1
;


--  campaign_settings table by parsing the raw data
insert into campaign_settings (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"settings"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"settings" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as settings_id,
    json:"authenticate"::boolean as authenticate,
    json:"auto_footer"::boolean as auto_footer,
    json:"auto_tweet"::boolean as auto_tweet,
    json:"drag_and_drop"::boolean as drag_and_drop,
    json:"fb_comments"::boolean as fb_comments,
    json:"from_name"::string as from_name,
    json:"inline_css"::boolean as inline_css,
    json:"preview_text"::string as preview_text,
    json:"reply_to"::string as reply_to,
    json:"subject_line"::string as subject_line,
    json:"template_id"::int as template_id,
    json:"timewarp"::boolean as timewarp,
    json:"title"::string as title,
    json:"use_conversation"::boolean as use_conversation

from parse_json_cte as e
group by all
    having created_id in (
    select settings_id from new_campaigns
    )

)
-- limit 1
;


-- campaign_tracking table by parsing the raw data
insert into campaign_tracking (
with parse_json_cte as (
    select
        hash(
            ((parse_json(r.json_data)):"_airbyte_data"):"tracking"
        )::int as created_id,
        ((parse_json(r.json_data)):"_airbyte_data"):"tracking" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_campaigns as r
)

select
    created_id as tracking_id,
    json:"ecomm360"::boolean as ecomm360,
    json:"goal_tracking"::boolean as goal_tracking,
    json:"html_clicks"::boolean as html_clicks,
    json:"opens"::boolean as opens,
    json:"text_clicks"::boolean as text_clicks

from parse_json_cte as e
group by all
    having created_id in (
    select tracking_id from new_campaigns
    )
)
-- limit 1
;


return 'Insert completed - campaign.';
end;
$$
;

create or replace procedure update_silver_layer_mailchimp_email()
returns varchar
language sql
as
$$
begin

--get new emails
create or replace table new_email_lists as (
-- Create email_activity table by parsing the raw data
with parse_json_cte as (
    select
        hash(r.json_data)::int as created_id,
        (parse_json(r.json_data)):"_airbyte_data" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_email_activity as r
)

select
    created_id as created_email_id,
from parse_json_cte as e
where json:"timestamp"::timestamp > (
select max(email_timestamp) from email_activity
)

);

--update emails

insert into email_activity (
with parse_json_cte as (
    select
        hash(r.json_data)::int as created_id,
        (parse_json(r.json_data)):"_airbyte_data" as json
    from jt_deng3_staging.airbyte_sync_mailchimp_data_raw_python_email_activity as r
)

select
    created_id as created_email_id,
    json:"action"::string as action,
    json:"campaign_id"::string as campaign_id,
    json:"email_address"::string as email_address,
    json:"email_id"::string as email_id,
    json:"ip"::string as email_ip,
    json:"list_id"::string as list_id,
    json:"list_is_active"::boolean as list_is_active,
    json:"timestamp"::timestamp as email_timestamp,
    json:"url"::string as email_url
from parse_json_cte as e
where created_id in (select created_email_id from new_email_lists)

)
-- limit 1
;

return 'Insert completed.';
end;
$$
;

call update_silver_layer_mailchimp_campaign();
call update_silver_layer_mailchimp_email();


-- create streams for the two underlying tables
create or replace stream mailchimp_campaign_raw_table_stream on table JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_campaigns;
create or replace stream mailchimp_email_raw_table_stream on table JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_EMAIL_ACTIVITY;


--run task based on stream
create or replace task run_mailchimp_campaign_refresh
warehouse = dataschool_wh
when SYSTEM$STREAM_HAS_DATA('mailchimp_email_raw_table_stream')
as
call update_silver_layer_mailchimp_campaign();

create or replace task run_mailchimp_email_refresh
warehouse = dataschool_wh
when SYSTEM$STREAM_HAS_DATA('mailchimp_email_raw_table_stream')
as
call update_silver_layer_mailchimp_email();








--fake insert rows
insert into JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_campaigns (
select parse_json('{
  "_airbyte_data": {
    "archive_url": "http://eepurl.com/ji0tto",
    "content_type": "template",
    "create_time": "2028-07-12T21:04:23+00:00",
    "delivery_status": {
      "enabled": false
    },
    "emails_sent": 9919,
    "id": "cc9e35ace5",
    "long_archive_url": "https://us2.campaign-archive.com/?u=8954cfd70761b8c7fd5d8b528&id=cc9e35ace5",
    "needs_block_refresh": false,
    "recipients": {
      "list_id": "fc1dab699f",
      "list_is_active": true,
      "list_name": "The Information Lab Newsletter",
      "recipient_count": 9919
    },
    "report_summary": {
      "click_rate": 0.01783479349186483,
      "clicks": 757,
      "ecommerce": {
        "total_orders": 0,
        "total_revenue": 0,
        "total_spent": 0
      },
      "open_rate": 0.35294117647058826,
      "opens": 5312,
      "subscriber_clicks": 171,
      "unique_opens": 3384
    },
    "resendable": false,
    "send_time": "2025-07-14T07:30:00+00:00",
    "settings": {
      "authenticate": true,
      "auto_footer": false,
      "auto_tweet": false,
      "drag_and_drop": false,
      "fb_comments": false,
      "inline_css": false,
      "preview_text": "Explore Tableau Next and register for upcoming events.  ",
      "template_id": 0,
      "timewarp": false,
      "title": "July 2025 - Newsletter #10",
      "use_conversation": false
    },
    "status": "sent",
    "tracking": {
      "ecomm360": true,
      "goal_tracking": false,
      "html_clicks": true,
      "opens": true,
      "text_clicks": false
    },
    "type": "variate",
    "variate_settings": {
      "combinations": [
        {
          "content_description": 0,
          "from_name": 0,
          "id": "6005e15a40",
          "recipients": 1240,
          "reply_to": 0,
          "send_time": 0,
          "subject_line": 0
        },
        {
          "content_description": 1,
          "from_name": 0,
          "id": "2a6111bc4e",
          "recipients": 1240,
          "reply_to": 0,
          "send_time": 0,
          "subject_line": 0
        },
        {
          "content_description": 0,
          "from_name": 0,
          "id": "d17b1b3d9e",
          "recipients": 1240,
          "reply_to": 0,
          "send_time": 0,
          "subject_line": 1
        },
        {
          "content_description": 1,
          "from_name": 0,
          "id": "7fe9b51cb7",
          "recipients": 1239,
          "reply_to": 0,
          "send_time": 0,
          "subject_line": 1
        }
      ],
      "contents": ["Stylized", "Plain Text"],
      "from_names": ["Mel Niere"],
      "reply_to_addresses": ["info@theinformationlab.co.uk"],
      "send_times": ["2025-07-14T07:30:00+00:00"],
      "subject_lines": [
        "The Information Lab Newsletter 📊",
        "New from The Information Lab: Events & Tableau Next 📊"
      ],
      "test_size": 50,
      "wait_time": 240,
      "winner_criteria": "opens",
      "winning_campaign_id": "48b8c5c6bc",
      "winning_combination_id": "7fe9b51cb7"
    },
    "web_id": 2822052
  },
  "_airbyte_extracted_at": 1752679551831,
  "_airbyte_generation_id": 0,
  "_airbyte_meta": {
    "changes": [],
    "sync_id": 42759878
  },
  "_airbyte_raw_id": "b8a1c2f7-e988-419d-8280-5eaf70dedec9"
}
') as json_data
from
JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_campaigns
limit 1
)
;
insert into JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_EMAIL_ACTIVITY (
select parse_json('{
  "_airbyte_data": {
    "action": "click",
    "campaign_id": "f551278adf",
    "email_address": "phillipnguyen@nbnco.com.au",
    "email_id": "015dde2a7e4f4aaafb5bf6d65b9d8def",
    "ip": "20.70.111.53",
    "list_id": "fc1dab699f",
    "list_is_active": true,
    "timestamp": "2027-10-11T09:17:36+00:00",
    "url": "https://substack.com/home/post/p-149678475?mc_cid=f551278adf&mc_eid=UNIQID"
  },
  "_airbyte_extracted_at": 1752678733277,
  "_airbyte_generation_id": 0,
  "_airbyte_meta": {
    "changes": [],
    "sync_id": 42758516
  },
  "_airbyte_raw_id": "a9622ed0-803b-477a-8ed7-b15ebcc7604e"
}
')as json_data
from
JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_EMAIL_ACTIVITY
limit 1
)
;

select * from JT_DENG3_STAGING.AIRBYTE_SYNC_MAILCHIMP_DATA_RAW_PYTHON_campaigns;
select count(*) from campaign;
call update_silver_layer_mailchimp_campaign();
select count(*) from email_activity;
call update_silver_layer_mailchimp_email();

