## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 

Before start,

Start Prefect Orion Server, 
```Bash
prefect orion start
```

Start Prefect Agent, 
```Bash
prefect agent start -q default
```

Working directory
```
2_workflow_orchestration/homework
```

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770 ✅
* 766,792
* 299,234
* 822,132

## Answer 1
In `2_workflow_orchestration/homework` directory, run
```Bash
python3 flows/q1.py
```

Output:
```Bash
16:21:25.880 | INFO    | prefect.engine - Created flow run 'greedy-fennec' for flow 'etl-web-to-gcs'
16:21:26.399 | INFO    | Flow run 'greedy-fennec' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
16:21:26.402 | INFO    | Flow run 'greedy-fennec' - Executing 'fetch-b4598a4a-0' immediately...
flows/q1.py:10: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
16:21:40.296 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
16:21:40.410 | INFO    | Flow run 'greedy-fennec' - Created task run 'clean-b9fd7e03-0' for task 'clean'
16:21:40.412 | INFO    | Flow run 'greedy-fennec' - Executing 'clean-b9fd7e03-0' immediately...
16:21:41.068 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime  ... payment_type  trip_type  congestion_surcharge
0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39  ...          1.0        1.0                   0.0
1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39  ...          1.0        2.0                   0.0

[2 rows x 20 columns]
16:21:41.071 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
16:21:41.073 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
16:21:41.180 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
16:21:41.337 | INFO    | Flow run 'greedy-fennec' - Created task run 'write_local-f322d1be-0' for task 'write_local'
16:21:41.338 | INFO    | Flow run 'greedy-fennec' - Executing 'write_local-f322d1be-0' immediately...
16:21:44.079 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
16:21:44.174 | INFO    | Flow run 'greedy-fennec' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
16:21:44.176 | INFO    | Flow run 'greedy-fennec' - Executing 'write_gcs-1145c921-0' immediately...
16:21:44.490 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'prefect-de-zoomcamp-375108'.
16:21:45.117 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/green/green_tripdata_2020-01.parquet') to the bucket 'prefect-de-zoomcamp-375108' path 'data/green/green_tripdata_2020-01.parquet'.
16:21:47.560 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
16:21:47.672 | INFO    | Flow run 'greedy-fennec' - Finished in state Completed('All states completed.')
```


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *` ✅
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

## Answer 2
In `2_workflow_orchestration/homework` directory, run
```Bash
python3 flows/q2.py
```

![Question 2 - Screenshot](./img/q2.png)

## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920 ✅
- 12,282,990
- 27,235,753
- 11,338,483

## Answer 3
In `2_workflow_orchestration/homework` directory, run
```Bash
prefect deployment run etl-web-to-gcs/question-2-flow --params '{"year":2019, "month":2}'
prefect deployment run etl-web-to-gcs/question-2-flow --params '{"year":2019, "month":3}'
```
This will ingest Feb 2019 & Mar 2019 Yellow taxi data to GCS. After that,
```Bash
python3 flows/q3.py
```
This will create a deployment. Finally, run
```Bash
prefect deployment run etl-gcs-to-bq-parent/question-3-flow --params '{"year":2019, "months":[2,3]}'
```

![Question 3 - Screenshot](./img/q3.png)


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605 ✅
- 190,225

## Answer 4
In `2_workflow_orchestration/homework` directory, run
```Bash
python3 blocks/make_github_storage_block.py
```
This will create Github Storage Block. After that, run
```Bash
python3 flows/q4.py
```
This will create a deployment flow. After that, run
```Bash
prefect deployment run etl-web-to-gcs/question-4-flow -p month=11
```
This will extract Nov 2020 Green taxi data and save it to GCS.

```Bash
11:02:04.624 | INFO    | prefect.agent - Submitting flow run '3f13ca08-c52e-401b-be22-4b044fff742b'
11:02:04.712 | INFO    | prefect.infrastructure.process - Opening process 'cordial-hyena'...
11:02:04.758 | INFO    | prefect.agent - Completed submission of flow run '3f13ca08-c52e-401b-be22-4b044fff742b'
/usr/lib/python3.8/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
11:02:07.230 | INFO    | Flow run 'cordial-hyena' - Downloading flow code from storage at ''
11:02:39.065 | INFO    | Flow run 'cordial-hyena' - Created task run 'fetch-ba00c645-0' for task 'fetch'
11:02:39.066 | INFO    | Flow run 'cordial-hyena' - Executing 'fetch-ba00c645-0' immediately...
/tmp/tmpugl8ci2rprefect/2_workflow_orchestration/homework/flows/q1.py:12: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
11:02:45.495 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Completed()
11:02:45.560 | INFO    | Flow run 'cordial-hyena' - Created task run 'clean-2c6af9f6-0' for task 'clean'
11:02:45.561 | INFO    | Flow run 'cordial-hyena' - Executing 'clean-2c6af9f6-0' immediately...
11:02:45.675 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0       2.0  2020-11-01 00:08:23  ...       1.0                 2.75
1       2.0  2020-11-01 00:23:32  ...       1.0                 0.00

[2 rows x 20 columns]
11:02:45.677 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
11:02:45.678 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
11:02:45.730 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
11:02:45.791 | INFO    | Flow run 'cordial-hyena' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
11:02:45.792 | INFO    | Flow run 'cordial-hyena' - Executing 'write_local-09e9d2b8-0' immediately...
11:02:46.147 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
11:02:46.203 | INFO    | Flow run 'cordial-hyena' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
11:02:46.204 | INFO    | Flow run 'cordial-hyena' - Executing 'write_gcs-67f8f48e-0' immediately...
11:02:46.361 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'prefect-de-zoomcamp-375108'.
11:02:46.758 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/green/green_tripdata_2020-11.parquet') to the bucket 'prefect-de-zoomcamp-375108' path 'data/green/green_tripdata_2020-11.parquet'.
11:02:47.866 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
11:02:47.933 | INFO    | Flow run 'cordial-hyena' - Finished in state Completed('All states completed.')
11:02:48.469 | INFO    | prefect.infrastructure.process - Process 'cordial-hyena' exited cleanly.
```


## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392` ✅

## Answer 5
Create a Slack notification block with the slack webhook link generated, run states set to *Completed*.
![Question 5 - Screenshot](./img/q5.png)
After that, run
```Bash
prefect deployment run etl-web-to-gcs/question-4-flow --params '{"year":2019, "month":4}'
```
This will ingest Apr 2019 Green taxi data to GCS.

```Bash
11:28:37.420 | INFO    | prefect.agent - Submitting flow run 'b8da7384-82c1-4a00-abe8-2ac596d58b42'
11:28:37.553 | INFO    | prefect.infrastructure.process - Opening process 'cornflower-tuna'...
11:28:37.593 | INFO    | prefect.agent - Completed submission of flow run 'b8da7384-82c1-4a00-abe8-2ac596d58b42'
/usr/lib/python3.8/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
11:28:39.476 | INFO    | Flow run 'cornflower-tuna' - Downloading flow code from storage at ''
11:29:08.069 | INFO    | Flow run 'cornflower-tuna' - Created task run 'fetch-ba00c645-0' for task 'fetch'
11:29:08.070 | INFO    | Flow run 'cornflower-tuna' - Executing 'fetch-ba00c645-0' immediately...
11:29:13.959 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Completed()
11:29:14.012 | INFO    | Flow run 'cornflower-tuna' - Created task run 'clean-2c6af9f6-0' for task 'clean'
11:29:14.012 | INFO    | Flow run 'cornflower-tuna' - Executing 'clean-2c6af9f6-0' immediately...
11:29:14.288 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0         2  2019-04-01 00:18:40  ...         1                 2.75
1         2  2019-04-01 00:18:24  ...         1                 0.00

[2 rows x 20 columns]
11:29:14.290 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                          int64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                        int64
PULocationID                      int64
DOLocationID                      int64
passenger_count                   int64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                      int64
trip_type                         int64
congestion_surcharge            float64
dtype: object
11:29:14.291 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392
11:29:14.339 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
11:29:14.402 | INFO    | Flow run 'cornflower-tuna' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
11:29:14.403 | INFO    | Flow run 'cornflower-tuna' - Executing 'write_local-09e9d2b8-0' immediately...
11:29:15.871 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
11:29:15.922 | INFO    | Flow run 'cornflower-tuna' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
11:29:15.923 | INFO    | Flow run 'cornflower-tuna' - Executing 'write_gcs-67f8f48e-0' immediately...
11:29:16.059 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'prefect-de-zoomcamp-375108'.
11:29:16.454 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('data/green/green_tripdata_2019-04.parquet') to the bucket 'prefect-de-zoomcamp-375108' path 'data/green/green_tripdata_2019-04.parquet'.
11:29:18.855 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
11:29:18.896 | INFO    | Flow run 'cornflower-tuna' - Finished in state Completed('All states completed.')
11:29:19.404 | INFO    | prefect.infrastructure.process - Process 'cornflower-tuna' exited cleanly.
```
![Question 5 - Screenshot 2](./img/q5b.png)


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8 ✅
- 10

## Answer 6
![Qeustion 6 - Screenshot](./img/q6.png)
