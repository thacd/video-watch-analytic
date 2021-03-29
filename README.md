[![Build Status](https://travis-ci.com/thacd/video-watch-analytic.svg?branch=main)](https://travis-ci.com/thacd/video-watch-analytic)
# Video Watch Analytic 
This is the code for automated data analytic using Google Cloud Platform (GCP) (https://cloud.google.com).

## Description
The data from CSV file are loaded to Google Cloud Storage. We then use Google Dataflow to transform data to the appropriate format and load to Google BigQuery for Analytic. We schedule the automated process from Google Cloud Storage to BigQuery using Google Composer. The data from Google BigQuery are then used to report using Google Data Studio.

## Platform
1. Google Cloud Storage
2. Google Dataflow
3. Google Bigquery
4. Google Data Studio

## Data
The format of the CSV file is as follow
```
DateTime,VideoTitle,events
2017-01-11T00:00:31.000Z,App Web|Clips|a-current-affair;2016|William Tyrrell twist,"157,120,160,104,162,161,163,164,165,166,171,229"
2017-01-11T00:00:53.000Z,news| Shark attacks spearfisherman,"127,157,120,160,104,162,161,171,206"
2017-01-11T00:00:21.000Z,news| Shark attacks spearfisherman,"127,157,120,160,104,162,161,163,164,165,166,171,229"
2017-01-11T00:01:27.000Z,news| Chilean navy films UFO,"157,120,160,104,162,161,170,171,237"
```
The data is then transformed as these rules for a star schema 
```
VideoWatched metric;
  From the “events” column, discard all rows containing “206”
Dimensions
  DimDate
  DimPlatform
  DimSite
  DimVideo

DimDate
  should go to the minute grain
DimPlatform
  Split VideoTitle by pipe |
  If VideoTitle.split(‘|’)[0] contains something that looks like a platform (iPhone, Android Phone etc) then use that as the platform
  If VideoTitle.split(‘|’)[0] doesn’t contain a platform but looks like a site, assume the platform is Desktop
  If VideoTitle.split(‘|’).count = 1, discard the row.
DimSite
  Split VideoTitle by pipe |
  If VideoTitle.split(‘|’).count = 1, discard the row.
  If VideoTitle.split(‘|’)[0] looks like a site name, save the site name
DimVideo
  Last piece of VideoTitle.split(‘|’) contains the video title
  Ignore any middle pieces
```

The data is then denormalized to gain the efficiency for Online Analytical Processing (OLAP) on Google BigQuery.
The schema for the table on Google Big Query
```
[
    {
        "name": "date_watch",
        "type": "DATE",
        "mode": "REQUIRED"
    },
    {
        "name": "time_watch",
        "type": "TIME",
        "mode": "REQUIRED"
    },
    {
        "name": "platform",
        "type": "STRING"
    },
    {
        "name": "site",
        "type": "STRING"
    },
    {
        "name": "video",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "metric",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "event",
                "type": "STRING",
                "mode": "REQUIRED"
            }
        ]
    }
]
```

## Configuration
#### 1. Create Project
Follow these step https://cloud.google.com/appengine/docs/standard/nodejs/building-app/creating-project
#### 2. Create a Google Cloud Storage Bucket
Follow these steps from 
https://cloud.google.com/storage/docs/quickstart-console
It should look like this
![GS](https://storage.googleapis.com/images_video_watch/cloud_storage.png)
#### 3. Clone the code from these git to your local computer
```
git clone https://github.com/thacd/video-watch-analytic
```
#### 4. Make the Template for Dataflow
```
python -m data_transformation \
    --runner DataflowRunner \
    --project infotrack-videowatch \
    --staging_location gs://**YOUR_BUCKET_NAME**/staging \
    --temp_location gs://**YOUR_BUCKET_NAME**/temp \
    --template_location gs://**YOUR_BUCKET_NAME**/pipelines/data_transformation
```
<img src="https://storage.googleapis.com/images_video_watch/dataflow.png" width="300" height="500" />

Remember to run **pytest** before deploy to Dataflow.
#### 5. Create a Scheduler Using Google Composer
Create a Composer environment
![GS](https://storage.googleapis.com/images_video_watch/composer_environment.png)
Upload file **src/composer-dataflow-dag.py** to dags/ folder.
Watch the Schedule on Airflow UI
![GS](https://storage.googleapis.com/images_video_watch/airflow1.png)
![GS](https://storage.googleapis.com/images_video_watch/airflow2.png)

#### 6. Report
The report to show the insight of the data is then presented using Google Data Studio https://datastudio.google.com/reporting/dd39abda-3d91-4d2c-bb5d-745d3685e9c1/page/ykz9B)
For example, using this Query from BigQuery
```
WITH HourlyWatch AS(
SELECT
  count(*) as hourly_watch, EXTRACT(HOUR FROM time_watch) as hour
FROM `infotrack-videowatchanalytic.video_watch.video_watch`
GROUP BY hour
ORDER BY hour)
SELECT * FROM HourlyWatch
```
We can fetch the data for Data Studio
![GS](https://storage.googleapis.com/images_video_watch/data_studio.png)
