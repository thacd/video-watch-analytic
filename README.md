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



