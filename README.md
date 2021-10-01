# ETL-pipeline

## Unfinised Job
1. haven't reset folder structure (Current only testluigi.py is mounted)
    *reset folder structure will:
        1. be able to mount every scripts.
        2. be able to mount the config file.
2. luigi pipeline is only for demonstration, haven't set the proper tasks 

## How to Run
python3 -m main

## Luigi UI
localhost:8082
## Change on Code
Delete pipeline container
docker-compose -f pipeline.yaml up -d 
## Re run task 
docker start <pipeline container>

## Current Structure:
Read all files in data/origin 
--> transform files to csv and generate dataset().csv in data/raw 
--> concat all data under data/raw and generate metadata.csv
--> test all data under data/raw and generate testraw.xlsx with 2 sheets (column test & NaN test)

## After task finish
1. http://localhost:8082/ to check test status & flow charts
2. ls to data folder to check if all file exists 
3. check data_root

## Pipeline Design
![alt text](https://github.com/Stephen-init/ETL-pipeline/blob/main/design.png)
