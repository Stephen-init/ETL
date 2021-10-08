# ETL-pipeline
How to run pipeline: 
   1. run luigid on bash or zash
   2. run main.py under app_dev folder
   3. required package: openpyxl pandas pyyaml luigi
 
##------------ the following hasn't been connected due to pipeline in dev -------------
## Unfinised Job
1. haven't reset folder structure
2. luigi pipeline is currently on app_dev folder. Haven't been connected to docker environment.

## How to Run
python3 -m main (current don't work due to pipeline in dev)

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

## Environment Design
![alt text](https://github.com/Stephen-init/ETL/blob/main/design.png)

## Pipeline Design
![alt text](https://github.com/Stephen-init/ETL/blob/main/Blank diagram-2.png)
