## Unfinised Job
1. unexpected exit code -9 happens when running luigi on docker (I have issued a ticket on luigi support. Currently waiting for their response)
2. The output exception.xlsx hasn't been properly formatted 
3. 
## How to Run
python3 -m main 

## Luigi UI
localhost:8082

## Change on Code & pickup pipeline tasks
Delete etl_pipeline_01 container
docker-compose -f pipeline.yaml up -d 

## Data Structure:

data
 - raw
   - payslips --- original payslips files on sharedrive converted to csv
   - timesheets --- original timesheets files on sharedrive converted to csv
 - metaraw
   - raw --- concat payslips / concat timesheets 
   - dupfree --- metaraw without dups
 - staging
   - raw --- transformed to staging files as requested
   - excepiton --- the final exception report on xlsx format
 - test
   - exceptions --- every exception test results on csv format
   - process --- test results on every stage of transforming data 


## After task finish
1. http://localhost:8082/ to check test status & flow charts
2. ls to data folder to check if all file exists 
3. check data_root

## Environment Design
![alt text](https://github.com/Stephen-init/ETL/blob/main/design/design.png)

## Pipeline Design
![alt text](https://github.com/Stephen-init/ETL/blob/main/design/pipeline.png)
