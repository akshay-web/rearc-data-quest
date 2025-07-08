# Rearc Data Quest

## Part 1 & Part 2: Data Ingestion

- `Ingestion.py` reads the data from the following sources:
  - [BLS Time Series](https://download.bls.gov/pub/time.series/pr/)
  - [Data USA Population API](https://datausa.io/api/data?drilldowns=Nation&measures=Population)
- It writes the data to the local folder: `/tmp`
- Then uploads it to the S3 bucket:  
  [https://rearc-data-quest-1.s3.us-east-2.amazonaws.com/bls/](https://rearc-data-quest-1.s3.us-east-2.amazonaws.com/bls/)

## Part 3: Data Analysis

- `data_analysis.ipynb` contains the PySpark code used for data analysis.
- The results are stored in the `output` folder.

## Part 4: CloudFormation Deployment

- The `rearc-data-quest.yaml` file contains the CloudFormation code.
- To deploy, use the following command:

```bash
aws cloudformation deploy \
  --template-file rearc-data-quest.yaml \
  --stack-name 'rearc-data-quest' \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-2 \
  --no-fail-on-empty-changeset
