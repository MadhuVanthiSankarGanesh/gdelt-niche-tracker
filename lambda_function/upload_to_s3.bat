@echo off
aws s3 cp lambda_function.py s3://gdelt-niche-data-bucket/lambda/gdelt-data-collector.py
echo Code uploaded to S3 successfully!
pause