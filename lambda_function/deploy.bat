@echo off
setlocal enabledelayedexpansion

REM Load environment variables
if exist "..\\.env" (
    echo Loading environment variables from .env
    for /F "usebackq tokens=* delims=" %%A in ("..\\.env") do (
        echo Processing: %%A
        set "%%A"
    )
)

REM Validate all required variables
set MISSING_VARS=0
if not defined NEWS_LAMBDA_NAME (
    echo ERROR: NEWS_LAMBDA_NAME not set
    set MISSING_VARS=1
)
if not defined AWS_ACCOUNT_ID (
    echo ERROR: AWS_ACCOUNT_ID not set
    set MISSING_VARS=1
)
if not defined AWS_DEFAULT_REGION (
    echo ERROR: AWS_DEFAULT_REGION not set
    set MISSING_VARS=1
)
if not defined AWS_S3_BUCKET (
    echo ERROR: AWS_S3_BUCKET not set
    set MISSING_VARS=1
)
if not defined WORKER_LAMBDA_NAME (
    echo ERROR: WORKER_LAMBDA_NAME not set
    set MISSING_VARS=1
)

if %MISSING_VARS% equ 1 (
    echo One or more required environment variables are missing
    exit /b 1
)

echo Current settings:
echo NEWS_LAMBDA_NAME: %NEWS_LAMBDA_NAME%
echo AWS_ACCOUNT_ID: %AWS_ACCOUNT_ID%
echo AWS_DEFAULT_REGION: %AWS_DEFAULT_REGION%
echo AWS_S3_BUCKET: %AWS_S3_BUCKET%
echo WORKER_LAMBDA_NAME: %WORKER_LAMBDA_NAME%

REM ============ SQS QUEUE SETUP ============
set QUEUE_NAME=gdelt-tasks-queue

echo Checking SQS queue...
for /f "tokens=*" %%i in ('aws sqs get-queue-url --queue-name %QUEUE_NAME% --region %AWS_DEFAULT_REGION% --query QueueUrl --output text 2^>nul') do set QUEUE_URL=%%i

if "%QUEUE_URL%"=="" (
    echo Creating SQS queue...
    call aws sqs create-queue ^
        --queue-name %QUEUE_NAME% ^
        --attributes "{\"VisibilityTimeout\": \"900\", \"MessageRetentionPeriod\": \"86400\"}" ^
        --region %AWS_DEFAULT_REGION%
    
    for /f "tokens=*" %%i in ('aws sqs get-queue-url --queue-name %QUEUE_NAME% --region %AWS_DEFAULT_REGION% --query QueueUrl --output text') do set QUEUE_URL=%%i
) else (
    echo Queue already exists: %QUEUE_URL%
)

set QUEUE_ARN=arn:aws:sqs:%AWS_DEFAULT_REGION%:%AWS_ACCOUNT_ID%:%QUEUE_NAME%

echo Queue URL: %QUEUE_URL%
echo Queue ARN: %QUEUE_ARN%

REM ============ MANUAL TRIGGER SETUP OPTION ============
echo.
echo =====================================================
echo SQS TRIGGER SETUP - MANUAL OPTION
echo =====================================================
echo.
echo If automated trigger setup fails, run this manually:
echo.
echo aws lambda create-event-source-mapping ^
echo     --function-name %WORKER_LAMBDA_NAME% ^
echo     --event-source-arn %QUEUE_ARN% ^
echo     --batch-size 1 ^
echo     --region %AWS_DEFAULT_REGION%
echo.
echo =====================================================
echo.

REM Remove any existing triggers first
echo Removing existing triggers...
for /f "tokens=*" %%i in ('aws lambda list-event-source-mappings --function-name %WORKER_LAMBDA_NAME% --region %AWS_DEFAULT_REGION% --query "EventSourceMappings[].UUID" --output text 2^>nul') do (
    echo Removing existing trigger: %%i
    aws lambda delete-event-source-mapping --uuid %%i --region %AWS_DEFAULT_REGION%
    
    REM Wait for deletion to complete
    echo Waiting for trigger deletion to complete...
    timeout /t 5 /nobreak
)

REM Add new SQS trigger with retry logic
echo Adding SQS trigger to worker Lambda...
set TRIGGER_CREATED=0
for /l %%x in (1,1,3) do (
    call aws lambda create-event-source-mapping ^
        --function-name %WORKER_LAMBDA_NAME% ^
        --event-source-arn %QUEUE_ARN% ^
        --batch-size 1 ^
        --region %AWS_DEFAULT_REGION% 2>nul && (
        set TRIGGER_CREATED=1
        echo SQS trigger created successfully
        goto :trigger_success
    )
    echo Attempt %%x failed, retrying...
    timeout /t 2 /nobreak
)

:trigger_success
if !TRIGGER_CREATED! equ 0 (
    echo WARNING: Failed to create SQS trigger after 3 attempts
    echo Please create the trigger manually using the command above
    goto :skip_trigger_verify
)

REM Verify trigger was created
echo Verifying SQS trigger...
call aws lambda list-event-source-mappings --function-name %WORKER_LAMBDA_NAME% --region %AWS_DEFAULT_REGION% --query "length(EventSourceMappings)" --output text >nul 2>&1
if errorlevel 1 (
    echo WARNING: Could not verify SQS trigger
) else (
    echo SQS trigger verified successfully
)

:skip_trigger_verify
REM Wait for trigger to be active
timeout /t 3 /nobreak

REM Deploy collector function
echo Deploying collector function...
if exist "collector-package" rd /s /q "collector-package"
mkdir collector-package
copy /Y lambda_function.py collector-package\
cd collector-package

REM Create requirements.txt
echo Creating requirements.txt...
(
echo boto3==1.34.0
echo requests==2.31.0
) > requirements.txt

echo Installing collector dependencies...
python -m pip install --no-cache-dir -r requirements.txt -t . --upgrade
if errorlevel 1 (
    echo ERROR: Failed to install collector dependencies
    cd ..
    exit /b 1
)

REM Clean collector package
echo Cleaning collector package...
for /d %%i in ("__pycache__", "*.dist-info", "tests") do (
    if exist "%%i" rd /s /q "%%i" 2>nul
)
if exist "requirements.txt" del /f /q requirements.txt 2>nul
cd ..

REM Deploy worker function
echo Deploying worker function...
if exist "worker-package" rd /s /q "worker-package"
mkdir worker-package
copy /Y gdelt-task-worker.py worker-package\lambda_function.py
cd worker-package

REM Create requirements.txt for worker
echo Creating worker requirements.txt...
(
echo boto3==1.34.0
echo requests==2.31.0
) > requirements.txt

echo Installing worker dependencies...
python -m pip install --no-cache-dir -r requirements.txt -t . --upgrade
if errorlevel 1 (
    echo ERROR: Failed to install worker dependencies
    cd ..
    exit /b 1
)

REM Clean worker package
echo Cleaning worker package...
for /d %%i in ("__pycache__", "*.dist-info", "tests") do (
    if exist "%%i" rd /s /q "%%i" 2>nul
)
if exist "requirements.txt" del /f /q requirements.txt 2>nul
cd ..

REM Create ZIP archives
echo Creating ZIP archives...
powershell -Command "$ErrorActionPreference = 'Stop'; Compress-Archive -Path .\collector-package\* -DestinationPath .\collector.zip -Force"
powershell -Command "$ErrorActionPreference = 'Stop'; Compress-Archive -Path .\worker-package\* -DestinationPath .\worker.zip -Force"

REM Update Lambda configurations
echo Updating Lambda configurations...
set LAMBDA_ENV="Variables={S3_BUCKET_NAME='%AWS_S3_BUCKET%',SQS_QUEUE_URL='%QUEUE_URL%',WORKER_LAMBDA_NAME='%WORKER_LAMBDA_NAME%',QUEUE_NAME='%QUEUE_NAME%',DEPLOY_REGION='%AWS_DEFAULT_REGION%'}"

REM Update collector function
echo Updating collector function configuration...
call aws lambda update-function-configuration ^
    --function-name %NEWS_LAMBDA_NAME% ^
    --role arn:aws:iam::%AWS_ACCOUNT_ID%:role/GDELT-Lambda-Role-1 ^
    --timeout 900 ^
    --memory-size 1024 ^
    --environment %LAMBDA_ENV% ^
    --region %AWS_DEFAULT_REGION%

if errorlevel 1 (
    echo ERROR: Failed to update collector configuration
    exit /b 1
)

REM Update worker function configuration
echo Updating worker function configuration...
call aws lambda update-function-configuration ^
    --function-name %WORKER_LAMBDA_NAME% ^
    --handler lambda_function.lambda_handler ^
    --role arn:aws:iam::%AWS_ACCOUNT_ID%:role/GDELT-Lambda-Role-1 ^
    --timeout 900 ^
    --memory-size 1024 ^
    --environment %LAMBDA_ENV% ^
    --region %AWS_DEFAULT_REGION%

REM Deploy Lambda code
echo Deploying Lambda functions...
call aws lambda update-function-code ^
    --function-name %NEWS_LAMBDA_NAME% ^
    --zip-file fileb://collector.zip ^
    --region %AWS_DEFAULT_REGION%

call aws lambda update-function-code ^
    --function-name %WORKER_LAMBDA_NAME% ^
    --zip-file fileb://worker.zip ^
    --region %AWS_DEFAULT_REGION%

REM Clean up
echo Cleaning up...
rd /s /q collector-package 2>nul
rd /s /q worker-package 2>nul
del /f /q collector.zip 2>nul
del /f /q worker.zip 2>nul

REM Final verification
echo Performing final verification...
call aws lambda list-event-source-mappings --function-name %WORKER_LAMBDA_NAME% --region %AWS_DEFAULT_REGION% --query "EventSourceMappings[].State" --output text >nul 2>&1
if errorlevel 1 (
    echo WARNING: Could not verify SQS trigger state - may need manual setup
) else (
    echo SQS trigger state verified
)

echo Checking Lambda function states...
call aws lambda get-function --function-name %WORKER_LAMBDA_NAME% --region %AWS_DEFAULT_REGION% --query "Configuration.State" --output text >nul 2>&1
if errorlevel 1 (
    echo ERROR: Worker Lambda is not in active state
    exit /b 1
)

call aws lambda get-function --function-name %NEWS_LAMBDA_NAME% --region %AWS_DEFAULT_REGION% --query "Configuration.State" --output text >nul 2>&1
if errorlevel 1 (
    echo ERROR: Collector Lambda is not in active state
    exit /b 1
)

echo.
echo =====================================================
echo DEPLOYMENT COMPLETED SUCCESSFULLY!
echo =====================================================
echo.
echo SQS Queue: %QUEUE_URL%
echo Worker Lambda: %WORKER_LAMBDA_NAME%
echo Collector Lambda: %NEWS_LAMBDA_NAME%
echo.
echo If SQS trigger is not working, run this manually:
echo aws lambda create-event-source-mapping ^
echo     --function-name %WORKER_LAMBDA_NAME% ^
echo     --event-source-arn %QUEUE_ARN% ^
echo     --batch-size 1 ^
echo     --region %AWS_DEFAULT_REGION%
echo.
pause