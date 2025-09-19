@echo off
REM Batch wrapper to run ETL pipeline using venv python and correct working directory

SET "PROJECT_DIR=C:\Users\User\OneDrive\Desktop\automated_etl_pipeline"
SET "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"

REM Go to project dir (important so relative paths like config.yaml work)
cd /d "%PROJECT_DIR%"

REM Run pipeline.py, append stdout & stderr to a run log (useful for debugging)
"%PYTHON_EXE%" pipeline.py >> "%PROJECT_DIR%\run_pipeline_stdout.log" 2>&1

REM exit with the same code as the python process
exit /b %ERRORLEVEL%
