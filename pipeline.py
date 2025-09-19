import subprocess
import logging
import sys
import yagmail
import yaml
import os
from datetime import datetime


# Setup logging (UTF-8 safe, no emojis)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Ensure UTF-8 output in Windows terminal
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass  # Ignore if not supported


# Load config

CONFIG_FILE = "config.yaml"

with open(CONFIG_FILE, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

sources = config.get("sources", {})


# Email notification

def send_email(subject, body):
    try:
        user = config["email"]["user"]          # <-- updated to match your config.yaml
        app_password = config["email"]["password"]  
        to = config["email"]["to"]

        yag = yagmail.SMTP(user, app_password)
        yag.send(to=to, subject=subject, contents=body)
        logging.info("Email notification sent successfully")
    except Exception as e:
        logging.error(f"Email failed: {e}")


# Run extractor

PYTHON = sys.executable  # Use venv Python explicitly

def run_extractor(source, config_file):
    try:
        logging.info(f"Running extractor for {source}")
        subprocess.run(
            [PYTHON, "extractor.py", "--config", config_file, "--source", source],
            check=True
        )
        logging.info(f"✅ Extractor succeeded for {source}")
        return True, None
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Extractor failed for {source}: {e}")
        return False, str(e)


# Run transformer

def run_transformer(source):
    try:
        logging.info(f"Running transformer for {source}")
        subprocess.run(
            [PYTHON, "transformer.py", "--source", source],
            check=True
        )
        logging.info(f"✅ Transformer succeeded for {source}")
        return True, None
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Transformer failed for {source}: {e}")
        return False, str(e)


# Main

if __name__ == "__main__":
    start_time = datetime.now()
    success_sources, failed_sources = [], []

    for src in sources.keys():
        # 1️⃣ Run extractor
        ok, error_msg = run_extractor(src, CONFIG_FILE)

        # 2️⃣ If extractor succeeded → run transformer
        if ok:
            ok_t, error_msg_t = run_transformer(src)
            if ok_t:
                success_sources.append(src)
            else:
                failed_sources.append(f"{src} (transformer)")
        else:
            failed_sources.append(f"{src} (extractor)")

    # Prepare email summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    subject = "ETL Pipeline Execution Report"
    body = f"""
    ETL Pipeline finished.

    ✅ Success: {success_sources if success_sources else 'None'}
    ❌ Failed: {failed_sources if failed_sources else 'None'}

    Start: {start_time}
    End: {end_time}
    Duration: {duration} seconds
    """

    send_email(subject, body)
    logging.info("Pipeline completed.")
