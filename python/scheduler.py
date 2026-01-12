#!/usr/bin/env python3
"""
Procurement Pipeline Scheduler
Runs the pipeline automatically at scheduled time (22:00-23:00)
"""

import requests
import schedule
import time
import subprocess
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/data/logs/scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def upload_to_hdfs(date_str):
    """Upload generated data to HDFS using WebHDFS API"""
    logger.info(f"Uploading data to HDFS for {date_str}")
    
    try:
        namenode_url = "http://namenode:9870/webhdfs/v1"
        
        # Upload orders directory
        orders_path = f"/data/raw/orders/{date_str}"
        if os.path.exists(orders_path):
            for filename in os.listdir(orders_path):
                if filename.endswith('.json'):
                    local_file = f"{orders_path}/{filename}"
                    hdfs_path = f"/data/raw/orders/{date_str}/{filename}"
                    
                    # Create directory
                    requests.put(f"{namenode_url}/data/raw/orders/{date_str}?op=MKDIRS&user.name=root")
                    
                    # Upload file
                    with open(local_file, 'rb') as f:
                        data = f.read()
                    
                    # Get redirect URL
                    create_url = f"{namenode_url}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
                    resp = requests.put(create_url, allow_redirects=False)
                    
                    if resp.status_code == 307:
                        upload_url = resp.headers['Location']
                        requests.put(upload_url, data=data)
                        logger.info(f"  ✓ Uploaded {filename}")
        
        # Upload stock directory
        stock_path = f"/data/raw/stock/{date_str}"
        if os.path.exists(stock_path):
            for filename in os.listdir(stock_path):
                if filename.endswith('.csv'):
                    local_file = f"{stock_path}/{filename}"
                    hdfs_path = f"/data/raw/stock/{date_str}/{filename}"
                    
                    # Create directory
                    requests.put(f"{namenode_url}/data/raw/stock/{date_str}?op=MKDIRS&user.name=root")
                    
                    # Upload file
                    with open(local_file, 'rb') as f:
                        data = f.read()
                    
                    # Get redirect URL
                    create_url = f"{namenode_url}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
                    resp = requests.put(create_url, allow_redirects=False)
                    
                    if resp.status_code == 307:
                        upload_url = resp.headers['Location']
                        requests.put(upload_url, data=data)
                        logger.info(f"  ✓ Uploaded {filename}")
        
        logger.info(f"✓ HDFS upload completed")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {e}")
        return False


def run_daily_pipeline():
    """Execute the complete daily procurement pipeline"""
    
    # Get today's date
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    logger.info("="*70)
    logger.info(f"STARTING DAILY PROCUREMENT PIPELINE - {date_str}")
    logger.info("="*70)
    
    try:
        # Step 1: Generate data
        logger.info("Step 1/3: Generating test data...")
        result = subprocess.run(
            ['python', '/app/generate_data_realistic.py', date_str], 
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Data generation failed: {result.stderr}")
            logger.error(f"Output: {result.stdout}")
            return
        
        logger.info("✓ Data generation completed")
        logger.info(result.stdout)

        logger.info("Step 2/3: Uploading to HDFS...")
        if not upload_to_hdfs(date_str):
           logger.error("HDFS upload failed, aborting pipeline")
           return
        logger.info("✓ HDFS upload completed")

        # Step 3: Run pipeline
        logger.info("Step 3/3: Running procurement pipeline...")
        result = subprocess.run(
            ['python', '/app/pipeline.py', '--date', date_str],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Pipeline execution failed: {result.stderr}")
            logger.error(f"Output: {result.stdout}")
            return
        
       
        logger.info(result.stdout)
        
       # logger.info("="*70)
       # logger.info("✅ DAILY PIPELINE COMPLETED SUCCESSFULLY")
        #logger.info("="*70)
        
    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
        logger.error("="*70)
        logger.error("✗ PIPELINE FAILED")
        logger.error("="*70)


def main():
    """Main scheduler function"""
    
    logger.info("="*70)
    logger.info("🕐 Procurement Pipeline Scheduler Started")
    logger.info("📅 Scheduled to run daily at 22:00")
    logger.info("⏰ Current time: " + datetime.now().strftime('%H:%M:%S'))
    logger.info("Press Ctrl+C to stop")
    logger.info("="*70)
    
    # Schedule the job to run daily at 21:30
    schedule.every().day.at("22:00").do(run_daily_pipeline)
    
    # FOR TESTING: Uncomment to run immediately
    # run_daily_pipeline()
    
    logger.info(f"⏳ Waiting for scheduled time (22:00)...")
    
    # Keep the scheduler running
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("\n🛑 Scheduler stopped by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            time.sleep(60)


if __name__ == '__main__':
    main()
