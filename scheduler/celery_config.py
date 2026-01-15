"""
Celery configuration for scheduled scraping tasks
"""
from celery import Celery
from celery.schedules import crontab
import os

# Redis URL for broker (default to localhost)
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Create Celery app
celery_app = Celery(
    'investorinsight',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['scheduler.tasks']
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    
    # Task execution settings
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    
    # Result backend settings
    result_expires=86400,  # Results expire after 24 hours
    
    # Beat schedule for periodic tasks
    beat_schedule={
        # SEC 13F filings - only check during filing months
        # 13F filings are due 45 days after quarter end:
        #   Q4 (Dec 31) → due Feb 14 → check Feb 1-28
        #   Q1 (Mar 31) → due May 15 → check May 1-31
        #   Q2 (Jun 30) → due Aug 14 → check Aug 1-31
        #   Q3 (Sep 30) → due Nov 14 → check Nov 1-30
        # Run daily at 6 PM ET during these months only
        'scrape-13f-february': {
            'task': 'scheduler.tasks.scrape_13f_filings',
            'schedule': crontab(hour=22, minute=0, day_of_month='1-28', month_of_year='2'),
            'options': {'queue': 'scraping'}
        },
        'scrape-13f-may': {
            'task': 'scheduler.tasks.scrape_13f_filings',
            'schedule': crontab(hour=22, minute=0, day_of_month='1-31', month_of_year='5'),
            'options': {'queue': 'scraping'}
        },
        'scrape-13f-august': {
            'task': 'scheduler.tasks.scrape_13f_filings',
            'schedule': crontab(hour=22, minute=0, day_of_month='1-31', month_of_year='8'),
            'options': {'queue': 'scraping'}
        },
        'scrape-13f-november': {
            'task': 'scheduler.tasks.scrape_13f_filings',
            'schedule': crontab(hour=22, minute=0, day_of_month='1-30', month_of_year='11'),
            'options': {'queue': 'scraping'}
        },
        
        # Congress trades - check daily at 7 PM ET
        # STOCK Act requires disclosure within 45 days, but many file sooner
        'scrape-congress-trades': {
            'task': 'scheduler.tasks.scrape_congress_trades',
            'schedule': crontab(hour=23, minute=0),  # 11 PM UTC = 7 PM ET
            'options': {'queue': 'scraping'}
        },
        
        # Net worth / Annual Financial Disclosures - check monthly
        # These are annual reports, filed by May 15 each year
        # Monthly check is sufficient to catch updates
        'scrape-net-worth-monthly': {
            'task': 'scheduler.tasks.scrape_net_worth',
            'schedule': crontab(hour=3, minute=0, day_of_month='1'),
            'options': {'queue': 'scraping'}
        },
        
        # Stock data enrichment - weekly (no need for daily)
        'enrich-stock-data': {
            'task': 'scheduler.tasks.enrich_stock_data',
            'schedule': crontab(hour=23, minute=0, day_of_week='saturday'),
            'options': {'queue': 'enrichment'}
        },
        
        # Database cleanup - weekly
        'cleanup-old-jobs': {
            'task': 'scheduler.tasks.cleanup_old_jobs',
            'schedule': crontab(hour=4, minute=0, day_of_week='monday'),
            'options': {'queue': 'maintenance'}
        },
    },
    
    # Task routing
    task_routes={
        'scheduler.tasks.scrape_*': {'queue': 'scraping'},
        'scheduler.tasks.enrich_*': {'queue': 'enrichment'},
        'scheduler.tasks.cleanup_*': {'queue': 'maintenance'},
    },
    
    # Rate limiting to be nice to APIs
    task_annotations={
        'scheduler.tasks.scrape_13f_filings': {'rate_limit': '10/m'},
        'scheduler.tasks.scrape_congress_trades': {'rate_limit': '20/m'},
    }
)

if __name__ == '__main__':
    celery_app.start()
