"""
Logging configuration for the ETL process.
"""
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
from config.config import LOG_DIR, LOG_FILE, LOG_LEVEL

def setup_logging():
    """
    Configure and set up logging for the application.
    Creates a logger that writes to both console and file.
    """
    # Create log directory if it doesn't exist
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Create a custom logger
    logger = logging.getLogger('etl_logger')
    
    # Set level based on configuration
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = RotatingFileHandler(
        LOG_FILE, 
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    
    # Create formatters and add them to handlers
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    console_handler.setFormatter(log_format)
    file_handler.setFormatter(log_format)
    
    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Create the global logger instance
logger = setup_logging()