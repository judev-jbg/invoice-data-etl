"""
Load module for the ETL process.
Handles saving processed data to JSON files.
"""
import json
import os
from utils.logging_config import logger

def load(data_dict, output_path):
    """
    Saves the processed dictionary to a JSON file.
    
    Args:
        data_dict (dict): Dictionary to save
        output_path (str): Path where the JSON file will be saved
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Saving processed data to {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data_dict, f, ensure_ascii=False, indent=4)
            
        file_size = os.path.getsize(output_path) / (1024 * 1024)  # Size in MB
        logger.info(f"File saved successfully: {output_path} ({file_size:.2f} MB)")
        return True
        
    except Exception as e:
        logger.error(f"Error saving data to {output_path}: {str(e)}", exc_info=True)
        return False