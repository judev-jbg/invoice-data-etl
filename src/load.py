"""
Load module for the ETL process.
Handles saving processed data to JSON files.
"""
import json
import os
from utils.logging_config import logger

def load(data, output_path):
    """
    Saves each dictionary from a list of dictionaries to individual JSON files.
    
    Args:
        data (list): List of dictionaries to save individually
        output_path (str): Template path where the JSON files will be saved
                          (should contain 'placeholder' to be replaced with references)
        
    Returns:
        bool: True if all files were saved successfully, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Starting to save {len(data)} processed invoices")

        for dict_invoice in data:
            try:

                output_path_file = output_path.replace("placeholder", dict_invoice['reference_invoice'])
                
                with open(output_path_file, 'w', encoding='utf-8') as f:
                    json.dump(dict_invoice['factura'], f, ensure_ascii=False, indent=4)
                    
                file_size = os.path.getsize(output_path_file) / (1024 * 1024)  # Size in MB
                logger.info(f"File saved successfully: {output_path_file} ({file_size:.2f} MB)")
            
            except Exception as e:
                logger.error(f"Error saving invoice {dict_invoice.get('reference_invoice', 'unknown')}: {str(e)}")

        
        return True
        
    except Exception as e:
        logger.error(f"Error in save process: {str(e)}", exc_info=True)
        return False