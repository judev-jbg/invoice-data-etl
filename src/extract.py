"""
Extract module for the ETL process.
Handles reading data from Excel files.
"""
import pandas as pd
import os
from utils.logging_config import logger
from src.transform import normalize_header

def extract(directory_path, prefix):
    """
    Reads an Excel file and returns a DataFrame.
    
    Args:
        directory_path (str): Path to the directory containing the Excel file
        prefix (str): Prefix to prepend to column names
        
    Returns:
        pd.DataFrame: The extracted data as a DataFrame or None if an error occurs
    """
    if not os.path.exists(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return None
        
    excel_files = [f for f in os.listdir(directory_path) 
                  if f.endswith(".xlsx") or f.endswith(".xls")]
    
    if not excel_files:
        logger.warning(f"No Excel files found in {directory_path}")
        return None
    
    file_name = excel_files[0]
    file_path = os.path.join(directory_path, file_name)
    
    try:
        logger.info(f"Reading file: {file_path}")
        df = pd.read_excel(file_path)
        
        # Process facturas data
        if "Id Artículo" in df.columns:
            logger.debug("Processing lineas_pedidos data")
            # Filter rows where quantity is greater than zero
            df = df[df['Cantidad'] > 0]
            
            # Convert date columns to datetime
            for date_col in ["Fecha Pedido", "Fecha Factura"]:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            # Extract year from date columns
            if "Fecha Pedido" in df.columns:
                df["Año numeracion pedido"] = df["Fecha Pedido"].dt.year
            
            if "Fecha Factura" in df.columns:
                df["Año numeracion factura"] = df["Fecha Factura"].dt.year
                
            logger.debug("Added year columns to dataframe")
        
        # Filter Serie Factura = 0 for facturas data
        if "Id Factura" in df.columns and "Serie Factura" in df.columns:
            logger.debug("Filtering by Serie Factura = 0")
            df = df[df['Serie Factura'] == 0]
        
        # Filter Serie = 0 for pedidos data
        if "Id Pedido" in df.columns and "NumPedido" in df.columns and "Serie" in df.columns:
            logger.debug("Filtering by Serie = 0")
            df = df[df['Serie'] == 0]
            df['Pedido Cliente'] = df.apply(
                lambda row: row['Observaciones'] if (row['Pedido Cliente'] == "" and row['Observaciones'] != "") 
                                                else row['Pedido Cliente'],
                axis=1
            )

        
        # Normalize column headers
        df.columns = [normalize_header(col, prefix) for col in df.columns]
        
        logger.info(f"Successfully extracted data from {file_name}: {len(df)} rows")
        return df
    
    except Exception as e:
        logger.error(f"Error during extraction of {file_name}: {str(e)}", exc_info=True)
        return None