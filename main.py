"""
Main module for the ETL process.
Orchestrates the extraction, transformation, and loading of data.
"""

import time
from config.config import (
    FACTURAS_DIR, CLIENTES_DIR, LINEAS_PEDIDOS_DIR, PEDIDOS_DIR,
    COLUMNS_FINAL, OUTPUT_FILE, PREFIX_MAP
)
from src.extract import extract
from src.transform import merge_dataframes, transform
from src.load import load
from utils.logging_config import logger

def main():
    """
    Orchestrates the ETL process.
    Extracts data from Excel files, transforms it, and loads it into a JSON file.
    """
    start_time = time.time()
    logger.info("Starting ETL process")
    
    # Extract data
    logger.info("Extracting data from Excel files")
    df_facturas = extract(FACTURAS_DIR, PREFIX_MAP['facturas'])
    df_clientes = extract(CLIENTES_DIR, PREFIX_MAP['clientes'])
    df_lineas_pedidos = extract(LINEAS_PEDIDOS_DIR, PREFIX_MAP['lineas_pedidos'])
    df_pedidos = extract(PEDIDOS_DIR, PREFIX_MAP['pedidos'])
    
    # Check if extraction was successful
    if any(df is None for df in [df_facturas, df_clientes, df_lineas_pedidos, df_pedidos]):
        logger.error("ETL process failed: One or more files could not be processed")
        return
        
    # Merge data
    logger.info("Merging dataframes")
    
    # Merge facturas with clientes
    df_facturas_clientes = merge_dataframes(
        df_facturas, 
        df_clientes, 
        'ft_id_cliente', 
        'cte_id_cliente'
    )
    
    # Merge lineas_pedidos with pedidos
    df_lineas_pedidos = merge_dataframes(
        df_lineas_pedidos,
        df_pedidos,
        ['lpd_numero_pedido', 'lpd_ano_numeracion_pedido'],
        ['pd_numpedido', 'pd_ano_numeracion']
    )
    
    # Check if merges were successful
    if df_facturas_clientes is None or df_lineas_pedidos is None:
        logger.error("ETL process failed: Error merging dataframes")
        return
        
    # Merge all data
    df_merged_full = merge_dataframes(
        df_lineas_pedidos,
        df_facturas_clientes,
        ['lpd_factura', 'lpd_ano_numeracion_factura'],
        ['ft_numfactura', 'ft_ano_numeracion']
    )
    
    if df_merged_full is None:
        logger.error("ETL process failed: Error merging all dataframes")
        return
        
    # Filter columns
    logger.info(f"Filtering dataframe to keep only necessary columns")
    available_columns = set(df_merged_full.columns)
    missing_columns = set(COLUMNS_FINAL) - available_columns
    
    if missing_columns:
        logger.warning(f"Missing columns in merged dataframe: {missing_columns}")
        # Only keep columns that exist in the dataframe
        columns_to_keep = [col for col in COLUMNS_FINAL if col in available_columns]
    else:
        columns_to_keep = COLUMNS_FINAL
        
    df_merged_full = df_merged_full[columns_to_keep]
    
    # Transform data
    logger.info("Transforming data to dictionary structure")
    dict_invoice = transform(df_merged_full)
    
    if dict_invoice is None or not dict_invoice.get('facturas'):
        logger.error("ETL process failed: Error transforming data")
        return
        
    # Load data
    logger.info("Loading data to JSON file")
    success = load(dict_invoice, OUTPUT_FILE)
    
    if success:
        invoice_count = len(dict_invoice['facturas'])
        execution_time = time.time() - start_time
        logger.info(f"ETL process completed successfully in {execution_time:.2f} seconds")
        logger.info(f"Processed {invoice_count} invoices")
    else:
        logger.error("ETL process failed: Error loading data")
        
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception in main process: {str(e)}", exc_info=True)