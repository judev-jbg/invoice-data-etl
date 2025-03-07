"""
Transform module for the ETL process.
Handles data cleaning, transformations and merging.
"""
import pandas as pd
import numpy as np
import unicodedata
from utils.logging_config import logger

def transform(df):
    """
    Processes the DataFrame and converts it to a structured dictionary.
    
    Args:
        df (pd.DataFrame): The dataframe to transform
        
    Returns:
        dict: A dictionary with the transformed data or None if an error occurs
    """
    try:
        logger.info("Starting transformation process")
        
        # Initialize the invoice dictionary
        dict_invoice = {"facturas": []}
        
        # Get unique invoice IDs
        facturas_unicas = df['ft_id_factura'].unique()
        logger.info(f"Found {len(facturas_unicas)} unique invoices to process")
        
        # Process each invoice
        for id_factura in facturas_unicas:
            # Filter dataframe to get only rows for this invoice
            filas_factura = df[df['ft_id_factura'] == id_factura]
            
            # Take common data from the first row (same for all rows of the same invoice)
            primera_fila = filas_factura.iloc[0]
            
            # Create the basic invoice structure
            factura_estructura = {
                "id": convert_to_native_type(primera_fila['ft_id_factura']),
                "num_factura": convert_to_native_type(primera_fila['ft_numfactura']),
                "año_factura": convert_to_native_type(primera_fila['ft_ano_numeracion']),
                "fecha_factura": date_formatting(primera_fila['ft_fecha_factura']),
                "total_iva_excl": convert_to_native_type(primera_fila['ft_total']),
                "tota_iva": convert_to_native_type(primera_fila['ft_ivatotal']),
                "total_iva_incl": convert_to_native_type(primera_fila['ft_totaliva']),
                "observaciones": convert_to_native_type(primera_fila.get('ft_observaciones', "")),
                "num_albaran": convert_to_native_type(primera_fila['lpd_albaran']),
                "fecha_albaran": date_formatting(primera_fila['lpd_fecha_albaran']),
                "id_pedido": convert_to_native_type(primera_fila['pd_id_pedido']),
                "num_pedido": convert_to_native_type(primera_fila['pd_numpedido']),
                "año_pedido": convert_to_native_type(primera_fila['pd_ano_numeracion']),
                "fecha_pedido": date_formatting(primera_fila['pd_fecha_pedido']),
                "id_pedido_cliente": convert_to_native_type(primera_fila['pd_pedido_cliente']),
                "id_cliente": convert_to_native_type(primera_fila['cte_id_cliente']),
                "cliente": convert_to_native_type(primera_fila['cte_cliente']),
                "direccion": convert_to_native_type(primera_fila['cte_direccion']),
                "cod_postal": convert_to_native_type(primera_fila['cte_codigo_postal']),
                "ciudad": convert_to_native_type(primera_fila['cte_ciudad']),
                "provincia": convert_to_native_type(primera_fila['cte_provincia']),
                "pais": convert_to_native_type(primera_fila['cte_pais']),
                "nif": convert_to_native_type(primera_fila['cte_nif']),
                "products": []
            }
            
            # Add products to the invoice
            for idx, fila in filas_factura.iterrows():

                factura_estructura["products"].append({"product": {
                    "id_articulo": convert_to_native_type(fila['lpd_id_articulo']),
                    "descripcion": convert_to_native_type(fila['lpd_descripcion_linea']),
                    "cantidad": convert_to_native_type(fila['lpd_cantidad']),
                    "precio": convert_to_native_type(fila['lpd_precio_euros']),
                    "descuento": convert_to_native_type(fila['lpd_descuento_linea']),
                    "total": convert_to_native_type(fila['lpd_total'])
                }
             })
            # Add invoice to the result
            # factura_id = f"factura_{id_factura}"
            dict_invoice["facturas"].append({"factura": factura_estructura})
            
            logger.debug(f"Processed invoice {id_factura} with {len(filas_factura)} products")

        logger.info(f"Transformation successful: converted dataframe to dictionary with {len(dict_invoice['facturas'])} invoices")
        return dict_invoice
    
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        return None

def merge_dataframes(left_df, right_df, left_keys, right_keys=None, how="inner"):
    """
    Merges two dataframes based on specified keys.
    
    Args:
        left_df (pd.DataFrame): Left dataframe
        right_df (pd.DataFrame): Right dataframe
        left_keys (list or str): Key(s) from left_df
        right_keys (list or str, optional): Key(s) from right_df. If None, uses left_keys.
        how (str, optional): Type of merge to perform. Defaults to "inner".
        
    Returns:
        pd.DataFrame: Merged dataframe or None if an error occurs
    """
    try:
        if right_keys is None:
            right_keys = left_keys
            
        # Convert single keys to lists
        if isinstance(left_keys, str):
            left_keys = [left_keys]
        if isinstance(right_keys, str):
            right_keys = [right_keys]
            
        logger.info(f"Merging dataframes: {len(left_df)} rows × {len(right_df)} rows")
        logger.debug(f"Merge keys: left={left_keys}, right={right_keys}, how={how}")
        
        # Check if keys exist in dataframes
        for key in left_keys:
            if key not in left_df.columns:
                logger.error(f"Key '{key}' not found in left dataframe")
                return None
                
        for key in right_keys:
            if key not in right_df.columns:
                logger.error(f"Key '{key}' not found in right dataframe")
                return None
        
        # Perform merge
        merged_df = pd.merge(
            left_df, 
            right_df, 
            how=how, 
            left_on=left_keys, 
            right_on=right_keys
        )
        
        logger.info(f"Merge completed: {len(merged_df)} rows in result")
        return merged_df
        
    except Exception as e:
        logger.error(f"Error during dataframe merge: {str(e)}", exc_info=True)
        return None

def normalize_header(header, prefix):
    """
    Normalizes a column header by removing accents, converting to lowercase,
    replacing spaces with underscores, and adding a prefix.
    
    Args:
        header (str): Original column header
        prefix (str): Prefix to add to the normalized header
        
    Returns:
        str: Normalized header
    """
    # Convert to lowercase
    header = header.lower()
    # Remove accents
    header = ''.join(c for c in unicodedata.normalize('NFD', header) 
                    if unicodedata.category(c) != 'Mn')
    # Replace spaces and hyphens with underscores
    header = header.replace(' ', '_').replace('-', '_')
    # Add prefix
    header = f"{prefix}_{header}"
    return header

def date_formatting(fecha):
    """
    Converts any date type to YYYY-MM-DD format.
    
    Args:
        fecha: Date value to format
        
    Returns:
        str: Formatted date or empty string if date is NA
    """
    if pd.isna(fecha):
        return ""
    
    # If it's a Timestamp or datetime object
    if hasattr(fecha, 'strftime'):
        return fecha.strftime('%Y-%m-%d')
    
    # If it's a string
    fecha_str = str(fecha)
    # Try to extract only the date part if it has a space
    if ' ' in fecha_str:
        return fecha_str.split()[0]
    
    return fecha_str

def convert_to_native_type(value):
    """
    Converts numpy values to native Python types for JSON serialization.
    
    Args:
        value: Value to convert
        
    Returns:
        Value converted to a native Python type
    """
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif pd.isna(value):
        return None
    else:
        return value