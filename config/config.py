"""
Configuration module for the ETL process.
Contains paths and column definitions.
"""
import os
from pathlib import Path

# Base directory paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Input directories
FACTURAS_DIR = os.path.join(DATA_DIR, 'facturas')
CLIENTES_DIR = os.path.join(DATA_DIR, 'clientes')
LINEAS_PEDIDOS_DIR = os.path.join(DATA_DIR, 'lineas_pedidos')
PEDIDOS_DIR = os.path.join(DATA_DIR, 'pedidos')

# Output directory
OUTPUT_DIR = "I:\\Mi unidad\\Facturas JSON"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'facturas.json')

# Column names for final dataframe
COLUMNS_FINAL = [
    'lpd_id_articulo', 'lpd_descripcion_linea', 'lpd_cantidad', 'lpd_precio_euros',
    'lpd_descuento_linea', 'lpd_total', 'lpd_albaran', 'lpd_fecha_albaran',
    'pd_id_pedido', 'pd_numpedido', 'pd_ano_numeracion', 'pd_fecha_pedido',
    'pd_pedido_cliente', 'ft_id_factura', 'ft_numfactura', 'ft_ano_numeracion',
    'ft_fecha_factura', 'ft_total', 'ft_ivatotal', 'ft_totaliva', 'ft_observaciones',
    'cte_id_cliente', 'cte_cliente', 'cte_direccion', 'cte_codigo_postal',
    'cte_ciudad', 'cte_provincia', 'cte_pais', 'cte_nif'
]

# Define prefixes for each data type
PREFIX_MAP = {
    'facturas': 'ft',
    'clientes': 'cte',
    'lineas_pedidos': 'lpd',
    'pedidos': 'pd'
}

# Log configuration
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'etl.log')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')