# Invoice Data ETL

A Python-based ETL (Extract, Transform, Load) pipeline for processing invoice data from Excel files and converting it to a structured JSON format.

## Overview

This application extracts invoice data, customer information, order lines, and order data from Excel files, merges them together, and transforms them into a JSON structure for further processing or analysis.

## Features

- Extracts data from Excel files (.xlsx, .xls)
- Normalizes column headers and data types
- Merges related data from multiple sources
- Transforms data into a structured JSON format
- Comprehensive logging system
- Error handling and data validation

## Project Structure

```
invoice-data-etl/
├── config/
│   └── config.py           # Configuration settings
├── data/                   # Input data directory
│   ├── facturas/           # Invoice files
│   ├── clientes/           # Customer files
│   ├── lineas_pedidos/     # Order line files
│   └── pedidos/            # Order files
├── logs/                   # Log files directory
├── src/
│   ├── extract.py          # Data extraction module
│   ├── transform.py        # Data transformation module
│   └── load.py             # Data loading module
├── utils/
│   └── logging_config.py   # Logging configuration
├── main.py                 # Main application entry point
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

## Requirements

- Python 3.8 or higher
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/judev-jbg/invoice-data-etl.git
   cd invoice-data-etl
   ```

2. Create and activate a virtual environment:

   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

4. Create required directories:
   ```
   mkdir -p data/facturas data/clientes data/lineas_pedidos data/pedidos logs
   ```

## Usage

1. Place your Excel files in the appropriate subdirectories under the `data` directory:

   - Invoices: `data/facturas/`
   - Customers: `data/clientes/`
   - Order lines: `data/lineas_pedidos/`
   - Orders: `data/pedidos/`

2. Run the ETL process:

   ```
   python main.py
   ```

3. Check the output JSON file in the `output` directory.

## Configuration

You can configure the application by modifying the `config/config.py` file or using environment variables:

- `OUTPUT_DIR`: Directory where the JSON output file will be saved
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

## Logging

Logs are stored in the `logs/etl.log` file. The log level can be configured in `config.py` or through the `LOG_LEVEL` environment variable.

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
