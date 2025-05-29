import pandas as pd
import json
import logging
from psycopg2 import sql, extras
from typing import List, Dict, Optional
from datetime import datetime
import os
from database import DatabaseConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HoldingsProcessor:
    def __init__(self):
         self.file_handlers = {
            '.xlsx': self._process_excel,
            '.json': self._process_json
        }
    
    def __enter__(self):
        #self.conn = psycopg2.connect(**DB_CONFIG)
        return self
         
    def __exit__(self, exc_type, exc_val, exc_tb):
        #if self.conn:
        #    self.conn.close()
        pass
    
    def determine_file_type(self, file_path: str) -> Optional[str]:
        """Determine file type based on extension"""
        _, ext = os.path.splitext(file_path)
        return ext.lower() if ext.lower() in self.file_handlers else None

    def extract_data(self, file_path: str) -> Optional[Dict]:
        """Extract data from file based on its type"""
        try:
            file_type = self.determine_file_type(file_path)
            if not file_type:
                logger.error(f"Unsupported file type: {file_path}")
                return None
                
            return self.file_handlers[file_type](file_path)
            
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {e}")
            return None
    
    def _process_excel(self, file_path: str) -> Optional[Dict]:
        """
        Simple Excel file parser that demonstrates basic operations
        """
        try:
            # Read the Excel file
            # excel_file = pd.ExcelFile(file_path)
            
            # print(f"\nFile '{file_path}' contains {len(excel_file.sheet_names)} sheets:")
            # print("Sheet names:", excel_file.sheet_names)
            
            # Read the Equity sheet (previously Sheet2)
            equity_df = pd.read_excel(file_path, sheet_name='Equity', header=None)
            
            # Extract client information
            client_data = {
                'client_name': equity_df.iat[3, 1],
                'client_id': equity_df.iat[4, 1],
                'relationship': equity_df.iat[4, 2],
                'download_date': pd.to_datetime(equity_df.iat[1, 1]).date()
            }

            print('-' * 80)
            print(client_data)  # index=False removes row numbers
            print('-' * 80)  

            # Extract equity summary
            equity_summary_mask = equity_df[0].str.contains('Summary of Equity Holdings|Client ID.*Total Equity', na=False, regex=True)
            if equity_summary_mask.any():
                equity_summary_start = equity_summary_mask.idxmax()
                headers = [h.strip() if isinstance(h, str) else h for h in equity_df.iloc[equity_summary_start, 0:6].tolist()]
                equity_summary_df = equity_df.iloc[equity_summary_start+1:equity_summary_start+3, 0:6].copy()
                equity_summary_df.columns = headers
                equity_summary_df = equity_summary_df.dropna(how='all')
            else:
                equity_summary_df = pd.DataFrame()

            # print('-' * 80)
            # print(equity_summary_df.to_string(index=False))  # index=False removes row numbers
            # print('-' * 80)

            # Extract equity details
            equity_details_mask = equity_df[0].str.contains('Equity Holdings Details|Client ID.*Company Name', na=False, regex=True)
            if equity_details_mask.any():
                equity_details_start = equity_details_mask.idxmax()
                #headers = [h.strip() if isinstance(h, str) else h for h in equity_df.iloc[equity_details_start, 0:21].tolist()]
                equity_details_df = equity_df.iloc[equity_details_start+1:, 0:21].copy()
                #equity_details_df.columns = headers
                
                
                # Columns to exclude (case insensitive)
                columns_to_exclude = [
                    'PayLater(MTF) Quantity',
                    'Unpaid(CUSA) Qty',
                    'Blocked_qty'  # Add any other columns you want to exclude
                ]
        
                # Filter columns - keep only those NOT in exclude list
                columns_to_keep = [col for col in equity_details_df.columns 
                                if not any(exclude_col.lower() in str(col).lower() 
                                for exclude_col in columns_to_exclude)]
            
                df = equity_details_df[columns_to_keep]

                df = df.dropna(how='all')

                # Clean numeric columns
                numeric_cols = ['Total Quantity', 'Free Quantity', 'Unsettled Quantity', 
                            'Margin Pledged Quantity', 'PayLater(MTF) Quantity',
                            'Unpaid(CUSA) Qty', 'Blocked_qty', 'Avg Trading Price',
                            'LTP', 'Invested Value', 'Market Value', 'Overall Gain/Loss',
                            'LTCG Quantity', 'LTCG Value', 'STCG Quantity', 'STCG Value']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df = pd.DataFrame()
            
            # print('-' * 80)
            # print(df.to_string(index=False))  # index=False removes row numbers
            # print('-' * 80)

            # eqd = equity_details_df.to_dict('records')
            # print(eqd)
            raw_data = {
                'equity_details': df.to_dict('records')
            }

            clean_data =  self.transform_data(raw_data)
            # print('-' * 80)
            # print(f"Clean data {clean_data}")
            
            # print('-' * 80)
            return {
                'equity_details': clean_data,
                'source_format': 'angel-one'
            }
    
        except Exception as e:
            print(f"\nError processing file: {e}")

    def transform_data(self, raw_data: dict) -> List[Dict]:
        """
        Transforms the raw dictionary data into a clean format for database insertion.
        Handles data type conversion and filters out invalid rows.
        """
        equity_details = raw_data.get('equity_details', [])
        
        if not equity_details or len(equity_details) < 2:
            return []
        
        # Extract headers from first row (assuming first item is header row)
        headers = {k: v for k, v in equity_details[0].items() if isinstance(v, str)}
        
        clean_data = []
        for row in equity_details[1:]:
            # Skip rows that don't have Client ID or are summary rows
            if not isinstance(row.get(0), str) or row.get(0) == 'Total':
                continue
                
            clean_row = {}
            for col_idx, header in headers.items():
                value = row.get(col_idx)
                
                # Convert numeric values
                if header in ['Total Quantity', 'Free Quantity', 'Unsettled Quantity',
                            'Margin Pledged Quantity', 'LTCG Quantity', 'STCG Quantity']:
                    value = int(float(value)) if value and str(value).replace('.', '').isdigit() else 0
                elif header in ['Avg Trading Price', 'LTP', 'Invested Value', 
                            'Market Value', 'Overall Gain/Loss', 'LTCG Value', 'STCG Value']:
                    value = float(value) if value and str(value).replace('.', '').replace('-', '').isdigit() else 0.0
                elif header == 'MarketCap' and value in ['LargeCap', 'MidCap', 'SmallCap']:
                    value = value  # Keep as is
                elif header in [
                            'PayLater(MTF) Quantity',
                            'Unpaid(CUSA) Qty','Blocked_qty']:
                    continue
                else:
                    value = str(value) if value is not None and str(value) != 'nan' else None
                    
                clean_row[header.lower().replace('(', '_').replace(')', '').replace(' ', '_').replace('/', '_')] = value
            
            clean_data.append(clean_row)
        
        return clean_data

    def _process_json(self, file_path: str) -> Optional[Dict]:
        """Process JSON file (UPX format)"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if not data.get('success', False) or not data.get('data', {}).get('active'):
                logger.error("Invalid or empty JSON data")
                return None
                
            # Extract client info (mock since JSON doesn't contain this)
            client_info = {
                'client_name': 'JSON Client',
                'client_id': 'D2',
                'relationship': 'Self',
                'download_date': datetime.now().date(),
                'extracted_at': datetime.now(),
                'source_type': 'json'
            }
            
            # Process active holdings
            holdings = []
            for item in data['data']['active']:
                if not item.get('instrument'):
                    continue
                    
                # Get primary instrument (first in array)
                primary_instrument = item['instrument'][0]
                
                holding = {
                    'client_id': 'D2',
                    'isin': primary_instrument['i'].split('|')[-1],
                    'company_name': primary_instrument['s'],
                    #'exchange': primary_instrument['e'],
                    'total_quantity': item['fillInfo']['demat']['qty'],
                    'free_quantity': item['fillInfo']['demat']['qty'] - item.get('usedQty', 0),
                    'invested_value': item['fillInfo']['demat']['amt'],
                    'avg_trading_price': item['fillInfo']['demat']['avgPrice'],
                    #'market_value': item['netInfo']['buyValue'],
                    # 'haircut': item.get('additionalInfo', {}).get('haircut', 0),
                    #'instrument_key': item['instrument_key']
                }
                
                holdings.append(holding)
            
            return {
                #'client_info': client_info,
                'equity_details': holdings,
                'source_format': 'upxstx'
            }
            
        except Exception as e:
            logger.error(f"Error processing JSON file: {e}")
            return None
        
    def save_to_database(self, data: Dict):
        
        if data.get('source_format') == 'angel-one':
            self._save_angel_holdings(data['equity_details'])
        elif data.get('source_format') == 'upxstx':
            self._save_upx_holdings(data['equity_details'])
        else:
            logger.warning(f"Unknown source format: {data.get('source_format')}")

    def _save_angel_holdings(self, data: List[Dict]):
        try:
            if not data:
                return
            
            columns = data[0].keys()
            query = sql.SQL("""
            INSERT INTO equity_holdings ({})
            VALUES %s
            ON CONFLICT (client_id, company_name, isin) 
            DO UPDATE SET
                marketcap = EXCLUDED.marketcap,
                sector = EXCLUDED.sector,
                total_quantity = EXCLUDED.total_quantity,
                avg_trading_price = EXCLUDED.avg_trading_price,
                invested_value = EXCLUDED.invested_value,
                market_value = EXCLUDED.market_value,
                overall_gain_loss = EXCLUDED.overall_gain_loss,
                created_at = EXCLUDED.created_at
            """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))
        
            values = [[row[col] for col in columns] for row in data]
            
            #conn = get_db_connection()
            conn = DatabaseConnection.get_connection()
            with conn.cursor() as cur:
                extras.execute_values(
                    cur,
                    query,
                    values,
                    template=None,
                    page_size=100
                )
                conn.commit()

            return True
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"\nDatabase error: {e}")
            return False
        finally:
            # Return connection to pool
            DatabaseConnection.return_connection(conn)

    def _save_upx_holdings(self, data: List[Dict]):
        try:
            if not data:
                return
            
            columns = data[0].keys()
            query = sql.SQL("""
            INSERT INTO equity_holdings ({})
            VALUES %s
            ON CONFLICT (client_id, company_name, isin) 
            DO UPDATE SET
                total_quantity = EXCLUDED.total_quantity,
                avg_trading_price = EXCLUDED.avg_trading_price,
                invested_value = EXCLUDED.invested_value,
                market_value = EXCLUDED.market_value,
                created_at = EXCLUDED.created_at
            """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))
        
            values = [[row[col] for col in columns] for row in data]
            
            conn = DatabaseConnection.get_connection()
            #conn = get_db_connection()
            with conn.cursor() as cur:
                extras.execute_values(
                    cur,
                    query,
                    values,
                    template=None,
                    page_size=100
                )
                conn.commit()

            return True
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"\nDatabase error: {e}")
            return False
        finally:
            # Return connection to pool
            DatabaseConnection.return_connection(conn)


def main():

    # Files to process
    files_to_process = [
        'HOLDINGG_A1.xlsx'
        ,
        'upx.json'
    ]

    try:
        with HoldingsProcessor() as processor:
            # Create tables if they don't exist
            #processor.create_tables()

            # Process each file
            for file_path in files_to_process:
                print(f"\nProcessing file: {file_path}")
                
                # Extract data
                extracted_data = processor.extract_data(file_path)
                print('-' * 80)
                print(f"Extracted data {extracted_data}")
                print('-' * 80)
                
                if extracted_data:
                    # Save to database
                    processor.save_to_database(extracted_data)
                    print(f"Successfully processed {file_path} ({extracted_data.get('source_format')})")
                else:
                    print(f"Failed to process {file_path}")

    except Exception as e:
        logger.error(f"Processing failed: {e}")


# Example usage
if __name__ == "__main__":
    main()