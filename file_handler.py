


import csv
from typing import List, Dict, Tuple


def read_sales_data(filename: str) -> List[str]:
    """
    Reads sales data from file handling encoding issues
    
    Args:
        filename (str): Path to the sales data file
        
    Returns:
        List[str]: List of raw lines (strings)
        
    Raises:
        FileNotFoundError: If file doesn't exist
        
    Expected Output Format:
    ['T001|2024-12-01|P101|Laptop|2|45000|C001|North', ...]
    
    Requirements:
    - Use 'with' statement
    - Handle different encodings (try 'utf-8', 'latin-1', 'cp1252')
    - Handle FileNotFoundError with appropriate error message
    - Skip the header row
    - Remove empty lines
    """
    raw_lines = []
    
    
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(filename, 'r', encoding=encoding) as file:
               
                lines = file.readlines()
                
               
                if lines:
                    header = lines[0]
                    data_lines = lines[1:]
                else:
                    data_lines = []
                
                
                for line in data_lines:
                   
                    line = line.strip()
                    
                    
                    if line:
                        raw_lines.append(line)
                
                
                if raw_lines:
                    print(f"Successfully read file with {encoding} encoding")
                    break
                    
        except UnicodeDecodeError:
            
            continue
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {filename}")
    
    return raw_lines
def parse_transactions(raw_lines: List[str]) -> List[Dict]:
    """
    Parses raw lines into clean list of dictionaries
    
    Args:
        raw_lines (List[str]): List of raw pipe-delimited strings
        
    Returns:
        List[Dict]: List of dictionaries with cleaned data
        
    Expected Output Format:
    [
        {
            'TransactionID': 'T001',
            'Date': '2024-12-01',
            'ProductID': 'P101',
            'ProductName': 'Laptop',
            'Quantity': 2,           # int type
            'UnitPrice': 45000.0,    # float type
            'CustomerID': 'C001',
            'Region': 'North'
        },
        ...
    ]
    
    Requirements:
    - Split by pipe delimiter '|'
    - Handle commas within ProductName (remove or replace)
    - Remove commas from numeric fields and convert to proper types
    - Convert Quantity to int
    - Convert UnitPrice to float
    - Skip rows with incorrect number of fields
    """
    parsed_transactions = []
    
    for line in raw_lines:
        try:
            
            parts = line.split('|')
            
           
            if len(parts) != 8:
                continue
            
            
            transaction_id = parts[0].strip()
            date = parts[1].strip()
            product_id = parts[2].strip()
            
           
            product_name = parts[3].strip().replace(',', '')
            
            
            quantity_str = parts[4].strip().replace(',', '')
            quantity = int(quantity_str) if quantity_str else 0
            
            
            unit_price_str = parts[5].strip().replace(',', '')
            unit_price = float(unit_price_str) if unit_price_str else 0.0
            
            customer_id = parts[6].strip()
            region = parts[7].strip()
            
            
            transaction = {
                'TransactionID': transaction_id,
                'Date': date,
                'ProductID': product_id,
                'ProductName': product_name,
                'Quantity': quantity,
                'UnitPrice': unit_price,
                'CustomerID': customer_id,
                'Region': region
            }
            
            parsed_transactions.append(transaction)
            
        except (ValueError, IndexError) as e:
            # Skip rows with conversion errors
            print(f"Warning: Skipping malformed line - {e}")
            continue
    
    return parsed_transactions

def validate_and_filter(transactions: List[Dict], region: str = None, 
                       min_amount: float = None, max_amount: float = None) -> Tuple[List[Dict], int, Dict]:
    """
    Validates transactions and applies optional filters
    
    Parameters:
    - transactions: list of transaction dictionaries
    - region: filter by specific region (optional)
    - min_amount: minimum transaction amount (Quantity * UnitPrice) (optional)
    - max_amount: maximum transaction amount (optional)
    
    Returns: tuple (valid_transactions, invalid_count, filter_summary)
    
    Expected Output Format:
    (
        [list of valid filtered transactions],
        5,  # count of invalid transactions
        {
            'total_input': 100,
            'invalid': 5,
            'filtered_by_region': 20,
            'filtered_by_amount': 10,
            'final_count': 65
        }
    )
    
    Validation Rules:
    - Quantity must be > 0
    - UnitPrice must be > 0
    - All required fields must be present
    - TransactionID must start with 'T'
    - ProductID must start with 'P'
    - CustomerID must start with 'C'
    
    Filter Display:
    - Print available regions to user before filtering
    - Print transaction amount range (min/max) to user
    - Show count of records after each filter applied
    """
    valid_transactions = []
    invalid_count = 0
    
    
    filter_summary = {
        'total_input': len(transactions),
        'invalid': 0,
        'filtered_by_region': 0,
        'filtered_by_amount': 0,
        'final_count': 0
    }
    
    
    print("\n=== Available Regions ===")
    regions = set()
    for transaction in transactions:
        reg = transaction.get('Region', '')
        if reg:
            regions.add(reg)
    print("Regions:", ", ".join(sorted(regions)))
    
    
    print("\n=== Transaction Amount Range ===")
    amounts = []
    for transaction in transactions:
        try:
            qty = transaction.get('Quantity', 0)
            price = transaction.get('UnitPrice', 0.0)
            amount = qty * price
            amounts.append(amount)
        except:
            continue
    
    if amounts:
        print(f"Min amount: {min(amounts):.2f}")
        print(f"Max amount: {max(amounts):.2f}")
        print(f"Avg amount: {sum(amounts)/len(amounts):.2f}")
    
    
    print("\n=== Step 1: Basic Validation ===")
    initial_count = len(transactions)
    print(f"Initial transactions: {initial_count}")
    
    for transaction in transactions:
        is_valid = True
        
        # Check all required fields are present
        required_fields = ['TransactionID', 'Date', 'ProductID', 'ProductName', 
                          'Quantity', 'UnitPrice', 'CustomerID', 'Region']
        for field in required_fields:
            if field not in transaction or not str(transaction.get(field, '')).strip():
                is_valid = False
                break
        
        if not is_valid:
            invalid_count += 1
            continue
        
       
        trans_id = str(transaction.get('TransactionID', ''))
        prod_id = str(transaction.get('ProductID', ''))
        cust_id = str(transaction.get('CustomerID', ''))
        quantity = transaction.get('Quantity', 0)
        unit_price = transaction.get('UnitPrice', 0.0)
        
        if not trans_id.startswith('T'):
            is_valid = False
        elif not prod_id.startswith('P'):
            is_valid = False
        elif not cust_id.startswith('C'):
            is_valid = False
        elif quantity <= 0:
            is_valid = False
        elif unit_price <= 0:
            is_valid = False
        
        if is_valid:
            valid_transactions.append(transaction)
        else:
            invalid_count += 1
    
    filter_summary['invalid'] = invalid_count
    print(f"Valid after basic validation: {len(valid_transactions)}")
    print(f"Invalid transactions: {invalid_count}")
    
    
    if region:
        print(f"\n=== Step 2: Applying Region Filter ('{region}') ===")
        print(f"Before region filter: {len(valid_transactions)}")
        
        filtered_by_region = []
        for transaction in valid_transactions:
            if transaction.get('Region', '').lower() == region.lower():
                filtered_by_region.append(transaction)
        
        filter_summary['filtered_by_region'] = len(valid_transactions) - len(filtered_by_region)
        valid_transactions = filtered_by_region
        print(f"After region filter: {len(valid_transactions)}")
    
    
    if min_amount is not None or max_amount is not None:
        print(f"\n=== Step 3: Applying Amount Filter (Min: {min_amount}, Max: {max_amount}) ===")
        print(f"Before amount filter: {len(valid_transactions)}")
        
        filtered_by_amount = []
        for transaction in valid_transactions:
            amount = transaction.get('Quantity', 0) * transaction.get('UnitPrice', 0.0)
            
           
            if min_amount is not None and amount < min_amount:
                continue
            
           
            if max_amount is not None and amount > max_amount:
                continue
            
            filtered_by_amount.append(transaction)
        
        filter_summary['filtered_by_amount'] = len(valid_transactions) - len(filtered_by_amount)
        valid_transactions = filtered_by_amount
        print(f"After amount filter: {len(valid_transactions)}")
    
    filter_summary['final_count'] = len(valid_transactions)
    
    return valid_transactions, invalid_count, filter_summary


def parse_sales_data(raw_lines: List[str]) -> Tuple[List[Dict], int]:
    """
    Parse raw lines into structured data.
    
    Args:
        raw_lines (List[str]): List of raw pipe-delimited strings
        
    Returns:
        Tuple[List[Dict], int]: (List of parsed records, total count)
    """
   
    parsed_records = parse_transactions(raw_lines)
    
    return parsed_records, len(parsed_records)

def clean_sales_data(parsed_records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Clean and validate sales data according to the specified rules.
    
    Args:
        parsed_records (List[Dict]): Parsed sales records
        
    Returns:
        Tuple[List[Dict], List[Dict]]: (valid_records, invalid_records)
    """
    valid_records = []
    invalid_records = []
    
    for record in parsed_records:
        
        record = record.copy()
        
        
        if not any(record.values()):
            continue
            
        
        transaction_id = record.get('TransactionID', '')
        if not transaction_id.startswith('T'):
            invalid_records.append(record)
            continue
            
       
        customer_id = record.get('CustomerID', '')
        region = record.get('Region', '')
        if not customer_id or not customer_id.strip() or not region or not region.strip():
            invalid_records.append(record)
            continue
            
       
        try:
            quantity = int(record.get('Quantity', '0'))
            if quantity <= 0:
                invalid_records.append(record)
                continue
        except (ValueError, TypeError):
            invalid_records.append(record)
            continue
            
        
        try:
            # Clean the UnitPrice string by removing commas
            unit_price_str = str(record.get('UnitPrice', '0')).replace(',', '')
            unit_price = float(unit_price_str)
            if unit_price <= 0:
                invalid_records.append(record)
                continue
        except (ValueError, TypeError):
            invalid_records.append(record)
            continue
            
        
        product_name = record.get('ProductName', '')
        if product_name:
            record['ProductName'] = product_name.replace(',', '')
        
        
        unit_price_str = str(record.get('UnitPrice', '0')).replace(',', '')
        record['UnitPrice'] = float(unit_price_str)
        
        
        record['Quantity'] = int(record.get('Quantity', '0'))
        
        
        valid_records.append(record)
    
    return valid_records, invalid_records


def validate_and_display_summary(raw_count: int, valid_records: List[Dict], invalid_records: List[Dict]) -> None:
    """
    Display validation summary as required.
    
    Args:
        raw_count (int): Total records parsed
        valid_records (List[Dict]): Valid records after cleaning
        invalid_records (List[Dict]): Invalid records removed
    """
    print(f"Total records parsed: {raw_count}")
    print(f"Invalid records removed: {len(invalid_records)}")
    print(f"Valid records after cleaning: {len(valid_records)}")