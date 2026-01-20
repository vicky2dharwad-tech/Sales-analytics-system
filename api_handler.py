
import requests
import time
from typing import List, Dict, Optional
import json


class APIHandler:
    
    
    BASE_URL = "https://dummyjson.com/products"
    
    def __init__(self, max_retries: int = 3, delay: float = 1.0):
        """
        Initialize API Handler.
        
        Args:
            max_retries (int): Maximum number of retry attempts for failed requests
            delay (float): Delay between retries in seconds
        """
        self.max_retries = max_retries
        self.delay = delay
        self.session = requests.Session()
        
    def make_request(self, url: str) -> Optional[Dict]:
        """
        Make HTTP request with retry logic.
        
        Args:
            url (str): API endpoint URL
            
        Returns:
            Optional[Dict]: JSON response as dictionary or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()  # Raise exception for bad status codes
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay)
                else:
                    print(f"Failed to fetch data from {url} after {self.max_retries} attempts")
                    return None
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON response: {e}")
                return None
        
        return None
    
    def fetch_all_products(self) -> List[Dict]:
        """
        Fetches all products from DummyJSON API
        
        Returns:
            List[Dict]: List of product dictionaries
            
        Expected Output Format:
        [
            {
                'id': 1,
                'title': 'iPhone 9',
                'category': 'smartphones',
                'brand': 'Apple',
                'price': 549,
                'rating': 4.69
            },
            ...
        ]
        
        Requirements:
        - Fetch all available products (use limit=100)
        - Handle connection errors with try-except
        - Return empty list if API fails
        - Print status message (success/failure)
        """
        url = f"{self.BASE_URL}?limit=100"
        
        try:
            print("Fetching all products from DummyJSON API...")
            data = self.make_request(url)
            
            if data and 'products' in data:
                # Extract only required fields
                products = []
                for product in data['products']:
                    products.append({
                        'id': product.get('id', 0),
                        'title': product.get('title', 'Unknown'),
                        'category': product.get('category', 'Unknown'),
                        'brand': product.get('brand', 'Unknown'),
                        'price': product.get('price', 0),
                        'rating': product.get('rating', 0)
                    })
                
                print(f"✓ Successfully fetched {len(products)} products")
                return products
            else:
                print("✗ Failed to fetch products: Invalid response from API")
                return []
                
        except Exception as e:
            print(f"✗ Failed to fetch products: {e}")
            return []



    def create_product_mapping(self, api_products: List[Dict]) -> Dict[int, Dict[str, any]]:
        """
        Creates a mapping of product IDs to product info
        
        Parameters:
            api_products: List of products from fetch_all_products()
            
        Returns:
            Dict[int, Dict[str, any]]: Dictionary mapping product IDs to info
            
        Expected Output Format:
        {
            1: {
                'title': 'iPhone 9',
                'category': 'smartphones',
                'brand': 'Apple',
                'rating': 4.69
            },
            2: {
                'title': 'iPhone X',
                'category': 'smartphones',
                'brand': 'Apple',
                'rating': 4.44
            },
            ...
        }
        """
        print("\nCreating product mapping...")
        
        product_mapping = {}
        
        for product in api_products:
            try:
                product_id = product.get('id')
                if product_id:
                    product_mapping[product_id] = {
                        'title': product.get('title', 'Unknown'),
                        'category': product.get('category', 'Unknown'),
                        'brand': product.get('brand', 'Unknown'),
                        'rating': product.get('rating', 0),
                        'price': product.get('price', 0)
                    }
            except (KeyError, TypeError):
                # Skip invalid products
                continue
        
        print(f"Created mapping for {len(product_mapping)} products")
        return product_mapping



    def extract_product_id(self, product_id_str: str) -> Optional[int]:
        """
        Helper function to extract numeric ID from ProductID string.
        
        Args:
            product_id_str (str): ProductID like 'P101', 'P5', etc.
            
        Returns:
            Optional[int]: Extracted numeric ID or None if invalid
        """
        try:
            # Remove 'P' prefix and convert to integer
            if product_id_str and product_id_str.startswith('P'):
                numeric_part = product_id_str[1:]  # Remove 'P'
                return int(numeric_part)
            return None
        except (ValueError, TypeError, AttributeError):
            return None


def enrich_sales_data(transactions: List[Dict], product_mapping: Dict[int, Dict]) -> List[Dict]:
    """
    Enriches transaction data with API product information
    
    Parameters:
    - transactions: list of transaction dictionaries
    - product_mapping: dictionary from create_product_mapping()
    
    Returns: list of enriched transaction dictionaries
    
    Expected Output Format (each transaction):
    {
        'TransactionID': 'T001',
        'Date': '2024-12-01',
        'ProductID': 'P101',
        'ProductName': 'Laptop',
        'Quantity': 2,
        'UnitPrice': 45000.0,
        'CustomerID': 'C001',
        'Region': 'North',
        
        'API_Category': 'laptops',
        'API_Brand': 'Apple',
        'API_Rating': 4.7,
        'API_Match': True  # True if enrichment successful, False otherwise
    }
    
    Enrichment Logic:
    - Extract numeric ID from ProductID (P101 → 101, P5 → 5)
    - If ID exists in product_mapping, add API fields
    - If ID doesn't exist, set API_Match to False and other fields to None
    - Handle all errors gracefully
    """
    print("\nEnriching sales data with API product information...")
    
    enriched_transactions = []
    match_count = 0
    api_handler = APIHandler()  # Create instance for helper method
    
    for transaction in transactions:
        # Create a copy to avoid modifying original
        enriched_transaction = transaction.copy()
        
        # Extract numeric product ID
        product_id_str = transaction.get('ProductID', '')
        numeric_id = api_handler.extract_product_id(product_id_str)
        
        if numeric_id and numeric_id in product_mapping:
            # Match found - enrich with API data
            api_product_info = product_mapping[numeric_id]
            
            enriched_transaction['API_Category'] = api_product_info.get('category')
            enriched_transaction['API_Brand'] = api_product_info.get('brand')
            enriched_transaction['API_Rating'] = api_product_info.get('rating')
            enriched_transaction['API_Match'] = True
            
            match_count += 1
        else:
            # No match found
            enriched_transaction['API_Category'] = None
            enriched_transaction['API_Brand'] = None
            enriched_transaction['API_Rating'] = None
            enriched_transaction['API_Match'] = False
        
        enriched_transactions.append(enriched_transaction)
    
    total_transactions = len(transactions)
    match_percentage = (match_count / total_transactions * 100) if total_transactions > 0 else 0
    
    print(f"Enrichment complete:")
    print(f"  Total transactions: {total_transactions}")
    print(f"  Matched with API: {match_count} ({match_percentage:.1f}%)")
    print(f"  Not matched: {total_transactions - match_count}")
    
    return enriched_transactions


def save_enriched_data(enriched_transactions: List[Dict], 
                       filename: str = 'data/enriched_sales_data.txt') -> bool:
    """
    Saves enriched transactions back to file
    
    Expected File Format:
    TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match
    T001|2024-12-01|P101|Laptop|2|45000.0|C001|North|laptops|Apple|4.7|True
    ...
    
    Requirements:
    - Create output file with all original + new fields
    - Use pipe delimiter
    - Handle None values appropriately
    """
    print(f"\nSaving enriched data to: {filename}")
    
    try:
        
        columns = [
            'TransactionID', 'Date', 'ProductID', 'ProductName',
            'Quantity', 'UnitPrice', 'CustomerID', 'Region',
            'API_Category', 'API_Brand', 'API_Rating', 'API_Match'
        ]
        
        with open(filename, 'w', encoding='utf-8') as file:
            
            file.write('|'.join(columns) + '\n')
            
            
            rows_written = 0
            for transaction in enriched_transactions:
                row_values = []
                for column in columns:
                    value = transaction.get(column)
                    
                   
                    if value is None:
                        value = ''
                    elif isinstance(value, bool):
                        value = str(value)
                    elif isinstance(value, (int, float)):
                        value = str(value)
                    else:
                        value = str(value)
                    
                   
                    value = value.replace('|', '\\|')
                    row_values.append(value)
                
                file.write('|'.join(row_values) + '\n')
                rows_written += 1
        
        print(f"✓ Successfully saved {rows_written} enriched transactions")
        print(f"  File: {filename}")
        
       
        import os
        if os.path.exists(filename):
            file_size = os.path.getsize(filename)
            print(f"  Size: {file_size} bytes")
            
           
            print(f"\n  First 2 lines of saved file:")
            with open(filename, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < 2:
                        print(f"  Line {i+1}: {line.strip()}")
                    else:
                        break
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to save enriched data: {e}")
        return False
    
    def get_product_by_id(self, product_id: int) -> Optional[Dict]:
        """
        Get a SINGLE product by ID.
        
        Args:
            product_id (int): Product ID
            
        Returns:
            Optional[Dict]: Single product object or None if failed
        """
        url = f"{self.BASE_URL}/{product_id}"
        print(f"Fetching product with ID: {product_id}...")
        
        product = self.make_request(url)
        if product and 'id' in product:
            print(f"Successfully fetched product: {product.get('title', 'Unknown')}")
            return product
        
        return None
    
    def get_products_by_limit(self, limit: int = 100) -> Optional[List[Dict]]:
        """
        Get specific number of products.
        
        Args:
            limit (int): Number of products to retrieve
            
        Returns:
            Optional[List[Dict]]: List of product dictionaries or None if failed
        """
        url = f"{self.BASE_URL}?limit={limit}"
        print(f"Fetching {limit} products...")
        
        data = self.make_request(url)
        if data and 'products' in data:
            print(f"Successfully fetched {len(data['products'])} products")
            return data['products']
        
        return None
    
    def search_products(self, query: str) -> Optional[List[Dict]]:
        """
        Search products by query.
        
        Args:
            query (str): Search query
            
        Returns:
            Optional[List[Dict]]: List of matching product dictionaries or None if failed
        """
        url = f"{self.BASE_URL}/search?q={query}"
        print(f"Searching products with query: '{query}'...")
        
        data = self.make_request(url)
        if data and 'products' in data:
            print(f"Found {len(data['products'])} products matching '{query}'")
            return data['products']
        
        return None
    
    def fetch_products_with_details(self) -> Dict[str, Dict]:
        """
        Fetch all available products and create a mapping by title/category.
        
        Returns:
            Dict[str, Dict]: Dictionary mapping product names to API product data
            
        Structure:
        {
            'product_name': {
                'category': 'electronics',
                'price': 999.99,
                'rating': 4.5,
                'stock': 100,
                'brand': 'Apple'
            },
            ...
        }
        """
        print("\nFetching product details from API...")
        
        
        products = self.get_products_by_limit(100)
        
        if not products:
            print("Failed to fetch products from API")
            return {}
        
        
        product_mapping = {}
        
        for product in products:
            title = product.get('title', '').lower().strip()
            category = product.get('category', '').lower().strip()
            
            if title:
                product_mapping[title] = {
                    'category': category,
                    'price': product.get('price', 0),
                    'rating': product.get('rating', 0),
                    'stock': product.get('stock', 0),
                    'brand': product.get('brand', 'Unknown'),
                    'description': product.get('description', 'No description')
                }
            
           
            if category and category not in product_mapping:
                product_mapping[category] = {
                    'category': category,
                    'price': product.get('price', 0),
                    'rating': product.get('rating', 0),
                    'stock': product.get('stock', 0),
                    'brand': product.get('brand', 'Unknown'),
                    'description': f"Sample {category} product"
                }
        
        print(f"Created mapping for {len(product_mapping)} products/categories")
        return product_mapping


def enrich_sales_data(transactions: List[Dict], api_handler: APIHandler) -> List[Dict]:
    """
    Enrich sales transactions with API product data.
    
    Args:
        transactions (List[Dict]): List of sales transaction dictionaries
        api_handler (APIHandler): API handler instance
        
    Returns:
        List[Dict]: Enriched transactions with API data
    """
    print("\nEnriching sales data with API information...")
    
    
    product_mapping = api_handler.fetch_products_with_details()
    
    if not product_mapping:
        print("No API data available. Returning original transactions.")
        return transactions
    
    enriched_transactions = []
    matched_count = 0
    
    for transaction in transactions:
        
        enriched_transaction = transaction.copy()
        
        product_name = transaction.get('ProductName', '').lower().strip()
        
        
        api_data = None
        
        
        if product_name in product_mapping:
            api_data = product_mapping[product_name]
            matched_count += 1
        
       
        if not api_data:
            for api_product_name, api_product_data in product_mapping.items():
                if api_product_name in product_name or product_name in api_product_name:
                    api_data = api_product_data
                    matched_count += 1
                    break
        
        
        if api_data:
            enriched_transaction['Category'] = api_data.get('category', 'Unknown')
            enriched_transaction['ProductRating'] = api_data.get('rating', 0)
            enriched_transaction['ProductStock'] = api_data.get('stock', 0)
            enriched_transaction['ProductBrand'] = api_data.get('brand', 'Unknown')
        else:
            
            enriched_transaction['Category'] = 'Unknown'
            enriched_transaction['ProductRating'] = 0
            enriched_transaction['ProductStock'] = 0
            enriched_transaction['ProductBrand'] = 'Unknown'
        
        enriched_transactions.append(enriched_transaction)
    
    print(f"Matched {matched_count} of {len(transactions)} transactions with API data")
    return enriched_transactions


def enrich_sales_data(transactions: List[Dict], product_mapping: Dict[int, Dict]) -> List[Dict]:
    """
    Enriches transaction data with API product information
    
    Parameters:
    - transactions: list of transaction dictionaries
    - product_mapping: dictionary from create_product_mapping()
    
    Returns: list of enriched transaction dictionaries
    
    Expected Output Format (each transaction):
    {
        'TransactionID': 'T001',
        'Date': '2024-12-01',
        'ProductID': 'P101',
        'ProductName': 'Laptop',
        'Quantity': 2,
        'UnitPrice': 45000.0,
        'CustomerID': 'C001',
        'Region': 'North',
        # NEW FIELDS ADDED FROM API:
        'API_Category': 'laptops',
        'API_Brand': 'Apple',
        'API_Rating': 4.7,
        'API_Match': True  # True if enrichment successful, False otherwise
    }
    
    Enrichment Logic:
    - Extract numeric ID from ProductID (P101 → 101, P5 → 5)
    - If ID exists in product_mapping, add API fields
    - If ID doesn't exist, set API_Match to False and other fields to None
    - Handle all errors gracefully
    """
    print("\nEnriching sales data with API product information...")
    
    enriched_transactions = []
    match_count = 0
    api_handler = APIHandler()  # Create instance for helper method
    
    for transaction in transactions:
       
        enriched_transaction = transaction.copy()
        
        
        product_id_str = transaction.get('ProductID', '')
        numeric_id = api_handler.extract_product_id(product_id_str)
        
        if numeric_id and numeric_id in product_mapping:
            
            api_product_info = product_mapping[numeric_id]
            
            enriched_transaction['API_Category'] = api_product_info.get('category')
            enriched_transaction['API_Brand'] = api_product_info.get('brand')
            enriched_transaction['API_Rating'] = api_product_info.get('rating')
            enriched_transaction['API_Match'] = True
            
            match_count += 1
        else:
            
            enriched_transaction['API_Category'] = None
            enriched_transaction['API_Brand'] = None
            enriched_transaction['API_Rating'] = None
            enriched_transaction['API_Match'] = False
        
        enriched_transactions.append(enriched_transaction)
    
    total_transactions = len(transactions)
    match_percentage = (match_count / total_transactions * 100) if total_transactions > 0 else 0
    
    print(f"Enrichment complete:")
    print(f"  Total transactions: {total_transactions}")
    print(f"  Matched with API: {match_count} ({match_percentage:.1f}%)")
    print(f"  Not matched: {total_transactions - match_count}")
    
    return enriched_transactions



def fetch_all_products(api_handler: APIHandler = None) -> List[Dict]:
    """Module-level wrapper for fetch_all_products."""
    if api_handler is None:
        api_handler = APIHandler()
    return api_handler.fetch_all_products()

def create_product_mapping(api_handler: APIHandler, api_products: List[Dict]) -> Dict[int, Dict]:
    """Module-level wrapper for create_product_mapping."""
    return api_handler.create_product_mapping(api_products)