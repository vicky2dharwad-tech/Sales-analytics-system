"""
Data Processor Module
"""

from typing import List, Dict, Tuple, Any
from collections import defaultdict


def calculate_total_revenue(transactions: List[Dict]) -> float:
    """
    Calculates total revenue from all transactions
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        
    Returns:
        float: Total revenue (sum of Quantity * UnitPrice)
        
    Expected Output: Single number representing sum of (Quantity * UnitPrice)
    Example: 1545000.50
    """
    total_revenue = 0.0
    
    for transaction in transactions:
        try:
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            total_revenue += quantity * unit_price
        except (TypeError, ValueError):
            # Skip transactions with invalid data
            continue
    
    return total_revenue


def region_wise_sales(transactions: List[Dict]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes sales by region
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary with region statistics
        
    Expected Output Format:
    {
        'North': {
            'total_sales': 450000.0,
            'transaction_count': 15,
            'percentage': 29.13
        },
        'South': {...},
        ...
    }
    
    Requirements:
    - Calculate total sales per region
    - Count transactions per region
    - Calculate percentage of total sales
    - Sort by total_sales in descending order
    """
    
    region_data = defaultdict(lambda: {
        'total_sales': 0.0,
        'transaction_count': 0
    })
    
    
    total_revenue = calculate_total_revenue(transactions)
    
    
    for transaction in transactions:
        try:
            region = transaction.get('Region', '').strip()
            if not region:
                continue
                
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            sales_amount = quantity * unit_price
            
           
            region_data[region]['total_sales'] += sales_amount
            region_data[region]['transaction_count'] += 1
            
        except (TypeError, ValueError):
            continue
    
    
    result = {}
    for region, data in region_data.items():
        percentage = (data['total_sales'] / total_revenue * 100) if total_revenue > 0 else 0
        
        result[region] = {
            'total_sales': round(data['total_sales'], 2),
            'transaction_count': data['transaction_count'],
            'percentage': round(percentage, 2)
        }
    
    
    result = dict(sorted(
        result.items(), 
        key=lambda x: x[1]['total_sales'], 
        reverse=True
    ))
    
    return result


def top_selling_products(transactions: List[Dict], n: int = 5) -> List[Tuple[str, int, float]]:
    """
    Finds top n products by total quantity sold
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        n (int): Number of top products to return (default: 5)
        
    Returns:
        List[Tuple[str, int, float]]: List of tuples (ProductName, TotalQuantity, TotalRevenue)
        
    Expected Output Format:
    [
        ('Laptop', 45, 2250000.0),  # (ProductName, TotalQuantity, TotalRevenue)
        ('Mouse', 38, 19000.0),
        ...
    ]
    
    Requirements:
    - Aggregate by ProductName
    - Calculate total quantity sold
    - Calculate total revenue for each product
    - Sort by TotalQuantity descending
    - Return top n products
    """
    
    product_data = defaultdict(lambda: {
        'total_quantity': 0,
        'total_revenue': 0.0
    })
    
    
    for transaction in transactions:
        try:
            product_name = transaction.get('ProductName', '').strip()
            if not product_name:
                continue
                
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            
            # Update product data
            product_data[product_name]['total_quantity'] += quantity
            product_data[product_name]['total_revenue'] += quantity * unit_price
            
        except (TypeError, ValueError):
            continue
    
    
    product_list = []
    for product_name, data in product_data.items():
        product_list.append((
            product_name,
            data['total_quantity'],
            round(data['total_revenue'], 2)
        ))
    
    product_list.sort(key=lambda x: x[1], reverse=True)
    
    
    return product_list[:n]


def customer_analysis(transactions: List[Dict]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes customer purchase patterns
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary of customer statistics
        
    Expected Output Format:
    {
        'C001': {
            'total_spent': 95000.0,
            'purchase_count': 3,
            'avg_order_value': 31666.67,
            'products_bought': ['Laptop', 'Mouse', 'Keyboard']
        },
        'C002': {...},
        ...
    }
    
    Requirements:
    - Calculate total amount spent per customer
    - Count number of purchases
    - Calculate average order value
    - List unique products bought
    - Sort by total_spent descending
    """
    
    customer_data = defaultdict(lambda: {
        'total_spent': 0.0,
        'purchase_count': 0,
        'products_bought': set()
    })
    
    
    for transaction in transactions:
        try:
            customer_id = transaction.get('CustomerID', '').strip()
            if not customer_id:
                continue
                
            product_name = transaction.get('ProductName', '').strip()
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            transaction_amount = quantity * unit_price
            
            
            customer_data[customer_id]['total_spent'] += transaction_amount
            customer_data[customer_id]['purchase_count'] += 1
            if product_name:
                customer_data[customer_id]['products_bought'].add(product_name)
                
        except (TypeError, ValueError):
            continue
    
    
    result = {}
    for customer_id, data in customer_data.items():
        avg_order_value = (
            data['total_spent'] / data['purchase_count'] 
            if data['purchase_count'] > 0 else 0
        )
        
        result[customer_id] = {
            'total_spent': round(data['total_spent'], 2),
            'purchase_count': data['purchase_count'],
            'avg_order_value': round(avg_order_value, 2),
            'products_bought': sorted(list(data['products_bought']))
        }
    
    
    result = dict(sorted(
        result.items(), 
        key=lambda x: x[1]['total_spent'], 
        reverse=True
    ))
    
    return result



def daily_sales_trend(transactions: List[Dict]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes sales trends by date
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary sorted by date
        
    Expected Output Format:
    {
        '2024-12-01': {
            'revenue': 125000.0,
            'transaction_count': 8,
            'unique_customers': 6
        },
        '2024-12-02': {...},
        ...
    }
    
    Requirements:
    - Group by date
    - Calculate daily revenue
    - Count daily transactions
    - Count unique customers per day
    - Sort chronologically
    """
    
    daily_data = defaultdict(lambda: {
        'revenue': 0.0,
        'transaction_count': 0,
        'customers': set()
    })
    
   
    for transaction in transactions:
        try:
            date = transaction.get('Date', '').strip()
            customer_id = transaction.get('CustomerID', '').strip()
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            
            if not date:
                continue
                
            
            transaction_amount = quantity * unit_price
            
            
            daily_data[date]['revenue'] += transaction_amount
            daily_data[date]['transaction_count'] += 1
            if customer_id:
                daily_data[date]['customers'].add(customer_id)
                
        except (TypeError, ValueError):
            continue
    
    
    result = {}
    for date, data in daily_data.items():
        result[date] = {
            'revenue': round(data['revenue'], 2),
            'transaction_count': data['transaction_count'],
            'unique_customers': len(data['customers'])
        }
    
   
    result = dict(sorted(result.items()))
    
    return result


def find_peak_sales_day(transactions: List[Dict]) -> Tuple[str, float, int]:
    """
    Identifies the date with highest revenue
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        
    Returns:
        Tuple[str, float, int]: (date, revenue, transaction_count)
        
    Expected Output Format:
    ('2024-12-15', 185000.0, 12)
    """
    
    daily_trend = daily_sales_trend(transactions)
    
    if not daily_trend:
        return ("No data", 0.0, 0)
    
    
    peak_date = max(daily_trend.items(), key=lambda x: x[1]['revenue'])
    
    date = peak_date[0]
    revenue = peak_date[1]['revenue']
    transaction_count = peak_date[1]['transaction_count']
    
    return (date, revenue, transaction_count)




def low_performing_products(transactions: List[Dict], threshold: int = 10) -> List[Tuple[str, int, float]]:
    """
    Identifies products with low sales
    
    Args:
        transactions (List[Dict]): List of transaction dictionaries
        threshold (int): Maximum quantity threshold (default: 10)
        
    Returns:
        List[Tuple[str, int, float]]: List of tuples (ProductName, TotalQuantity, TotalRevenue)
        
    Expected Output Format:
    [
        ('Webcam', 4, 12000.0),  # (ProductName, TotalQuantity, TotalRevenue)
        ('Headphones', 7, 10500.0),
        ...
    ]
    
    Requirements:
    - Find products with total quantity < threshold
    - Include total quantity and revenue
    - Sort by TotalQuantity ascending
    """
    
    product_data = defaultdict(lambda: {
        'total_quantity': 0,
        'total_revenue': 0.0
    })
    
   
    for transaction in transactions:
        try:
            product_name = transaction.get('ProductName', '').strip()
            if not product_name:
                continue
                
            quantity = transaction.get('Quantity', 0)
            unit_price = transaction.get('UnitPrice', 0.0)
            
            # Update product data
            product_data[product_name]['total_quantity'] += quantity
            product_data[product_name]['total_revenue'] += quantity * unit_price
            
        except (TypeError, ValueError):
            continue
    
    
    low_performing = []
    for product_name, data in product_data.items():
        if data['total_quantity'] < threshold:
            low_performing.append((
                product_name,
                data['total_quantity'],
                round(data['total_revenue'], 2)
            ))
    
    
    low_performing.sort(key=lambda x: x[1])
    
    return low_performing 
