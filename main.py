"""
Main Application for Sales Analytics System
"""

import sys
import os
from typing import List, Dict

# Import all modules
from utils.file_handler import (
    read_sales_data, 
    parse_transactions, 
    validate_and_filter
)
from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products
)
from utils.api_handler import (
    APIHandler,
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
    save_enriched_data
)
from utils.report_generator import generate_sales_report


def print_step(step_num: int, total_steps: int, message: str, success: bool = True):
    """Print a step in the workflow with status."""
    print(f"[{step_num}/{total_steps}] {message}")
    return step_num + 1

def main():
    """
    Main execution function
    
    Workflow:
    1. Print welcome message
    2. Read sales data file (handle encoding)
    3. Parse and clean transactions
    4. Display filter options to user
       - Show available regions
       - Show transaction amount range
       - Ask if user wants to filter (y/n)
    5. If yes, ask for filter criteria and apply
    6. Validate transactions
    7. Display validation summary
    8. Perform all data analyses (call all functions from Part 2)
    9. Fetch products from API
    10. Enrich sales data with API info
    11. Save enriched data to file
    12. Generate comprehensive report
    13. Print success message with file locations
    
    Error Handling:
    - Wrap entire process in try-except
    - Display user-friendly error messages
    - Don't let program crash on errors
    """
    
    total_steps = 10
    current_step = 1
    
    try:
        # ========================================
        # WELCOME MESSAGE
        # ========================================
        print("\n" + "=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40 + "\n")
        
        # ========================================
        # STEP 1: Read sales data file
        # ========================================
        current_step = print_step(1, total_steps, "Reading sales data...")
        
        if not os.path.exists('data/sales_data.txt'):
            print(f"✗ Error: File 'data/sales_data.txt' not found!")
            print("  Please ensure the data file exists in the data/ folder.")
            return
        
        raw_lines = read_sales_data('data/sales_data.txt')
        print(f"Successfully read {len(raw_lines)} transactions")
        
        # ========================================
        # STEP 2: Parse and clean transactions
        # ========================================
        current_step = print_step(2, total_steps, "Parsing and cleaning data...")
        
        parsed_transactions = parse_transactions(raw_lines)
        print(f"Parsed {len(parsed_transactions)} records")
        
        # ========================================
        # STEP 3: Display filter options
        # ========================================
        current_step = print_step(3, total_steps, "Filter Options Available:")
        
        # Get available regions and amount range for display
        regions = set()
        amounts = []
        
        for transaction in parsed_transactions:
            region = transaction.get('Region', '').strip()
            if region:
                regions.add(region)
            
            try:
                qty = transaction.get('Quantity', 0)
                price = transaction.get('UnitPrice', 0.0)
                amount = qty * price
                amounts.append(amount)
            except:
                continue
        
        print(f"  Regions: {', '.join(sorted(regions))}")
        if amounts:
            print(f"  Amount Range: ₹{min(amounts):,.0f} - ₹{max(amounts):,.0f}")
        else:
            print(f"  Amount Range: N/A")
        
        # Ask if user wants to filter
        filter_choice = input("\n  Do you want to filter data? (y/n): ").strip().lower()
        
        # ========================================
        # STEP 4: Apply filters if requested
        # ========================================
        region_filter = None
        min_amount = None
        max_amount = None
        
        if filter_choice == 'y':
            current_step = print_step(4, total_steps, "Applying filters...")
            
            # Region filter
            print(f"\n  Available regions: {', '.join(sorted(regions))}")
            region_input = input("  Enter region to filter (or press Enter to skip): ").strip()
            if region_input and region_input in regions:
                region_filter = region_input
                print(f"    ✓ Will filter by region: {region_filter}")
            elif region_input:
                print(f"    ⚠ Region '{region_input}' not found. Skipping region filter.")
            
            # Amount filter
            amount_filter = input("\n  Filter by amount range? (y/n): ").strip().lower()
            if amount_filter == 'y':
                min_input = input("  Enter minimum amount (or press Enter to skip): ").strip()
                max_input = input("  Enter maximum amount (or press Enter to skip): ").strip()
                
                try:
                    if min_input:
                        min_amount = float(min_input)
                        print(f"    ✓ Minimum amount: ₹{min_amount:,.0f}")
                except ValueError:
                    print(f"    ⚠ Invalid minimum amount. Skipping.")
                
                try:
                    if max_input:
                        max_amount = float(max_input)
                        print(f"    ✓ Maximum amount: ₹{max_amount:,.0f}")
                except ValueError:
                    print(f"    ⚠ Invalid maximum amount. Skipping.")
        else:
            current_step = print_step(4, total_steps, "Skipping filters...")
            print("  No filters applied")
        
        # ========================================
        # STEP 5: Validate transactions
        # ========================================
        current_step = print_step(5, total_steps, "Validating transactions...")
        
        valid_transactions, invalid_count, filter_summary = validate_and_filter(
            parsed_transactions,
            region=region_filter,
            min_amount=min_amount,
            max_amount=max_amount
        )
        
        print(f"  Valid: {len(valid_transactions)} | Invalid: {invalid_count}")
        
        if not valid_transactions:
            print(f"\n✗ Error: No valid transactions after filtering.")
            print("  Please adjust your filters and try again.")
            return
        
        # ========================================
        # STEP 6: Perform all data analyses
        # ========================================
        current_step = print_step(6, total_steps, "Analyzing sales data...")
        
        # Perform all analyses from Part 2
        try:
            total_revenue = calculate_total_revenue(valid_transactions)
            region_sales = region_wise_sales(valid_transactions)
            top_products = top_selling_products(valid_transactions, n=5)
            customer_stats = customer_analysis(valid_transactions)
            daily_trend = daily_sales_trend(valid_transactions)
            peak_day = find_peak_sales_day(valid_transactions)
            low_products = low_performing_products(valid_transactions, threshold=10)
            
            print(f"  ✓ Analysis complete")
            print(f"    - Total Revenue: ₹{total_revenue:,.2f}")
            print(f"    - Top Region: {list(region_sales.keys())[0]} ({region_sales[list(region_sales.keys())[0]]['percentage']}%)")
            print(f"    - Top Product: {top_products[0][0]} ({top_products[0][1]} units)")
            
        except Exception as e:
            print(f"  ⚠ Some analyses failed: {e}")
            print("  Continuing with available data...")
        
        # ========================================
        # STEP 7: Fetch products from API
        # ========================================
        current_step = print_step(7, total_steps, "Fetching product data from API...")
        
        api_handler = APIHandler()
        api_products = []
        
        try:
            api_products = fetch_all_products(api_handler)
            if api_products:
                print(f"  ✓ Fetched {len(api_products)} products")
            else:
                print(f"  ⚠ Failed to fetch products from API")
        except Exception as e:
            print(f"  ⚠ API fetch failed: {e}")
            print("  Continuing without API data...")
        
        # ========================================
        # STEP 8: Enrich sales data with API info
        # ========================================
        current_step = print_step(8, total_steps, "Enriching sales data...")
        
        enriched_transactions = []
        
        if api_products:
            try:
                product_mapping = create_product_mapping(api_handler, api_products)
                
                # Enrich all valid transactions
                enriched_transactions = enrich_sales_data(valid_transactions, product_mapping)
                
                # Count successful matches
                matched_count = sum(1 for t in enriched_transactions if t.get('API_Match') == True)
                total_count = len(enriched_transactions)
                success_rate = (matched_count / total_count * 100) if total_count > 0 else 0
                
                print(f"  ✓ Enriched {matched_count}/{total_count} transactions ({success_rate:.1f}%)")
                
            except Exception as e:
                print(f"  ⚠ Enrichment failed: {e}")
                print("  Continuing without enriched data...")
                enriched_transactions = []
        else:
            print("  ⚠ No API data available. Skipping enrichment.")
            enriched_transactions = []
        
        # ========================================
        # STEP 9: Save enriched data to file
        # ========================================
        current_step = print_step(9, total_steps, "Saving enriched data...")
        
        if enriched_transactions:
            try:
                output_file = 'data/enriched_sales_data.txt'
                if save_enriched_data(enriched_transactions, output_file):
                    print(f"  ✓ Saved to: {output_file}")
                else:
                    print(f"  ⚠ Failed to save enriched data")
            except Exception as e:
                print(f"  ⚠ Save failed: {e}")
        else:
            print("  ⚠ No enriched data to save")
        
        # ========================================
        # STEP 10: Generate comprehensive report
        # ========================================
        current_step = print_step(10, total_steps, "Generating report...")
        
        try:
            report_file = 'output/sales_report.txt'
            if generate_sales_report(valid_transactions, enriched_transactions, report_file):
                print(f"  ✓ Report saved to: {report_file}")
                
                # Show report statistics
                if os.path.exists(report_file):
                    file_size = os.path.getsize(report_file)
                    print(f"    File size: {file_size} bytes")
            else:
                print(f"  ⚠ Failed to generate report")
        except Exception as e:
            print(f"  ⚠ Report generation failed: {e}")
        
        # ========================================
        # SUCCESS MESSAGE
        # ========================================
        print("\n" + "=" * 40)
        print("PROCESS COMPLETE!")
        print("=" * 40)
        
        print("\nGenerated Files:")
        print("-" * 40)
        
        files_to_check = [
            ('data/enriched_sales_data.txt', 'Enriched Data'),
            ('output/sales_report.txt', 'Sales Report'),
            ('output/enriched_sales_data.txt', 'Output Enriched Data')
        ]
        
        for filepath, description in files_to_check:
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                print(f"✓ {description}: {filepath} ({file_size} bytes)")
            else:
                print(f"✗ {description}: Not generated")
        
        print("\n" + "=" * 40)
        print("Thank you for using Sales Analytics System!")
        print("=" * 40 + "\n")
        
    except KeyboardInterrupt:
        print("\n\n✗ Process interrupted by user.")
        print("Exiting...\n")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n✗ An unexpected error occurred: {e}")
        print("Please check your data and try again.\n")
        sys.exit(1)


if __name__ == "__main__":
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    # Run the main application
    main()