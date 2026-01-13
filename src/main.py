import pandas as pd
import numpy as np
import os
import re
import glob
from datetime import datetime, timedelta

# Configuration
DATA_DIR = 'data'
OUTPUT_DIR = 'output'
OUTPUT_FILE = 'Delivery_Analysis_Report.xlsx'

# Product Normalization Map
PRODUCT_MAP = {
    'LD': 'LD', 
    'LD-DYED': 'LD-Dyed', 
    'LD - DYED': 'LD-Dyed',
    'UR': 'UR', 
    'LP': 'LP',
    'UNLEADED': 'UR',
    'RED DIESEL': 'LD-Dyed',
    'CLEAR DIESEL': 'LD'
}

def parse_date_from_filename(filename):
    """
    Extracts date from filenames like 'NOV25.xlsx - NOV 29.csv'
    """
    base = os.path.basename(filename).upper()
    # Remove extensions for cleaner parsing
    base = base.replace('.CSV', '').replace('.XLSX', '')
    
    # Month mapping
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
        'SEPT': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    try:
        # Split by hyphen to find the specific day part (usually at the end)
        parts = base.split('-')
        day_part = parts[-1].strip()
        
        # Regex to find Month Word and Day Number (e.g., "NOV 29")
        match = re.search(r'([A-Z]+)\s*(\d{1,2})', day_part)
        if match:
            m_str, d_str = match.groups()
            # Fuzzy match month key
            for k, v in month_map.items():
                if m_str.startswith(k):
                    # Assuming year 2025 based on file naming convention
                    return datetime(2025, v, int(d_str))
    except Exception as e:
        print(f"Warning: Could not parse date from {filename}")
        return None
    return None

def process_files():
    print("Scanning for files...")
    all_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not all_files:
        print("No CSV files found in 'data/' directory.")
        return pd.DataFrame()

    all_data = []

    for f in all_files:
        file_date = parse_date_from_filename(f)
        if not file_date:
            continue
            
        try:
            # Read CSV with no header initially to locate the data block
            df_raw = pd.read_csv(f, header=None)
            
            # Locate the header row containing 'Customer Name'
            header_idx = -1
            for i, row in df_raw.iterrows():
                row_str = ' '.join(row.astype(str)).lower()
                if 'customer name' in row_str and 'product' in row_str:
                    header_idx = i
                    break
            
            if header_idx == -1:
                continue

            # Reload using the found header
            df = pd.read_csv(f, header=header_idx)
            
            # Normalize column names
            df.columns = [str(c).strip().title() for c in df.columns]
            
            # Identify critical columns by keyword
            cust_col = next((c for c in df.columns if 'Customer' in c), None)
            prod_col = next((c for c in df.columns if 'Product' in c), None)
            # Look for Gallons/Qty column
            gal_cols = [c for c in df.columns if 'Gallons' in c or 'Qty' in c]
            gal_col = gal_cols[0] if gal_cols else None

            if not (cust_col and prod_col and gal_col):
                continue

            for _, row in df.iterrows():
                cust = str(row[cust_col]).strip().upper()
                
                # Skip invalid rows or totals
                if cust in ['NAN', '', 'TOTAL', 'GRAND TOTAL'] or 'SUM OF' in cust:
                    continue
                    
                prod = str(row[prod_col]).strip().upper()
                
                # Parse Gallons (handle commas)
                try:
                    gal_val = str(row[gal_col]).replace(',', '')
                    gallons = float(gal_val)
                except ValueError:
                    continue

                if gallons > 0:
                    # Clean Product Name
                    clean_prod = prod
                    for key, val in PRODUCT_MAP.items():
                        if key in prod:
                            clean_prod = val
                            break
                    
                    all_data.append({
                        'Date': file_date,
                        'Customer': cust,
                        'Product': clean_prod,
                        'Gallons': gallons,
                        'Source_File': os.path.basename(f)
                    })

        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    return pd.DataFrame(all_data)

def analyze_patterns(df):
    if df.empty:
        return pd.DataFrame()

    summary_list = []
    
    # Group by Customer and Product
    grouped = df.groupby(['Customer', 'Product'])
    
    for (cust, prod), group in grouped:
        dates = sorted(group['Date'].unique())
        count = len(dates)
        total_vol = group['Gallons'].sum()
        last_delivery = dates[-1]
        
        freq_label = "Irregular/One-off"
        pattern_day = "N/A"
        next_date = "N/A"
        avg_days = 0

        if count > 1:
            # Calculate intervals between deliveries
            intervals = np.diff(dates).astype('timedelta64[D]').astype(int)
            avg_days = np.mean(intervals)
            
            # Determine Frequency Category
            if 5 <= avg_days <= 9:
                freq_label = "Weekly"
            elif 12 <= avg_days <= 16:
                freq_label = "Bi-Weekly"
            elif 25 <= avg_days <= 35:
                freq_label = "Monthly"
            else:
                freq_label = f"Custom ({int(avg_days)} days)"

            # Forecast Next Date
            next_delivery = last_delivery + timedelta(days=int(avg_days))
            next_date = next_delivery.strftime('%Y-%m-%d')

            # Identify Day of Week Pattern (if frequent)
            if count >= 3:
                days_of_week = [d.strftime('%A') for d in pd.to_datetime(dates)]
                pattern_day = max(set(days_of_week), key=days_of_week.count)

        summary_list.append({
            'Customer': cust,
            'Product': prod,
            'Frequency': freq_label,
            'Avg Interval (Days)': round(avg_days, 1),
            'Pattern Day': pattern_day,
            'Last Delivery': last_delivery.strftime('%Y-%m-%d'),
            'Forecasted Date': next_date,
            'Total Deliveries': count,
            'Total Gallons': total_vol
        })

    return pd.DataFrame(summary_list)

if __name__ == "__main__":
    # 1. Extraction
    df_raw = process_files()
    
    if not df_raw.empty:
        print(f"Successfully extracted {len(df_raw)} records.")
        
        # 2. Analysis
        df_summary = analyze_patterns(df_raw)
        
        # 3. Export
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        with pd.ExcelWriter(output_path) as writer:
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            df_raw.to_excel(writer, sheet_name='Raw Data', index=False)
            
        print(f"Analysis complete. Report saved to: {output_path}")
    else:
        print("No valid data found. Please ensure CSV files are in the 'data/' folder.")