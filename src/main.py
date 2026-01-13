import pandas as pd
import numpy as np
import os
import re
import glob
from datetime import datetime, timedelta
import warnings

# Suppress openpyxl warnings for cleaner output
warnings.filterwarnings("ignore")

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

def find_date_in_sheet(df, sheet_name, filename):
    """
    Tries to find a date inside the sheet content. 
    If not found, tries to parse the Sheet Name (e.g., 'JAN 1').
    """
    # 1. Search in the first 10 rows of the sheet
    for i in range(min(15, len(df))):
        row_str = " ".join(df.iloc[i].astype(str).values)
        # Look for YYYY-MM-DD or MM-DD-YYYY pattern
        match = re.search(r'(\d{4}-\d{2}-\d{2})', row_str)
        if match:
            return pd.to_datetime(match.group(1))
        
        # Look for typical Excel date formats if converted to string
        match_alt = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', row_str)
        if match_alt:
            try:
                return pd.to_datetime(match_alt.group(1))
            except:
                pass

    # 2. Fallback: Parse the Sheet Name (e.g., "JAN 1", "NOV 29")
    try:
        # Clean sheet name
        clean_name = sheet_name.upper().replace('.', ' ').strip()
        # Extract Month and Day
        match = re.search(r'([A-Z]+)\s*(\d{1,2})', clean_name)
        if match:
            m_str, d_str = match.groups()
            month_map = {
                'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
                'SEPT': 9
            }
            for k, v in month_map.items():
                if m_str.startswith(k):
                    # Assume 2025 based on file naming
                    return datetime(2025, v, int(d_str))
    except:
        pass
        
    return None

def process_files():
    print("Scanning for Excel files in 'data/'...")
    all_files = glob.glob(os.path.join(DATA_DIR, "*.xlsx"))
    
    if not all_files:
        print("No .xlsx files found. Please make sure the Excel files are in the 'data' folder.")
        return pd.DataFrame()

    all_data = []

    for f in all_files:
        print(f"Processing file: {os.path.basename(f)}...")
        try:
            # Read the Excel file (all sheets)
            xls = pd.read_excel(f, sheet_name=None, header=None)
            
            for sheet_name, df_raw in xls.items():
                # Skip likely non-data sheets
                if 'sheet' in sheet_name.lower() and len(df_raw) < 5:
                    continue

                # Identify Date
                file_date = find_date_in_sheet(df_raw, sheet_name, f)
                if not file_date:
                    # Skip if no date found (likely a summary tab or empty)
                    continue

                # Locate the header row containing 'Customer Name'
                header_idx = -1
                for i, row in df_raw.iterrows():
                    row_str = ' '.join(row.astype(str)).lower()
                    if 'customer name' in row_str and 'product' in row_str:
                        header_idx = i
                        break
                
                if header_idx == -1:
                    continue

                # Extract data block
                df = df_raw.iloc[header_idx+1:].copy()
                # Set proper column names from the header row
                df.columns = df_raw.iloc[header_idx].astype(str).str.strip().str.title()
                
                # Identify critical columns
                cust_col = next((c for c in df.columns if 'Customer' in str(c)), None)
                prod_col = next((c for c in df.columns if 'Product' in str(c)), None)
                gal_cols = [c for c in df.columns if 'Gallons' in str(c) or 'Qty' in str(c)]
                gal_col = gal_cols[0] if gal_cols else None

                if not (cust_col and prod_col and gal_col):
                    continue

                for _, row in df.iterrows():
                    cust = str(row[cust_col]).strip().upper()
                    
                    # Stop if we hit a total line or empty block
                    if cust in ['NAN', '', 'TOTAL', 'GRAND TOTAL', 'NONE'] or 'SUM OF' in cust:
                        continue
                        
                    prod = str(row[prod_col]).strip().upper()
                    
                    # Parse Gallons
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
                            'Source_File': f"{os.path.basename(f)} - {sheet_name}"
                        })

        except Exception as e:
            print(f"Error reading {f}: {e}")
            continue

    return pd.DataFrame(all_data)

def analyze_patterns(df):
    if df.empty:
        return pd.DataFrame()

    print("Analyzing delivery patterns...")
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
            # Calculate intervals
            intervals = np.diff(dates).astype('timedelta64[D]').astype(int)
            avg_days = np.mean(intervals)
            
            # Determine Frequency
            if 5 <= avg_days <= 9:
                freq_label = "Weekly"
            elif 12 <= avg_days <= 16:
                freq_label = "Bi-Weekly"
            elif 25 <= avg_days <= 35:
                freq_label = "Monthly"
            else:
                freq_label = f"Custom ({int(avg_days)} days)"

            # Forecast Next Date
            next_delivery = pd.to_datetime(last_delivery) + timedelta(days=int(avg_days))
            next_date = next_delivery.strftime('%Y-%m-%d')

            # Identify Day of Week Pattern
            if count >= 3:
                days_of_week = [pd.to_datetime(d).strftime('%A') for d in dates]
                pattern_day = max(set(days_of_week), key=days_of_week.count)

        summary_list.append({
            'Customer': cust,
            'Product': prod,
            'Frequency': freq_label,
            'Avg Interval (Days)': round(avg_days, 1),
            'Pattern Day': pattern_day,
            'Last Delivery': pd.to_datetime(last_delivery).strftime('%Y-%m-%d'),
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
        print("No valid data found.")