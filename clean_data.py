import pandas as pd
import glob
import os

def find_csv_file():
    """Find the first CSV file in the current directory"""
    csv_files = glob.glob("*.csv")
    if not csv_files:
        raise FileNotFoundError("No CSV files found in the current directory")
    return csv_files[0]  # Return the first CSV file found

def detect_csv_separator(input_file):
    """Detect whether CSV uses comma or semicolon as separator"""
    with open(input_file, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        # Count occurrences of each separator
        comma_count = first_line.count(',')
        semicolon_count = first_line.count(';')
        
        # Return the separator that appears more frequently
        return ',' if comma_count > semicolon_count else ';'

def find_footer_start(df):
    """Find where the footer section starts"""
    for i, row in df.iterrows():
        first_col_str = str(row.iloc[0]).strip()
        
        # Check for footer indicators including the new "max on free plan" message
        if (first_col_str.startswith("Found profiles count:") or 
            first_col_str == "IG DM BOT:" or
            "profiles max on free plan" in first_col_str or
            "max on free plan" in first_col_str or
            first_col_str == "" or
            first_col_str == "nan"):
            
            # Double-check by looking at the pattern - if we find empty rows or footer text
            # Check next few rows to confirm this is the footer section
            footer_confirmed = False
            for j in range(i, min(i + 5, len(df))):
                if j < len(df):
                    check_str = str(df.iloc[j, 0]).strip()
                    if (check_str.startswith("Found profiles count:") or 
                        check_str == "IG DM BOT:" or
                        "profiles max on free plan" in check_str or
                        "max on free plan" in check_str or
                        check_str in ["", "nan"] or
                        "socialdeck.ai" in check_str.lower()):
                        footer_confirmed = True
                        break
            
            if footer_confirmed:
                return i
    
    return None

def clean_csv(input_file):
    """Clean the CSV file according to specifications"""
    # Detect the separator
    separator = detect_csv_separator(input_file)
    print(f"Detected separator: '{separator}'")
    
    # Read the CSV file with detected separator and proper quoting
    df = pd.read_csv(input_file, sep=separator, quotechar='"', skipinitialspace=True)
    
    print(f"Original DataFrame shape: {df.shape}")
    
    # Find where footer starts and remove everything from that point
    footer_start = find_footer_start(df)
    
    if footer_start is not None:
        print(f"Footer section detected starting at row {footer_start}")
        print(f"Sample of footer content: {str(df.iloc[footer_start, 0])}")
        df = df.iloc[:footer_start]  # Keep only rows before footer
        print(f"After footer removal: {df.shape}")
    else:
        print("No footer section detected")
    
    # Additional cleanup - remove any remaining problematic rows
    initial_rows = len(df)
    
    # Remove rows where first column contains footer-like content
    footer_patterns = [
        "Found profiles count:",
        "IG DM BOT:",
        "socialdeck.ai",
        "https://socialdeck.ai",
        "profiles max on free plan",
        "max on free plan"
    ]
    
    for pattern in footer_patterns:
        mask = ~df.iloc[:, 0].astype(str).str.contains(pattern, case=False, na=False)
        df = df[mask]
    
    # Remove completely empty rows (where all values are NaN or empty strings)
    df = df.dropna(how='all')
    
    # Remove rows where first column is empty or just whitespace
    df = df[df.iloc[:, 0].astype(str).str.strip() != '']
    df = df[df.iloc[:, 0].astype(str).str.strip() != 'nan']
    
    if len(df) != initial_rows:
        print(f"Removed {initial_rows - len(df)} additional problematic rows")
   
    # Remove specified columns
    columns_to_remove = ['profileUrl', 'avatarUrl', 'isVerified', 'followedByYou']
    existing_columns_to_remove = [col for col in columns_to_remove if col in df.columns]
    if existing_columns_to_remove:
        df = df.drop(columns=existing_columns_to_remove)
        print(f"Removed columns: {existing_columns_to_remove}")
   
    # Rename columns
    column_rename_map = {
        'userName': 'user_name',
        'fullName': 'full_name',
        'login': 'user_name',
        'name': 'full_name'
    }
    
    renamed_columns = []
    for old_name, new_name in column_rename_map.items():
        if old_name in df.columns:
            renamed_columns.append(f"{old_name} -> {new_name}")
    
    df = df.rename(columns=column_rename_map)
    
    if renamed_columns:
        print(f"Renamed columns: {', '.join(renamed_columns)}")
    
    # Ensure user_name and full_name columns exist, fill with empty spaces if missing
    if 'user_name' not in df.columns:
        df['user_name'] = ''
        print("Added missing 'user_name' column")
    if 'full_name' not in df.columns:
        df['full_name'] = ''
        print("Added missing 'full_name' column")
    
    # Replace any 0 values in name columns with empty spaces
    df['user_name'] = df['user_name'].astype(str).replace('0', '').replace('nan', '')
    df['full_name'] = df['full_name'].astype(str).replace('0', '').replace('nan', '')
   
    return df, separator

def main():
    try:
        # Find CSV file automatically
        input_file = find_csv_file()
        print(f"Found CSV file: {input_file}")
       
        # Clean the CSV
        cleaned_df, separator = clean_csv(input_file)
       
        # Create output filename
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_cleaned.csv"
       
        # Save cleaned CSV with the same separator as the original
        cleaned_df.to_csv(output_file, index=False, sep=separator)
       
        print(f"\n✅ Cleaned CSV saved as: {output_file}")
        
        # Read original for comparison
        original_df = pd.read_csv(input_file, sep=separator)
        print(f"📊 Original rows: {original_df.shape[0]}")
        print(f"📊 Cleaned rows: {cleaned_df.shape[0]}")
        print(f"📊 Rows removed: {original_df.shape[0] - cleaned_df.shape[0]}")
        print(f"📊 Remaining columns: {list(cleaned_df.columns)}")
       
        # Display first few rows of cleaned data
        if not cleaned_df.empty:
            print(f"\n📋 First {min(3, len(cleaned_df))} rows of cleaned data:")
            print(cleaned_df.head(3).to_string(index=False))
        else:
            print("\n⚠️ Warning: No data rows remain after cleaning!")
       
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()