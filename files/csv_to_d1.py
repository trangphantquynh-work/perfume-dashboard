#!/usr/bin/env python3
"""
Parfumelite CSV to D1 SQL Converter
Converts CSV files to SQL INSERT statements for D1 ingestion.

Usage:
    python3 csv_to_d1.py --auto Auto.csv --demographics Age_Gender.csv --regions Region.csv
"""

import csv
import argparse
import re
from datetime import datetime
from pathlib import Path


def parse_vietnamese_number(value):
    """Parse Vietnamese number format (e.g., '5.676,62' -> 5676.62)"""
    if not value or value == '':
        return 0
    if isinstance(value, (int, float)):
        return value
    
    # Standardize string
    val_str = str(value).strip()
    
    # Handle "5.676,62" format
    # 1. Remove dots (thousands separator)
    val_str = val_str.replace('.', '')
    # 2. Replace comma with dot (decimal separator)
    val_str = val_str.replace(',', '.')
    
    try:
        return float(val_str)
    except ValueError:
        return 0


def parse_date(date_str):
    """Parse date string to YYYYMMDD integer Key, YYYY-MM-DD string, and datetime object"""
    if not date_str:
        return None, None, None
    try:
        # Try different formats
        dt = None
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
            try:
                dt = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        
        if dt:
            return int(dt.strftime('%Y%m%d')), dt.strftime('%Y-%m-%d'), dt
        return None, None, None
    except:
        return None, None, None


def get_objective(campaign_name):
    """Extract objective from campaign name"""
    name = campaign_name.lower()
    if 'impression' in name:
        return 'Impression'
    if 'visit' in name:
        return 'Visit'
    if 'mess' in name:
        return 'Message'
    return 'Unknown'


def generate_dim_date_sql(date_key, full_date_str, dt):
    """Generate SQL to insert date dimension"""
    if not dt:
        return ""
    
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    month_names = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']
    
    year = dt.year
    month = dt.month
    day = dt.day
    day_of_week = dt.isoweekday() # 1=Mon, 7=Sun
    day_name = day_names[day_of_week-1]
    month_name = month_names[month-1]
    quarter = (month - 1) // 3 + 1
    week_of_year = dt.isocalendar()[1]
    is_weekend = 1 if day_of_week >= 6 else 0
    
    # We use INSERT OR IGNORE
    return f"INSERT OR IGNORE INTO dim_date (date_key, full_date, year, quarter, month, month_name, week_of_year, day_of_month, day_of_week, day_name, is_weekend) VALUES ({date_key}, '{full_date_str}', {year}, {quarter}, {month}, '{month_name}', {week_of_year}, {day}, {day_of_week}, '{day_name}', {is_weekend});"


def get_platform(campaign_name):
    """Extract platform from campaign name"""
    name = campaign_name.lower()
    if 'ig' in name or 'instagram' in name:
        return 'Instagram'
    return 'Facebook'

def process_auto_csv(filepath):
    """Process Auto (Performance) CSV to SQL"""
    sql_statements = []
    
    print(f"  Reading {filepath}...")
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_key, full_date, dt = parse_date(row.get('Date', ''))
            if not date_key:
                continue
            
            # Dimension: Date
            sql_statements.append(generate_dim_date_sql(date_key, full_date, dt))
            
            campaign = row.get('Campaign', '').strip()
            if not campaign:
                continue
                
            # Dimension: Campaign
            objective = get_objective(campaign)
            platform = get_platform(campaign)
            sql_statements.append(f"INSERT OR IGNORE INTO dim_campaign (campaign_name, objective, platform) VALUES ('{campaign}', '{objective}', '{platform}');")
            
            # Facts
            adset = row.get('AdSet', '').strip().replace("'", "''")
            ad = row.get('Ad', '').strip().replace("'", "''")
            indicator = row.get('Indicator', '').strip()
            action_key = row.get('ActionKey', '').strip()
            spent = parse_vietnamese_number(row.get('AmountSpent', 0))
            results = round(parse_vietnamese_number(row.get('Results', 0)))
            cpr = parse_vietnamese_number(row.get('CostPerResult', 0))
            impressions = round(parse_vietnamese_number(row.get('Impressions', 0)))
            
            # We need to look up campaign_id. For bulk insert SQL, we can use subquery.
            sql = f"""
INSERT INTO fact_ads_performance (date_key, campaign_id, adset_name, ad_name, indicator, action_key, amount_spent, results, cost_per_result, impressions)
SELECT {date_key}, campaign_id, '{adset}', '{ad}', '{indicator}', '{action_key}', {spent}, {results}, {cpr}, {impressions}
FROM dim_campaign WHERE campaign_name = '{campaign}';
"""
            sql_statements.append(sql.strip())
            
    return sql_statements


def process_demographics_csv(filepath):
    """Process Age_Gender CSV to SQL"""
    sql_statements = []
    
    print(f"  Reading {filepath}...")
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_key, full_date, dt = parse_date(row.get('Date', ''))
            if not date_key:
                continue
            
            # Dimension: Date
            sql_statements.append(generate_dim_date_sql(date_key, full_date, dt))
            
            campaign = row.get('Campaign', '').strip()
            if not campaign:
                continue
                
            objective = get_objective(campaign)
            platform = get_platform(campaign)
            sql_statements.append(f"INSERT OR IGNORE INTO dim_campaign (campaign_name, objective, platform) VALUES ('{campaign}', '{objective}', '{platform}');")
            
            age = row.get('Age', '').strip()
            gender = row.get('Gender', '').strip().lower()
            action_key = row.get('ActionKey', '').strip()
            spent = parse_vietnamese_number(row.get('Spend', 0))
            impressions = round(parse_vietnamese_number(row.get('Impressions', 0)))
            
            # Ensure Dimensions for Age/Gender (Pre-populated usually, but safe to ignore)
            sql_statements.append(f"INSERT OR IGNORE INTO dim_age_group (age_range) VALUES ('{age}');")
            sql_statements.append(f"INSERT OR IGNORE INTO dim_gender (gender) VALUES ('{gender}');")
            
            sql = f"""
INSERT INTO fact_ads_demographics (date_key, campaign_id, action_key, age_id, gender_id, spend, impressions)
SELECT {date_key}, c.campaign_id, '{action_key}', a.age_id, g.gender_id, {spent}, {impressions}
FROM dim_campaign c, dim_age_group a, dim_gender g
WHERE c.campaign_name = '{campaign}' AND a.age_range = '{age}' AND g.gender = '{gender}';
"""
            sql_statements.append(sql.strip())
            
    return sql_statements


def process_regions_csv(filepath):
    """Process Region CSV to SQL"""
    sql_statements = []
    
    print(f"  Reading {filepath}...")
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_key, full_date, dt = parse_date(row.get('Date', ''))
            if not date_key:
                continue
            
            # Dimension: Date
            sql_statements.append(generate_dim_date_sql(date_key, full_date, dt))
            
            campaign = row.get('Campaign', '').strip()
            if not campaign:
                continue
                
            objective = get_objective(campaign)
            sql_statements.append(f"INSERT OR IGNORE INTO dim_campaign (campaign_name, objective) VALUES ('{campaign}', '{objective}');")
            
            region_name = row.get('Region', '').strip().replace("'", "''")
            spent = parse_vietnamese_number(row.get('Spend', 0))
            impressions = round(parse_vietnamese_number(row.get('Impressions', 0)))
            
            # Ensure Region Dimension
            region_type = 'City' if 'City' in region_name else 'Province'
            sql_statements.append(f"INSERT OR IGNORE INTO dim_region (region_name, region_type) VALUES ('{region_name}', '{region_type}');")
            
            sql = f"""
INSERT INTO fact_ads_regions (date_key, campaign_id, region_id, spend, impressions)
SELECT {date_key}, c.campaign_id, r.region_id, {spent}, {impressions}
FROM dim_campaign c, dim_region r
WHERE c.campaign_name = '{campaign}' AND r.region_name = '{region_name}';
"""
            sql_statements.append(sql.strip())
            
    return sql_statements


def main():
    parser = argparse.ArgumentParser(description='Convert Parfumelite CSV to SQL')
    parser.add_argument('--auto', help='Path to Auto (Performance) CSV')
    parser.add_argument('--demographics', help='Path to Age_Gender CSV')
    parser.add_argument('--regions', help='Path to Region CSV')
    parser.add_argument('--output-dir', default='.', help='Output directory')
    
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / 'import_data.sql'
    
    all_sql = []
    all_sql.append("-- Auto-generated import script")
    # all_sql.append("BEGIN TRANSACTION;") # Removed to avoid D1 error
    all_sql.append("")
    
    # Always process if arg is provided
    if args.auto:
        print(f"📊 Processing Performance from {args.auto}...")
        all_sql.extend(process_auto_csv(args.auto))
        all_sql.append("")
    else:
        print("⚠️ No Performance CSV provided.")

    if args.demographics:
        print(f"👥 Processing Demographics from {args.demographics}...")
        all_sql.extend(process_demographics_csv(args.demographics))
        all_sql.append("")
    else:
        print("⚠️ No Demographics CSV provided.")
        
    if args.regions:
        print(f"🗺️ Processing Regions from {args.regions}...")
        all_sql.extend(process_regions_csv(args.regions))
        all_sql.append("")
    else:
        print("⚠️ No Regions CSV provided.")
    
    # all_sql.append("COMMIT;") # Removed to avoid D1 error
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(all_sql))
        
    print(f"\n✅ Generated SQL script at: {output_file}")

if __name__ == '__main__':
    main()
