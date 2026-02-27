import json
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_holding(holding, date):
	"""Process a single holding entry from the JSON data"""
	try:
		# Skip if not equity
		if holding[3] != 'Equity':
			return None
			
		return {
			'date': date,
			'ticker': holding[0],
			'name': holding[1],
			'sector': holding[2],
			'weight': holding[5].get('raw', 0) if isinstance(holding[5], dict) else 0
		}
	except (IndexError, AttributeError) as e:
		logger.warning(f"Error processing holding: {e}")
		return None

def process_json_file(file_path):
	"""Process a single JSON file and return list of holdings"""
	try:
		with open(file_path, 'r', encoding='utf-8') as f:
			data = json.load(f)
		
		if not data.get('aaData'):
			return []

		date = file_path.stem  # Get date from filename (YYYYMMDD)
		formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"  # Convert to YYYY-MM-DD
		
		holdings = []
		for holding in data['aaData']:
			processed = process_holding(holding, formatted_date)
			if processed:
				holdings.append(processed)
		
		return holdings

	except Exception as e:
		logger.error(f"Error processing {file_path}: {str(e)}")
		return []

def save_single_file(df, output_dir):
	"""Save all data as a single CSV file"""
	filepath = output_dir / "historical.csv"
	df.to_csv(filepath, index=False)
	logger.info(f"Saved combined CSV to {filepath}")

def save_daily_files(df, output_dir):
	"""Save separate CSV files for each date"""
	daily_dir = output_dir / "daily"
	daily_dir.mkdir(exist_ok=True)
	
	for date in df['date'].unique():
		date_df = df[df['date'] == date].copy()
		# Remove date column since it's in filename
		date_df = date_df.drop('date', axis=1)
		# Sort by weight descending
		date_df = date_df.sort_values('weight', ascending=False)
		# Add rank column
		date_df['rank'] = range(1, len(date_df) + 1)
		
		# Reorder columns
		columns = ['rank', 'ticker', 'name', 'sector', 'weight']
		date_df = date_df[columns]
		
		# Convert date to filename format
		filename = f"{date.replace('-', '')}.csv"
		date_df.to_csv(daily_dir / filename, index=False)
	
	logger.info(f"Saved {len(df['date'].unique())} daily files to {daily_dir}")

def main():
	# Input and output paths
	INDEX_NAME = "msci_us_sri"
	input_dir = Path(f"constituents/{INDEX_NAME}/")  # Update this to your input directory
	output_dir = Path(f"processed_data/{INDEX_NAME}/")
	output_dir.mkdir(exist_ok=True, parents=True)

	# Process all JSON files
	all_holdings = []
	json_files = sorted(input_dir.glob("*.json"))
	total_files = len(json_files)
	
	logger.info(f"Processing {total_files} files...")
	
	for i, file_path in enumerate(json_files, 1):
		holdings = process_json_file(file_path)
		all_holdings.extend(holdings)
		if i % 100 == 0:
			logger.info(f"Processed {i}/{total_files} files")

	# Convert to DataFrame
	df = pd.DataFrame(all_holdings)
	
	# Sort by date and weight (descending)
	df = df.sort_values(['date', 'weight'], ascending=[True, False])
	
	# Add rank column for each date
	df['rank'] = df.groupby('date')['weight'].rank(method='min', ascending=False)
	
	# Reorder columns
	columns = ['date', 'rank', 'ticker', 'name', 'sector', 'weight']
	df = df[columns]
	
	# Save both formats
	save_single_file(df, output_dir)
	save_daily_files(df, output_dir)
	
	logger.info("\nDataset Summary:")
	logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
	logger.info(f"Total entries: {len(df)}")
	logger.info(f"Unique companies: {df['ticker'].nunique()}")
	logger.info(f"Unique dates: {df['date'].nunique()}")

if __name__ == "__main__":
	main()