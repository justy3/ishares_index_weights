import os
import qt
import json
import time
import requests

from qt import dt, np, pd, os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# helper functions
def format_date(date):
	"""Format datetime object to YYYYMMDD string"""
	return date.strftime("%Y%m%d")

index_to_url = {
	"spx500" : f"https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax",
	"nikkei400" : f"https://www.ishares.com/us/products/239831/ishares-japan-largecap-etf/1467271812596.ajax",
}

def get_constituents(date_str, index_name="spx500"):
	"""Get constituents for a specific date with error handling"""
	
	# only for indices in dictionary
	assert index_name in index_to_url, f"unsupported index : {index_name}"

	# parameter and headers
	params = {
		"tab": "all",
		"fileType": "json",
		"asOfDate": date_str,
		"_": "1730362062597"
	}
	headers = {
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
	}

	# get url
	url = index_to_url[index_name]
	
	try:
		qt.log.info(f"sending request for date {date_str} for index = {index_name}")
		response = requests.get(url, params=params, headers=headers, timeout=10)
		response.raise_for_status()
		
		# Handle UTF-8 BOM in response
		text = response.text.encode().decode('utf-8-sig')
		data = json.loads(text)
		
		# Check if data is empty
		if data.get("aaData") == []:
			qt.log.info(f"No data available for date: {date_str}")
			return None

		return data
		
	except requests.exceptions.RequestException as e:
		qt.log.error(f"Error fetching data for {date_str}: {str(e)}")
		return None

	except json.JSONDecodeError as e:
		qt.log.error(f"Error parsing JSON for {date_str}: {str(e)}")
		return None

	except Exception as e:
		qt.log.error(f"Unexpected error for {date_str}: {str(e)}")
		return None

def save_to_file(date_str, data, index_name):
	"""Save constituent data to a JSON file"""
	output_dir = Path(f"ivv_constituents/{index_name}/")
	output_dir.mkdir(exist_ok=True)
	
	output_file = output_dir / f"{date_str}.json"
	with open(output_file, 'w', encoding='utf-8') as f:
		json.dump(data, f, indent=2)

	qt.log.info(f"saved data for {date_str}")

def process_date(date_str):
	index_name = "spx500"
	"""Process a single date - fetch and save if data exists"""
	data = get_constituents(date_str, index_name)

	if data is not None:
		save_to_file(date_str, data, index_name)

	# Add delay to prevent overwhelming the server
	time.sleep(1)
	return date_str, data is not None

def main():
	# Start date and configurations
	start_date = datetime(2015, 1, 1)
	end_date = datetime(2015, 2, 7)
	# end_date = datetime.now()
	max_workers = 5  # Limit concurrent requests
	
	# Generate list of dates to process
	current_date = start_date
	dates_to_process = []
	
	while current_date <= end_date:
		dates_to_process.append(format_date(current_date))
		current_date += timedelta(days=1)
	
	# Process dates in parallel with throttling
	successful_dates = 0
	total_dates = len(dates_to_process)
	
	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		future_to_date = {executor.submit(process_date, date_str): date_str 
						 for date_str in dates_to_process}
		
		for future in as_completed(future_to_date):
			date_str = future_to_date[future]
			try:
				_, success = future.result()
				if success:
					successful_dates += 1
				qt.log.info(f"Progress: {successful_dates}/{total_dates} dates processed")
			except Exception as e:
				qt.log.error(f"Error processing {date_str}: {str(e)}")

if __name__ == "__main__":
	main()