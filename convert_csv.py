
import json
import csv
import os

def convert_json_to_csv():
    """Convert correspondence_results.json to correspondence_results.csv"""
    
    # Input and output file paths
    input_file = 'data/correspondence_results.json'
    output_file = 'data/correspondence_results.csv'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return
    
    try:
        # Load the JSON data
        print(f"Loading data from {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Prepare CSV output
        print(f"Converting to CSV format...")
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(['incentive_id', 'company1id', 'company2id', 'company3id', 'company4id', 'company5id'])
            
            # Process each incentive
            for incentive_key, incentive_data in data.items():
                incentive_id = incentive_data['incentive']['incentive_id']
                companies = incentive_data['companies']
                
                # Extract company IDs (up to 5 companies)
                company_ids = [company['id'] for company in companies[:5]]
                
                # Pad with empty strings if less than 5 companies
                while len(company_ids) < 5:
                    company_ids.append('')
                
                # Write row
                writer.writerow([incentive_id] + company_ids)
        
        print(f"Successfully converted data to {output_file}")
        
        # Print summary statistics
        total_incentives = len(data)
        total_companies = sum(len(incentive_data['companies']) for incentive_data in data.values())
        print(f"Summary:")
        print(f"  Total incentives: {total_incentives}")
        print(f"  Total company associations: {total_companies}")
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    convert_json_to_csv()
