import csv
import os


def load_companies():
    # Load companies data from data/companies.csv; returns list of dictionaries
    companies = []
    file_path = os.path.join('data', 'companies.csv')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                companies.append(row)
    except FileNotFoundError:
        print(f"Error: Could not find {file_path}")
        return []
    except Exception as e:
        print(f"Error reading companies data: {e}")
        return []
    
    return companies


def load_incentives():
    # Load incentives data from data/incentives.csv; returns list of dictionaries
    incentives = []
    file_path = os.path.join('data', 'incentives.csv')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                incentives.append(row)
    except FileNotFoundError:
        print(f"Error: Could not find {file_path}")
        return []
    except Exception as e:
        print(f"Error reading incentives data: {e}")
        return []
    
    return incentives


# Example usage
if __name__ == "__main__":
    # Load and display sample data
    companies = load_companies()
    print(f"Loaded {len(companies)} companies")
    if companies:
        print("Sample company:", companies[0])
    
    print("\n" + "="*50 + "\n")
    
    incentives = load_incentives()
    print(f"Loaded {len(incentives)} incentives")
    if incentives:
        print("Sample incentive:", incentives[0])
