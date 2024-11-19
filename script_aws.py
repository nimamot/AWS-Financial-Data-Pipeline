import csv
import boto3
import json
from decimal import Decimal
from collections import Counter
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb", region_name="ca-central-1")
table = dynamodb.Table("cpsc436c-g9-statements")

def check_table_connection(table_name):
    try:
        table = dynamodb.Table(table_name)
        response = table.table_status
        print(f"Connection successful! Table '{table_name}' status: {response}")
    except ClientError as e:
        print(f"Error connecting to table '{table_name}': {e.response['Error']['Message']}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

check_table_connection("cpsc436c-g9-statements")
print(table.attribute_definitions)

def query_historical_data(user_id, year_month):
    try:
        # Query the DynamoDB table for the user's historical data
        response = table.get_item(Key={"UserId": user_id, "YearMonth": year_month})
        
        # Check if item is found
        if "Item" in response:
            historical_data = response["Item"]["transactions"]
            return historical_data
        else:
            print(f"No historical data found for UserId {user_id}, YearMonth {year_month}")
            return []
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        print(f"Error querying DynamoDB: {error_message}")
        return []

# Load new transaction data from CSV
def load_new_transactions(csv_path):
    grouped_items = {}
    try:
        with open(csv_path, mode="r", encoding="utf-8-sig") as file:
            csv_reader = csv.DictReader(file)
            
            # Process each row in the CSV file
            for row in csv_reader:
                transaction = {
                    "id": row["transactions.id"],
                    "date": row["transactions.date"],
                    "vendor": row["transactions.vendor"],
                    "category": row["transactions.category"],
                    "amount": Decimal(row["transactions.amount"]),
                    "currency": row["transactions.currency"],
                    "recurring": row["transactions.recurring"].lower() == "true",
                    "type": row["transactions.type"],
                    "location": row["transactions.location"],
                    "description": row["transactions.description"],
                }
                
                key = (row["UserId"], row["YearMonth"])
                if key not in grouped_items:
                    grouped_items[key] = []
                grouped_items[key].append(transaction)
        print("Successfully loaded new transactions.")
    except Exception as e:
        print(f"Error loading new transactions: {str(e)}")
    return grouped_items

def calculate_historical_average(historical_data):
    if historical_data:
        total_spending = sum(float(item["amount"]) for item in historical_data)
        average_spending = total_spending / len(historical_data)
        return average_spending
    else:
        return 0


def determine_home_country(historical_data):
    country_counts = {}
   
    for item in historical_data:
        country = item["location"].split("-")[0]
        if country in country_counts:
            country_counts[country] += 1
        else:
            country_counts[country] = 1
    
    #  country with the highest count = home countery
    max_count = 0
    home_country = None
    for country, count in country_counts.items():
        if count > max_count:
            max_count = count
            home_country = country
    
    return home_country

def flag_risky_transactions(current_transactions, home_country, historical_average):
    flagged_transactions = []
    for item in current_transactions:
        
        risk_level = None
        
        if item["location"].split("-")[0] != home_country:
            if float(item["amount"]) > historical_average:
                risk_level = "High Risk"
            else:
                risk_level = "Moderate Risk"
        
        if risk_level:
            flagged_transactions.append({
                "transaction_id": item["id"],
                "amount": float(item["amount"]),
                "location": item["location"].split("-")[0],
                "risk_level": risk_level,
                "home_counter": home_country,
            })
    return flagged_transactions




 ################# UPLOAD NEW STSTEMENT  TO DYNAMO ################     
# def upload_to_dynamodb(items):
#     try:
#         print("Trying upload")
#         for item in items:
#             response = table.put_item(Item=item)
#         print("Successfully inserted all rows")
        
#     except ClientError as e:
#         print(f"Error inserting item into DynamoDB: {e.response['Error']['Message']}")
#     except Exception as e:
#         print(f"An unexpected error occurred: {str(e)}")
        
# transaction_rows = process_csv('user1data.csv')
# upload_to_dynamodb(transaction_rows)
 #################  ################     





def main():
    new_transactions_path = "new_transactions.csv"

    new_data = load_new_transactions(new_transactions_path)

    for (user_id, year_month), current_transactions in new_data.items():
        previous_year_month = str(int(year_month) - 1)  # Adjust year-month
        historical_data = query_historical_data(user_id, previous_year_month)

        home_country = determine_home_country(historical_data)

        historical_average = calculate_historical_average(historical_data)

        flagged_transactions = flag_risky_transactions(current_transactions, home_country, historical_average)

        print(f"Flagged transactions for UserId {user_id}:")
        output_file = f"flagged_transactions_user_{user_id}.json"
        with open(output_file, "w") as file:
            json.dump(flagged_transactions, file, indent=2)
        print(f"Flagged transactions saved to {output_file}")




main()
