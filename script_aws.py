import csv
import boto3
import json
from decimal import Decimal
from collections import Counter
from botocore.exceptions import ClientError
import pycountry
import matplotlib.pyplot as plt


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

# def query_historical_data(user_id, year_month):
#     try:
#         # Query the DynamoDB table for the user's historical data
#         response = table.get_item(Key={"UserId": user_id, "YearMonth": year_month})
        
#         # Check if item is found
#         if "Item" in response:
#             historical_data = response["Item"]["transactions"]
#             return historical_data
#         else:
#             print(f"No historical data found for UserId {user_id}, YearMonth {year_month}")
#             return []
#     except ClientError as e:
#         error_message = e.response["Error"]["Message"]
#         print(f"Error querying DynamoDB: {error_message}")
#         return []

def query_historical_data(user_id):
    try:
        # Query the DynamoDB table for all YearMonths for the given UserId
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("UserId").eq(user_id)
        )
        
        # Check if items are found
        if "Items" in response:
            all_historical_data = []
            for item in response["Items"]:
                all_historical_data.extend(item["transactions"])  # Combine transactions from all months
            return all_historical_data
        else:
            print(f"No historical data found for UserId {user_id}")
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
        country_code = item["location"][:2]
        country = pycountry.countries.get(alpha_2=country_code).name
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
        loc = pycountry.countries.get(alpha_2=item["location"][:2]).name # loc of new transaction
        if loc != home_country:
            category = item["category"]
            if float(item["amount"]) > historical_average:
                risk_level = "High Risk"
            else:
                risk_level = "Moderate Risk"
        
        if risk_level:
            flagged_transactions.append({
                "transaction_id": item["id"],
                "amount": float(item["amount"]),
                "avarage_amount": historical_average,
                "location": loc,
                "risk_level": risk_level,
                "home_counter": home_country,
                "categoty": category
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

def spending_by_category(current_transactions):
    spending_by_category = {}
    for transaction in current_transactions:
        category = transaction["category"]
        amount = float(transaction["amount"])
        if category in spending_by_category:
            spending_by_category[category] += amount
        else:
            spending_by_category[category] = amount
    return spending_by_category

def generate_pie_chart(spending_by_category, user_id, year_month):
    labels = spending_by_category.keys()
    sizes = spending_by_category.values()

    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(f'Spending by Category for User {user_id} ({year_month})')

    pie_chart_path = f'user_{user_id}_spending_by_category_{year_month}.png'
    plt.savefig(pie_chart_path)
    plt.close()
    return pie_chart_path

def identify_high_value_transactions(current_transactions, historical_average):
    high_value_transactions = []
    for item in current_transactions:
        if float(item["amount"]) > historical_average:
            high_value_transactions.append({
                "transaction_id": item["id"],
                "amount": float(item["amount"]), 
                "vendor": item["vendor"],
                "category": item["category"],
                "date": item["date"],
                "location": item["location"]
            })
    return high_value_transactions

def analyze_recurring_transactions(current_transactions, historical_data, year):
    recurring_transactions_summary = {}

    all_transactions = current_transactions + historical_data

    for transaction in all_transactions:
        transaction_year = transaction["date"][:4] 
        if transaction_year == year and transaction["recurring"]:
            vendor = transaction["vendor"]
            if vendor not in recurring_transactions_summary:
                recurring_transactions_summary[vendor] = 0
            recurring_transactions_summary[vendor] += float(transaction["amount"])

    return recurring_transactions_summary


def calculate_monthly_spending_trend(historical_data, current_transactions):
    all_transactions = current_transactions + historical_data

    monthly_spending = {}
    for transaction in all_transactions:
        year_month = transaction["date"][:7].replace("-", "") 
        if year_month not in monthly_spending:
            monthly_spending[year_month] = 0
        monthly_spending[year_month] += float(transaction["amount"])

    # Sort spending by month 
    sorted_months = sorted(monthly_spending.keys(), reverse=True)

    # the trend over the last 3 months
    if len(sorted_months) >= 3:
        last_months = sorted_months[:3]
        trend_values = [monthly_spending[month] for month in last_months]
        if trend_values[0] > trend_values[1] > trend_values[2]:
            trend = "Up"
        elif trend_values[0] < trend_values[1] < trend_values[2]:
            trend = "Down"
        else:
            trend = "Stable"
    else:
        trend = "Not enough data"

    return {
        "MonthlySpending": monthly_spending,
        "Trend": trend
    }
def main():
    new_transactions_path = "new_transactions.csv"

    new_data = load_new_transactions(new_transactions_path)

    for (user_id, year_month), current_transactions in new_data.items():
        previous_year_month = str(int(year_month) - 1)  # Adjust year-month
        # historical_data = query_historical_data(user_id, previous_year_month)
        historical_data = query_historical_data(user_id)


        home_country = determine_home_country(historical_data)
        historical_average = calculate_historical_average(historical_data)

        flagged_transactions = flag_risky_transactions(current_transactions, home_country, historical_average)

        spending_by_cat = spending_by_category(current_transactions)

        pie_chart_path = generate_pie_chart(spending_by_cat, user_id, year_month)
        
        high_value_transaction = identify_high_value_transactions(current_transactions, historical_average)
       
        current_year = year_month[:4]
        recurring_transactions_summary = analyze_recurring_transactions(current_transactions, historical_data, current_year)
        monthly_spending_trend = calculate_monthly_spending_trend(historical_data, current_transactions)
        report = {
            "UserId": user_id,
            "YearMonth": year_month,
            "PieChartPath": pie_chart_path,
            "FlaggedTransactions": flagged_transactions,
            "SpendingByCategory" : spending_by_cat,
            "HighValueTransaction": high_value_transaction,
            "RecurringTransactionsYearToDate": recurring_transactions_summary,
            "MonthlySpendingTrend": monthly_spending_trend
        }

        report_file = f"user_{user_id}_report_{year_month}.json"
        with open(report_file, "w") as file:
            json.dump(report, file, indent=2)
        print(f"Monthly report saved to {report_file}")


        ## TODO: upload the new statement to S3 bucket
        ## TODO : delete the csv file added by the bank



main()
