import csv
import boto3
import os
import json
from decimal import Decimal
from collections import Counter
from botocore.exceptions import ClientError
import pycountry
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from fpdf import FPDF
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm


s3_client = boto3.client('s3')
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
        average_spending = round((total_spending / len(historical_data)),2)
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
                "categoty": category,
                'vendor': item["vendor"],
                'date': item["date"]
            })
    return flagged_transactions

def spending_by_category(current_transactions):
    spending_by_category = {}
    for transaction in current_transactions:
        category = transaction["category"]
        amount = round(float(transaction["amount"]), 2)
        if category in spending_by_category:
            spending_by_category[category] += round(amount, 2)
        else:
            spending_by_category[category] = round(amount, 2)
    return spending_by_category

def get_previous_month_data(all_transactions, current_year_month):
    year = int(current_year_month[:4])
    month = int(current_year_month[4:])

    if month == 1:  # January -> Go to December of the previous year
        previous_year = year - 1
        previous_month = 12
    else:
        previous_year = year
        previous_month = month - 1

    previous_year_month = f"{previous_year}{previous_month:02d}"

    # Filter transactions 
    previous_month_transactions = [
        transaction for transaction in all_transactions
        if transaction["date"][:7].replace("-", "") == previous_year_month
    ]

    return spending_by_category(previous_month_transactions)

def generate_pie_chart(spending_by_category_current, spending_by_category_previous, user_id, year_month):


    os.environ["MPLCONFIGDIR"] = "/tmp"

    labels_current = list(spending_by_category_current.keys())
    sizes_current = list(spending_by_category_current.values())

    labels_previous = list(spending_by_category_previous.keys())
    sizes_previous = list(spending_by_category_previous.values())

    colors_current = cm.get_cmap("tab20c")(range(len(sizes_current)))
    colors_previous = cm.get_cmap("tab20c")(range(len(sizes_previous)))

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))  

    axes[0].pie(
        sizes_current,
        labels=labels_current,
        autopct='%1.1f%%',
        startangle=140,
        colors=colors_current,
        textprops={'fontsize': 10},
        wedgeprops={'edgecolor': 'white'},
        pctdistance=0.85,
        labeldistance=1.1
    )
    axes[0].set_title(f'Current Month: {year_month}', fontsize=14, pad=20)

    axes[1].pie(
        sizes_previous,
        labels=labels_previous,
        autopct='%1.1f%%',
        startangle=140,
        colors=colors_previous,
        textprops={'fontsize': 10},
        wedgeprops={'edgecolor': 'white'},
        pctdistance=0.85,
        labeldistance=1.1
    )
    previous_month = str(int(year_month) - 1)  # Calculate the previous month
    axes[1].set_title(f'Previous Month: {previous_month}', fontsize=14, pad=20)

    plt.tight_layout()
    pie_chart_path = f"/tmp/user_{user_id}_spending_by_category_{year_month}_comparison.png"
    plt.savefig(pie_chart_path, bbox_inches='tight')
    plt.close()
    return pie_chart_path

def identify_high_value_transactions(current_transactions, historical_average):
    high_value_transactions = []
    for item in current_transactions:
        if float(item["amount"]) > historical_average:
            high_value_transactions.append({
                "transaction_id": item["id"],
                "amount": round(float(item["amount"]), 2), 
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
            recurring_transactions_summary[vendor] += round(float(transaction["amount"]), 2)

    return recurring_transactions_summary

def calculate_monthly_spending_trend(historical_data, current_transactions):
    all_transactions = current_transactions + historical_data

    monthly_spending = {}
    for transaction in all_transactions:
        year_month = transaction["date"][:7].replace("-", "") 
        if year_month not in monthly_spending:
            monthly_spending[year_month] = 0
        monthly_spending[year_month] += round(float(transaction["amount"]), 2)

    # Sort spending by month 
    sorted_months = sorted(monthly_spending.keys(), reverse=True)

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

def generate_bar_line_chart(monthly_spending, user_id, year_month):

    os.environ["MPLCONFIGDIR"] = "/tmp"  

    sorted_months = sorted(monthly_spending.keys())
    spending_values = [monthly_spending[month] for month in sorted_months]

    plt.figure(figsize=(8, 5))
    
    plt.bar(sorted_months, spending_values, color='lightblue', alpha=0.7, label="Monthly Spending")
    
    plt.plot(sorted_months, spending_values, marker='o', color='b', label="Spending Trend")

    plt.xlabel("YearMonth")
    plt.ylabel("Spending ($)")
    plt.title(f"Monthly Spending Trend for User {user_id}")
    plt.xticks(rotation=45)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend()

    trend_chart_path = f"/tmp/user_{user_id}_spending_trend_{year_month}.png"
    plt.tight_layout()
    plt.savefig(trend_chart_path)
    plt.close()

    return trend_chart_path

def get_top_high_value_transactions(high_value_transaction, limit=3):

    sorted_transactions = sorted(high_value_transaction, key=lambda x: x['amount'], reverse=True)
    return sorted_transactions[:limit]

def generate_pdf_report(user_id, year_month, pie_chart_path, trend_chart_path, recurring_graph_path, high_value_transaction, flagged_transactions):
    pdf = FPDF(orientation='P', unit='mm', format='A4')
    pdf.add_page()

    # Title
    pdf.set_font('Arial', 'B', 16)
    pdf.cell(0, 10, f"Monthly Report for User {user_id} ({year_month})", ln=True, align='C')

    # Spending Breakdown
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "Spending Breakdown", ln=True)
    pdf.image(pie_chart_path, x=40, w=130)

    # Monthly Spending Trend
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "Monthly Spending Trend", ln=True)
    pdf.image(trend_chart_path, x=40, w=130)

    # Recurring Transactions Analysis
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "Recurring Transactions Analysis", ln=True)
    pdf.image(recurring_graph_path, x=40, w=130)

    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "High-Value Transactions", ln=True)

    pdf.set_font('Arial', '', 12)
    top_transactions = get_top_high_value_transactions(high_value_transaction)
    for transaction in top_transactions:
        pdf.cell(50, 10, transaction["vendor"], 1, 0, 'C')
        pdf.cell(40, 10, f"${transaction['amount']:.2f}", 1, 0, 'C')
        pdf.cell(50, 10, transaction["date"], 1, 1, 'C') 

    ## flagged transactions 
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "Suspicious Transactions", ln=True)
    pdf.set_font('Arial', '', 12)
    for transaction in flagged_transactions:
        pdf.cell(50, 10, transaction["vendor"], 1, 0, 'C')
        pdf.cell(40, 10, f"${transaction['amount']:.2f}", 1, 0, 'C')
        pdf.cell(50, 10, transaction["date"], 1, 0, 'C')
        pdf.cell(30, 10, transaction["risk_level"], 1, 0, 'C')
        pdf.cell(30, 10, transaction["location"], 1, 1, 'C')  

    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "Detailed Analysis of Suspicious Transactions", ln=True)

    pdf.set_font('Arial', '', 12)
    for transaction in flagged_transactions:
        details = f"Transaction of ${transaction['amount']:.2f} at {transaction['vendor']} on {transaction['date']} was flagged as {transaction['risk_level']} because:"
        pdf.multi_cell(0, 10, details)

        reasons = []
        if transaction.get("location") != transaction.get("home_counter"):
            reasons.append(
                f"it was performed in {transaction['location']}, whereas most transactions are done in {transaction['home_counter']}."
            )
        if transaction.get("amount") > transaction.get("avarage_amount", 0):
            reasons.append(
                f"the amount of ${transaction['amount']:.2f} exceeds your historical average spending of ${transaction['avarage_amount']:.2f}."
            )

        if reasons:
            for reason in reasons:
                pdf.multi_cell(0, 10, f"- {reason}")
        else:
            pdf.multi_cell(0, 10, "- No specific anomaly detected.")

        pdf.cell(0, 5, "", ln=True)

    pdf_path = f"/tmp/user_{user_id}_report_{year_month}.pdf"
    pdf.output(pdf_path)
    return pdf_path
def generate_recurring_transactions_graph(recurring_data, user_id, year_month):
    vendors = list(recurring_data.keys())
    current_amounts = [round(value, 2) for value in recurring_data.values()]

    current_month = int(year_month[4:])  
    months_elapsed = current_month
    months_remaining = 12 - months_elapsed

    predicted_amounts = [
        round(amount + (amount / months_elapsed) * months_remaining, 2)
        for amount in current_amounts
    ]

    x = np.arange(len(vendors))  
    bar_width = 0.6

    plt.figure(figsize=(10, 6))
    bars1 = plt.bar(x, current_amounts, bar_width, color='teal', label='Current Spending')
    bars2 = plt.bar(x, predicted_amounts, bar_width, alpha=0.4, color='teal', label='Predicted Total Spending')

    plt.xticks(x, vendors, rotation=45, ha='right', fontsize=10)
    plt.xlabel('Recurring Transactions (Vendors)', fontsize=12)
    plt.ylabel('Amount Spent ($)', fontsize=12)
    plt.title(f'Recurring Transactions for User {user_id} ({year_month})', fontsize=14)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.legend()

    for i, bar1 in enumerate(bars1):
        plt.text(
            bar1.get_x() + bar1.get_width() / 2,
            bar1.get_height() + 5,
            f"${current_amounts[i]}",
            ha='center',
            va='bottom',
            fontsize=9,
            color='black',
        )

    recurring_graph_path = f"/tmp/user_{user_id}_recurring_transactions_{year_month}.png"
    plt.tight_layout()
    plt.savefig(recurring_graph_path)
    plt.close()
    return recurring_graph_path

def upload_to_s3(file_path, bucket_name, key):
    try:
        s3_client.upload_file(file_path, "cpsc436c-g9-customer-reports", key)

        print(f"Uploaded {file_path} to {bucket_name}/{key}")
    except ClientError as e:
        print(f"Error uploading to S3: {e.response['Error']['Message']}")

### used for uploading the new statements to DynamoDB
def process_csv(csv_path):
    grouped_items = {}
    try:
        with open(csv_path, mode="r", encoding="utf-8-sig") as file:
            csv_reader = csv.DictReader(file)

            # Group transactions by UserId and YearMonth
            for row in csv_reader:
                transaction = {
                    "amount": Decimal(row["transactions.amount"]),
                    "category": row["transactions.category"],
                    "currency": row["transactions.currency"],
                    "date": row["transactions.date"],
                    "description": row["transactions.description"],
                    "id": row["transactions.id"],
                    "location": row["transactions.location"],
                    "recurring": row["transactions.recurring"].lower() == "true",
                    "type": row["transactions.type"],
                    "vendor": row["transactions.vendor"],
                }

                key = (row["UserId"], row["YearMonth"])
                if key not in grouped_items:
                    grouped_items[key] = []
                grouped_items[key].append(transaction)

        # Prepare items for DynamoDB
        dynamo_data = []
        for (user_id, year_month), transactions in grouped_items.items():
            dynamo_data.append({
                "UserId": user_id,  # String directly
                "YearMonth": year_month,  # String directly
                "transactions": transactions  # List of dictionaries
            })

        print("Successfully formatted new transactions for DynamoDB.")
        return dynamo_data

    except Exception as e:
        print(f"Error loading new transactions: {str(e)}")
        return []

def lambda_handler(event, context):
    try:
        ingest_bucket = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']


        local_csv_path = f"/tmp/{file_key.split('/')[-1]}"
        s3_client.download_file(ingest_bucket, file_key, local_csv_path)
        dynamo_data = process_csv(local_csv_path)

        new_data = load_new_transactions(local_csv_path)

        for (user_id, year_month), current_transactions in new_data.items():
            historical_data = query_historical_data(user_id)
            all_transactions = historical_data + current_transactions
            home_country = determine_home_country(historical_data)
            historical_average = calculate_historical_average(historical_data)
            flagged_transactions = flag_risky_transactions(current_transactions, home_country, historical_average)
            spending_by_cat = spending_by_category(current_transactions)
            spending_by_cat_prev = get_previous_month_data(all_transactions, year_month)
            pie_chart_path = generate_pie_chart(spending_by_cat, spending_by_cat_prev, user_id, year_month)
            high_value_transaction = identify_high_value_transactions(current_transactions, historical_average)
            current_year = year_month[:4]
            recurring_transactions_summary = analyze_recurring_transactions(
                current_transactions, historical_data, current_year
            )
            monthly_spending_trend = calculate_monthly_spending_trend(historical_data, current_transactions)
            trend_chart_path = generate_bar_line_chart(monthly_spending_trend["MonthlySpending"], user_id, year_month)
            report = {
                "UserId": user_id,
                "YearMonth": year_month,
                "PieChartPath": pie_chart_path,
                 "TrendChartPath": trend_chart_path,
                "FlaggedTransactions": flagged_transactions,
                "SpendingByCategory": spending_by_cat,
                "HighValueTransaction": high_value_transaction,
                "RecurringTransactionsYearToDate": recurring_transactions_summary,
                "MonthlySpending_Trend": monthly_spending_trend,     
            }
            recurring_graph_path = generate_recurring_transactions_graph(report["RecurringTransactionsYearToDate"], user_id, year_month)

            report_file = f"/tmp/user_{user_id}_report_{year_month}.json"
            with open(report_file, "w") as file:
                json.dump(report, file, indent=2)


 
            report_s3_key = f"reports/user_{user_id}_report_{year_month}.json"
            # pie_chart_s3_key = f"reports/user_{user_id}_spending_by_category_{year_month}.png"

            pdf_s3_key = f"reports/user_{user_id}_report_{year_month}.pdf"
            # trend_chart_s3_key = f"reports/user_{user_id}_spending_trend_{year_month}.png"

            pdf_path = generate_pdf_report(user_id, year_month, pie_chart_path, trend_chart_path, recurring_graph_path, high_value_transaction, flagged_transactions)
            
            
            #upload_to_s3(report_file, "cpsc436c-g9-customer-reports", report_s3_key)
            # upload_to_s3(pie_chart_path, "cpsc436c-g9-customer-reports", pie_chart_s3_key)
            upload_to_s3(pdf_path, "cpsc436c-g9-customer-reports", pdf_s3_key)
            # upload_to_s3(trend_chart_path, "cpsc436c-g9-customer-reports", trend_chart_s3_key)
       ####### Uplodad to Dynamo  #######
        for item in dynamo_data:
            table.put_item(Item=item)

        
        s3_client.delete_object(Bucket=ingest_bucket, Key=file_key)
        print(f"Deleted processed file: {file_key} from bucket: {ingest_bucket}")
        return {"statusCode": 200, "body": "Processing complete!"}
        
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {"statusCode": 500, "body": "An error occurred."}

