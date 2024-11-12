import csv
import json
import boto3
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
from io import StringIO
from datetime import datetime


s3_client = boto3.client('s3')

def load_new_transactions_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = obj['Body'].read().decode('utf-8').splitlines()
    csv_reader = csv.DictReader(csv_content)
    
    new_transactions = defaultdict(list)
    for row in csv_reader:
        user_id = row['user_id']
        transaction = {
            'transaction_id': row['transaction_id'],
            'transaction_date': row['transaction_date'],
            'vendor': row['vendor'],
            'category': row['category'],
            'amount': float(row['amount']),
            'currency': row['currency'],
            'recurring': row['recurring'] == 'Yes',
            'transaction_type': row['transaction_type'],
            'description': row['description'],
            'country': row['country']
        }
        new_transactions[user_id].append(transaction)
    return new_transactions

#  historical data from JSON in S3
def load_historical_data_from_s3(bucket_name, file_key):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    historical_data = json.loads(obj['Body'].read().decode('utf-8'))
    return historical_data

#
def determine_home_country(historical_data):
    country_counts = Counter(item['country'] for item in historical_data)
    home_country = country_counts.most_common(1)[0][0] if country_counts else None
    return home_country


def run_analysis(new_data, historical_data):
    results = {}
    for user_id, current_transactions in new_data.items():
        # Get user's historical data from S3 (in-memory as JSON)
        user_historical_data = historical_data.get(user_id, [])
        
        # Calculate total spending and category breakdown
        total_spending = sum(item['amount'] for item in current_transactions)
        spending_by_category = defaultdict(float)
        for item in current_transactions:
            spending_by_category[item['category']] += item['amount']
        
        # pie chart for current month's spending 
        pie_chart_path = generate_pie_chart(spending_by_category, user_id)

        # high-value transactions
        historical_average = calculate_historical_average(user_historical_data)
        high_value_transactions = identify_high_value_transactions(current_transactions, historical_average)

        # monthly spending (all-time data)
        monthly_spending = calculate_monthly_spending(user_historical_data + current_transactions)
        
        # monthly spending change
        month_over_month_change = calculate_month_over_month_change(monthly_spending)

        #  home country
        home_country = determine_home_country(user_historical_data)
        
        # Flag transactions + risk level
        flagged_transactions = flag_risky_transactions(current_transactions, home_country, historical_average)

        results[user_id] = {
            'total_spending': total_spending,
            'spending_by_category': dict(spending_by_category),
            'monthly_spending': monthly_spending,
            'historical_average': historical_average,
            'high_value_transactions': high_value_transactions,
            'spending_by_category_chart': pie_chart_path,
            'month_over_month_change': month_over_month_change,
            'flagged_transactions': flagged_transactions
        }

        # Update the historical data to include new transactions
        historical_data[user_id] = user_historical_data + current_transactions

    return results

# Save updated historical data back to S3 as JSON
def save_historical_data_to_s3(historical_data, bucket_name, file_key):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=json.dumps(historical_data, indent=2)
    )

# Flag transactions as moderate or high risk based on location and amount
def flag_risky_transactions(current_transactions, home_country, historical_average):
    flagged_transactions = []
    for item in current_transactions:
        risk_level = None
        if item['country'] != home_country:
            risk_level = 'High Risk' if item['amount'] > historical_average else 'Moderate Risk'
        
        if risk_level:
            flagged_transactions.append({
                'transaction_id': item['transaction_id'],
                'amount': item['amount'],
                'country': item['country'],
                'risk_level': risk_level
            })
    return flagged_transactions

# monthly spending 
def calculate_monthly_spending(transactions):
    monthly_spending = defaultdict(float)
    for item in transactions:
        month = item['transaction_date'][:7]
        monthly_spending[month] += item['amount']
    return dict(monthly_spending)

# change in spending compared to the last month
def calculate_month_over_month_change(monthly_spending):
    sorted_months = sorted(monthly_spending.keys(), reverse=True)
    if len(sorted_months) < 2:
        return 0
    
    current_month = sorted_months[0]
    previous_month = sorted_months[1]
    return monthly_spending[current_month] - monthly_spending[previous_month]

# average spending
def calculate_historical_average(historical_data):
    if historical_data:
        return sum(float(item['amount']) for item in historical_data) / len(historical_data)
    return 0

#  high-value transactions
def identify_high_value_transactions(current_data, historical_average):
    return [item for item in current_data if float(item['amount']) > historical_average]

#  current month spending chart
def generate_pie_chart(spending_by_category, user_id):
    labels = spending_by_category.keys()
    sizes = spending_by_category.values()
    
    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(f'Spending by Category for User {user_id} (Current Month)')
    
    pie_chart_path = f'/tmp/user_{user_id}_spending_by_category.png'
    plt.savefig(pie_chart_path)
    plt.close()
    return pie_chart_path

# Save analysis results to S3 bucket
def save_analysis_results_to_s3(results, bucket_name, report_key):
    s3_client.put_object(
        Bucket=bucket_name,
        Key=report_key,
        Body=json.dumps(results, indent=2)
    )

# Lambda handler function---- entry point error
def lambda_handler(event, context):
    # File and bucket info from the S3 event
    ingest_bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    historical_bucket = 'transaction-historical-bucket'
    historical_file_key = 'historical_data.json'
    report_bucket = 'transaction-report-bucket'
    
    # Load and process new transactions
    new_data = load_new_transactions_from_s3(ingest_bucket, file_key)
    
    # Load historical data from S3
    historical_data = load_historical_data_from_s3(historical_bucket, historical_file_key)
    
    # Run analysis
    analysis_results = run_analysis(new_data, historical_data)

    # Save results to the report S3 bucket
    report_key = f'report/analysis_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    save_analysis_results_to_s3(analysis_results, report_bucket, report_key)

    # Update historical data (in S3)
    save_historical_data_to_s3(historical_data, historical_bucket, historical_file_key)

    return {
        'statusCode': 200,
        'body': json.dumps('completed.')
    }
