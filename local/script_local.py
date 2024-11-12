import csv
import json
import matplotlib.pyplot as plt
from collections import defaultdict, Counter
from datetime import datetime

# Load the new transaction data from CSV & convert to key-value for dynamo 
def load_new_transactions(file_path):
    new_transactions = defaultdict(list)
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
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
                'country': row['country']  # Add country column
            }
            new_transactions[user_id].append(transaction)
    return new_transactions

# Load historical transaction data from JSON (key-value format)
def load_historical_data(file_path):
    with open(file_path, 'r') as file:
        historical_data = json.load(file)
    return historical_data

# Combine new and historical data for each user
def combine_data(new_data, historical_data):
    for user_id, transactions in new_data.items():
        if user_id in historical_data:
            historical_data[user_id].extend(transactions)
        else:
            historical_data[user_id] = transactions
    return historical_data

# user's home country: based on most frequent transaction location
def determine_home_country(historical_data):
    country_counts = Counter(item['country'] for item in historical_data)
    home_country = country_counts.most_common(1)[0][0] if country_counts else None
    return home_country

# Run analysis on combined data
def run_analysis(new_data, historical_data):
    results = {}
    for user_id, current_transactions in new_data.items():
        # Calculate insights for current month
        total_spending = sum(item['amount'] for item in current_transactions)
        spending_by_category = defaultdict(float)
        for item in current_transactions:
            spending_by_category[item['category']] += item['amount']

        # Generate pie chart for current month's spending by category
        pie_chart_path = generate_pie_chart(spending_by_category, user_id)

        # Calculate historical average and flag high-value transactions
        user_historical_data = historical_data.get(user_id, [])
        historical_average = calculate_historical_average(user_historical_data)
        high_value_transactions = identify_high_value_transactions(current_transactions, historical_average)

        # Calculate monthly spending (all-time data)
        monthly_spending = calculate_monthly_spending(user_historical_data + current_transactions)
        
        # Calculate month-over-month spending change
        month_over_month_change = calculate_month_over_month_change(monthly_spending)

        # Determine user's home country
        home_country = determine_home_country(user_historical_data)
        
        # Flag transactions based on risk level
        flagged_transactions = flag_risky_transactions(current_transactions, home_country, historical_average)

        results[user_id] = {
            'total_spending': total_spending,
            'spending_by_category': dict(spending_by_category),
            'monthly_spending': monthly_spending,
            'historical_average': historical_average,
            'high_value_transactions': high_value_transactions,
            'spending_by_category_chart': pie_chart_path,  # Path to pie chart image
            'month_over_month_change': month_over_month_change,
            'flagged_transactions': flagged_transactions
        }
    return results

# Flag transactions as moderate || high risk based on location & amount
def flag_risky_transactions(current_transactions, home_country, historical_average):
    flagged_transactions = []
    for item in current_transactions:
        risk_level = None
        if item['country'] != home_country:
            if item['amount'] > historical_average:
                risk_level = 'High Risk'
            else:
                risk_level = 'Moderate Risk'
        
        if risk_level: ## append to the flagged transactions
            flagged_transactions.append({
                'transaction_id': item['transaction_id'],
                'amount': item['amount'],
                'country': item['country'],
                'risk_level': risk_level
            })
    return flagged_transactions

# monthly spending for a user based on transaction data
def calculate_monthly_spending(transactions):
    monthly_spending = defaultdict(float)
    for item in transactions:
        month = item['transaction_date'][:7]  # Extract YYYY-MM format
        monthly_spending[month] += item['amount']
    return dict(monthly_spending)

# the change in spending compared to the last month
def calculate_month_over_month_change(monthly_spending):
    sorted_months = sorted(monthly_spending.keys(), reverse=True)
    if len(sorted_months) < 2:
        return 0  #NEI
    
    current_month = sorted_months[0]
    previous_month = sorted_months[1]
    current_spending = monthly_spending[current_month]
    previous_spending = monthly_spending[previous_month]
    
    return current_spending - previous_spending

# Function to calculate historical average spending
def calculate_historical_average(historical_data):
    if historical_data:
        historical_average = sum(float(item['amount']) for item in historical_data) / len(historical_data)
    else:
        historical_average = 0  
    return historical_average

# Function to identify high-value transactions
def identify_high_value_transactions(current_data, historical_average):
    high_value_transactions = [
        item for item in current_data if float(item['amount']) > historical_average
    ]
    return high_value_transactions

# Generate and save a pie chart of spending by category for the current month
def generate_pie_chart(spending_by_category, user_id):
    labels = spending_by_category.keys()
    sizes = spending_by_category.values()
    
    plt.figure(figsize=(6, 6))
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    plt.title(f'Spending by Category for User {user_id} (Current Month)')
    
    # Save the pie chart as a PNG file
    pie_chart_path = f'user_{user_id}_spending_by_category.png'
    plt.savefig(pie_chart_path)
    plt.close()  # Close the plot to free memory
    return pie_chart_path

# Save analysis results to JSON
def save_analysis_results(results, output_path):
    with open(output_path, 'w') as file:
        json.dump(results, file, indent=2)

# Main function to run the workflow
def main():
    # File paths
    new_transactions_path = 'new_transactions.csv'  # New transactions CSV
    historical_data_path = 'historical_data.json'   # Historical data JSON <key-value>
    output_results_path = 'analysis_results.json'   # Output file for analysis results

    # Load data
    new_data = load_new_transactions(new_transactions_path)
    historical_data = load_historical_data(historical_data_path)

    # Combine data and run analysis
    combined_data = combine_data(new_data, historical_data)
    analysis_results = run_analysis(new_data, historical_data)

    # Save results
    save_analysis_results(analysis_results, output_results_path)

    print("Analysis completed and results saved to", output_results_path)

# Run the script
if __name__ == '__main__':
    main()
