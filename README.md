# Automated Financial Statement Analysis
<img width="1595" height="674" alt="Screenshot 2025-09-04 at 5 07 16 PM" src="https://github.com/user-attachments/assets/8d72843f-9415-4806-87a6-f13152aba5e4" />
## Demo:
https://youtu.be/qQftj19BvmA
## Project Overview
This project automates the analysis of financial statements by leveraging AWS cloud services. It enables seamless ingestion, processing, and reporting of user-uploaded financial data. The pipeline is built using AWS Lambda, S3, and DynamoDB, providing a scalable solution for financial data analysis.

## Features
- **Data Ingestion:** Upload user statements in CSV format to the designated S3 ingestion bucket.
- **Analysis:** Perform in-depth analysis including:
  - Spending by category.
  - Flagged transactions based on unusual location or high amounts.
  - Recurring transaction insights.
  - Monthly spending trends.
- **Report Generation:** Generate detailed PDF reports with visualizations and transaction summaries.
- **Cloud Integration:** Automatically update DynamoDB with new transaction data and clean up processed files from the ingestion bucket.

## How It Works
1. **Upload:** Place CSV files into the S3 ingestion bucket (`cpsc436c-g9-statement-ingestion`).
2. **Trigger:** The upload triggers an AWS Lambda function that processes the data.
3. **Analysis and Reporting:** The system processes transactions, performs analysis, and generates a PDF report saved to the S3 reports bucket (`cpsc436c-g9-customer-reports`).
4. **Cleanup:** The ingestion bucket is emptied, and DynamoDB is updated with the new data.

