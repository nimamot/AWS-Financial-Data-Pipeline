# Use the AWS Lambda Python runtime as a base image
FROM public.ecr.aws/lambda/python:3.7

# Set the working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set the command to the Lambda function handler
CMD ["lambda_function.lambda_handler"]
