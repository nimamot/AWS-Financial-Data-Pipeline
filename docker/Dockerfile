# Base Python image compatible with AWS Lambda
FROM public.ecr.aws/lambda/python:3.9

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Set the CMD to your Lambda handler function
CMD ["lambda_function.lambda_handler"]
