FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Setting the compatible versions of libraries
ARG ARROW_VERSION=16.1.0
ARG PYICEBERG_VERSION=0.7.1

# Perform system updates and install dependencies --
RUN pip install --upgrade pip && \
    pip install pyarrow==$ARROW_VERSION pyiceberg[s3fs,hive,pyarrow,glue]==$PYICEBERG_VERSION boto3

# Setting  up the ENV vars for local code
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_REGION=""
ENV AWS_SESSION_TOKEN=""

# Copy utils code
COPY src/utils_lib.py ${LAMBDA_TASK_ROOT}

# Copy process code
COPY src/process_files.py ${LAMBDA_TASK_ROOT}

# Copy database manager code
COPY src/dynamo_event.py ${LAMBDA_TASK_ROOT}

# Copy function code
COPY src/lambda_function.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "lambda_function.lambda_handler" ]