FROM python:3.12-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY src/ ./src/


ENV MONGO_DB_NAME=combined_config
ENV MONGO_COLLECTION_NAME=combined_config_collection

ENV MONGO_DB_NAME_VOLUME_TRACKER=volume_tracker
ENV MONGO_COLLECTION_NAME_VOLUME_TRACKER=exchange_configs

ENV MONGO_DB_NAME_TRADING_TOOLS=trading_tools
ENV MONGO_COLLECTION_NAME_TRADING_TOOLS=trading_tools_collection

# Command to run the application
CMD ["python", "src/main.py"]