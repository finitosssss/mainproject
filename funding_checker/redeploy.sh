#!/bin/bash
# Complete Re-deploy script for Azure Container Instance
# This script builds the image in ACR and updates the ACI with current .env variables.

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

source /Users/macbook/.gemini/antigravity/scratch/mainproject-main/funding_checker/.venv/bin/activate

echo "--- 1/3 Building fresh image in ACR ---"
AZ_PATH="/Users/macbook/.gemini/antigravity/scratch/mainproject-main/funding_checker/.venv/bin/az"

$AZ_PATH acr build --registry checkers --image fundingchecker:latest .

echo "--- 2/3 Deleting old container instance ---"
$AZ_PATH container delete --resource-group checkers --name checkers --yes

echo "--- 3/3 Creating fresh container with all .env variables ---"
$AZ_PATH container create \
  --resource-group checkers \
  --name checkers \
  --image checkers-f8ezc5dwg9cvceeh.azurecr.io/fundingchecker:latest \
  --registry-login-server checkers-f8ezc5dwg9cvceeh.azurecr.io \
  --registry-username $($AZ_PATH acr credential show --name checkers --query username --output tsv) \
  --registry-password $($AZ_PATH acr credential show --name checkers --query 'passwords[0].value' --output tsv) \
  --os-type Linux \
  --cpu 2 \
  --memory 3 \
  --restart-policy Always \
  --environment-variables \
    PYTHONUNBUFFERED=1 \
    REQUESTS_TIMEOUT=30 \
    AIOHTTP_TIMEOUT=30 \
    MONGO_URI="$MONGO_URI" \
    MONGO_DB_NAME_HEDGE_STRATEGY="$MONGO_DB_NAME_HEDGE_STRATEGY" \
    MONGO_COLLECTION_NAME_HEDGE_STRATEGY="$MONGO_COLLECTION_NAME_HEDGE_STRATEGY" \
    MONGO_DB_NAME_FUNDING_MONITOR="$MONGO_DB_NAME_FUNDING_MONITOR" \
    MONGO_COLLECTION_NAME_FUNDING_MONITOR="$MONGO_COLLECTION_NAME_FUNDING_MONITOR" \
    MONGO_DB_NAME_VOLUME_TRACKER="$MONGO_DB_NAME_VOLUME_TRACKER" \
    MONGO_COLLECTION_NAME_VOLUME_TRACKER="$MONGO_COLLECTION_NAME_VOLUME_TRACKER" \
    MONGO_DB_NAME_TRADING_TOOLS="$MONGO_DB_NAME_TRADING_TOOLS" \
    MONGO_COLLECTION_NAME_TRADING_TOOLS="$MONGO_COLLECTION_NAME_TRADING_TOOLS" \
    VOLUME_TRACKER_TOKEN="$VOLUME_TRACKER_TOKEN" \
    VOLUME_TRACKER_CHAT_IDS="$VOLUME_TRACKER_CHAT_IDS" \
    TRADING_TOOLS_TOKEN="$TRADING_TOOLS_TOKEN" \
    TRADING_TOOLS_CHAT_IDS="$TRADING_TOOLS_CHAT_IDS" \
    UNIQUE_STRATEGY_TOKEN="$UNIQUE_STRATEGY_TOKEN" \
    UNIQUE_STRATEGY_CHAT_IDS="$UNIQUE_STRATEGY_CHAT_IDS" \
    MONGO_DB_NAME_UNIQUE_STRATEGY="$MONGO_DB_NAME_UNIQUE_STRATEGY" \
    MONGO_COLLECTION_NAME_UNIQUE_STRATEGY="$MONGO_COLLECTION_NAME_UNIQUE_STRATEGY" \
    FUNDING_BOT_TOKEN="$FUNDING_BOT_TOKEN" \
    HEDGE_BOT_TOKEN="$HEDGE_BOT_TOKEN" \
    TELEGRAM_CHAT_ID="$TELEGRAM_CHAT_ID" \
    BYBIT_API_KEY="$BYBIT_API_KEY" \
    BYBIT_API_SECRET="$BYBIT_API_SECRET"

echo "--- DEPLOYMENT COMPLETE! ---"
