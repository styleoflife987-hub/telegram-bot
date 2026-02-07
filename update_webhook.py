import requests

# REPLACE WITH YOUR ACTUAL BOT TOKEN
BOT_TOKEN = "7965048668:AAHFhqx-u5lVJk4Z5yuBOyBOzVwGQqD3ZK0"

# Your Fly.io app URL (after deployment)
FLY_URL = "https://diamond-trading-bot-fly.fly.dev/webhook"

# Update webhook
response = requests.post(
    f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
    json={"url": FLY_URL}
)

print("Status Code:", response.status_code)
print("Response:", response.json())
