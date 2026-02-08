# 💎 Diamond Trading Bot (Render + GitHub + AWS)

A complete diamond trading platform built as a Telegram bot, deployed on Render with AWS S3 storage.

## 🌟 Features
- **Multi-role system** (Admin, Supplier, Client)
- **Excel-based diamond inventory management**
- **Smart search and filtering**
- **Deal negotiation system**
- **Real-time analytics**
- **AWS S3 cloud storage**
- **Render deployment**

## 🚀 Quick Deployment

### Step 1: Create AWS Resources
1. Go to [AWS CloudFormation Console](https://console.aws.amazon.com/cloudformation)
2. Click "Create stack" → "With new resources"
3. Upload the `setup.yaml` file
4. Click "Next" and create the stack
5. Save the outputs (Access Key, Secret Key, Bucket Name)

### Step 2: Create Telegram Bot
1. Open Telegram, search for [@BotFather](https://t.me/botfather)
2. Send `/newbot` command
3. Follow instructions to create your bot
4. Save the bot token

### Step 3: Deploy to Render
1. Fork this repository to your GitHub account
2. Go to [render.com](https://render.com)
3. Click "New" → "Web Service"
4. Connect your GitHub repository
5. Configure:
   - **Name**: `diamond-trading-bot`
   - **Environment**: `Python`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host=0.0.0.0 --port=$PORT`
6. Add environment variables:
   - `BOT_TOKEN`: 7965048668:AAHFhqx-u5lVJk4Z5yuBOyBOzVwGQqD3ZK0
   - `AWS_ACCESS_KEY_ID`: AKIA3SFAMUMTMUPPJRH6
   - `AWS_SECRET_ACCESS_KEY`: 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
   - `AWS_BUCKET`: diamond-bucket-styleoflifes
   - `AWS_REGION`: `ap-south-1`
   - `ENVIRONMENT`: `production`
7. Click "Create Web Service"

### Step 4: Set up Webhook
After deployment is complete:
1. Get your Render URL (e.g., `https://diamond-trading-bot.onrender.com`)
2. Run the webhook setup script:
```bash
# Set environment variables
export BOT_TOKEN=7965048668:AAHFhqx-u5lVJk4Z5yuBOyBOzVwGQqD3ZK0
export RENDER_URL=https://telegram-bot-6iil.onrender.com

# Run webhook setup
python update_webhook.py
