import os
import sys
import requests
from urllib.parse import urljoin

def update_telegram_webhook():
    """
    Update Telegram webhook URL for your bot.
    Run this script after deploying your bot to set the webhook URL.
    """
    
    # Configuration - Set these values
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    
    # If not set in environment, prompt user
    if not BOT_TOKEN:
        print("⚠️  BOT_TOKEN not found in environment variables.")
        BOT_TOKEN = input("Enter your Telegram Bot Token: ").strip()
    
    if not WEBHOOK_URL:
        print("⚠️  WEBHOOK_URL not found in environment variables.")
        base_url = input("Enter your application URL (e.g., https://your-app.herokuapp.com): ").strip()
        WEBHOOK_URL = urljoin(base_url, "/webhook")
    
    if not BOT_TOKEN or not WEBHOOK_URL:
        print("❌ Both BOT_TOKEN and WEBHOOK_URL are required.")
        sys.exit(1)
    
    print("\n" + "="*60)
    print("🤖 Telegram Webhook Configuration")
    print("="*60)
    print(f"Bot Token: {BOT_TOKEN[:10]}...{BOT_TOKEN[-10:]}")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print("="*60 + "\n")
    
    # First, get current webhook info
    print("📡 Getting current webhook information...")
    try:
        info_response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getWebhookInfo",
            timeout=10
        )
        
        if info_response.status_code == 200:
            info_data = info_response.json()
            if info_data.get("ok"):
                current_url = info_data["result"].get("url", "Not set")
                print(f"Current webhook URL: {current_url}")
                
                if current_url == WEBHOOK_URL:
                    print("✅ Webhook is already set to the correct URL.")
                    return
            else:
                print("ℹ️  Could not retrieve current webhook info")
        else:
            print(f"⚠️  Failed to get webhook info: {info_response.status_code}")
    except Exception as e:
        print(f"⚠️  Error getting webhook info: {e}")
    
    # Delete existing webhook first
    print("\n🗑️  Deleting any existing webhook...")
    try:
        delete_response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            timeout=10
        )
        
        if delete_response.status_code == 200:
            delete_data = delete_response.json()
            if delete_data.get("ok"):
                print("✅ Existing webhook deleted successfully")
            else:
                print(f"⚠️  Failed to delete webhook: {delete_data.get('description')}")
        else:
            print(f"⚠️  Failed to delete webhook: {delete_response.status_code}")
    except Exception as e:
        print(f"⚠️  Error deleting webhook: {e}")
    
    # Set new webhook
    print(f"\n📡 Setting new webhook to: {WEBHOOK_URL}")
    try:
        webhook_data = {
            "url": WEBHOOK_URL,
            "max_connections": 50,
            "allowed_updates": ["message", "callback_query", "chat_member", "my_chat_member"],
            "drop_pending_updates": True
        }
        
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json=webhook_data,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Response: {result}")
            
            if result.get("ok"):
                print("\n" + "="*60)
                print("✅ WEBHOOK SET SUCCESSFULLY!")
                print("="*60)
                print(f"Your bot is now configured to receive updates at:")
                print(f"{WEBHOOK_URL}")
                print("\nYour bot is ready to use! 🚀")
            else:
                print(f"\n❌ Failed to set webhook: {result.get('description')}")
        else:
            print(f"\n❌ HTTP Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out. Please try again.")
    except requests.exceptions.ConnectionError:
        print("❌ Connection error. Please check your internet connection.")
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")

def test_bot(token):
    """Test if bot is responding"""
    print("\n🧪 Testing bot connection...")
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/getMe",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                bot_info = data["result"]
                print(f"✅ Bot connected successfully!")
                print(f"   Bot ID: {bot_info['id']}")
                print(f"   Bot Name: {bot_info['first_name']}")
                print(f"   Username: @{bot_info.get('username', 'N/A')}")
                return True
            else:
                print(f"❌ Invalid bot token: {data.get('description')}")
        else:
            print(f"❌ Failed to connect to bot: {response.status_code}")
    except Exception as e:
        print(f"❌ Error testing bot: {e}")
    
    return False

def get_deployment_instructions():
    """Show deployment instructions"""
    print("\n" + "="*60)
    print("📋 DEPLOYMENT INSTRUCTIONS")
    print("="*60)
    print("\n1. Deploy your bot to a hosting platform:")
    print("   • Heroku: heroku create your-app-name")
    print("   • Render: https://render.com")
    print("   • Railway: https://railway.app")
    print("   • VPS: Any server with Python 3.11+")
    
    print("\n2. Set environment variables:")
    print("   • BOT_TOKEN: Your Telegram bot token")
    print("   • AWS_ACCESS_KEY_ID: AWS Access Key")
    print("   • AWS_SECRET_ACCESS_KEY: AWS Secret Key")
    print("   • AWS_BUCKET: Your S3 bucket name")
    print("   • WEBHOOK_URL: Your app URL + /webhook")
    
    print("\n3. Deploy your code")
    print("\n4. Run this script to set the webhook:")
    print("   python update_webhook.py")
    print("="*60)

if __name__ == "__main__":
    print("🤖 Diamond Trading Bot - Webhook Setup")
    print("="*60)
    
    # Show deployment instructions
    get_deployment_instructions()
    
    # Ask user if they want to proceed
    proceed = input("\nDo you want to set up the webhook now? (y/n): ").strip().lower()
    
    if proceed == 'y':
        # Test bot token first
        BOT_TOKEN = os.getenv("BOT_TOKEN")
        if not BOT_TOKEN:
            BOT_TOKEN = input("Enter your Telegram Bot Token: ").strip()
        
        if test_bot(BOT_TOKEN):
            os.environ["BOT_TOKEN"] = BOT_TOKEN
            update_telegram_webhook()
        else:
            print("\n❌ Please check your bot token and try again.")
    else:
        print("\nℹ️  You can run this script later with:")
        print("   python update_webhook.py")
        print("\nOr set environment variables and run:")
        print("   export BOT_TOKEN=your_token")
        print("   export WEBHOOK_URL=https://your-app.com/webhook")
        print("   python update_webhook.py")
