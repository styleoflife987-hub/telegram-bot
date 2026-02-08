import os
import sys
import requests
from dotenv import load_dotenv

def setup_webhook_for_render():
    """
    Setup webhook for Diamond Trading Bot deployed on Render
    """
    print("="*60)
    print("🤖 Diamond Trading Bot - Webhook Setup for Render")
    print("="*60)
    
    # Try to load from .env file
    load_dotenv()
    
    # Get bot token
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN not found in environment variables.")
        BOT_TOKEN = input("Enter your Telegram Bot Token: ").strip()
        if not BOT_TOKEN:
            print("❌ Bot token is required.")
            sys.exit(1)
    
    # Get Render app URL
    RENDER_URL = os.getenv("RENDER_URL") or os.getenv("WEBHOOK_URL")
    if not RENDER_URL:
        print("\n🌐 Enter your Render application URL")
        print("   Example: https://diamond-trading-bot.onrender.com")
        RENDER_URL = input("Render URL: ").strip()
        if not RENDER_URL.startswith("https://"):
            RENDER_URL = f"https://{RENDER_URL}"
    
    # Ensure it has the /webhook endpoint
    if not RENDER_URL.endswith("/webhook"):
        RENDER_URL = f"{RENDER_URL.rstrip('/')}/webhook"
    
    print("\n" + "="*60)
    print("📋 CONFIGURATION SUMMARY")
    print("="*60)
    print(f"Bot Token: {BOT_TOKEN[:10]}...{BOT_TOKEN[-10:]}")
    print(f"Webhook URL: {RENDER_URL}")
    print("="*60 + "\n")
    
    # Test if Render app is accessible
    print("🔍 Testing Render application...")
    try:
        health_check = RENDER_URL.replace("/webhook", "/health")
        response = requests.get(health_check, timeout=10)
        if response.status_code == 200:
            print("✅ Render application is accessible")
        else:
            print(f"⚠️  Render application returned status {response.status_code}")
            print(f"   Make sure your bot is deployed and running on Render")
    except Exception as e:
        print(f"⚠️  Could not reach Render application: {e}")
        print("   Make sure your bot is deployed and running")
    
    # Delete existing webhook
    print("\n🗑️  Deleting existing webhook...")
    try:
        delete_response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
            timeout=10
        )
        if delete_response.status_code == 200:
            print("✅ Existing webhook deleted")
    except Exception as e:
        print(f"⚠️  Error deleting webhook: {e}")
    
    # Set new webhook
    print(f"\n📡 Setting webhook to Render...")
    try:
        webhook_data = {
            "url": RENDER_URL,
            "max_connections": 50,
            "allowed_updates": ["message", "callback_query"],
            "drop_pending_updates": True
        }
        
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json=webhook_data,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get("ok"):
                print("\n" + "="*60)
                print("✅ WEBHOOK SETUP SUCCESSFUL!")
                print("="*60)
                print(f"Your bot is now connected to Render at:")
                print(f"{RENDER_URL}")
                print("\n🎉 Your Diamond Trading Bot is ready to use!")
                print("\n📋 NEXT STEPS:")
                print("1. Open Telegram and search for your bot")
                print("2. Start a chat with /start command")
                print("3. Create an account with /createaccount")
                print("4. Test all features")
            else:
                print(f"\n❌ Failed to set webhook: {result.get('description')}")
        else:
            print(f"\n❌ HTTP Error {response.status_code}")
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out. Please try again.")
    except requests.exceptions.ConnectionError:
        print("❌ Connection error. Please check your internet.")
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")

def check_bot_status(token):
    """Check if bot is working"""
    print("\n🔧 Checking bot status...")
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/getMe",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                bot = data["result"]
                print(f"✅ Bot is working:")
                print(f"   Name: {bot['first_name']}")
                print(f"   Username: @{bot.get('username', 'N/A')}")
                print(f"   ID: {bot['id']}")
                return True
    except Exception as e:
        print(f"❌ Error checking bot: {e}")
    
    return False

def get_webhook_info(token):
    """Get current webhook information"""
    print("\n📡 Getting current webhook info...")
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{token}/getWebhookInfo",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                info = data["result"]
                print(f"Current webhook URL: {info.get('url', 'Not set')}")
                print(f"Pending updates: {info.get('pending_update_count', 0)}")
                if info.get('last_error_date'):
                    print(f"Last error: {info.get('last_error_message')}")
                return info
    except Exception as e:
        print(f"❌ Error getting webhook info: {e}")
    
    return None

if __name__ == "__main__":
    print("🚀 Diamond Trading Bot Setup")
    print("="*60)
    
    # Check if running in Render-like environment
    is_render = os.getenv("RENDER", False)
    if is_render:
        print("🌐 Detected Render environment")
        print("Webhook setup will be automatic after deployment")
        sys.exit(0)
    
    # Manual setup
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        BOT_TOKEN = input("Enter your Telegram Bot Token: ").strip()
    
    if not BOT_TOKEN:
        print("❌ Bot token is required.")
        sys.exit(1)
    
    # Check bot status
    if check_bot_status(BOT_TOKEN):
        # Show current webhook info
        get_webhook_info(BOT_TOKEN)
        
        # Ask user if they want to setup webhook
        print("\n" + "="*60)
        choice = input("Do you want to setup webhook for Render? (y/n): ").strip().lower()
        
        if choice == 'y':
            os.environ["BOT_TOKEN"] = BOT_TOKEN
            setup_webhook_for_render()
        else:
            print("\nℹ️  Webhook setup skipped.")
            print("You can run this script later when you're ready.")
    else:
        print("\n❌ Invalid bot token. Please check and try again.")
