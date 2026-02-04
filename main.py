import asyncio
import nest_asyncio
import pandas as pd
import boto3
import re
from io import BytesIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import os
import json
import pytz
import uuid
import time
import unicodedata
import uvicorn
from typing import Optional, Dict, Any, List
import logging
import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# -------- SETUP LOGGING --------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------- GLOBAL FLAGS --------
BOT_STARTED = False
READ_ONLY_ACCOUNTS = False

# -------- TIMEZONE --------
IST = pytz.timezone("Asia/Kolkata")

# -------- CONFIGURATION --------
def load_env_config():
    """Load and validate all environment variables"""
    config = {
        "BOT_TOKEN": os.getenv("BOT_TOKEN"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION", "ap-south-1"),
        "AWS_BUCKET": os.getenv("AWS_BUCKET"),
        "PORT": int(os.environ.get("PORT", "10000")),  # Render PORT
        "PYTHON_VERSION": os.getenv("PYTHON_VERSION", "3.9"),
    }
    
    if not config["BOT_TOKEN"]:
        raise ValueError("❌ BOT_TOKEN environment variable not set")
    
    return config

CONFIG = load_env_config()

# -------- AWS CONFIGURATION --------
AWS_CONFIG = {
    "aws_access_key_id": CONFIG["AWS_ACCESS_KEY_ID"],
    "aws_secret_access_key": CONFIG["AWS_SECRET_ACCESS_KEY"],
    "region_name": CONFIG["AWS_REGION"]
}

# -------- S3 KEYS --------
ACCOUNTS_KEY = "users/accounts.xlsx"
STOCK_KEY = "stock/diamonds.xlsx"
SUPPLIER_STOCK_FOLDER = "stock/suppliers/"
COMBINED_STOCK_KEY = "stock/combined/all_suppliers_stock.xlsx"
ACTIVITY_LOG_FOLDER = "activity_logs/"
DEALS_FOLDER = "deals/"
DEAL_HISTORY_KEY = "deals/deal_history.xlsx"
NOTIFICATIONS_FOLDER = "notifications/"
SESSION_KEY = "sessions/logged_in_users.json"

# -------- INITIALIZE AWS CLIENTS --------
s3 = boto3.client("s3", **{k: v for k, v in AWS_CONFIG.items() if v})

# -------- INITIALIZE BOT --------
bot = Bot(token=CONFIG["BOT_TOKEN"])
dp = Dispatcher()

# -------- GLOBAL DATA STORES --------
logged_in_users = {}
user_state = {}
user_rate_limit = {}

# -------- KEYBOARDS --------
admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💎 View All Stock")],
        [KeyboardButton(text="👥 View Users")],
        [KeyboardButton(text="⏳ Pending Accounts")],
        [KeyboardButton(text="🏆 Supplier Leaderboard")],
        [KeyboardButton(text="🤝 View Deals")],
        [KeyboardButton(text="📑 User Activity Report")],
        [KeyboardButton(text="🗑 Delete Supplier Stock")],
        [KeyboardButton(text="🚪 Logout")]
    ],
    resize_keyboard=True
)

client_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💎 Search Diamonds")],
        [KeyboardButton(text="🔥 Smart Deals")],
        [KeyboardButton(text="🤝 Request Deal")],
        [KeyboardButton(text="🚪 Logout")]
    ],
    resize_keyboard=True
)

supplier_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📤 Upload Excel")],
        [KeyboardButton(text="📦 My Stock")],
        [KeyboardButton(text="📊 My Analytics")],
        [KeyboardButton(text="🤝 View Deals")],
        [KeyboardButton(text="📥 Download Sample Excel")],
        [KeyboardButton(text="🚪 Logout")]
    ],
    resize_keyboard=True
)

# -------- KEEP ALIVE FUNCTION --------
async def keep_alive_ping():
    """Keep Render service awake"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://localhost:{CONFIG["PORT"]}/health', timeout=5) as resp:
                logger.debug(f"Keep-alive ping: {resp.status}")
    except Exception as e:
        logger.debug(f"Keep-alive ping failed: {e}")

# -------- LIFESPAN MANAGER --------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager for startup/shutdown"""
    global BOT_STARTED
    
    # Startup
    logger.info("🤖 Diamond Trading Bot starting up...")
    
    try:
        # Load sessions
        load_sessions()
        
        # Delete webhook for polling
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook deleted")
        
        # Start keep-alive scheduler (દર 3 મિનિટે ping)
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            keep_alive_ping,
            IntervalTrigger(minutes=3),
            id='keep_alive'
        )
        scheduler.start()
        logger.info("✅ Keep-alive scheduler started (every 3 minutes)")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
    
    BOT_STARTED = True
    
    # Start bot polling
    asyncio.create_task(dp.start_polling(bot))
    
    # Start background tasks
    asyncio.create_task(session_cleanup_loop())
    asyncio.create_task(user_state_cleanup_loop())
    
    logger.info("✅ Bot startup complete")
    
    yield  # App runs here
    
    # Shutdown
    logger.info("🤖 Diamond Trading Bot shutting down...")
    
    # Save sessions before shutdown
    save_sessions()
    
    BOT_STARTED = False
    logger.info("✅ Bot shutdown complete")

# -------- FASTAPI APP --------
app = FastAPI(title="Diamond Trading Bot", lifespan=lifespan)

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Diamond Trading Bot",
        "bot_started": BOT_STARTED,
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(logged_in_users)
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy" if BOT_STARTED else "starting",
        "bot": "running" if BOT_STARTED else "stopped",
        "timestamp": datetime.now().isoformat()
    }

# -------- TEXT CLEANING FUNCTIONS --------
def clean_text(value: Any) -> str:
    """Clean and normalize text values"""
    if value is None:
        return ""
    value = str(value)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u00A0", " ").replace("\u200B", "")
    value = value.replace("\n", " ").replace("\r", " ")  # Replace newlines with spaces
    value = re.sub(r"\s+", " ", value)  # Collapse multiple spaces
    return value.strip()

def clean_password(val: Any) -> str:
    """Clean password, handling Excel .0 issue"""
    val = clean_text(val)
    if val.endswith(".0"):  # Excel password issue
        val = val[:-2]
    return val

def normalize_text(x: Any) -> str:
    """Normalize text for comparison"""
    return clean_text(x).lower()

def safe_excel(val: Any) -> Any:
    """Prevent Excel formula injection"""
    if isinstance(val, str) and val.startswith(("=", "+", "-", "@")):
        return "'" + val
    return val

# -------- USER MANAGEMENT FUNCTIONS --------
def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    """Get user by username from logged_in_users"""
    username = normalize_text(username)
    for uid, user_data in logged_in_users.items():
        if normalize_text(user_data.get("USERNAME", "")) == username:
            return {"TELEGRAM_ID": uid, **user_data}
    return None

def is_admin(user: Optional[Dict[str, Any]]) -> bool:
    """Check if user is admin - ONLY based on Excel file, not hardcoded"""
    if not user:
        return False
    
    role = normalize_text(user.get("ROLE", ""))
    return role == "admin"

def get_logged_user(uid: int) -> Optional[Dict[str, Any]]:
    """Get logged in user with session validation"""
    user = logged_in_users.get(uid)
    if not user:
        return None

    # Check session timeout
    last_active = user.get("last_active", 0)
    if time.time() - last_active > 3600:  # 1 hour timeout
        logged_in_users.pop(uid, None)
        save_sessions()
        return None

    # Update last active time
    user["last_active"] = time.time()
    return user

def touch_session(uid: int):
    """Update user's last active time"""
    if uid in logged_in_users:
        logged_in_users[uid]["last_active"] = time.time()
        save_sessions()

# -------- SESSION MANAGEMENT --------
def save_sessions():
    """Save sessions to S3"""
    try:
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=SESSION_KEY,
            Body=json.dumps(logged_in_users, default=str),
            ContentType="application/json"
        )
        logger.info(f"Saved {len(logged_in_users)} active sessions")
    except Exception as e:
        logger.error(f"Failed to save sessions: {e}")

def load_sessions():
    """Load sessions from S3"""
    global logged_in_users
    try:
        obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=SESSION_KEY)
        raw = json.loads(obj["Body"].read())
        logged_in_users = {int(k): v for k, v in raw.items()}
        logger.info(f"Loaded {len(logged_in_users)} sessions from S3")
    except Exception as e:
        logger.warning(f"No existing sessions or error loading: {e}")
        logged_in_users = {}

def cleanup_sessions():
    """Remove expired sessions"""
    now = time.time()
    expired = []
    
    for uid, data in list(logged_in_users.items()):
        if now - data.get("last_active", now) > 3600:  # 1 hour
            expired.append(uid)
    
    for uid in expired:
        user_data = logged_in_users.pop(uid, None)
        if user_data:
            logger.info(f"Expired session for user: {user_data.get('USERNAME')}")
    
    if expired:
        save_sessions()

async def session_cleanup_loop():
    """Background task to clean up expired sessions"""
    while True:
        try:
            cleanup_sessions()
            logger.debug("Session cleanup completed")
        except Exception as e:
            logger.error(f"Session cleanup error: {e}")
        await asyncio.sleep(600)  # Run every 10 minutes

async def user_state_cleanup_loop():
    """Background task to clean up old user states"""
    while True:
        try:
            now = time.time()
            stale_users = []
            
            for uid, state in list(user_state.items()):
                last_active = state.get("last_updated", 0)
                if now - last_active > 1800:  # 30 minutes
                    stale_users.append(uid)
            
            for uid in stale_users:
                user_state.pop(uid, None)
            
            if stale_users:
                logger.info(f"Cleaned up {len(stale_users)} stale user states")
                
        except Exception as e:
            logger.error(f"User state cleanup error: {e}")
        
        await asyncio.sleep(300)  # Run every 5 minutes

# -------- COMMAND HANDLERS --------
@dp.message(Command("start"))
async def start(message: types.Message):
    """Handle /start command"""
    await message.reply(
        "💎 Welcome to Diamond Trading Bot!\n\n"
        "Use /login to sign in\n"
        "Use /createaccount to register\n"
        "Use /help for assistance",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def help_command(message: types.Message):
    """Handle /help command"""
    help_text = """
🤖 **Diamond Trading Bot Help**

**Commands:**
• /start - Start the bot
• /login - Login to your account
• /createaccount - Register new account
• /logout - Logout from current session
• /reset - Reset login state

**Roles:**
• 👑 **Admin** - Manage users, view all stock, approve deals
• 💎 **Supplier** - Upload stock, view deals, analytics
• 🥂 **Client** - Search diamonds, request deals, smart deals

**Need help?** Contact system administrator.
"""
    await message.reply(help_text)

@dp.message(Command("createaccount"))
async def create_account(message: types.Message):
    """Start account creation process"""
    uid = message.from_user.id
    
    user_state[uid] = {
        "step": "create_username",
        "last_updated": time.time()
    }
    
    await message.reply(
        "📝 **Account Creation**\n\n"
        "Enter your desired username (minimum 3 characters):"
    )

@dp.message(Command("login"))
async def login_command(message: types.Message):
    """Start login process"""
    uid = message.from_user.id
    
    # Check if already logged in
    user = get_logged_user(uid)
    if user:
        await message.reply(
            f"ℹ️ You're already logged in as {user['USERNAME']}.\n"
            "Use /logout to sign out first."
        )
        return
    
    # Clear any existing state and start new login
    user_state.pop(uid, None)
    user_state[uid] = {
        "step": "login_username",
        "last_updated": time.time()
    }
    
    await message.reply("👤 Enter your username:")

@dp.message(Command("logout"))
async def logout_command(message: types.Message):
    """Handle /logout command"""
    uid = message.from_user.id
    user = get_logged_user(uid)
    
    if not user:
        await message.reply("ℹ️ You are not logged in.")
        return
    
    # Remove from logged in users
    logged_in_users.pop(uid, None)
    user_state.pop(uid, None)
    save_sessions()
    
    await message.reply(
        "✅ Successfully logged out.\n"
        "Use /login to sign in again.",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.message(Command("reset"))
async def reset_state_command(message: types.Message):
    """Reset user state"""
    uid = message.from_user.id
    user_state.pop(uid, None)
    await message.reply("✅ Login state reset. Use /login to start again.")

# -------- ACCOUNT CREATION FLOW --------
@dp.message(F.text.in_(["🔐 login", "login"]))
async def start_login_button(message: types.Message):
    """Handle login button press"""
    await login_command(message)

@dp.message()
async def handle_all_messages(message: types.Message):
    """Main message handler"""
    uid = message.from_user.id
    
    # Update session activity if logged in
    user = get_logged_user(uid)
    if user:
        touch_session(uid)
    
    # Handle state-based flows
    state = user_state.get(uid)
    if state:
        state["last_updated"] = time.time()
        
        # Account creation flow
        if state.get("step") == "create_username":
            username = message.text.strip().lower()
            
            if len(username) < 3:
                await message.reply("❌ Username must be at least 3 characters.")
                return
            
            # Create new account
            await message.reply(
                f"✅ Account created successfully!\n\n"
                f"Username: {username}\n"
                f"Password: 1234\n"
                f"Role: client\n\n"
                "Use /login to sign in."
            )
            user_state.pop(uid, None)
            return
        
        # Login flow
        elif state.get("step") == "login_username":
            username = message.text.strip()
            state["login_username"] = username
            state["step"] = "login_password"
            
            await message.reply("🔐 Enter password:")
            return
        
        elif state.get("step") == "login_password":
            password = message.text.strip()
            username = state.get("login_username", "")
            
            # Simple login - accept any username/password for testing
            logged_in_users[uid] = {
                "USERNAME": username,
                "ROLE": "client",
                "SUPPLIER_KEY": f"supplier_{username.lower()}" if username == "supplier" else None,
                "last_active": time.time()
            }
            save_sessions()
            
            # Determine keyboard based on role
            if username == "admin":
                kb = admin_kb
                welcome_msg = f"👑 Welcome Admin {username.capitalize()}"
            elif username == "supplier":
                kb = supplier_kb
                welcome_msg = f"💎 Welcome Supplier {username.capitalize()}"
            else:  # client
                kb = client_kb
                welcome_msg = f"🥂 Welcome {username.capitalize()}"
            
            await message.reply(welcome_msg, reply_markup=kb)
            user_state.pop(uid, None)
            return
    
    # Handle button presses for logged in users
    if user:
        await handle_logged_in_buttons(message, user)
    else:
        await message.reply(
            "🔒 Please login first using /login\n"
            "Or create an account using /createaccount"
        )

# -------- LOGGED IN BUTTON HANDLERS --------
async def handle_logged_in_buttons(message: types.Message, user: Dict):
    """Handle button presses for logged in users"""
    text = message.text
    
    # Admin buttons
    if user["ROLE"] == "admin":
        if text == "💎 View All Stock":
            await message.reply("📊 Stock view feature coming soon...")
        elif text == "👥 View Users":
            await message.reply("👥 User management coming soon...")
        elif text == "⏳ Pending Accounts":
            await message.reply("⏳ No pending accounts.")
        elif text == "🏆 Supplier Leaderboard":
            await message.reply("🏆 Leaderboard coming soon...")
        elif text == "🤝 View Deals":
            await message.reply("🤝 Deals view coming soon...")
        elif text == "📑 User Activity Report":
            await message.reply("📑 Activity report coming soon...")
        elif text == "🗑 Delete Supplier Stock":
            await message.reply("🗑 Delete feature coming soon...")
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")
    
    # Supplier buttons
    elif user["ROLE"] == "supplier":
        if text == "📤 Upload Excel":
            await message.reply("📤 Upload feature coming soon...")
        elif text == "📦 My Stock":
            await message.reply("📦 Your stock view coming soon...")
        elif text == "📊 My Analytics":
            await message.reply("📊 Analytics coming soon...")
        elif text == "🤝 View Deals":
            await message.reply("🤝 Deals view coming soon...")
        elif text == "📥 Download Sample Excel":
            await message.reply("📥 Sample Excel coming soon...")
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")
    
    # Client buttons
    else:
        if text == "💎 Search Diamonds":
            await message.reply("💎 Search feature coming soon...")
        elif text == "🔥 Smart Deals":
            await message.reply("🔥 Smart deals coming soon...")
        elif text == "🤝 Request Deal":
            await message.reply("🤝 Deal request coming soon...")
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")

# -------- MAIN ENTRY POINT --------
if __name__ == "__main__":
    nest_asyncio.apply()
    
    # Render uses PORT environment variable
    port = int(os.environ.get("PORT", 10000))
    
    logger.info(f"🚀 Starting Diamond Trading Bot")
    logger.info(f"📊 Python: {CONFIG['PYTHON_VERSION']}")
    logger.info(f"🌐 Port: {port}")
    logger.info(f"🤖 Bot Token: {CONFIG['BOT_TOKEN'][:10]}...")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
