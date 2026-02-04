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

# -------- STATUS CONSTANTS --------
YES = "YES"
NO = "NO"
STATUS_PENDING = "PENDING"
STATUS_ACCEPTED = "ACCEPTED"
STATUS_REJECTED = "REJECTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_CLOSED = "CLOSED"

# -------- CONFIGURATION --------
def load_env_config():
    """Load and validate all environment variables"""
    config = {
        "BOT_TOKEN": os.getenv("BOT_TOKEN"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION", "ap-south-1"),
        "AWS_BUCKET": os.getenv("AWS_BUCKET"),
        "PORT": int(os.getenv("PORT", "10000")),
        "PYTHON_VERSION": os.getenv("PYTHON_VERSION", "3.9"),
        "SESSION_TIMEOUT": int(os.getenv("SESSION_TIMEOUT", "3600")),  # 1 hour
        "RATE_LIMIT": int(os.getenv("RATE_LIMIT", "5")),  # messages per window
        "RATE_LIMIT_WINDOW": int(os.getenv("RATE_LIMIT_WINDOW", "10")),  # seconds
    }
    
    # Validate required configurations
    if not config["BOT_TOKEN"]:
        raise ValueError("❌ BOT_TOKEN environment variable not set")
    
    if not all([config["AWS_ACCESS_KEY_ID"], config["AWS_SECRET_ACCESS_KEY"], config["AWS_BUCKET"]]):
        logger.warning("AWS credentials not fully set. Some features may not work.")
    
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
    if time.time() - last_active > CONFIG["SESSION_TIMEOUT"]:
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
        if now - data.get("last_active", now) > CONFIG["SESSION_TIMEOUT"]:
            expired.append(uid)
    
    for uid in expired:
        user_data = logged_in_users.pop(uid, None)
        if user_data:
            log_activity(user_data, "SESSION_EXPIRED")
            logger.info(f"Expired session for user: {user_data.get('USERNAME')}")
    
    if expired:
        save_sessions()

# -------- RATE LIMITING --------
def is_rate_limited(uid: int) -> bool:
    """Check if user is rate limited"""
    now = time.time()
    window = CONFIG["RATE_LIMIT_WINDOW"]
    limit = CONFIG["RATE_LIMIT"]
    
    history = user_rate_limit.get(uid, [])
    history = [t for t in history if now - t < window]
    
    if len(history) >= limit:
        return True
    
    history.append(now)
    user_rate_limit[uid] = history[-10:]  # Keep last 10 timestamps
    return False

# -------- DATA LOADING/SAVING --------
def load_accounts() -> pd.DataFrame:
    """Load accounts from Excel file in S3"""
    try:
        local_path = "/tmp/accounts.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], ACCOUNTS_KEY, local_path)
        df = pd.read_excel(local_path, dtype=str)
        
        # Clean columns
        required_cols = ["USERNAME", "PASSWORD", "ROLE", "APPROVED"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
            
            df[col] = df[col].fillna("").astype(str).apply(clean_text)
        
        # Log loaded accounts (without passwords for security)
        logger.info(f"Loaded {len(df)} accounts from S3")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load accounts: {e}")
        # Create default dataframe with prince as admin
        return pd.DataFrame({
            "USERNAME": ["prince"],
            "PASSWORD": ["1234"],
            "ROLE": ["admin"],
            "APPROVED": ["YES"]
        })

def save_accounts(df: pd.DataFrame):
    """Save accounts to Excel file in S3"""
    if READ_ONLY_ACCOUNTS:
        logger.warning("Accounts file is READ ONLY. Skipping save.")
        return
    
    try:
        local_path = "/tmp/accounts.xlsx"
        df.to_excel(local_path, index=False)
        s3.upload_file(local_path, CONFIG["AWS_BUCKET"], ACCOUNTS_KEY)
        logger.info(f"Saved {len(df)} accounts to S3")
    except Exception as e:
        logger.error(f"Failed to save accounts: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

def load_stock() -> pd.DataFrame:
    """Load combined stock from S3"""
    try:
        local_path = "/tmp/all_suppliers_stock.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY, local_path)
        df = pd.read_excel(local_path)
        logger.info(f"Loaded {len(df)} stock items from S3")
        return df
    except Exception as e:
        logger.warning(f"Failed to load stock: {e}")
        return pd.DataFrame()

# -------- ACTIVITY LOGGING --------
def log_activity(user: Dict[str, Any], action: str, details: Optional[Dict] = None):
    """Log user activity to S3"""
    try:
        ist_time = datetime.now(IST)
        log_entry = {
            "date": ist_time.strftime("%Y-%m-%d"),
            "time": ist_time.strftime("%H:%M:%S"),
            "login_id": user.get("USERNAME"),
            "role": user.get("ROLE"),
            "action": action,
            "details": details or {},
            "telegram_id": user.get("TELEGRAM_ID", "N/A")
        }
        
        key = f"{ACTIVITY_LOG_FOLDER}{log_entry['date']}/{log_entry['login_id']}.json"
        
        # Try to load existing logs
        try:
            obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
            data = json.loads(obj["Body"].read())
        except:
            data = []
        
        data.append(log_entry)
        
        # Save to S3
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        logger.info(f"Logged activity: {user.get('USERNAME')} - {action}")
        
    except Exception as e:
        logger.error(f"Failed to log activity: {e}")

def generate_activity_excel() -> Optional[str]:
    """Generate Excel report of all activities"""
    try:
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=ACTIVITY_LOG_FOLDER
        )

        if "Contents" not in objs or not objs["Contents"]:
            return None

        rows = []

        for obj in objs["Contents"]:
            if not obj["Key"].endswith(".json"):
                continue

            try:
                raw = s3.get_object(
                    Bucket=CONFIG["AWS_BUCKET"],
                    Key=obj["Key"]
                )["Body"].read().decode("utf-8")

                data = json.loads(raw)
                for entry in data:
                    rows.append({
                        "Date": entry.get("date"),
                        "Time": entry.get("time"),
                        "Login ID": entry.get("login_id"),
                        "Role": entry.get("role"),
                        "Action": entry.get("action"),
                        "Details": json.dumps(entry.get("details", {}))
                    })
            except Exception as e:
                logger.error(f"Failed to read activity file {obj['Key']}: {e}")
                continue

        if not rows:
            return None

        df = pd.DataFrame(rows)
        path = "/tmp/user_activity_report.xlsx"
        df.to_excel(path, index=False)
        
        logger.info(f"Generated activity report with {len(rows)} entries")
        return path

    except Exception as e:
        logger.error(f"Activity report error: {e}")
        return None

# -------- NOTIFICATION SYSTEM --------
def save_notification(username: str, role: str, message: str):
    """Save notification for user"""
    try:
        key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
        
        # Load existing notifications
        try:
            obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
            data = json.loads(obj["Body"].read())
        except:
            data = []
        
        # Add new notification
        data.append({
            "message": message,
            "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            "read": False
        })
        
        # Save to S3
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
    except Exception as e:
        logger.error(f"Failed to save notification: {e}")

def fetch_unread_notifications(username: str, role: str) -> List[Dict]:
    """Fetch unread notifications for user"""
    try:
        key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
        obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
        data = json.loads(obj["Body"].read())
        
        unread = [n for n in data if not n.get("read")]
        
        # Mark as read
        for n in data:
            n["read"] = True
        
        # Save updated data
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        return unread
        
    except Exception:
        return []

# -------- STOCK MANAGEMENT --------
def rebuild_combined_stock():
    """Rebuild combined stock from all supplier files"""
    try:
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=SUPPLIER_STOCK_FOLDER
        )
        
        if "Contents" not in objs:
            return
        
        dfs = []
        
        for obj in objs.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".xlsx"):
                continue
            
            try:
                local_path = f"/tmp/{key.split('/')[-1]}"
                s3.download_file(CONFIG["AWS_BUCKET"], key, local_path)
                df = pd.read_excel(local_path)
                df["SUPPLIER"] = key.split("/")[-1].replace(".xlsx", "").lower()
                dfs.append(df)
                
                # Clean up temp file
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
            except Exception as e:
                logger.error(f"Failed to process {key}: {e}")
                continue
        
        if not dfs:
            return
        
        # Combine all dataframes
        final_df = pd.concat(dfs, ignore_index=True)
        
        # Define desired columns
        desired_columns = [
            "Stock #", "Availability", "Shape", "Weight", "Color", "Clarity", "Cut", "Polish", "Symmetry",
            "Fluorescence Color", "Measurements", "Shade", "Milky", "Eye Clean", "Lab", "Report #", "Location",
            "Treatment", "Discount", "Price Per Carat", "Final Price", "Depth %", "Table %", "Girdle Thin",
            "Girdle Thick", "Girdle %", "Girdle Condition", "Culet Size", "Culet Condition", "Crown Height",
            "Crown Angle", "Pavilion Depth", "Pavilion Angle", "Inscription", "Cert comment", "KeyToSymbols",
            "White Inclusion", "Black Inclusion", "Open Inclusion", "Fancy Color", "Fancy Color Intensity",
            "Fancy Color Overtone", "Country", "State", "City", "CertFile", "Diamond Video", "Diamond Image",
            "SUPPLIER", "LOCKED", "Diamond Type"
        ]
        
        # Add missing columns
        for col in desired_columns:
            if col not in final_df.columns:
                final_df[col] = ""
        
        if "Diamond Type" not in final_df.columns:
            final_df["Diamond Type"] = "Unknown"
        
        final_df["LOCKED"] = final_df.get("LOCKED", "NO")
        final_df = final_df[desired_columns]
        
        # Save to local file and upload to S3
        local_path = "/tmp/all_suppliers_stock.xlsx"
        final_df.to_excel(local_path, index=False)
        s3.upload_file(local_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        logger.info(f"Rebuilt combined stock with {len(final_df)} items from {len(dfs)} suppliers")
        
        # Clean up
        if os.path.exists(local_path):
            os.remove(local_path)
            
    except Exception as e:
        logger.error(f"Error rebuilding combined stock: {e}")

def atomic_lock_stone(stone_id: str) -> bool:
    """Atomically lock a stone to prevent race conditions"""
    try:
        local_path = "/tmp/current_stock.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY, local_path)
        df = pd.read_excel(local_path)
        
        if df.empty or "Stock #" not in df.columns or "LOCKED" not in df.columns:
            return False
        
        # Check if stone exists and is not locked
        mask = (df["Stock #"] == stone_id) & (df["LOCKED"] != "YES")
        if not mask.any():
            return False
        
        # Lock the stone
        df.loc[mask, "LOCKED"] = "YES"
        
        # Prevent formula injection
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)
        
        # Save changes
        temp_path = "/tmp/locked_stock.xlsx"
        df.to_excel(temp_path, index=False)
        s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        # Also update in supplier's individual file
        stone_row = df[df["Stock #"] == stone_id].iloc[0]
        supplier = stone_row.get("SUPPLIER", "")
        
        if supplier:
            supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier}.xlsx"
            try:
                s3.download_file(CONFIG["AWS_BUCKET"], supplier_file, "/tmp/supplier_stock.xlsx")
                supplier_df = pd.read_excel("/tmp/supplier_stock.xlsx")
                
                if "Stock #" in supplier_df.columns and "LOCKED" in supplier_df.columns:
                    supplier_df.loc[supplier_df["Stock #"] == stone_id, "LOCKED"] = "YES"
                    
                    # Prevent formula injection
                    for col in supplier_df.select_dtypes(include="object"):
                        supplier_df[col] = supplier_df[col].map(safe_excel)
                    
                    supplier_df.to_excel("/tmp/supplier_stock.xlsx", index=False)
                    s3.upload_file("/tmp/supplier_stock.xlsx", CONFIG["AWS_BUCKET"], supplier_file)
            except Exception as e:
                logger.error(f"Failed to update supplier file: {e}")
        
        # Clean up temp files
        for path in [local_path, temp_path, "/tmp/supplier_stock.xlsx"]:
            if os.path.exists(path):
                os.remove(path)
        
        logger.info(f"Locked stone: {stone_id}")
        return True
        
    except Exception as e:
        logger.error(f"Atomic lock failed for stone {stone_id}: {e}")
        return False

def unlock_stone(stone_id: str):
    """Unlock a stone"""
    try:
        df = load_stock()
        if df.empty:
            return
        
        if "Stock #" not in df.columns or "LOCKED" not in df.columns:
            return
        
        # Unlock the stone
        df.loc[df["Stock #"] == stone_id, "LOCKED"] = "NO"
        
        # Save to combined stock
        temp_path = "/tmp/all_suppliers_stock.xlsx"
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)
        
        df.to_excel(temp_path, index=False)
        s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        # Also update in supplier's file
        stone_row = df[df["Stock #"] == stone_id].iloc[0]
        supplier = stone_row.get("SUPPLIER", "")
        
        if supplier:
            supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier}.xlsx"
            try:
                s3.download_file(CONFIG["AWS_BUCKET"], supplier_file, "/tmp/supplier_stock.xlsx")
                supplier_df = pd.read_excel("/tmp/supplier_stock.xlsx")
                
                if "Stock #" in supplier_df.columns and "LOCKED" in supplier_df.columns:
                    supplier_df.loc[supplier_df["Stock #"] == stone_id, "LOCKED"] = "NO"
                    supplier_df.to_excel("/tmp/supplier_stock.xlsx", index=False)
                    s3.upload_file("/tmp/supplier_stock.xlsx", CONFIG["AWS_BUCKET"], supplier_file)
            except:
                pass
        
        # Clean up
        for path in [temp_path, "/tmp/supplier_stock.xlsx"]:
            if os.path.exists(path):
                os.remove(path)
        
        logger.info(f"Unlocked stone: {stone_id}")
        
    except Exception as e:
        logger.error(f"Failed to unlock stone {stone_id}: {e}")

def remove_stone_from_supplier_and_combined(stone_id: str):
    """Remove stone from both supplier and combined stock"""
    try:
        # Remove from combined stock
        df = load_stock()
        if not df.empty and "Stock #" in df.columns:
            df = df[df["Stock #"] != stone_id]
            
            temp_path = "/tmp/all_suppliers_stock.xlsx"
            df.to_excel(temp_path, index=False)
            s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
            
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        # Remove from supplier's individual file
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=SUPPLIER_STOCK_FOLDER
        )
        
        for obj in objs.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".xlsx"):
                continue
            
            local_path = "/tmp/tmp_supplier.xlsx"
            s3.download_file(CONFIG["AWS_BUCKET"], key, local_path)
            sdf = pd.read_excel(local_path)
            
            if "Stock #" in sdf.columns and stone_id in sdf["Stock #"].values:
                sdf = sdf[sdf["Stock #"] != stone_id]
                sdf.to_excel(local_path, index=False)
                s3.upload_file(local_path, CONFIG["AWS_BUCKET"], key)
                break
        
        logger.info(f"Removed stone {stone_id} from all stock files")
        
    except Exception as e:
        logger.error(f"Failed to remove stone {stone_id}: {e}")

# -------- DEAL MANAGEMENT --------
def log_deal_history(deal: Dict[str, Any]):
    """Log deal to history file"""
    try:
        local_path = "/tmp/deal_history.xlsx"
        
        # Try to load existing history
        try:
            s3.download_file(CONFIG["AWS_BUCKET"], DEAL_HISTORY_KEY, local_path)
            df = pd.read_excel(local_path)
        except:
            df = pd.DataFrame(columns=[
                "Deal ID", "Stone ID", "Supplier", "Client", "Actual Price",
                "Offer Price", "Supplier Action", "Admin Action", "Final Status", "Created At"
            ])
        
        # Add new deal entry
        new_row = pd.DataFrame([{
            "Deal ID": deal.get("deal_id"),
            "Stone ID": deal.get("stone_id"),
            "Supplier": deal.get("supplier_username"),
            "Client": deal.get("client_username"),
            "Actual Price": deal.get("actual_stock_price"),
            "Offer Price": deal.get("client_offer_price"),
            "Supplier Action": deal.get("supplier_action"),
            "Admin Action": deal.get("admin_action"),
            "Final Status": deal.get("final_status"),
            "Created At": deal.get("created_at"),
        }])
        
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_excel(local_path, index=False)
        s3.upload_file(local_path, CONFIG["AWS_BUCKET"], DEAL_HISTORY_KEY)
        
        logger.info(f"Logged deal to history: {deal.get('deal_id')}")
        
    except Exception as e:
        logger.error(f"Failed to log deal history: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# -------- BACKGROUND TASKS --------
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
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
    
    BOT_STARTED = True
    
    # Start background tasks
    asyncio.create_task(dp.start_polling(bot))
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
        "active_users": len(logged_in_users),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/sessions")
async def get_sessions():
    """Admin endpoint to view active sessions"""
    return {
        "active_sessions": len(logged_in_users),
        "sessions": logged_in_users
    }

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
    
    if is_rate_limited(uid):
        await message.reply("⏳ Please wait before creating another account.")
        return
    
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
    
    if is_rate_limited(uid):
        await message.reply("⏳ Please wait before trying to login again.")
        return
    
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
    
    # Log activity
    log_activity(user, "LOGOUT")
    
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
    
    # Rate limiting
    if is_rate_limited(uid):
        await message.reply("⏳ Too many messages. Please slow down.")
        return
    
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
            
            # Check if username exists
            df = load_accounts()
            if not df[df["USERNAME"].str.lower() == username].empty:
                await message.reply("❌ Username already exists.")
                user_state.pop(uid, None)
                return
            
            state["username"] = username
            state["step"] = "create_password"
            
            await message.reply("🔐 Enter password (minimum 4 characters):")
            return
        
        elif state.get("step") == "create_password":
            password = message.text.strip()
            
            if len(password) < 4:
                await message.reply("❌ Password must be at least 4 characters.")
                return
            
            username = state["username"]
            
            # Create new account
            df = load_accounts()
            new_row = {
                "USERNAME": username,
                "PASSWORD": clean_password(password),
                "ROLE": "client",  # Default role for new users
                "APPROVED": "NO"   # Needs admin approval
            }
            
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            save_accounts(df)
            
            user_state.pop(uid, None)
            
            await message.reply(
                "✅ Account created successfully!\n\n"
                "⏳ Your account is pending admin approval.\n"
                "You'll be notified once approved.\n\n"
                "Use /login after approval."
            )
            
            # Notify admins
            admin_df = df[df["ROLE"] == "admin"]
            for _, admin in admin_df.iterrows():
                save_notification(
                    admin["USERNAME"],
                    "admin",
                    f"📝 New account pending approval: {username}"
                )
            
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
            
            # Debug logging
            logger.info(f"Login attempt - Username entered: '{username}'")
            logger.info(f"Login attempt - Password entered: '{password}'")
            
            # Validate login
            df = load_accounts()
            
            # Debug: Show what's in the dataframe
            logger.info(f"Accounts in database: {df[['USERNAME', 'ROLE', 'APPROVED']].to_dict('records')}")
            
            # Clean and normalize data
            df["USERNAME"] = df["USERNAME"].apply(normalize_text)
            df["PASSWORD"] = df["PASSWORD"].apply(clean_password)
            df["APPROVED"] = df["APPROVED"].apply(normalize_text).str.upper()
            df["ROLE"] = df["ROLE"].apply(normalize_text)
            
            username_clean = normalize_text(username)
            password_clean = clean_password(password)
            
            logger.info(f"Cleaned username: '{username_clean}'")
            logger.info(f"Cleaned password: '{password_clean}'")
            logger.info(f"Available usernames in DB: {df['USERNAME'].tolist()}")
            logger.info(f"Available roles in DB: {df['ROLE'].tolist()}")
            
            # Find matching user
            user_row = df[
                (df["USERNAME"] == username_clean) &
                (df["PASSWORD"] == password_clean) &
                (df["APPROVED"] == "YES")
            ]
            
            if user_row.empty:
                logger.warning(f"Login failed for username: {username_clean}")
                logger.warning(f"Available usernames: {df['USERNAME'].tolist()}")
                logger.warning(f"Approved statuses: {df['APPROVED'].tolist()}")
                
                await message.reply(
                    "❌ Invalid login credentials\n\n"
                    "Possible reasons:\n"
                    "• Username/password incorrect\n"
                    "• Account not approved\n"
                    "• Account doesn't exist\n\n"
                    "Please check your credentials and try again."
                )
                user_state.pop(uid, None)
                return
            
            # Login successful - Get role DIRECTLY from Excel
            user_data = user_row.iloc[0].to_dict()
            role = user_data["ROLE"].lower()
            
            logger.info(f"User {username} logged in with role: {role} (from Excel)")
            
            # Store user session - NO forced admin role
            logged_in_users[uid] = {
                "USERNAME": user_data["USERNAME"],
                "ROLE": role,  # Direct from Excel
                "SUPPLIER_KEY": f"supplier_{user_data['USERNAME'].lower()}" if role == "supplier" else None,
                "last_active": time.time()
            }
            save_sessions()
            
            # Log activity
            log_activity(logged_in_users[uid], "LOGIN")
            
            # Determine keyboard based on role from Excel
            if role == "admin":
                kb = admin_kb
                welcome_msg = f"👑 Welcome Admin {user_data['USERNAME'].capitalize()}"
            elif role == "supplier":
                kb = supplier_kb
                welcome_msg = f"💎 Welcome Supplier {user_data['USERNAME'].capitalize()}"
            else:  # client or any other role
                kb = client_kb
                welcome_msg = f"🥂 Welcome {user_data['USERNAME'].capitalize()}"
            
            await message.reply(welcome_msg, reply_markup=kb)
            
            # Check for notifications
            notifications = fetch_unread_notifications(user_data["USERNAME"], role)
            if notifications:
                note_msg = "🔔 **Unread Notifications**\n\n"
                for note in notifications[:5]:  # Show only first 5
                    note_msg += f"• {note['message']}\n   🕒 {note['time']}\n\n"
                
                if len(notifications) > 5:
                    note_msg += f"... and {len(notifications) - 5} more\n"
                
                await message.reply(note_msg)
            
            user_state.pop(uid, None)
            return
        
        # Diamond search flow
        elif state.get("step", "").startswith("search_"):
            await handle_search_flow(message, state)
            return
        
        # Deal request flow
        elif state.get("step", "").startswith("deal_"):
            await handle_deal_flow(message, state)
            return
    
    # Handle button presses for logged in users
    if user:
        await handle_logged_in_buttons(message, user)
    else:
        await message.reply(
            "🔒 Please login first using /login\n"
            "Or create an account using /createaccount"
        )

# -------- SEARCH FLOW HANDLER --------
async def handle_search_flow(message: types.Message, state: Dict):
    """Handle diamond search flow"""
    uid = message.from_user.id
    text = message.text.strip().lower()
    current_step = state["step"]
    search = state.setdefault("search", {})
    
    if current_step == "search_carat":
        search["carat"] = text
        state["step"] = "search_shape"
        await message.reply("Enter Shape(s) (e.g., round, oval) or 'any':")
        
    elif current_step == "search_shape":
        search["shape"] = text
        state["step"] = "search_color"
        await message.reply("Enter Color(s) (e.g., d, e, f) or 'any':")
        
    elif current_step == "search_color":
        search["color"] = text
        state["step"] = "search_clarity"
        await message.reply("Enter Clarity(ies) (e.g., vs, vvs) or 'any':")
        
    elif current_step == "search_clarity":
        search["clarity"] = text
        
        # Perform search
        user = get_logged_user(uid)
        if not user:
            await message.reply("❌ Session expired. Please login again.")
            user_state.pop(uid, None)
            return
        
        df = load_stock()
        if df.empty:
            await message.reply("❌ No diamonds available in stock.")
            user_state.pop(uid, None)
            return
        
        # Apply filters
        filtered_df = df.copy()
        
        # Carat filter
        if search["carat"] != "any":
            try:
                if "-" in search["carat"]:
                    min_carat, max_carat = map(float, search["carat"].split("-"))
                    filtered_df = filtered_df[
                        (filtered_df["Weight"] >= min_carat) & 
                        (filtered_df["Weight"] <= max_carat)
                    ]
                else:
                    target_carat = float(search["carat"])
                    filtered_df = filtered_df[
                        (filtered_df["Weight"] >= target_carat * 0.9) & 
                        (filtered_df["Weight"] <= target_carat * 1.1)
                    ]
            except:
                await message.reply("❌ Invalid carat format. Use like '1.5' or '1-2'")
                user_state.pop(uid, None)
                return
        
        # Shape filter
        if search["shape"] != "any":
            shapes = [s.strip() for s in search["shape"].split(",")]
            filtered_df = filtered_df[
                filtered_df["Shape"].str.lower().isin([s.lower() for s in shapes])
            ]
        
        # Color filter
        if search["color"] != "any":
            colors = [c.strip().upper() for c in search["color"].split(",")]
            filtered_df = filtered_df[
                filtered_df["Color"].str.upper().isin(colors)
            ]
        
        # Clarity filter
        if search["clarity"] != "any":
            clarities = [c.strip().upper() for c in search["clarity"].split(",")]
            filtered_df = filtered_df[
                filtered_df["Clarity"].str.upper().isin(clarities)
            ]
        
        if filtered_df.empty:
            await message.reply("❌ No diamonds match your search criteria.")
            user_state.pop(uid, None)
            return
        
        # Show results
        total_diamonds = len(filtered_df)
        total_carats = filtered_df["Weight"].sum()
        
        if total_diamonds > 10:
            # Send as Excel file
            excel_path = "/tmp/search_results.xlsx"
            filtered_df.to_excel(excel_path, index=False)
            
            await message.reply_document(
                types.FSInputFile(excel_path),
                caption=(
                    f"💎 Found {total_diamonds} diamonds\n"
                    f"📊 Total weight: {total_carats:.2f} ct\n"
                    f"🎯 Your filters:\n"
                    f"• Carat: {search['carat']}\n"
                    f"• Shape: {search['shape']}\n"
                    f"• Color: {search['color']}\n"
                    f"• Clarity: {search['clarity']}"
                )
            )
            
            if os.path.exists(excel_path):
                os.remove(excel_path)
        else:
            # Send individual messages
            for _, row in filtered_df.iterrows():
                msg = (
                    f"💎 **{row['Stock #']}**\n"
                    f"📐 Shape: {row.get('Shape', 'N/A')}\n"
                    f"⚖️ Weight: {row.get('Weight', 'N/A')} ct\n"
                    f"🎨 Color: {row.get('Color', 'N/A')}\n"
                    f"✨ Clarity: {row.get('Clarity', 'N/A')}\n"
                    f"💰 Price: ${row.get('Price Per Carat', 'N/A')}/ct\n"
                    f"🔒 Status: {row.get('LOCKED', 'NO')}\n"
                    f"🏛 Lab: {row.get('Lab', 'N/A')}"
                )
                await message.reply(msg)
        
        # Log activity
        log_activity(user, "SEARCH", {
            "filters": search,
            "results": total_diamonds
        })
        
        user_state.pop(uid, None)

# -------- DEAL FLOW HANDLER --------
async def handle_deal_flow(message: types.Message, state: Dict):
    """Handle deal request flow"""
    uid = message.from_user.id
    text = message.text.strip()
    current_step = state["step"]
    
    if current_step == "deal_stone":
        state["stone_id"] = text
        state["step"] = "deal_price"
        await message.reply("💰 Enter your offer price ($ per carat):")
        
    elif current_step == "deal_price":
        try:
            offer_price = float(text)
            if offer_price <= 0:
                await message.reply("❌ Price must be greater than zero.")
                return
        except:
            await message.reply("❌ Please enter a valid number.")
            return
        
        user = get_logged_user(uid)
        if not user:
            await message.reply("❌ Session expired. Please login again.")
            user_state.pop(uid, None)
            return
        
        stone_id = state["stone_id"]
        
        # Check stone availability
        df = load_stock()
        stone_row = df[df["Stock #"] == stone_id]
        
        if stone_row.empty:
            await message.reply("❌ Stone not found.")
            user_state.pop(uid, None)
            return
        
        if stone_row.iloc[0].get("LOCKED") == "YES":
            await message.reply("🔒 This stone is already locked in another deal.")
            user_state.pop(uid, None)
            return
        
        # Create deal
        deal_id = f"DEAL-{uuid.uuid4().hex[:10].upper()}"
        stone_data = stone_row.iloc[0]
        
        deal = {
            "deal_id": deal_id,
            "stone_id": stone_id,
            "supplier_username": stone_data.get("SUPPLIER", "").replace("supplier_", ""),
            "client_username": user["USERNAME"],
            "actual_stock_price": float(stone_data.get("Price Per Carat", 0)),
            "client_offer_price": offer_price,
            "supplier_action": "PENDING",
            "admin_action": "PENDING",
            "final_status": "OPEN",
            "created_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Lock the stone
        if not atomic_lock_stone(stone_id):
            await message.reply("🔒 Stone is no longer available.")
            user_state.pop(uid, None)
            return
        
        # Save deal to S3
        deal_key = f"{DEALS_FOLDER}{deal_id}.json"
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=deal_key,
            Body=json.dumps(deal, indent=2),
            ContentType="application/json"
        )
        
        # Log deal history
        log_deal_history(deal)
        
        # Notify supplier
        save_notification(
            deal["supplier_username"],
            "supplier",
            f"📩 New deal offer for Stone {stone_id}\n"
            f"💰 Offer: ${offer_price}/ct"
        )
        
        # Log activity
        log_activity(user, "REQUEST_DEAL", {
            "stone_id": stone_id,
            "offer_price": offer_price,
            "deal_id": deal_id
        })
        
        await message.reply(
            f"✅ Deal request sent successfully!\n\n"
            f"📋 **Deal ID:** {deal_id}\n"
            f"💎 **Stone ID:** {stone_id}\n"
            f"💰 **Your Offer:** ${offer_price}/ct\n"
            f"⏳ **Status:** Waiting for supplier response\n\n"
            f"Use '🤝 View Deals' to check status."
        )
        
        user_state.pop(uid, None)

# -------- LOGGED IN BUTTON HANDLERS --------
async def handle_logged_in_buttons(message: types.Message, user: Dict):
    """Handle button presses for logged in users"""
    text = message.text
    
    # Admin buttons - ONLY if role is admin in Excel
    if user["ROLE"] == "admin":
        if text == "💎 View All Stock":
            await view_all_stock(message, user)
        elif text == "👥 View Users":
            await view_users(message, user)
        elif text == "⏳ Pending Accounts":
            await pending_accounts(message, user)
        elif text == "🏆 Supplier Leaderboard":
            await supplier_leaderboard(message, user)
        elif text == "🤝 View Deals":
            await view_deals(message, user)
        elif text == "📑 User Activity Report":
            await user_activity_report(message, user)
        elif text == "🗑 Delete Supplier Stock":
            await delete_supplier_stock(message, user)
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")
    
    # Supplier buttons - ONLY if role is supplier in Excel
    elif user["ROLE"] == "supplier":
        if text == "📤 Upload Excel":
            await upload_excel_prompt(message, user)
        elif text == "📦 My Stock":
            await supplier_my_stock(message, user)
        elif text == "📊 My Analytics":
            await supplier_analytics(message, user)
        elif text == "🤝 View Deals":
            await view_deals(message, user)
        elif text == "📥 Download Sample Excel":
            await download_sample_excel(message, user)
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")
    
    # Client buttons - Default for everyone else
    else:
        if text == "💎 Search Diamonds":
            await search_diamonds_start(message, user)
        elif text == "🔥 Smart Deals":
            await smart_deals(message, user)
        elif text == "🤝 Request Deal":
            await request_deal_start(message, user)
        elif text == "🚪 Logout":
            await logout_command(message)
        else:
            await message.reply("Please use the menu buttons.")

# -------- ADMIN HANDLERS --------
async def view_all_stock(message: types.Message, user: Dict):
    """Admin: View all stock"""
    df = load_stock()
    
    if df.empty:
        await message.reply("❌ No stock available.")
        return
    
    total_diamonds = len(df)
    total_carats = df["Weight"].sum()
    total_value = (df["Weight"] * df["Price Per Carat"]).sum()
    
    # Send summary
    summary = (
        f"📊 **Stock Summary**\n\n"
        f"💎 Total Diamonds: {total_diamonds}\n"
        f"⚖️ Total Carats: {total_carats:.2f}\n"
        f"💰 Estimated Value: ${total_value:,.2f}\n"
        f"👥 Suppliers: {df['SUPPLIER'].nunique()}\n\n"
        f"**Top Shapes:**\n"
    )
    
    shape_counts = df["Shape"].value_counts().head(5)
    for shape, count in shape_counts.items():
        summary += f"• {shape}: {count}\n"
    
    await message.reply(summary)
    
    # Send Excel file
    excel_path = "/tmp/all_stock.xlsx"
    df.to_excel(excel_path, index=False)
    
    await message.reply_document(
        types.FSInputFile(excel_path),
        caption=f"📊 Complete Stock List ({total_diamonds} diamonds)"
    )
    
    if os.path.exists(excel_path):
        os.remove(excel_path)
    
    log_activity(user, "VIEW_ALL_STOCK")

async def view_users(message: types.Message, user: Dict):
    """Admin: View all users"""
    df = load_accounts()
    
    if df.empty:
        await message.reply("❌ No users found.")
        return
    
    # Group by role and approval status
    role_stats = df.groupby("ROLE").size()
    approval_stats = df.groupby("APPROVED").size()
    
    stats_msg = (
        f"📊 **User Statistics**\n\n"
        f"👥 Total Users: {len(df)}\n\n"
        f"**By Role:**\n"
    )
    
    for role, count in role_stats.items():
        stats_msg += f"• {role.title()}: {count}\n"
    
    stats_msg += f"\n**By Approval Status:**\n"
    for status, count in approval_stats.items():
        stats_msg += f"• {status}: {count}\n"
    
    await message.reply(stats_msg)
    
    # Send Excel file
    excel_path = "/tmp/all_users.xlsx"
    df.to_excel(excel_path, index=False)
    
    await message.reply_document(
        types.FSInputFile(excel_path),
        caption=f"👥 User List ({len(df)} users)"
    )
    
    if os.path.exists(excel_path):
        os.remove(excel_path)
    
    log_activity(user, "VIEW_USERS")

async def pending_accounts(message: types.Message, user: Dict):
    """Admin: View pending accounts"""
    df = load_accounts()
    
    pending_df = df[df["APPROVED"] != "YES"]
    
    if pending_df.empty:
        await message.reply("✅ No pending accounts.")
        return
    
    for _, row in pending_df.iterrows():
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve:{row['USERNAME']}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"reject:{row['USERNAME']}")
        ]])
        
        await message.reply(
            f"👤 **Username:** {row['USERNAME']}\n"
            f"🔑 **Role:** {row['ROLE']}\n"
            f"⏳ **Status:** Pending Approval",
            reply_markup=kb
        )
    
    log_activity(user, "VIEW_PENDING_ACCOUNTS")

async def supplier_leaderboard(message: types.Message, user: Dict):
    """Admin: Supplier leaderboard"""
    df = load_stock()
    
    if df.empty or "SUPPLIER" not in df.columns:
        await message.reply("❌ No supplier data available.")
        return
    
    # Calculate supplier statistics
    supplier_stats = df.groupby("SUPPLIER").agg(
        Stones=("SUPPLIER", "count"),
        Total_Carats=("Weight", "sum"),
        Avg_Price_Per_Carat=("Price Per Carat", "mean"),
        Total_Value=("Weight", lambda x: (x * df.loc[x.index, "Price Per Carat"]).sum())
    ).round(2)
    
    supplier_stats = supplier_stats.sort_values("Stones", ascending=False)
    
    # Create message
    leaderboard_msg = "🏆 **Supplier Leaderboard**\n\n"
    
    for i, (supplier, stats) in enumerate(supplier_stats.head(10).iterrows(), 1):
        supplier_name = supplier.replace("supplier_", "").title()
        leaderboard_msg += (
            f"{i}. **{supplier_name}**\n"
            f"   💎 Stones: {stats['Stones']}\n"
            f"   ⚖️ Carats: {stats['Total_Carats']:.2f}\n"
            f"   💰 Avg Price: ${stats['Avg_Price_Per_Carat']:,.2f}/ct\n"
            f"   🏦 Total Value: ${stats['Total_Value']:,.2f}\n\n"
        )
    
    await message.reply(leaderboard_msg)
    
    # Send Excel file
    excel_path = "/tmp/supplier_leaderboard.xlsx"
    supplier_stats.to_excel(excel_path)
    
    await message.reply_document(
        types.FSInputFile(excel_path),
        caption="📊 Supplier Leaderboard Details"
    )
    
    if os.path.exists(excel_path):
        os.remove(excel_path)
    
    log_activity(user, "VIEW_SUPPLIER_LEADERBOARD")

async def user_activity_report(message: types.Message, user: Dict):
    """Admin: Generate activity report"""
    excel_path = generate_activity_excel()
    
    if not excel_path:
        await message.reply("❌ No activity logs found.")
        return
    
    await message.reply_document(
        types.FSInputFile(excel_path),
        caption="📑 User Activity Report"
    )
    
    if os.path.exists(excel_path):
        os.remove(excel_path)
    
    log_activity(user, "DOWNLOAD_ACTIVITY_REPORT")

async def delete_supplier_stock(message: types.Message, user: Dict):
    """Admin: Delete all supplier stock (with confirmation)"""
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Yes, Delete All", callback_data="confirm_delete_stock"),
        InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_delete")
    ]])
    
    await message.reply(
        "⚠️ **WARNING: This will delete ALL supplier stock files.**\n\n"
        "This action cannot be undone.\n"
        "Are you sure you want to continue?",
        reply_markup=kb
    )

# -------- SUPPLIER HANDLERS --------
async def upload_excel_prompt(message: types.Message, user: Dict):
    """Supplier: Prompt for Excel upload"""
    await message.reply(
        "📤 **Upload Stock Excel File**\n\n"
        "Please send an Excel file with your diamond stock.\n\n"
        "**Required Columns:**\n"
        "• Stock # (Unique ID)\n"
        "• Shape, Weight, Color, Clarity\n"
        "• Price Per Carat\n"
        "• Lab, Report #\n"
        "• Diamond Type, Description\n\n"
        "**File Requirements:**\n"
        "• Max size: 10MB\n"
        "• Format: .xlsx or .xls\n"
        "• No duplicate Stock #\n\n"
        "Send your file now or use '📥 Download Sample Excel' first."
    )

async def supplier_my_stock(message: types.Message, user: Dict):
    """Supplier: View own stock"""
    supplier_key = user.get("SUPPLIER_KEY", f"supplier_{user['USERNAME'].lower()}")
    stock_key = f"{SUPPLIER_STOCK_FOLDER}{supplier_key}.xlsx"
    
    try:
        local_path = "/tmp/my_stock.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], stock_key, local_path)
        
        df = pd.read_excel(local_path)
        
        # Calculate statistics
        total_stones = len(df)
        total_carats = df["Weight"].sum()
        avg_price = df["Price Per Carat"].mean()
        total_value = (df["Weight"] * df["Price Per Carat"]).sum()
        
        stats_msg = (
            f"📦 **Your Stock Summary**\n\n"
            f"💎 Total Diamonds: {total_stones}\n"
            f"⚖️ Total Carats: {total_carats:.2f}\n"
            f"💰 Average Price: ${avg_price:,.2f}/ct\n"
            f"🏦 Total Value: ${total_value:,.2f}\n\n"
            f"**Stock Distribution:**\n"
        )
        
        if "Shape" in df.columns:
            shape_counts = df["Shape"].value_counts().head(5)
            for shape, count in shape_counts.items():
                stats_msg += f"• {shape}: {count}\n"
        
        await message.reply(stats_msg)
        
        # Send the file
        await message.reply_document(
            types.FSInputFile(local_path),
            caption=f"📦 Your Stock File ({total_stones} diamonds)"
        )
        
    except Exception as e:
        logger.error(f"Failed to load supplier stock: {e}")
        await message.reply("❌ You haven't uploaded any stock yet.")
    
    finally:
        if os.path.exists("/tmp/my_stock.xlsx"):
            os.remove("/tmp/my_stock.xlsx")

async def supplier_analytics(message: types.Message, user: Dict):
    """Supplier: Price analytics"""
    supplier_key = user.get("SUPPLIER_KEY", f"supplier_{user['USERNAME'].lower()}")
    
    # Load combined stock
    df = load_stock()
    if df.empty:
        await message.reply("❌ No market data available.")
        return
    
    # Filter supplier's stones
    my_stones = df[df["SUPPLIER"].str.lower() == supplier_key.lower()]
    
    if my_stones.empty:
        await message.reply("❌ You have no stones in the market.")
        return
    
    # Calculate market comparison
    results = []
    
    for _, stone in my_stones.iterrows():
        # Find similar stones in market
        similar = df[
            (df["Shape"] == stone["Shape"]) &
            (df["Color"] == stone["Color"]) &
            (df["Clarity"] == stone["Clarity"]) &
            (df["Diamond Type"] == stone.get("Diamond Type", "")) &
            (abs(df["Weight"] - stone["Weight"]) <= 0.2)  # ±0.2 carat
        ]
        
        if len(similar) > 1:  # Has comparable stones
            market_avg = similar["Price Per Carat"].mean()
            my_price = stone["Price Per Carat"]
            price_diff = my_price - market_avg
            price_diff_pct = (price_diff / market_avg * 100) if market_avg > 0 else 0
            
            results.append({
                "Stock #": stone["Stock #"],
                "Weight": stone["Weight"],
                "Shape": stone["Shape"],
                "Color": stone["Color"],
                "Clarity": stone["Clarity"],
                "Your Price": my_price,
                "Market Avg": market_avg,
                "Price Diff": price_diff,
                "Diff %": price_diff_pct,
                "Status": "Above Market" if price_diff > 0 else "Below Market" if price_diff < 0 else "Market Average"
            })
    
    if not results:
        await message.reply("ℹ️ No comparable stones found in market for analysis.")
        return
    
    # Create DataFrame and sort
    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values("Diff %", ascending=False)
    
    # Generate summary
    above_market = len(results_df[results_df["Diff %"] > 5])
    below_market = len(results_df[results_df["Diff %"] < -5])
    in_range = len(results_df) - above_market - below_market
    
    summary_msg = (
        f"📊 **Price Analytics**\n\n"
        f"📈 Analyzed {len(results_df)} comparable stones\n\n"
        f"**Price Positioning:**\n"
        f"• 📈 Above Market (>5%): {above_market}\n"
        f"• 📉 Below Market (<-5%): {below_market}\n"
        f"• ↔️ In Range: {in_range}\n\n"
        f"**Recommendations:**\n"
    )
    
    if above_market > below_market:
        summary_msg += "• Consider adjusting prices down for competitive edge\n"
    elif below_market > above_market:
        summary_msg += "• Your prices are competitive, good position\n"
    else:
        summary_msg += "• Prices are well balanced with market\n"
    
    await message.reply(summary_msg)
    
    # Send detailed Excel
    excel_path = "/tmp/price_analytics.xlsx"
    results_df.to_excel(excel_path, index=False)
    
    await message.reply_document(
        types.FSInputFile(excel_path),
        caption=f"📊 Detailed Price Analysis ({len(results_df)} stones)"
    )
    
    if os.path.exists(excel_path):
        os.remove(excel_path)
    
    log_activity(user, "VIEW_ANALYTICS")

async def download_sample_excel(message: types.Message, user: Dict):
    """Supplier: Download sample Excel template"""
    sample_data = {
        "Stock #": ["D001", "D002", "D003"],
        "Shape": ["Round", "Oval", "Princess"],
        "Weight": [1.0, 1.5, 2.0],
        "Color": ["D", "E", "F"],
        "Clarity": ["VVS1", "VS1", "SI1"],
        "Cut": ["Excellent", "Very Good", "Good"],
        "Polish": ["Excellent", "Very Good", "Good"],
        "Symmetry": ["Excellent", "Very Good", "Good"],
        "Lab": ["GIA", "IGI", "HRD"],
        "Report #": ["1234567890", "2345678901", "3456789012"],
        "Price Per Carat": [10000, 8500, 7000],
        "Total Price": [10000, 12750, 14000],
        "Diamond Type": ["Natural", "Natural", "LGD"],
        "Description": ["Excellent cut round", "Nice oval diamond", "Good princess cut"],
        "Location": ["Mumbai", "Delhi", "Bangalore"],
        "Availability": ["Available", "Available", "Available"]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Create Excel in memory
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Stock')
        
        # Add instructions sheet
        instructions = pd.DataFrame({
            "Column": [
                "Stock #", "Shape", "Weight", "Color", "Clarity", 
                "Price Per Carat", "Lab", "Report #", "Diamond Type", "Description"
            ],
            "Required": ["Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"],
            "Description": [
                "Unique identifier for each diamond",
                "Shape of diamond (Round, Oval, Princess, etc.)",
                "Weight in carats (e.g., 1.0, 1.5)",
                "Color grade (D, E, F, etc.)",
                "Clarity grade (VVS1, VS1, SI1, etc.)",
                "Price per carat in USD",
                "Certification lab (GIA, IGI, HRD, etc.)",
                "Certificate number",
                "Type (Natural, LGD, HPHT)",
                "Brief description of the diamond"
            ],
            "Example": ["D001", "Round", "1.0", "D", "VVS1", "10000", "GIA", "123456", "Natural", "Excellent cut"]
        })
        instructions.to_excel(writer, index=False, sheet_name='Instructions')
    
    buffer.seek(0)
    
    await message.reply_document(
        BufferedInputFile(buffer.read(), filename="diamond_stock_template.xlsx"),
        caption=(
            "📥 **Sample Stock Upload Template**\n\n"
            "This Excel file contains:\n"
            "1. 📋 Sample data (3 diamonds)\n"
            "2. 📝 Instructions sheet\n\n"
            "**Important:**\n"
            "• Fill in your actual diamond data\n"
            "• Keep column names exactly as shown\n"
            "• Stock # must be unique\n"
            "• Remove sample rows before uploading"
        )
    )

# -------- CLIENT HANDLERS --------
async def search_diamonds_start(message: types.Message, user: Dict):
    """Client: Start diamond search"""
    user_state[message.from_user.id] = {
        "step": "search_carat",
        "search": {},
        "last_updated": time.time()
    }
    
    await message.reply(
        "💎 **Diamond Search**\n\n"
        "Enter the carat weight you're looking for:\n\n"
        "**Examples:**\n"
        "• 1.5 (for approximately 1.5 carat)\n"
        "• 1-2 (for range 1 to 2 carats)\n"
        "• any (for any carat weight)"
    )

async def smart_deals(message: types.Message, user: Dict):
    """Client: Find smart deals (discounted diamonds)"""
    df = load_stock()
    
    if df.empty:
        await message.reply("❌ No diamonds available.")
        return
    
    # Calculate market averages for comparison
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
    
    # Group by characteristics to find market average
    group_cols = ["Shape", "Color", "Clarity", "Diamond Type"]
    df["Market_Avg"] = df.groupby(group_cols)["Price Per Carat"].transform("median")
    
    # Calculate discount percentage
    df["Discount_%"] = ((df["Market_Avg"] - df["Price Per Carat"]) / df["Market_Avg"] * 100).round(1)
    
    # Filter for good deals (at least 10% below market)
    good_deals = df[df["Discount_%"] >= 10].sort_values("Discount_%", ascending=False)
    
    if good_deals.empty:
        await message.reply("😔 No strong deals found right now.")
        return
    
    # Show top deals
    top_deals = good_deals.head(5)
    
    deals_msg = "🔥 **Smart Deals Found**\n\n"
    deals_msg += f"Found {len(good_deals)} diamonds priced 10%+ below market\n\n"
    
    for i, (_, deal) in enumerate(top_deals.iterrows(), 1):
        deals_msg += (
            f"{i}. 💎 **{deal['Stock #']}**\n"
            f"   📐 {deal['Shape']} | ⚖️ {deal['Weight']}ct\n"
            f"   🎨 {deal['Color']} | ✨ {deal['Clarity']}\n"
            f"   💰 ${deal['Price Per Carat']:,.0f}/ct\n"
            f"   📉 {deal['Discount_%']}% below market\n"
            f"   🔒 Status: {deal.get('LOCKED', 'NO')}\n\n"
        )
    
    await message.reply(deals_msg)
    
    # Send full list if more than 5 deals
    if len(good_deals) > 5:
        excel_path = "/tmp/smart_deals.xlsx"
        good_deals[["Stock #", "Shape", "Weight", "Color", "Clarity", "Price Per Carat", "Discount_%", "Lab"]].to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=f"📊 Complete Smart Deals List ({len(good_deals)} diamonds)"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
    
    log_activity(user, "VIEW_SMART_DEALS")

async def request_deal_start(message: types.Message, user: Dict):
    """Client: Start deal request process"""
    df = load_stock()
    
    if df.empty:
        await message.reply("❌ No diamonds available for deals.")
        return
    
    # Check if there are many diamonds (use bulk mode)
    if len(df) > 20:
        # Provide bulk Excel template
        template_df = pd.DataFrame(columns=["Stock #", "Offer Price ($/ct)"])
        
        # Add some available stones as examples
        available_stones = df[df["LOCKED"] != "YES"].head(10)
        for _, stone in available_stones.iterrows():
            template_df = pd.concat([template_df, pd.DataFrame([{
                "Stock #": stone["Stock #"],
                "Offer Price ($/ct)": ""
            }])], ignore_index=True)
        
        excel_path = "/tmp/deal_request_template.xlsx"
        template_df.to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=(
                "📊 **Bulk Deal Request Template**\n\n"
                "**Instructions:**\n"
                "1. Fill in your offer price for each stone\n"
                "2. Save the file\n"
                "3. Send it back to me\n\n"
                "**Notes:**\n"
                "• Only fill prices for stones you want\n"
                "• Leave blank to skip\n"
                "• Prices should be $ per carat\n"
                "• Remove example rows if not needed"
            )
        )
        
        user_state[message.from_user.id] = {
            "step": "bulk_deal_excel",
            "last_updated": time.time()
        }
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
            
    else:
        # Individual deal mode
        user_state[message.from_user.id] = {
            "step": "deal_stone",
            "last_updated": time.time()
        }
        
        # Show available stones
        available_stones = df[df["LOCKED"] != "YES"].head(5)
        
        if available_stones.empty:
            await message.reply("❌ No stones available for deals at the moment.")
            user_state.pop(message.from_user.id, None)
            return
        
        stones_msg = "💎 **Available Stones for Deal**\n\n"
        
        for _, stone in available_stones.iterrows():
            stones_msg += (
                f"• **{stone['Stock #']}**\n"
                f"  {stone['Shape']} | {stone['Weight']}ct\n"
                f"  {stone['Color']} | {stone['Clarity']}\n"
                f"  ${stone['Price Per Carat']:,.0f}/ct\n\n"
            )
        
        stones_msg += "Enter the **Stock #** of the stone you want to make an offer on:"
        
        await message.reply(stones_msg)

# -------- DEAL VIEWING --------
async def view_deals(message: types.Message, user: Dict):
    """View deals based on user role"""
    try:
        # List all deal files
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=DEALS_FOLDER
        )
        
        if "Contents" not in objs:
            await message.reply("ℹ️ No deals found.")
            return
        
        # Load all deals
        deals = []
        for obj in objs["Contents"]:
            if not obj["Key"].endswith(".json"):
                continue
            
            try:
                deal_data = s3.get_object(
                    Bucket=CONFIG["AWS_BUCKET"],
                    Key=obj["Key"]
                )["Body"].read().decode("utf-8")
                
                deal = json.loads(deal_data)
                deals.append(deal)
            except Exception as e:
                logger.error(f"Failed to load deal {obj['Key']}: {e}")
                continue
        
        if not deals:
            await message.reply("ℹ️ No deals available.")
            return
        
        # Sort by creation date (newest first)
        deals.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        
        # Filter based on user role
        user_role = user["ROLE"]
        username = user["USERNAME"].lower()
        
        if user_role == "admin":
            # Admin sees all deals
            filtered_deals = deals
            title = "All Deals"
            
        elif user_role == "supplier":
            # Supplier sees their deals
            filtered_deals = [
                d for d in deals 
                if d.get("supplier_username", "").lower() == username
            ]
            title = "Your Deals"
            
        elif user_role == "client":
            # Client sees their deals
            filtered_deals = [
                d for d in deals 
                if d.get("client_username", "").lower() == username
            ]
            title = "Your Deal Requests"
            
        else:
            await message.reply("❌ Unauthorized access.")
            return
        
        if not filtered_deals:
            await message.reply(f"ℹ️ No {title.lower()} found.")
            return
        
        # Create summary
        summary_msg = f"🤝 **{title}**\n\n"
        summary_msg += f"Total: {len(filtered_deals)} deals\n\n"
        
        # Count by status
        status_counts = {}
        for deal in filtered_deals:
            status = deal.get("final_status", "OPEN")
            status_counts[status] = status_counts.get(status, 0) + 1
        
        for status, count in status_counts.items():
            summary_msg += f"• {status}: {count}\n"
        
        await message.reply(summary_msg)
        
        # Send Excel file with details
        excel_data = []
        for deal in filtered_deals:
            excel_data.append({
                "Deal ID": deal.get("deal_id"),
                "Stone ID": deal.get("stone_id"),
                "Supplier": deal.get("supplier_username"),
                "Client": deal.get("client_username"),
                "Actual Price": deal.get("actual_stock_price"),
                "Offer Price": deal.get("client_offer_price"),
                "Supplier Action": deal.get("supplier_action"),
                "Admin Action": deal.get("admin_action"),
                "Final Status": deal.get("final_status"),
                "Created At": deal.get("created_at")
            })
        
        df = pd.DataFrame(excel_data)
        excel_path = f"/tmp/{username}_deals.xlsx"
        df.to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=f"📊 {title} Details"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
        
        log_activity(user, f"VIEW_{user_role.upper()}_DEALS")
        
    except Exception as e:
        logger.error(f"Failed to view deals: {e}")
        await message.reply("❌ Error loading deals. Please try again.")

# -------- CALLBACK QUERY HANDLERS --------
@dp.callback_query(F.data.startswith("approve:"))
async def approve_user_callback(callback: types.CallbackQuery):
    """Approve pending user account"""
    admin = get_logged_user(callback.from_user.id)
    
    if not is_admin(admin):
        await callback.answer("❌ Admin only", show_alert=True)
        return
    
    username = callback.data.split(":")[1]
    
    # Load accounts and approve
    df = load_accounts()
    
    if df[df["USERNAME"] == username].empty:
        await callback.answer("❌ User not found", show_alert=True)
        return
    
    # Update approval status
    df.loc[df["USERNAME"] == username, "APPROVED"] = "YES"
    save_accounts(df)
    
    # Send notification to user
    save_notification(username, "client", "✅ Your account has been approved by admin!")
    
    # Log activity
    log_activity(admin, "APPROVE_USER", {"username": username})
    
    await callback.message.edit_text(
        f"✅ **{username}** approved successfully!",
        reply_markup=None
    )
    await callback.answer("Approved ✅")

@dp.callback_query(F.data.startswith("reject:"))
async def reject_user_callback(callback: types.CallbackQuery):
    """Reject pending user account"""
    admin = get_logged_user(callback.from_user.id)
    
    if not is_admin(admin):
        await callback.answer("❌ Admin only", show_alert=True)
        return
    
    username = callback.data.split(":")[1]
    
    # Load accounts and remove
    df = load_accounts()
    
    if df[df["USERNAME"] == username].empty:
        await callback.answer("❌ User not found", show_alert=True)
        return
    
    # Remove user
    df = df[df["USERNAME"] != username]
    save_accounts(df)
    
    # Log activity
    log_activity(admin, "REJECT_USER", {"username": username})
    
    await callback.message.edit_text(
        f"❌ **{username}** rejected and removed.",
        reply_markup=None
    )
    await callback.answer("Rejected ❌")

@dp.callback_query(F.data == "confirm_delete_stock")
async def confirm_delete_stock(callback: types.CallbackQuery):
    """Confirm and delete all supplier stock"""
    admin = get_logged_user(callback.from_user.id)
    
    if not is_admin(admin):
        await callback.answer("❌ Admin only", show_alert=True)
        return
    
    try:
        # Delete all supplier files
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=SUPPLIER_STOCK_FOLDER
        )
        
        deleted_count = 0
        if "Contents" in objs:
            for obj in objs["Contents"]:
                s3.delete_object(Bucket=CONFIG["AWS_BUCKET"], Key=obj["Key"])
                deleted_count += 1
        
        # Delete combined stock
        try:
            s3.delete_object(Bucket=CONFIG["AWS_BUCKET"], Key=COMBINED_STOCK_KEY)
        except:
            pass
        
        # Log activity
        log_activity(admin, "DELETE_ALL_STOCK", {"deleted_files": deleted_count})
        
        await callback.message.edit_text(
            f"🗑 **All supplier stock deleted successfully!**\n\n"
            f"Deleted {deleted_count} files.",
            reply_markup=None
        )
        await callback.answer("Deleted ✅")
        
    except Exception as e:
        logger.error(f"Failed to delete stock: {e}")
        await callback.answer("❌ Error deleting stock", show_alert=True)

@dp.callback_query(F.data == "cancel_delete")
async def cancel_delete(callback: types.CallbackQuery):
    """Cancel stock deletion"""
    await callback.message.edit_text(
        "❌ Stock deletion cancelled.",
        reply_markup=None
    )
    await callback.answer("Cancelled")

# -------- DOCUMENT HANDLER --------
@dp.message(F.document)
async def handle_document(message: types.Message):
    """Handle document uploads (Excel files)"""
    uid = message.from_user.id
    user = get_logged_user(uid)
    
    if not user:
        await message.reply("🔒 Please login first.")
        return
    
    # Check file size (max 10MB)
    if message.document.file_size > 10 * 1024 * 1024:
        await message.reply("❌ File too large. Max size is 10MB.")
        return
    
    # Check file extension
    file_name = message.document.file_name.lower()
    if not file_name.endswith(('.xlsx', '.xls')):
        await message.reply("❌ Only Excel files (.xlsx, .xls) are allowed.")
        return
    
    # Download file
    try:
        file = await bot.get_file(message.document.file_id)
        temp_path = f"/tmp/{uid}_{int(time.time())}_{file_name}"
        await bot.download_file(file.file_path, temp_path)
    except Exception as e:
        logger.error(f"Failed to download file: {e}")
        await message.reply("❌ Failed to download file.")
        return
    
    try:
        # Read Excel file
        df = pd.read_excel(temp_path)
        
        # Handle based on user role and state
        state = user_state.get(uid, {})
        
        if user["ROLE"] == "supplier" and not state.get("step") == "bulk_deal_excel":
            # Supplier stock upload
            await handle_supplier_stock_upload(message, user, df, temp_path)
            
        elif user["ROLE"] == "client" and state.get("step") == "bulk_deal_excel":
            # Client bulk deal requests
            await handle_bulk_deal_requests(message, user, df, temp_path)
            
        elif user["ROLE"] == "admin":
            # Admin deal approvals
            await handle_admin_deal_approvals(message, user, df, temp_path)
            
        elif user["ROLE"] == "supplier" and "Deal ID" in df.columns:
            # Supplier deal responses
            await handle_supplier_deal_responses(message, user, df, temp_path)
            
        else:
            await message.reply("❌ Invalid file format or action.")
            
    except Exception as e:
        logger.error(f"Failed to process Excel file: {e}")
        await message.reply(f"❌ Error processing file: {str(e)}")
        
    finally:
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)

async def handle_supplier_stock_upload(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle supplier stock upload"""
    # Check required columns
    required_cols = [
        "Stock #", "Shape", "Weight", "Color", "Clarity",
        "Price Per Carat", "Lab", "Report #", "Diamond Type", "Description"
    ]
    
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        await message.reply(f"❌ Missing required columns: {', '.join(missing_cols)}")
        return
    
    # Validate data
    if df["Stock #"].isnull().any():
        await message.reply("❌ Stock # cannot be empty")
        return
    
    if df["Stock #"].duplicated().any():
        duplicates = df[df["Stock #"].duplicated()]["Stock #"].unique()
        await message.reply(f"❌ Duplicate Stock # found: {', '.join(duplicates[:5])}")
        return
    
    # Convert numeric columns
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
    
    if df["Weight"].isnull().any() or (df["Weight"] <= 0).any():
        await message.reply("❌ Invalid weight values")
        return
    
    if df["Price Per Carat"].isnull().any() or (df["Price Per Carat"] <= 0).any():
        await message.reply("❌ Invalid price values")
        return
    
    # Add supplier info
    supplier_key = user.get("SUPPLIER_KEY", f"supplier_{user['USERNAME'].lower()}")
    df["SUPPLIER"] = supplier_key
    df["LOCKED"] = "NO"
    
    # Save to supplier folder
    supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier_key}.xlsx"
    temp_supplier_path = f"/tmp/{supplier_key}.xlsx"
    
    # Prevent formula injection
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(safe_excel)
    
    df.to_excel(temp_supplier_path, index=False)
    s3.upload_file(temp_supplier_path, CONFIG["AWS_BUCKET"], supplier_file)
    
    # Rebuild combined stock
    rebuild_combined_stock()
    
    # Calculate statistics
    total_stones = len(df)
    total_carats = df["Weight"].sum()
    total_value = (df["Weight"] * df["Price Per Carat"]).sum()
    
    # Send success message
    success_msg = (
        f"✅ **Stock Upload Successful!**\n\n"
        f"📊 **Statistics:**\n"
        f"• 💎 Diamonds: {total_stones}\n"
        f"• ⚖️ Total Carats: {total_carats:.2f}\n"
        f"• 💰 Total Value: ${total_value:,.2f}\n\n"
        f"📈 **Price Range:**\n"
        f"• Min: ${df['Price Per Carat'].min():,.0f}/ct\n"
        f"• Avg: ${df['Price Per Carat'].mean():,.0f}/ct\n"
        f"• Max: ${df['Price Per Carat'].max():,.0f}/ct\n\n"
        f"🔄 Combined stock has been updated."
    )
    
    await message.reply(success_msg)
    
    # Log activity
    log_activity(user, "UPLOAD_STOCK", {
        "stones": total_stones,
        "carats": total_carats,
        "value": total_value
    })
    
    # Clean up
    if os.path.exists(temp_supplier_path):
        os.remove(temp_supplier_path)

async def handle_bulk_deal_requests(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle client bulk deal requests"""
    if "Stock #" not in df.columns or "Offer Price ($/ct)" not in df.columns:
        await message.reply("❌ Invalid format. Need 'Stock #' and 'Offer Price ($/ct)' columns.")
        return
    
    # Filter out empty rows
    df = df.dropna(subset=["Stock #", "Offer Price ($/ct)"])
    
    if df.empty:
        await message.reply("❌ No valid deal requests found.")
        return
    
    # Load current stock
    stock_df = load_stock()
    
    successful_deals = 0
    failed_deals = []
    
    for _, row in df.iterrows():
        stone_id = str(row["Stock #"]).strip()
        
        try:
            offer_price = float(row["Offer Price ($/ct)"])
            if offer_price <= 0:
                failed_deals.append(f"{stone_id}: Invalid price")
                continue
        except:
            failed_deals.append(f"{stone_id}: Invalid price format")
            continue
        
        # Check stone availability
        stone_row = stock_df[stock_df["Stock #"] == stone_id]
        
        if stone_row.empty:
            failed_deals.append(f"{stone_id}: Not found")
            continue
        
        if stone_row.iloc[0].get("LOCKED") == "YES":
            failed_deals.append(f"{stone_id}: Already locked")
            continue
        
        # Create deal
        deal_id = f"DEAL-{uuid.uuid4().hex[:10].upper()}"
        stone_data = stone_row.iloc[0]
        
        deal = {
            "deal_id": deal_id,
            "stone_id": stone_id,
            "supplier_username": stone_data.get("SUPPLIER", "").replace("supplier_", ""),
            "client_username": user["USERNAME"],
            "actual_stock_price": float(stone_data.get("Price Per Carat", 0)),
            "client_offer_price": offer_price,
            "supplier_action": "PENDING",
            "admin_action": "PENDING",
            "final_status": "OPEN",
            "created_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Lock the stone
        if not atomic_lock_stone(stone_id):
            failed_deals.append(f"{stone_id}: Lock failed")
            continue
        
        # Save deal
        deal_key = f"{DEALS_FOLDER}{deal_id}.json"
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=deal_key,
            Body=json.dumps(deal, indent=2),
            ContentType="application/json"
        )
        
        log_deal_history(deal)
        
        # Notify supplier
        save_notification(
            deal["supplier_username"],
            "supplier",
            f"📩 New bulk deal offer for Stone {stone_id}"
        )
        
        successful_deals += 1
    
    # Send results
    result_msg = f"📊 **Bulk Deal Results**\n\n"
    result_msg += f"✅ Successful: {successful_deals}\n"
    result_msg += f"❌ Failed: {len(failed_deals)}\n"
    
    if failed_deals:
        result_msg += f"\n**Failed deals:**\n"
        for fail in failed_deals[:10]:  # Show first 10 failures
            result_msg += f"• {fail}\n"
        
        if len(failed_deals) > 10:
            result_msg += f"... and {len(failed_deals) - 10} more\n"
    
    await message.reply(result_msg)
    
    # Clear user state
    user_state.pop(message.from_user.id, None)
    
    # Log activity
    log_activity(user, "BULK_DEAL_REQUEST", {
        "successful": successful_deals,
        "failed": len(failed_deals)
    })

async def handle_admin_deal_approvals(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle admin deal approval Excel"""
    required_cols = ["Deal ID", "Admin Action (YES/NO)"]
    for col in required_cols:
        if col not in df.columns:
            await message.reply(f"❌ Missing column: {col}")
            return
    
    processed = 0
    for _, row in df.iterrows():
        if pd.isna(row["Deal ID"]):
            continue
        
        deal_id = str(row["Deal ID"]).strip()
        action = str(row["Admin Action (YES/NO)"]).strip().upper()
        
        if action not in ["YES", "NO"]:
            continue
        
        try:
            deal_key = f"{DEALS_FOLDER}{deal_id}.json"
            deal_data = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=deal_key)
            deal = json.loads(deal_data["Body"].read())
            
            if deal.get("final_status") in ["COMPLETED", "CLOSED"]:
                continue
            
            if action == "YES":
                deal["admin_action"] = "APPROVED"
                deal["final_status"] = "COMPLETED"
                
                # Remove stone from stock
                remove_stone_from_supplier_and_combined(deal["stone_id"])
                
                # Notify parties
                save_notification(
                    deal["client_username"],
                    "client",
                    f"✅ Deal {deal_id} approved for Stone {deal['stone_id']}"
                )
                
                save_notification(
                    deal["supplier_username"],
                    "supplier",
                    f"✅ Deal {deal_id} approved. Please deliver Stone {deal['stone_id']}"
                )
                
            else:  # NO
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"
                
                # Unlock stone
                unlock_stone(deal["stone_id"])
                
                # Notify client
                save_notification(
                    deal["client_username"],
                    "client",
                    f"❌ Deal {deal_id} rejected for Stone {deal['stone_id']}"
                )
            
            # Save updated deal
            s3.put_object(
                Bucket=CONFIG["AWS_BUCKET"],
                Key=deal_key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )
            
            log_deal_history(deal)
            processed += 1
            
        except Exception as e:
            logger.error(f"Failed to process deal {deal_id}: {e}")
            continue
    
    await message.reply(f"✅ Processed {processed} deal approvals.")
    log_activity(user, "PROCESS_DEAL_APPROVALS", {"count": processed})

async def handle_supplier_deal_responses(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle supplier deal response Excel"""
    if "Deal ID" not in df.columns or "Supplier Action (ACCEPT/REJECT)" not in df.columns:
        await message.reply("❌ Invalid format. Need 'Deal ID' and 'Supplier Action (ACCEPT/REJECT)' columns.")
        return
    
    processed = 0
    for _, row in df.iterrows():
        if pd.isna(row["Deal ID"]):
            continue
        
        deal_id = str(row["Deal ID"]).strip()
        action = str(row["Supplier Action (ACCEPT/REJECT)"]).strip().upper()
        
        if action not in ["ACCEPT", "REJECT"]:
            continue
        
        try:
            deal_key = f"{DEALS_FOLDER}{deal_id}.json"
            deal_data = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=deal_key)
            deal = json.loads(deal_data["Body"].read())
            
            # Check if supplier owns this deal
            if deal.get("supplier_username", "").lower() != user["USERNAME"].lower():
                continue
            
            if deal.get("final_status") in ["COMPLETED", "CLOSED"]:
                continue
            
            if action == "ACCEPT":
                deal["supplier_action"] = "ACCEPTED"
                deal["admin_action"] = "PENDING"
                
                save_notification(
                    deal["client_username"],
                    "client",
                    f"✅ Supplier accepted deal {deal_id} for Stone {deal['stone_id']}"
                )
                
                # Notify admins
                admin_df = load_accounts()
                admins = admin_df[admin_df["ROLE"] == "admin"]["USERNAME"].tolist()
                for admin in admins:
                    save_notification(
                        admin,
                        "admin",
                        f"📝 Deal {deal_id} awaiting admin approval"
                    )
                    
            else:  # REJECT
                deal["supplier_action"] = "REJECTED"
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"
                
                # Unlock stone
                unlock_stone(deal["stone_id"])
                
                save_notification(
                    deal["client_username"],
                    "client",
                    f"❌ Supplier rejected deal {deal_id} for Stone {deal['stone_id']}"
                )
            
            # Save updated deal
            s3.put_object(
                Bucket=CONFIG["AWS_BUCKET"],
                Key=deal_key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )
            
            log_deal_history(deal)
            processed += 1
            
        except Exception as e:
            logger.error(f"Failed to process deal {deal_id}: {e}")
            continue
    
    await message.reply(f"✅ Processed {processed} deal responses.")
    log_activity(user, "PROCESS_DEAL_RESPONSES", {"count": processed})

# -------- MAIN ENTRY POINT --------
if __name__ == "__main__":
    nest_asyncio.apply()
    
    logger.info(f"🚀 Starting Diamond Trading Bot v1.0")
    logger.info(f"📊 Python: {CONFIG['PYTHON_VERSION']}")
    logger.info(f"🌐 Port: {CONFIG['PORT']}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=CONFIG["PORT"],
        reload=False,
        log_level="info"
    )
