import asyncio
import pandas as pd
import boto3
import re
from io import BytesIO
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from contextlib import asynccontextmanager
import os
import json
import pytz
import uuid
import time
import unicodedata
import uvicorn
from typing import Optional, Dict, Any, List, Tuple
import logging
from fastapi.responses import JSONResponse, FileResponse
import atexit
import httpx

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
        "AWS_BUCKET": os.getenv("AWS_BUCKET", "diamond-bucket-styleoflifes"),
        "PORT": int(os.getenv("PORT", "10000")),
        "PYTHON_VERSION": os.getenv("PYTHON_VERSION", "3.11.0"),
        "SESSION_TIMEOUT": int(os.getenv("SESSION_TIMEOUT", "3600")),
        "RATE_LIMIT": int(os.getenv("RATE_LIMIT", "5")),
        "RATE_LIMIT_WINDOW": int(os.getenv("RATE_LIMIT_WINDOW", "10")),
        "WEBHOOK_URL": os.getenv("WEBHOOK_URL", ""),
        "TEST_CHAT_ID": os.getenv("TEST_CHAT_ID", ""),
        "RENDER_EXTERNAL_URL": os.getenv("RENDER_EXTERNAL_URL", "https://telegram-bot-6iil.onrender.com"),
    }
    
    # Validate required configurations
    if not config["BOT_TOKEN"]:
        raise ValueError("❌ BOT_TOKEN environment variable not set")
    
    if not all([config["AWS_ACCESS_KEY_ID"], config["AWS_SECRET_ACCESS_KEY"], config["AWS_BUCKET"]]):
        logger.warning("AWS credentials not fully set. Some features may not work.")
    
    # Auto-generate webhook URL if not set
    if not config["WEBHOOK_URL"]:
        render_url = config["RENDER_EXTERNAL_URL"]
        config["WEBHOOK_URL"] = f"{render_url}/webhook"
        logger.info(f"Auto-generated webhook URL: {config['WEBHOOK_URL']}")
    
    logger.info(f"✅ Config loaded: BOT_TOKEN present: {bool(config['BOT_TOKEN'])}")
    logger.info(f"🌐 Webhook URL: {config['WEBHOOK_URL']}")
    logger.info(f"📦 S3 Bucket: {config['AWS_BUCKET']}")
    
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

# -------- STARTUP CACHE --------
startup_cache = {
    "accounts": None,
    "stock": None,
    "last_loaded": 0
}

# -------- INITIALIZE AWS CLIENTS --------
try:
    s3 = boto3.client("s3", **{k: v for k, v in AWS_CONFIG.items() if v})
    logger.info("✅ AWS S3 client initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize S3 client: {e}")
    s3 = None

# -------- INITIALIZE BOT --------
bot = Bot(token=CONFIG["BOT_TOKEN"])
dp = Dispatcher()
router = Router()
dp.include_router(router)

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
    if isinstance(value, float) and pd.isna(value):
        return ""
    value = str(value)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u00A0", " ").replace("\u200B", "")
    value = value.replace("\n", " ").replace("\r", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()

def clean_password(val: Any) -> str:
    """Clean password, handling Excel .0 issue"""
    val = clean_text(val)
    if val.endswith(".0"):
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
    """Check if user is admin - ONLY based on Excel file"""
    if not user:
        return False
    
    role = normalize_text(user.get("ROLE", ""))
    return role == "admin"

def get_logged_user(uid: int) -> Optional[Dict[str, Any]]:
    """Get logged in user with session validation"""
    user = logged_in_users.get(uid)
    if not user:
        return None

    last_active = user.get("last_active", 0)
    if time.time() - last_active > CONFIG["SESSION_TIMEOUT"]:
        logged_in_users.pop(uid, None)
        save_sessions()
        return None

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
        if s3:
            s3.put_object(
                Bucket=CONFIG["AWS_BUCKET"],
                Key=SESSION_KEY,
                Body=json.dumps(logged_in_users, default=str),
                ContentType="application/json"
            )
            logger.info(f"✅ Saved {len(logged_in_users)} active sessions")
    except Exception as e:
        logger.error(f"❌ Failed to save sessions: {e}")

def load_sessions():
    """Load sessions from S3"""
    global logged_in_users
    try:
        if s3:
            obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=SESSION_KEY)
            raw = json.loads(obj["Body"].read())
            logged_in_users = {int(k): v for k, v in raw.items()}
            logger.info(f"✅ Loaded {len(logged_in_users)} sessions from S3")
    except Exception as e:
        logger.warning(f"⚠️ No existing sessions or error loading: {e}")
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
    user_rate_limit[uid] = history[-10:]
    return False

# -------- DATA LOADING/SAVING --------
def load_accounts(cached=True) -> pd.DataFrame:
    """Load accounts from Excel file in S3 with caching"""
    global startup_cache
    
    try:
        # Return cached data if available and not too old (5 minutes)
        if cached and startup_cache["accounts"] is not None and (time.time() - startup_cache["last_loaded"]) < 300:
            logger.info("✅ Using cached accounts data")
            return startup_cache["accounts"]
            
        if not s3:
            return pd.DataFrame(columns=["USERNAME", "PASSWORD", "ROLE", "APPROVED"])
            
        local_path = "/tmp/accounts.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], ACCOUNTS_KEY, local_path)
        df = pd.read_excel(local_path, dtype=str)
        
        required_cols = ["USERNAME", "PASSWORD", "ROLE", "APPROVED"]
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")
            
            df[col] = df[col].fillna("").astype(str).apply(clean_text)
        
        df["PASSWORD"] = df["PASSWORD"].apply(clean_password)
        
        logger.info(f"✅ Loaded {len(df)} accounts from S3")
        
        # Cache the data
        startup_cache["accounts"] = df
        startup_cache["last_loaded"] = time.time()
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load accounts: {e}")
        return pd.DataFrame(columns=["USERNAME", "PASSWORD", "ROLE", "APPROVED"])

def save_accounts(df: pd.DataFrame):
    """Save accounts to Excel file in S3"""
    if READ_ONLY_ACCOUNTS:
        logger.warning("⚠️ Accounts file is READ ONLY. Skipping save.")
        return
    
    try:
        if not s3:
            logger.error("❌ S3 client not available")
            return
            
        local_path = "/tmp/accounts.xlsx"
        df.to_excel(local_path, index=False)
        s3.upload_file(local_path, CONFIG["AWS_BUCKET"], ACCOUNTS_KEY)
        logger.info(f"✅ Saved {len(df)} accounts to S3")
        
        # Update cache
        startup_cache["accounts"] = df
        startup_cache["last_loaded"] = time.time()
        
    except Exception as e:
        logger.error(f"❌ Failed to save accounts: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

def load_stock(cached=True) -> pd.DataFrame:
    """Load combined stock from S3 with caching"""
    global startup_cache
    
    try:
        # Return cached data if available and not too old (5 minutes)
        if cached and startup_cache["stock"] is not None and (time.time() - startup_cache["last_loaded"]) < 300:
            logger.info("✅ Using cached stock data")
            return startup_cache["stock"]
            
        if not s3:
            return pd.DataFrame()
            
        local_path = "/tmp/all_suppliers_stock.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY, local_path)
        df = pd.read_excel(local_path)
        logger.info(f"✅ Loaded {len(df)} stock items from S3")
        
        # Cache the data
        startup_cache["stock"] = df
        startup_cache["last_loaded"] = time.time()
        
        return df
    except Exception as e:
        logger.warning(f"⚠️ Failed to load stock: {e}")
        return pd.DataFrame()

# -------- ACTIVITY LOGGING --------
def log_activity(user: Dict[str, Any], action: str, details: Optional[Dict] = None):
    """Log user activity to S3"""
    try:
        if not s3:
            return
            
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
        
        try:
            obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
            data = json.loads(obj["Body"].read())
        except:
            data = []
        
        data.append(log_entry)
        
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        logger.info(f"📝 Logged activity: {user.get('USERNAME')} - {action}")
        
    except Exception as e:
        logger.error(f"❌ Failed to log activity: {e}")

def generate_activity_excel() -> Optional[str]:
    """Generate Excel report of all activities"""
    try:
        if not s3:
            return None
            
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
        
        logger.info(f"✅ Generated activity report with {len(rows)} entries")
        return path

    except Exception as e:
        logger.error(f"❌ Activity report error: {e}")
        return None

# -------- NOTIFICATION SYSTEM --------
def save_notification(username: str, role: str, message: str):
    """Save notification for user"""
    try:
        if not s3:
            return
            
        key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
        
        try:
            obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
            data = json.loads(obj["Body"].read())
        except:
            data = []
        
        data.append({
            "message": message,
            "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            "read": False
        })
        
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to save notification: {e}")

def fetch_unread_notifications(username: str, role: str) -> List[Dict]:
    """Fetch unread notifications for user"""
    try:
        if not s3:
            return []
            
        key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
        obj = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=key)
        data = json.loads(obj["Body"].read())
        
        unread = [n for n in data if not n.get("read")]
        
        for n in data:
            n["read"] = True
        
        s3.put_object(
            Bucket=CONFIG["AWS_BUCKET"],
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        
        return unread
        
    except Exception:
        return []

# -------- EXCEL VALIDATION & PARSING --------
class DiamondExcelValidator:
    """Validate diamond stock Excel files with flexible optional columns"""
    
    # REQUIRED columns - MUST have values
    REQUIRED_COLUMNS = [
        'Stock #',
        'Shape', 
        'Weight',
        'Color',
        'Clarity',
        'Price Per Carat',
        'Lab', 
        'Report #',
        'Diamond Type',
        'Description'
    ]
    
    # OPTIONAL columns - can be BLANK/EMPTY
    OPTIONAL_COLUMNS = [
        'CUT',
        'Polish',
        'Symmetry'
    ]
    
    ALL_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
    
    @staticmethod
    def validate_and_parse(df: pd.DataFrame, supplier_name: str) -> Tuple[bool, pd.DataFrame, List[str], List[str]]:
        """
        Validate and clean Excel data
        
        Args:
            df: Pandas DataFrame from Excel
            supplier_name: Name of supplier
            
        Returns:
            Tuple: (success, cleaned_df, errors, warnings)
        """
        errors = []
        warnings = []
        
        try:
            # Clean column names
            df.columns = [str(col).strip() for col in df.columns]
            
            # Check for missing REQUIRED columns
            missing_required = []
            for req_col in DiamondExcelValidator.REQUIRED_COLUMNS:
                if req_col not in df.columns:
                    missing_required.append(req_col)
            
            if missing_required:
                errors.append(f'Missing required columns: {", ".join(missing_required)}')
                return False, pd.DataFrame(), errors, warnings
            
            # Check for OPTIONAL columns (just warning, not error)
            missing_optional = []
            for opt_col in DiamondExcelValidator.OPTIONAL_COLUMNS:
                if opt_col not in df.columns:
                    missing_optional.append(opt_col)
            
            if missing_optional:
                warnings.append(f'Optional columns not found (will be ignored): {", ".join(missing_optional)}')
            
            # Clean data
            df = df.copy()
            for col in df.columns:
                if col in df.select_dtypes(include=['object']).columns:
                    df[col] = df[col].fillna('').astype(str).apply(clean_text)
            
            # Validate REQUIRED columns are not empty
            for req_col in DiamondExcelValidator.REQUIRED_COLUMNS:
                if req_col in df.columns:
                    empty_mask = df[req_col].isna() | (df[req_col] == '')
                    if empty_mask.any():
                        empty_count = empty_mask.sum()
                        errors.append(f'{req_col}: {empty_count} rows are empty (required)')
            
            # Check for duplicate Stock #
            if 'Stock #' in df.columns:
                duplicate_mask = df.duplicated('Stock #', keep=False)
                if duplicate_mask.any():
                    duplicates = df[duplicate_mask]['Stock #'].unique().tolist()
                    errors.append(f'Duplicate Stock # found: {", ".join(duplicates[:5])}')
            
            # Validate numeric columns
            if 'Weight' in df.columns:
                try:
                    df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce')
                    invalid_weights = df['Weight'].isna() | (df['Weight'] <= 0)
                    if invalid_weights.any():
                        invalid_count = invalid_weights.sum()
                        errors.append(f'Weight: {invalid_count} rows have invalid values (must be > 0)')
                except:
                    errors.append('Weight: Could not convert to numeric values')
            
            if 'Price Per Carat' in df.columns:
                try:
                    df['Price Per Carat'] = pd.to_numeric(df['Price Per Carat'], errors='coerce')
                    # FIXED LINE 608: Changed "Price Per Carat" to 'Price Per Carat'
                    invalid_prices = df['Price Per Carat'].isna() | (df['Price Per Carat'] <= 0)
                    if invalid_prices.any():
                        invalid_count = invalid_prices.sum()
                        errors.append(f'Price Per Carat: {invalid_count} rows have invalid values (must be > 0)')
                except:
                    errors.append('Price Per Carat: Could not convert to numeric values')
            
            if errors:
                return False, pd.DataFrame(), errors, warnings
            
            # Add metadata columns
            df['SUPPLIER'] = supplier_name
            df['LOCKED'] = 'NO'
            df['UPLOADED_AT'] = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')
            
            # Ensure all columns are present
            for col in DiamondExcelValidator.ALL_COLUMNS:
                if col not in df.columns:
                    df[col] = ''
            
            # Handle optional columns - if empty, leave as empty string
            for opt_col in DiamondExcelValidator.OPTIONAL_COLUMNS:
                if opt_col in df.columns:
                    # Already cleaned, keep as is (can be empty)
                    pass
            
            # Reorder columns for consistency
            desired_order = ['Stock #', 'Availability', 'Shape', 'Weight', 'Color', 'Clarity', 
                           'Cut', 'Polish', 'Symmetry', 'Fluorescence Color', 'Measurements', 
                           'Shade', 'Milky', 'Eye Clean', 'Lab', 'Report #', 'Location', 
                           'Treatment', 'Discount', 'Price Per Carat', 'Final Price', 'Depth %', 
                           'Table %', 'Girdle Thin', 'Girdle Thick', 'Girdle %', 'Girdle Condition', 
                           'Culet Size', 'Culet Condition', 'Crown Height', 'Crown Angle', 
                           'Pavilion Depth', 'Pavilion Angle', 'Inscription', 'Cert comment', 
                           'KeyToSymbols', 'White Inclusion', 'Black Inclusion', 'Open Inclusion', 
                           'Fancy Color', 'Fancy Color Intensity', 'Fancy Color Overtone', 
                           'Country', 'State', 'City', 'CertFile', 'Diamond Video', 'Diamond Image', 
                           'SUPPLIER', 'LOCKED', 'Diamond Type', 'UPLOADED_AT', 'Description']
            
            # Add missing columns with empty values
            for col in desired_order:
                if col not in df.columns:
                    df[col] = ''
            
            # Select only desired columns in order
            df = df[desired_order]
            
            # Apply safe_excel to all string columns
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(lambda x: safe_excel(x) if isinstance(x, str) else x)
            
            return True, df, errors, warnings
            
        except Exception as e:
            errors.append(f'Validation error: {str(e)}')
            return False, pd.DataFrame(), errors, warnings

# -------- STOCK MANAGEMENT --------
def rebuild_combined_stock():
    """Rebuild combined stock from all supplier files"""
    try:
        if not s3:
            return
            
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
                
                if os.path.exists(local_path):
                    os.remove(local_path)
                    
            except Exception as e:
                logger.error(f"Failed to process {key}: {e}")
                continue
        
        if not dfs:
            return
        
        final_df = pd.concat(dfs, ignore_index=True)
        
        desired_columns = [
            "Stock #", "Availability", "Shape", "Weight", "Color", "Clarity", "Cut", "Polish", "Symmetry",
            "Fluorescence Color", "Measurements", "Shade", "Milky", "Eye Clean", "Lab", "Report #", "Location",
            "Treatment", "Discount", "Price Per Carat", "Final Price", "Depth %", "Table %", "Girdle Thin",
            "Girdle Thick", "Girdle %", "Girdle Condition", "Culet Size", "Culet Condition", "Crown Height",
            "Crown Angle", "Pavilion Depth", "Pavilion Angle", "Inscription", "Cert comment", "KeyToSymbols",
            "White Inclusion", "Black Inclusion", "Open Inclusion", "Fancy Color", "Fancy Color Intensity",
            "Fancy Color Overtone", "Country", "State", "City", "CertFile", "Diamond Video", "Diamond Image",
            "SUPPLIER", "LOCKED", "Diamond Type", "UPLOADED_AT", "Description"
        ]
        
        for col in desired_columns:
            if col not in final_df.columns:
                final_df[col] = ""
        
        if "Diamond Type" not in final_df.columns:
            final_df["Diamond Type"] = "Unknown"
        
        final_df["LOCKED"] = final_df.get("LOCKED", "NO")
        final_df = final_df[desired_columns]
        
        local_path = "/tmp/all_suppliers_stock.xlsx"
        final_df.to_excel(local_path, index=False)
        s3.upload_file(local_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        logger.info(f"✅ Rebuilt combined stock with {len(final_df)} items from {len(dfs)} suppliers")
        
        # Update cache
        startup_cache["stock"] = final_df
        startup_cache["last_loaded"] = time.time()
        
        if os.path.exists(local_path):
            os.remove(local_path)
            
    except Exception as e:
        logger.error(f"❌ Error rebuilding combined stock: {e}")

def atomic_lock_stone(stone_id: str) -> bool:
    """Atomically lock a stone to prevent race conditions"""
    try:
        if not s3:
            return False
            
        local_path = "/tmp/current_stock.xlsx"
        s3.download_file(CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY, local_path)
        df = pd.read_excel(local_path)
        
        if df.empty or "Stock #" not in df.columns or "LOCKED" not in df.columns:
            return False
        
        mask = (df["Stock #"] == stone_id) & (df["LOCKED"] != "YES")
        if not mask.any():
            return False
        
        df.loc[mask, "LOCKED"] = "YES"
        
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)
        
        temp_path = "/tmp/locked_stock.xlsx"
        df.to_excel(temp_path, index=False)
        s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        stone_row = df[df["Stock #"] == stone_id].iloc[0]
        supplier = stone_row.get("SUPPLIER", "")
        
        if supplier:
            supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier}.xlsx"
            try:
                s3.download_file(CONFIG["AWS_BUCKET"], supplier_file, "/tmp/supplier_stock.xlsx")
                supplier_df = pd.read_excel("/tmp/supplier_stock.xlsx")
                
                if "Stock #" in supplier_df.columns and "LOCKED" in supplier_df.columns:
                    supplier_df.loc[supplier_df["Stock #"] == stone_id, "LOCKED"] = "YES"
                    
                    for col in supplier_df.select_dtypes(include="object"):
                        supplier_df[col] = supplier_df[col].map(safe_excel)
                    
                    supplier_df.to_excel("/tmp/supplier_stock.xlsx", index=False)
                    s3.upload_file("/tmp/supplier_stock.xlsx", CONFIG["AWS_BUCKET"], supplier_file)
            except Exception as e:
                logger.error(f"Failed to update supplier file: {e}")
        
        for path in [local_path, temp_path, "/tmp/supplier_stock.xlsx"]:
            if os.path.exists(path):
                os.remove(path)
        
        logger.info(f"✅ Locked stone: {stone_id}")
        
        # Clear cache to force reload
        startup_cache["stock"] = None
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Atomic lock failed for stone {stone_id}: {e}")
        return False

def unlock_stone(stone_id: str):
    """Unlock a stone"""
    try:
        df = load_stock()
        if df.empty:
            return
        
        if "Stock #" not in df.columns or "LOCKED" not in df.columns:
            return
        
        df.loc[df["Stock #"] == stone_id, "LOCKED"] = "NO"
        
        temp_path = "/tmp/all_suppliers_stock.xlsx"
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)
        
        df.to_excel(temp_path, index=False)
        
        if s3:
            s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
        
        stone_row = df[df["Stock #"] == stone_id].iloc[0]
        supplier = stone_row.get("SUPPLIER", "")
        
        if supplier and s3:
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
        
        for path in [temp_path, "/tmp/supplier_stock.xlsx"]:
            if os.path.exists(path):
                os.remove(path)
        
        logger.info(f"✅ Unlocked stone: {stone_id}")
        
        # Clear cache to force reload
        startup_cache["stock"] = None
        
    except Exception as e:
        logger.error(f"❌ Failed to unlock stone {stone_id}: {e}")

def remove_stone_from_supplier_and_combined(stone_id: str):
    """Remove stone from both supplier and combined stock"""
    try:
        df = load_stock()
        if not df.empty and "Stock #" in df.columns:
            df = df[df["Stock #"] != stone_id]
            
            temp_path = "/tmp/all_suppliers_stock.xlsx"
            df.to_excel(temp_path, index=False)
            
            if s3:
                s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], COMBINED_STOCK_KEY)
            
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        if s3:
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
        
        logger.info(f"✅ Removed stone {stone_id} from all stock files")
        
        # Clear cache to force reload
        startup_cache["stock"] = None
        
    except Exception as e:
        logger.error(f"❌ Failed to remove stone {stone_id}: {e}")

# -------- DEAL MANAGEMENT --------
def log_deal_history(deal: Dict[str, Any]):
    """Log deal to history file"""
    try:
        if not s3:
            return
            
        local_path = "/tmp/deal_history.xlsx"
        
        try:
            s3.download_file(CONFIG["AWS_BUCKET"], DEAL_HISTORY_KEY, local_path)
            df = pd.read_excel(local_path)
        except:
            df = pd.DataFrame(columns=[
                "Deal ID", "Stone ID", "Supplier", "Client", "Actual Price",
                "Offer Price", "Supplier Action", "Admin Action", "Final Status", "Created At"
            ])
        
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
        
        logger.info(f"✅ Logged deal to history: {deal.get('deal_id')}")
        
    except Exception as e:
        logger.error(f"❌ Failed to log deal history: {e}")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

# -------- BACKGROUND TASKS --------
async def session_cleanup_loop():
    """Background task to clean up expired sessions"""
    while True:
        try:
            cleanup_sessions()
            logger.debug("✅ Session cleanup completed")
        except Exception as e:
            logger.error(f"❌ Session cleanup error: {e}")
        await asyncio.sleep(600)

async def user_state_cleanup_loop():
    """Background task to clean up old user states"""
    while True:
        try:
            now = time.time()
            stale_users = []
            
            for uid, state in list(user_state.items()):
                last_active = state.get("last_updated", 0)
                if now - last_active > 1800:
                    stale_users.append(uid)
            
            for uid in stale_users:
                user_state.pop(uid, None)
            
            if stale_users:
                logger.info(f"✅ Cleaned up {len(stale_users)} stale user states")
                
        except Exception as e:
            logger.error(f"❌ User state cleanup error: {e}")
        
        await asyncio.sleep(300)

async def keep_alive_pinger():
    """Ping the server periodically to keep it alive"""
    logger.info("🚀 Starting keep-alive pinger...")
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(f"{CONFIG['RENDER_EXTERNAL_URL']}/keep-alive")
                logger.info(f"✅ Keep-alive ping: {response.status_code} at {datetime.now()}")
        except Exception as e:
            logger.warning(f"⚠️ Keep-alive ping failed: {e}")
        await asyncio.sleep(300)  # Ping every 5 minutes

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
        
        # Set webhook
        webhook_url = CONFIG["WEBHOOK_URL"]
        if webhook_url and "your-app-name" not in webhook_url:
            await bot.set_webhook(
                url=webhook_url,
                drop_pending_updates=True,
                allowed_updates=dp.resolve_used_update_types()
            )
            logger.info(f"✅ Webhook set to: {webhook_url}")
        else:
            logger.warning("⚠️ No valid webhook URL set")
        
    except Exception as e:
        logger.error(f"❌ Startup error: {e}")
    
    BOT_STARTED = True
    
    # Start background tasks
    asyncio.create_task(session_cleanup_loop())
    asyncio.create_task(user_state_cleanup_loop())
    # Start keep-alive pinger (NEW)
    asyncio.create_task(keep_alive_pinger())
    
    logger.info("✅ Bot startup complete")
    
    yield  # App runs here
    
    # Shutdown
    logger.info("🤖 Diamond Trading Bot shutting down...")
    
    # Remove webhook
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook removed")
    except Exception as e:
        logger.error(f"❌ Failed to remove webhook: {e}")
    
    # Save sessions before shutdown
    save_sessions()
    
    # Close bot session
    try:
        await bot.session.close()
        logger.info("✅ Bot session closed")
    except Exception as e:
        logger.error(f"❌ Error closing bot session: {e}")
    
    BOT_STARTED = False
    logger.info("✅ Bot shutdown complete")

# -------- PRELOAD DATA --------
def preload_data():
    """Preload data on startup for faster response"""
    logger.info("🔄 Preloading data on startup...")
    try:
        # Load accounts in background
        load_accounts()
        logger.info("✅ Accounts preloaded")
        
        # Load stock in background
        load_stock()
        logger.info("✅ Stock preloaded")
        
    except Exception as e:
        logger.error(f"❌ Preloading failed: {e}")

# -------- FASTAPI APP --------
app = FastAPI(title="Diamond Trading Bot", lifespan=lifespan)

# -------- HEALTH CHECK ENDPOINTS --------
@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Diamond Trading Bot",
        "version": "1.0",
        "bot_started": BOT_STARTED,
        "webhook_url": CONFIG["WEBHOOK_URL"],
        "timestamp": datetime.now().isoformat(),
        "active_sessions": len(logged_in_users),
        "aws_connected": s3 is not None,
        "bucket": CONFIG["AWS_BUCKET"]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for Render monitoring"""
    status = "healthy" if BOT_STARTED else "starting"
    
    # Test AWS connection if available
    aws_status = "connected" if s3 else "disconnected"
    bucket_accessible = False
    
    if s3:
        try:
            s3.head_bucket(Bucket=CONFIG["AWS_BUCKET"])
            bucket_accessible = True
        except:
            pass
    
    return {
        "status": status,
        "bot": "running" if BOT_STARTED else "stopped",
        "webhook": "set" if CONFIG["WEBHOOK_URL"] else "not set",
        "aws": aws_status,
        "bucket_accessible": bucket_accessible,
        "active_users": len(logged_in_users),
        "timestamp": datetime.now().isoformat(),
        "message": "Use /keep-alive endpoint for uptime monitoring"
    }

@app.get("/ping")
async def ping():
    """Simple ping endpoint"""
    return {
        "status": "pong",
        "service": "Diamond Trading Bot",
        "timestamp": datetime.now().isoformat(),
        "bot_status": "running" if BOT_STARTED else "starting",
        "active_users": len(logged_in_users),
        "message": "Bot is alive and responding!"
    }

@app.get("/keep-alive")
async def keep_alive():
    """Endpoint for uptime monitoring services to ping"""
    return {
        "status": "alive",
        "timestamp": datetime.now().isoformat(),
        "bot_status": "running" if BOT_STARTED else "starting",
        "active_sessions": len(logged_in_users),
        "cache_hit": startup_cache["accounts"] is not None,
        "message": "Bot is kept alive by monitoring service",
        "keep_alive_url": f"{CONFIG['RENDER_EXTERNAL_URL']}/health"
    }

@app.get("/status")
async def status_check():
    """Comprehensive status check"""
    status = {
        "service": "Diamond Trading Bot",
        "status": "healthy" if BOT_STARTED else "starting",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0",
        
        "bot": {
            "started": BOT_STARTED,
            "token_set": bool(CONFIG.get("BOT_TOKEN")),
            "webhook_url": CONFIG.get("WEBHOOK_URL"),
            "active_sessions": len(logged_in_users)
        },
        
        "aws": {
            "s3_connected": s3 is not None,
            "bucket": CONFIG.get("AWS_BUCKET"),
            "region": CONFIG.get("AWS_REGION"),
            "bucket_accessible": None
        },
        
        "system": {
            "python_version": CONFIG.get("PYTHON_VERSION"),
            "port": CONFIG.get("PORT"),
            "session_timeout": CONFIG.get("SESSION_TIMEOUT"),
            "cache_loaded": startup_cache["accounts"] is not None,
            "cache_age_seconds": time.time() - startup_cache["last_loaded"] if startup_cache["last_loaded"] > 0 else 0
        }
    }
    
    # Test AWS connection
    if s3:
        try:
            s3.list_objects_v2(Bucket=CONFIG["AWS_BUCKET"], MaxKeys=1)
            status["aws"]["bucket_accessible"] = True
        except Exception as e:
            status["aws"]["bucket_accessible"] = False
            status["aws"]["error"] = str(e)
    
    return status

@app.get("/sessions")
async def get_sessions():
    """Admin endpoint to view active sessions"""
    return {
        "active_sessions": len(logged_in_users),
        "sessions": logged_in_users
    }

# -------- NEW API ENDPOINTS FOR EXCEL UPLOAD --------
@app.post("/api/upload-excel")
async def api_upload_excel(
    file: UploadFile = File(...),
    telegram_id: str = Form(...),
    username: str = Form(...)
):
    """API endpoint for Excel upload with flexible optional columns"""
    try:
        # Check if user exists and is supplier
        user = get_user_by_username(username)
        if not user or user.get("ROLE") != "supplier":
            return JSONResponse(
                status_code=403,
                content={"success": False, "message": "Only suppliers can upload stock"}
            )
        
        # Check file size
        if file.size > 10 * 1024 * 1024:  # 10MB
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "File size exceeds 10MB limit"}
            )
        
        # Check file extension
        if not file.filename.endswith(('.xlsx', '.xls')):
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "Only Excel files (.xlsx, .xls) are allowed"}
            )
        
        # Read Excel file
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))
        
        # Validate and parse Excel
        supplier_name = f"supplier_{username.lower()}"
        validator = DiamondExcelValidator()
        success, cleaned_df, errors, warnings = validator.validate_and_parse(df, supplier_name)
        
        if not success:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Validation failed",
                    "errors": errors,
                    "warnings": warnings
                }
            )
        
        # Save to S3
        supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier_name}.xlsx"
        temp_path = f"/tmp/{supplier_name}.xlsx"
        cleaned_df.to_excel(temp_path, index=False)
        
        if s3:
            s3.upload_file(temp_path, CONFIG["AWS_BUCKET"], supplier_file)
            logger.info(f"✅ Uploaded {len(cleaned_df)} diamonds for supplier {username}")
        
        # Rebuild combined stock
        rebuild_combined_stock()
        
        # Calculate statistics
        total_stones = len(cleaned_df)
        total_carats = cleaned_df["Weight"].sum() if "Weight" in cleaned_df.columns else 0
        total_value = (cleaned_df["Weight"] * cleaned_df["Price Per Carat"]).sum() if "Weight" in cleaned_df.columns and "Price Per Carat" in cleaned_df.columns else 0
        
        # Log activity
        log_activity(user, "API_UPLOAD_STOCK", {
            "stones": total_stones,
            "carats": total_carats,
            "value": total_value,
            "warnings": warnings
        })
        
        # Clean up
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Successfully uploaded {total_stones} diamonds",
                "stats": {
                    "total_diamonds": total_stones,
                    "total_carats": float(total_carats),
                    "total_value": float(total_value),
                    "avg_price_per_carat": float(cleaned_df["Price Per Carat"].mean()) if "Price Per Carat" in cleaned_df.columns else 0
                },
                "warnings": warnings
            }
        )
        
    except Exception as e:
        logger.error(f"❌ API upload error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Server error: {str(e)}"}
        )

@app.get("/api/download-template")
async def api_download_template():
    """Download sample Excel template"""
    try:
        # Create sample Excel template
        sample_data = {
            "Stock #": ["DIA001", "DIA002", "DIA003"],
            "Shape": ["Round", "Princess", "Oval"],
            "Weight": [1.20, 0.90, 1.50],
            "Color": ["D", "F", "G"],
            "Clarity": ["IF", "VVS1", "VS1"],
            "Price Per Carat": [12000, 9500, 7500],
            "Lab": ["GIA", "IGI", "HRD"],
            "Report #": ["1234567890", "2345678901", "3456789012"],
            "Diamond Type": ["Natural", "Natural", "Lab Grown"],
            "Description": ["Eye clean round", "Excellent princess", "Nice oval"],
            
            # OPTIONAL COLUMNS (can be blank)
            "CUT": ["EX", "VG", ""],
            "Polish": ["EX", "", "VG"],
            "Symmetry": ["EX", "VG", ""]
        }
        
        df = pd.DataFrame(sample_data)
        
        # Create Excel file in memory
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Sample Stock', index=False)
            
            # Add instructions sheet
            instructions_data = {
                "Column Name": DiamondExcelValidator.ALL_COLUMNS,
                "Required?": ["REQUIRED"] * len(DiamondExcelValidator.REQUIRED_COLUMNS) + 
                             ["OPTIONAL"] * len(DiamondExcelValidator.OPTIONAL_COLUMNS),
                "Description": [
                    "Unique identifier for each diamond",
                    "Shape of the diamond (Round, Princess, Oval, etc.)",
                    "Weight in carats (e.g., 1.20)",
                    "Color grade (D, E, F, etc.)",
                    "Clarity grade (IF, VVS1, VS2, etc.)",
                    "Price per carat in USD",
                    "Certification lab (GIA, IGI, HRD, etc.)",
                    "Certificate/report number",
                    "Type of diamond (Natural, Lab Grown, etc.)",
                    "Description or comments about the diamond",
                    "Cut grade (EX, VG, G, F, P) - CAN BE BLANK",
                    "Polish grade (EX, VG, G, F, P) - CAN BE BLANK",
                    "Symmetry grade (EX, VG, G, F, P) - CAN BE BLANK"
                ],
                "Example": [
                    "DIA001, STK100, 12345",
                    "Round, Princess, Oval",
                    "1.20, 0.90, 1.50",
                    "D, F, G",
                    "IF, VVS1, VS2",
                    "12000, 9500, 7500",
                    "GIA, IGI, HRD",
                    "1234567890, G12345",
                    "Natural, Lab Grown",
                    "Eye clean, No fluorescence",
                    "EX, VG, G",
                    "EX, VG, G",
                    "EX, VG, G"
                ]
            }
            
            instructions_df = pd.DataFrame(instructions_data)
            instructions_df.to_excel(writer, sheet_name='Instructions', index=False)
            
            # Format column widths
            for column in instructions_df:
                column_length = max(
                    instructions_df[column].astype(str).map(len).max(),
                    len(str(column))
                )
                col_idx = instructions_df.columns.get_loc(column)
                writer.sheets['Instructions'].column_dimensions[chr(65 + col_idx)].width = column_length + 2
        
        buffer.seek(0)
        
        # Return as file download
        return FileResponse(
            path=buffer,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename='diamond_stock_template.xlsx'
        )
        
    except Exception as e:
        logger.error(f"❌ Template generation error: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": f"Error generating template: {str(e)}"}
        )

@app.post("/webhook")
async def telegram_webhook(request: Request):
    """Handle Telegram webhook updates"""
    try:
        update_data = await request.json()
        logger.info(f"📨 Received webhook update type: {update_data.get('update_id')}")
        
        telegram_update = types.Update(**update_data)
        await dp.feed_update(bot=bot, update=telegram_update)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/setwebhook")
async def set_webhook_endpoint():
    """Manual endpoint to set webhook (for testing)"""
    try:
        webhook_url = CONFIG["WEBHOOK_URL"]
        if not webhook_url or "your-app-name" in webhook_url:
            return {"status": "error", "error": "Please set a valid WEBHOOK_URL in environment variables"}
        
        await bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=dp.resolve_used_update_types()
        )
        return {"status": "success", "webhook_url": webhook_url}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/deletewebhook")
async def delete_webhook_endpoint():
    """Manual endpoint to delete webhook"""
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        return {"status": "success", "message": "Webhook deleted"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/test")
async def test_bot():
    """Test if bot can send messages"""
    try:
        # Send a test message to yourself
        test_chat_id = CONFIG.get("TEST_CHAT_ID")
        if test_chat_id and test_chat_id.isdigit():
            await bot.send_message(
                chat_id=int(test_chat_id),
                text="🤖 Bot is working! Test message."
            )
            return {"status": "success", "message": "Test message sent"}
        else:
            return {"status": "warning", "message": "TEST_CHAT_ID not set or is invalid"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

# -------- COMMAND HANDLERS --------
@dp.message(Command("start"))
async def start(message: types.Message):
    """Handle /start command"""
    try:
        await message.reply(
            "💎 Welcome to Diamond Trading Bot!\n\n"
            "Use /login to sign in\n"
            "Use /createaccount to register\n"
            "Use /help for assistance",
            reply_markup=types.ReplyKeyboardRemove()
        )
        logger.info(f"✅ /start command handled for user {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Error in /start handler: {e}")

@dp.message(Command("help"))
async def help_command(message: types.Message):
    """Handle /help command"""
    try:
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

**For Suppliers:**
• Optional columns (CUT, Polish, Symmetry) can be left blank
• Required columns must have values
• Stock # must be unique

**Need help?** Contact system administrator.
"""
        await message.reply(help_text)
        logger.info(f"✅ /help command handled for user {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Error in /help handler: {e}")

@dp.message(Command("createaccount"))
async def create_account(message: types.Message):
    """Start account creation process"""
    try:
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
        logger.info(f"✅ Account creation started for user {uid}")
    except Exception as e:
        logger.error(f"❌ Error in create_account handler: {e}")

@dp.message(Command("login"))
async def login_command(message: types.Message):
    """Start login process"""
    try:
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
        
        # Start new login
        user_state[uid] = {
            "step": "login_username",
            "last_updated": time.time()
        }
        
        await message.reply("👤 Enter your username:")
        logger.info(f"✅ Login started for user {uid}")
    except Exception as e:
        logger.error(f"❌ Error in login_command handler: {e}")

@dp.message(Command("logout"))
async def logout_command(message: types.Message):
    """Handle /logout command"""
    try:
        uid = message.from_user.id
        user = get_logged_user(uid)
        
        if not user:
            await message.reply("ℹ️ You are not logged in.")
            return
        
        log_activity(user, "LOGOUT")
        
        logged_in_users.pop(uid, None)
        user_state.pop(uid, None)
        save_sessions()
        
        await message.reply(
            "✅ Successfully logged out.\n"
            "Use /login to sign in again.",
            reply_markup=types.ReplyKeyboardRemove()
        )
        logger.info(f"✅ User {user['USERNAME']} logged out")
    except Exception as e:
        logger.error(f"❌ Error in logout_command handler: {e}")

@dp.message(Command("reset"))
async def reset_state_command(message: types.Message):
    """Reset user state"""
    try:
        uid = message.from_user.id
        user_state.pop(uid, None)
        await message.reply("✅ Login state reset. Use /login to start again.")
        logger.info(f"✅ User state reset for user {uid}")
    except Exception as e:
        logger.error(f"❌ Error in reset_state_command handler: {e}")

# -------- STATE HANDLER --------
@dp.message()
async def handle_all_messages(message: types.Message):
    """Main message handler for state-based flows"""
    try:
        uid = message.from_user.id
        text = message.text.strip()
        
        logger.info(f"📩 Received message from {uid}: {text}")
        
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
                username = text
                
                if len(username) < 3:
                    await message.reply("❌ Username must be at least 3 characters.")
                    return
                
                df = load_accounts()
                if not df[df["USERNAME"].str.lower() == username.lower()].empty:
                    await message.reply("❌ Username already exists.")
                    user_state.pop(uid, None)
                    return
                
                state["username"] = username
                state["step"] = "create_password"
                
                await message.reply("🔐 Enter password (minimum 4 characters):")
                return
            
            elif state.get("step") == "create_password":
                password = text
                
                if len(password) < 4:
                    await message.reply("❌ Password must be at least 4 characters.")
                    return
                
                username = state["username"]
                
                df = load_accounts()
                new_row = {
                    "USERNAME": username,
                    "PASSWORD": clean_password(password),
                    "ROLE": "client",
                    "APPROVED": "NO"
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
                
                admin_df = df[df["ROLE"].str.lower() == "admin"]
                for _, admin in admin_df.iterrows():
                    save_notification(
                        admin["USERNAME"],
                        "admin",
                        f"📝 New account pending approval: {username}"
                    )
                
                log_activity({"USERNAME": username, "ROLE": "client", "TELEGRAM_ID": uid}, "ACCOUNT_CREATED")
                return
            
            # Login flow
            elif state.get("step") == "login_username":
                username = text
                state["login_username"] = username
                state["step"] = "login_password"
                
                await message.reply("🔐 Enter password:")
                return
            
            elif state.get("step") == "login_password":
                password = text
                username = state.get("login_username", "")
                
                df = load_accounts()
                
                if df.empty:
                    await message.reply("❌ No accounts found in system.")
                    user_state.pop(uid, None)
                    return
                
                df["USERNAME"] = df["USERNAME"].apply(clean_text)
                df["PASSWORD"] = df["PASSWORD"].apply(clean_password)
                df["APPROVED"] = df["APPROVED"].apply(clean_text).str.upper()
                df["ROLE"] = df["ROLE"].apply(clean_text).str.lower()
                
                username_clean = clean_text(username)
                password_clean = clean_password(password)
                
                user_row = df[
                    (df["USERNAME"].str.lower() == username_clean.lower()) &
                    (df["PASSWORD"] == password_clean) &
                    (df["APPROVED"] == "YES")
                ]
                
                if user_row.empty:
                    await message.reply(
                        "❌ Invalid login credentials\n\n"
                        "Possible reasons:\n"
                        "• Username/password incorrect\n"
                        "• Account not approved by admin\n"
                        "• Account doesn't exist\n\n"
                        "Please check your credentials and try again."
                    )
                    user_state.pop(uid, None)
                    return
                
                user_data = user_row.iloc[0].to_dict()
                role = user_data["ROLE"].lower()
                
                logged_in_users[uid] = {
                    "USERNAME": user_data["USERNAME"],
                    "ROLE": role,
                    "SUPPLIER_KEY": f"supplier_{user_data['USERNAME'].lower()}" if role == "supplier" else None,
                    "last_active": time.time()
                }
                save_sessions()
                
                log_activity(logged_in_users[uid], "LOGIN")
                
                if role == "admin":
                    kb = admin_kb
                    welcome_msg = f"👑 Welcome Admin {user_data['USERNAME']}"
                elif role == "supplier":
                    kb = supplier_kb
                    welcome_msg = f"💎 Welcome Supplier {user_data['USERNAME']}"
                else:
                    kb = client_kb
                    welcome_msg = f"🥂 Welcome {user_data['USERNAME']}"
                
                await message.reply(welcome_msg, reply_markup=kb)
                
                notifications = fetch_unread_notifications(user_data["USERNAME"], role)
                if notifications:
                    note_msg = "🔔 **Unread Notifications**\n\n"
                    for note in notifications[:5]:
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
            
    except Exception as e:
        logger.error(f"❌ Error in handle_all_messages: {e}", exc_info=True)
        await message.reply("❌ An error occurred. Please try again.")

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
        
        total_diamonds = len(filtered_df)
        total_carats = filtered_df["Weight"].sum()
        
        if total_diamonds > 10:
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
        
        if not atomic_lock_stone(stone_id):
            await message.reply("🔒 Stone is no longer available.")
            user_state.pop(uid, None)
            return
        
        if s3:
            deal_key = f"{DEALS_FOLDER}{deal_id}.json"
            s3.put_object(
                Bucket=CONFIG["AWS_BUCKET"],
                Key=deal_key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )
        
        log_deal_history(deal)
        
        save_notification(
            deal["supplier_username"],
            "supplier",
            f"📩 New deal offer for Stone {stone_id}\n"
            f"💰 Offer: ${offer_price}/ct"
        )
        
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
    try:
        text = message.text
        role = user.get("ROLE", "").lower()
        
        logger.info(f"Button pressed: {text} by {user['USERNAME']} (role: {role})")
        
        # Admin buttons
        if role == "admin":
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
        
        # Supplier buttons
        elif role == "supplier":
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
        
        # Client buttons
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
                
    except Exception as e:
        logger.error(f"❌ Error in handle_logged_in_buttons: {e}", exc_info=True)
        await message.reply("❌ An error occurred. Please try again.")

# -------- ADMIN HANDLERS --------
async def view_all_stock(message: types.Message, user: Dict):
    """Admin: View all stock"""
    try:
        df = load_stock()
        
        if df.empty:
            await message.reply("❌ No stock available.")
            return
        
        total_diamonds = len(df)
        total_carats = df["Weight"].sum()
        total_value = (df["Weight"] * df["Price Per Carat"]).sum()
        
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
        
        excel_path = "/tmp/all_stock.xlsx"
        df.to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=f"📊 Complete Stock List ({total_diamonds} diamonds)"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
        
        log_activity(user, "VIEW_ALL_STOCK")
        
    except Exception as e:
        logger.error(f"❌ Error in view_all_stock: {e}")
        await message.reply("❌ Failed to load stock data.")

async def view_users(message: types.Message, user: Dict):
    """Admin: View all users"""
    try:
        df = load_accounts()
        
        if df.empty:
            await message.reply("❌ No users found.")
            return
        
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
        
        excel_path = "/tmp/all_users.xlsx"
        df.to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=f"👥 User List ({len(df)} users)"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
        
        log_activity(user, "VIEW_USERS")
        
    except Exception as e:
        logger.error(f"❌ Error in view_users: {e}")
        await message.reply("❌ Failed to load user data.")

async def pending_accounts(message: types.Message, user: Dict):
    """Admin: View pending accounts"""
    try:
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
        
    except Exception as e:
        logger.error(f"❌ Error in pending_accounts: {e}")
        await message.reply("❌ Failed to load pending accounts.")

async def supplier_leaderboard(message: types.Message, user: Dict):
    """Admin: Supplier leaderboard"""
    try:
        df = load_stock()
        
        if df.empty or "SUPPLIER" not in df.columns:
            await message.reply("❌ No supplier data available.")
            return
        
        supplier_stats = df.groupby("SUPPLIER").agg(
            Stones=("SUPPLIER", "count"),
            Total_Carats=("Weight", "sum"),
            Avg_Price_Per_Carat=("Price Per Carat", "mean"),
            Total_Value=("Weight", lambda x: (x * df.loc[x.index, "Price Per Carat"]).sum())
        ).round(2)
        
        supplier_stats = supplier_stats.sort_values("Stones", ascending=False)
        
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
        
        excel_path = "/tmp/supplier_leaderboard.xlsx"
        supplier_stats.to_excel(excel_path)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption="📊 Supplier Leaderboard Details"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
        
        log_activity(user, "VIEW_SUPPLIER_LEADERBOARD")
        
    except Exception as e:
        logger.error(f"❌ Error in supplier_leaderboard: {e}")
        await message.reply("❌ Failed to load supplier data.")

async def user_activity_report(message: types.Message, user: Dict):
    """Admin: Generate activity report"""
    try:
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
        
    except Exception as e:
        logger.error(f"❌ Error in user_activity_report: {e}")
        await message.reply("❌ Failed to generate activity report.")

async def delete_supplier_stock(message: types.Message, user: Dict):
    """Admin: Delete all supplier stock (with confirmation)"""
    try:
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
        
    except Exception as e:
        logger.error(f"❌ Error in delete_supplier_stock: {e}")
        await message.reply("❌ An error occurred.")

# -------- SUPPLIER HANDLERS --------
async def upload_excel_prompt(message: types.Message, user: Dict):
    """Supplier: Prompt for Excel upload"""
    try:
        await message.reply(
            "📤 **Upload Stock Excel File**\n\n"
            "Please send an Excel file with your diamond stock.\n\n"
            "**Required Columns:**\n"
            "• Stock # (Unique ID)\n"
            "• Shape, Weight, Color, Clarity\n"
            "• Price Per Carat\n"
            "• Lab, Report #\n"
            "• Diamond Type, Description\n\n"
            "**Optional Columns (can be blank):**\n"
            "• CUT, Polish, Symmetry\n\n"
            "**File Requirements:**\n"
            "• Max size: 10MB\n"
            "• Format: .xlsx or .xls\n"
            "• No duplicate Stock #\n\n"
            "Send your file now or use '📥 Download Sample Excel' first."
        )
        
        log_activity(user, "UPLOAD_PROMPT")
        
    except Exception as e:
        logger.error(f"❌ Error in upload_excel_prompt: {e}")
        await message.reply("❌ An error occurred.")

async def supplier_my_stock(message: types.Message, user: Dict):
    """Supplier: View own stock"""
    try:
        if not s3:
            await message.reply("❌ AWS connection not available.")
            return
            
        supplier_key = user.get("SUPPLIER_KEY", f"supplier_{user['USERNAME'].lower()}")
        stock_key = f"{SUPPLIER_STOCK_FOLDER}{supplier_key}.xlsx"
        
        try:
            local_path = "/tmp/my_stock.xlsx"
            s3.download_file(CONFIG["AWS_BUCKET"], stock_key, local_path)
            
            df = pd.read_excel(local_path)
            
            total_stones = len(df)
            total_carats = df["Weight"].sum() if "Weight" in df.columns else 0
            avg_price = df["Price Per Carat"].mean() if "Price Per Carat" in df.columns else 0
            total_value = (df["Weight"] * df["Price Per Carat"]).sum() if "Weight" in df.columns and "Price Per Carat" in df.columns else 0
            
            stats_msg = (
                f"📦 **Your Stock Summary**\n\n"
                f"💎 Total Diamonds: {total_stones}\n"
                f"⚖️ Total Carats: {total_carats:.2f}\n"
                f"💰 Average Price: ${avg_price:,.2f}/ct\n"
                f"🏦 Total Value: ${total_value:,.2f}\n\n"
            )
            
            if "Shape" in df.columns and not df["Shape"].empty:
                shape_counts = df["Shape"].value_counts().head(5)
                if not shape_counts.empty:
                    stats_msg += f"**Stock Distribution:**\n"
                    for shape, count in shape_counts.items():
                        stats_msg += f"• {shape}: {count}\n"
            
            await message.reply(stats_msg)
            
            await message.reply_document(
                types.FSInputFile(local_path),
                caption=f"📦 Your Stock File ({total_stones} diamonds)"
            )
            
            log_activity(user, "VIEW_MY_STOCK")
            
        except Exception as e:
            logger.error(f"❌ Error loading supplier stock: {e}")
            await message.reply("❌ You haven't uploaded any stock yet or there was an error loading it.")
    except Exception as e:
        logger.error(f"❌ Error in supplier_my_stock: {e}")
        await message.reply("❌ Failed to load stock data.")
    finally:
        if os.path.exists("/tmp/my_stock.xlsx"):
            os.remove("/tmp/my_stock.xlsx")

async def supplier_analytics(message: types.Message, user: Dict):
    """Supplier: Price analytics"""
    try:
        supplier_key = user.get("SUPPLIER_KEY", f"supplier_{user['USERNAME'].lower()}")
        
        df = load_stock()
        if df.empty:
            await message.reply("❌ No market data available.")
            return
        
        my_stones = df[df["SUPPLIER"].str.lower() == supplier_key.lower()]
        
        if my_stones.empty:
            await message.reply("❌ You have no stones in the market.")
            return
        
        results = []
        
        for _, stone in my_stones.iterrows():
            similar = df[
                (df["Shape"] == stone["Shape"]) &
                (df["Color"] == stone["Color"]) &
                (df["Clarity"] == stone["Clarity"]) &
                (df["Diamond Type"] == stone.get("Diamond Type", "")) &
                (abs(df["Weight"] - stone["Weight"]) <= 0.2)
            ]
            
            if len(similar) > 1:
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
        
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values("Diff %", ascending=False)
        
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
        
        excel_path = "/tmp/price_analytics.xlsx"
        results_df.to_excel(excel_path, index=False)
        
        await message.reply_document(
            types.FSInputFile(excel_path),
            caption=f"📊 Detailed Price Analysis ({len(results_df)} stones)"
        )
        
        if os.path.exists(excel_path):
            os.remove(excel_path)
        
        log_activity(user, "VIEW_ANALYTICS")
        
    except Exception as e:
        logger.error(f"❌ Error in supplier_analytics: {e}")
        await message.reply("❌ Failed to load analytics data.")

async def download_sample_excel(message: types.Message, user: Dict):
    """Supplier: Download sample Excel template"""
    try:
        # Create sample data with optional columns blank
        sample_data = {
            "Stock #": ["D001", "D002", "D003"],
            "Shape": ["Round", "Oval", "Princess"],
            "Weight": [1.0, 1.5, 2.0],
            "Color": ["D", "E", "F"],
            "Clarity": ["VVS1", "VS1", "SI1"],
            "Price Per Carat": [10000, 8500, 7000],
            "Lab": ["GIA", "IGI", "HRD"],
            "Report #": ["1234567890", "2345678901", "3456789012"],
            "Diamond Type": ["Natural", "Natural", "LGD"],
            "Description": ["Excellent cut round", "Nice oval diamond", "Good princess cut"],
            
            # OPTIONAL COLUMNS - Some can be blank
            "CUT": ["EX", "VG", ""],
            "Polish": ["EX", "", "VG"],
            "Symmetry": ["EX", "VG", ""]
        }
        
        df = pd.DataFrame(sample_data)
        
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Stock')
            
            instructions = pd.DataFrame({
                "Column": DiamondExcelValidator.ALL_COLUMNS,
                "Required": ["Yes"] * len(DiamondExcelValidator.REQUIRED_COLUMNS) + 
                           ["No"] * len(DiamondExcelValidator.OPTIONAL_COLUMNS),
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
                    "Brief description of the diamond",
                    "Cut grade (EX, VG, G, F, P) - CAN BE BLANK",
                    "Polish grade (EX, VG, G, F, P) - CAN BE BLANK",
                    "Symmetry grade (EX, VG, G, F, P) - CAN BE BLANK"
                ],
                "Example": [
                    "D001", "Round", "1.0", "D", "VVS1", "10000", "GIA", "123456", "Natural", "Excellent cut",
                    "EX", "EX", "EX"
                ]
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
                "• Remove sample rows before uploading\n"
                "• Optional columns (CUT, Polish, Symmetry) can be left blank"
            )
        )
        
        log_activity(user, "DOWNLOAD_SAMPLE_EXCEL")
        
    except Exception as e:
        logger.error(f"❌ Error in download_sample_excel: {e}")
        await message.reply("❌ Failed to generate sample template.")

# -------- CLIENT HANDLERS --------
async def search_diamonds_start(message: types.Message, user: Dict):
    """Client: Start diamond search"""
    try:
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
        
        log_activity(user, "START_SEARCH")
        
    except Exception as e:
        logger.error(f"❌ Error in search_diamonds_start: {e}")
        await message.reply("❌ An error occurred. Please try again.")

async def smart_deals(message: types.Message, user: Dict):
    """Client: Find smart deals (discounted diamonds)"""
    try:
        df = load_stock()
        
        if df.empty:
            await message.reply("❌ No diamonds available.")
            return
        
        df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
        df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
        
        group_cols = ["Shape", "Color", "Clarity", "Diamond Type"]
        df["Market_Avg"] = df.groupby(group_cols)["Price Per Carat"].transform("median")
        
        df["Discount_%"] = ((df["Market_Avg"] - df["Price Per Carat"]) / df["Market_Avg"] * 100).round(1)
        
        good_deals = df[df["Discount_%"] >= 10].sort_values("Discount_%", ascending=False)
        
        if good_deals.empty:
            await message.reply("😔 No strong deals found right now.")
            return
        
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
        
    except Exception as e:
        logger.error(f"❌ Error in smart_deals: {e}")
        await message.reply("❌ Failed to load smart deals.")

async def request_deal_start(message: types.Message, user: Dict):
    """Client: Start deal request process"""
    try:
        df = load_stock()
        
        if df.empty:
            await message.reply("❌ No diamonds available for deals.")
            return
        
        if len(df) > 20:
            template_df = pd.DataFrame(columns=["Stock #", "Offer Price ($/ct)"])
            
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
            user_state[message.from_user.id] = {
                "step": "deal_stone",
                "last_updated": time.time()
            }
            
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
        
        log_activity(user, "START_DEAL_REQUEST")
        
    except Exception as e:
        logger.error(f"❌ Error in request_deal_start: {e}")
        await message.reply("❌ Failed to load available stones.")

# -------- DEAL VIEWING --------
async def view_deals(message: types.Message, user: Dict):
    """View deals based on user role"""
    try:
        if not s3:
            await message.reply("❌ AWS connection not available.")
            return
            
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=DEALS_FOLDER
        )
        
        if "Contents" not in objs:
            await message.reply("ℹ️ No deals found.")
            return
        
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
        
        deals.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        
        user_role = user["ROLE"]
        username = user["USERNAME"].lower()
        
        if user_role == "admin":
            filtered_deals = deals
            title = "All Deals"
            
        elif user_role == "supplier":
            filtered_deals = [
                d for d in deals 
                if d.get("supplier_username", "").lower() == username
            ]
            title = "Your Deals"
            
        elif user_role == "client":
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
        
        summary_msg = f"🤝 **{title}**\n\n"
        summary_msg += f"Total: {len(filtered_deals)} deals\n\n"
        
        status_counts = {}
        for deal in filtered_deals:
            status = deal.get("final_status", "OPEN")
            status_counts[status] = status_counts.get(status, 0) + 1
        
        for status, count in status_counts.items():
            summary_msg += f"• {status}: {count}\n"
        
        await message.reply(summary_msg)
        
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
        logger.error(f"❌ Error in view_deals: {e}")
        await message.reply("❌ Failed to load deals.")

# -------- CALLBACK QUERY HANDLERS --------
@dp.callback_query(F.data.startswith("approve:"))
async def approve_user_callback(callback: types.CallbackQuery):
    """Approve pending user account"""
    try:
        admin = get_logged_user(callback.from_user.id)
        
        if not is_admin(admin):
            await callback.answer("❌ Admin only", show_alert=True)
            return
        
        username = callback.data.split(":")[1]
        
        df = load_accounts()
        
        if df[df["USERNAME"] == username].empty:
            await callback.answer("❌ User not found", show_alert=True)
            return
        
        df.loc[df["USERNAME"] == username, "APPROVED"] = "YES"
        save_accounts(df)
        
        save_notification(username, "client", "✅ Your account has been approved by admin!")
        
        log_activity(admin, "APPROVE_USER", {"username": username})
        
        await callback.message.edit_text(
            f"✅ **{username}** approved successfully!",
            reply_markup=None
        )
        await callback.answer("Approved ✅")
        
    except Exception as e:
        logger.error(f"❌ Error in approve_user_callback: {e}")
        await callback.answer("❌ Error approving user", show_alert=True)

@dp.callback_query(F.data.startswith("reject:"))
async def reject_user_callback(callback: types.CallbackQuery):
    """Reject pending user account"""
    try:
        admin = get_logged_user(callback.from_user.id)
        
        if not is_admin(admin):
            await callback.answer("❌ Admin only", show_alert=True)
            return
        
        username = callback.data.split(":")[1]
        
        df = load_accounts()
        
        if df[df["USERNAME"] == username].empty:
            await callback.answer("❌ User not found", show_alert=True)
            return
        
        df = df[df["USERNAME"] != username]
        save_accounts(df)
        
        log_activity(admin, "REJECT_USER", {"username": username})
        
        await callback.message.edit_text(
            f"❌ **{username}** rejected and removed.",
            reply_markup=None
        )
        await callback.answer("Rejected ❌")
        
    except Exception as e:
        logger.error(f"❌ Error in reject_user_callback: {e}")
        await callback.answer("❌ Error rejecting user", show_alert=True)

@dp.callback_query(F.data == "confirm_delete_stock")
async def confirm_delete_stock(callback: types.CallbackQuery):
    """Confirm and delete all supplier stock"""
    try:
        admin = get_logged_user(callback.from_user.id)
        
        if not is_admin(admin):
            await callback.answer("❌ Admin only", show_alert=True)
            return
        
        if not s3:
            await callback.answer("❌ AWS connection not available", show_alert=True)
            return
        
        objs = s3.list_objects_v2(
            Bucket=CONFIG["AWS_BUCKET"],
            Prefix=SUPPLIER_STOCK_FOLDER
        )
        
        deleted_count = 0
        if "Contents" in objs:
            for obj in objs["Contents"]:
                s3.delete_object(Bucket=CONFIG["AWS_BUCKET"], Key=obj["Key"])
                deleted_count += 1
        
        try:
            s3.delete_object(Bucket=CONFIG["AWS_BUCKET"], Key=COMBINED_STOCK_KEY)
        except:
            pass
        
        log_activity(admin, "DELETE_ALL_STOCK", {"deleted_files": deleted_count})
        
        await callback.message.edit_text(
            f"🗑 **All supplier stock deleted successfully!**\n\n"
            f"Deleted {deleted_count} files.",
            reply_markup=None
        )
        await callback.answer("Deleted ✅")
        
    except Exception as e:
        logger.error(f"❌ Error in confirm_delete_stock: {e}")
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
    try:
        uid = message.from_user.id
        user = get_logged_user(uid)
        
        if not user:
            await message.reply("🔒 Please login first.")
            return
        
        if message.document.file_size > 10 * 1024 * 1024:
            await message.reply("❌ File too large. Max size is 10MB.")
            return
        
        file_name = message.document.file_name.lower()
        if not file_name.endswith(('.xlsx', '.xls')):
            await message.reply("❌ Only Excel files (.xlsx, .xls) are allowed.")
            return
        
        file = await bot.get_file(message.document.file_id)
        temp_path = f"/tmp/{uid}_{int(time.time())}_{file_name}"
        await bot.download_file(file.file_path, temp_path)
        
        df = pd.read_excel(temp_path)
        
        state = user_state.get(uid, {})
        
        if user["ROLE"] == "supplier" and not state.get("step") == "bulk_deal_excel":
            await handle_supplier_stock_upload(message, user, df, temp_path)
            
        elif user["ROLE"] == "client" and state.get("step") == "bulk_deal_excel":
            await handle_bulk_deal_requests(message, user, df, temp_path)
            
        elif user["ROLE"] == "admin":
            await handle_admin_deal_approvals(message, user, df, temp_path)
            
        elif user["ROLE"] == "supplier" and "Deal ID" in df.columns:
            await handle_supplier_deal_responses(message, user, df, temp_path)
            
        else:
            await message.reply("❌ Invalid file format or action.")
            
    except Exception as e:
        logger.error(f"❌ Error in handle_document: {e}", exc_info=True)
        await message.reply(f"❌ Error processing file: {str(e)}")
        
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

async def handle_supplier_stock_upload(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle supplier stock upload"""
    try:
        # Validate using the new validator
        supplier_name = f"supplier_{user['USERNAME'].lower()}"
        validator = DiamondExcelValidator()
        success, cleaned_df, errors, warnings = validator.validate_and_parse(df, supplier_name)
        
        if not success:
            error_msg = "❌ **Upload Failed**\n\n"
            error_msg += "**Errors:**\n"
            for error in errors[:5]:
                error_msg += f"• {error}\n"
            
            if warnings:
                error_msg += "\n**Warnings:**\n"
                for warning in warnings[:3]:
                    error_msg += f"⚠️ {warning}\n"
            
            await message.reply(error_msg)
            return
        
        # Save to S3
        supplier_file = f"{SUPPLIER_STOCK_FOLDER}{supplier_name}.xlsx"
        temp_supplier_path = f"/tmp/{supplier_name}.xlsx"
        
        cleaned_df.to_excel(temp_supplier_path, index=False)
        
        if s3:
            s3.upload_file(temp_supplier_path, CONFIG["AWS_BUCKET"], supplier_file)
        
        # Rebuild combined stock
        rebuild_combined_stock()
        
        total_stones = len(cleaned_df)
        total_carats = cleaned_df["Weight"].sum() if "Weight" in cleaned_df.columns else 0
        total_value = (cleaned_df["Weight"] * cleaned_df["Price Per Carat"]).sum() if "Weight" in cleaned_df.columns and "Price Per Carat" in cleaned_df.columns else 0
        
        success_msg = (
            f"✅ **Stock Upload Successful!**\n\n"
            f"📊 **Statistics:**\n"
            f"• 💎 Diamonds: {total_stones}\n"
            f"• ⚖️ Total Carats: {total_carats:.2f}\n"
            f"• 💰 Total Value: ${total_value:,.2f}\n\n"
            f"📈 **Price Range:**\n"
            f"• Min: ${cleaned_df['Price Per Carat'].min():,.0f}/ct\n"
            f"• Avg: ${cleaned_df['Price Per Carat'].mean():,.0f}/ct\n"
            f"• Max: ${cleaned_df['Price Per Carat'].max():,.0f}/ct\n\n"
            f"🔄 Combined stock has been updated."
        )
        
        if warnings:
            success_msg += "\n\n**Warnings:**\n"
            for warning in warnings[:3]:
                success_msg += f"⚠️ {warning}\n"
        
        await message.reply(success_msg)
        
        log_activity(user, "UPLOAD_STOCK", {
            "stones": total_stones,
            "carats": total_carats,
            "value": total_value,
            "warnings": warnings
        })
        
        if os.path.exists(temp_supplier_path):
            os.remove(temp_supplier_path)
            
    except Exception as e:
        logger.error(f"❌ Error in handle_supplier_stock_upload: {e}")
        await message.reply("❌ Failed to upload stock.")

async def handle_bulk_deal_requests(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle client bulk deal requests"""
    try:
        if "Stock #" not in df.columns or "Offer Price ($/ct)" not in df.columns:
            await message.reply("❌ Invalid format. Need 'Stock #' and 'Offer Price ($/ct)' columns.")
            return
        
        df = df.dropna(subset=["Stock #", "Offer Price ($/ct)"])
        
        if df.empty:
            await message.reply("❌ No valid deal requests found.")
            return
        
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
            
            stone_row = stock_df[stock_df["Stock #"] == stone_id]
            
            if stone_row.empty:
                failed_deals.append(f"{stone_id}: Not found")
                continue
            
            if stone_row.iloc[0].get("LOCKED") == "YES":
                failed_deals.append(f"{stone_id}: Already locked")
                continue
            
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
                "created_at": datetime.now(IST).strftime("%Y-%m-d %H:%M:%S")
            }
            
            if not atomic_lock_stone(stone_id):
                failed_deals.append(f"{stone_id}: Lock failed")
                continue
            
            if s3:
                deal_key = f"{DEALS_FOLDER}{deal_id}.json"
                s3.put_object(
                    Bucket=CONFIG["AWS_BUCKET"],
                    Key=deal_key,
                    Body=json.dumps(deal, indent=2),
                    ContentType="application/json"
                )
            
            log_deal_history(deal)
            
            save_notification(
                deal["supplier_username"],
                "supplier",
                f"📩 New bulk deal offer for Stone {stone_id}"
            )
            
            successful_deals += 1
        
        result_msg = f"📊 **Bulk Deal Results**\n\n"
        result_msg += f"✅ Successful: {successful_deals}\n"
        result_msg += f"❌ Failed: {len(failed_deals)}\n"
        
        if failed_deals:
            result_msg += f"\n**Failed deals:**\n"
            for fail in failed_deals[:10]:
                result_msg += f"• {fail}\n"
            
            if len(failed_deals) > 10:
                result_msg += f"... and {len(failed_deals) - 10} more\n"
        
        await message.reply(result_msg)
        
        user_state.pop(message.from_user.id, None)
        
        log_activity(user, "BULK_DEAL_REQUEST", {
            "successful": successful_deals,
            "failed": len(failed_deals)
        })
        
    except Exception as e:
        logger.error(f"❌ Error in handle_bulk_deal_requests: {e}")
        await message.reply("❌ Failed to process bulk deal requests.")

async def handle_admin_deal_approvals(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle admin deal approval Excel"""
    try:
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
                if not s3:
                    continue
                    
                deal_key = f"{DEALS_FOLDER}{deal_id}.json"
                deal_data = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=deal_key)
                deal = json.loads(deal_data["Body"].read())
                
                if deal.get("final_status") in ["COMPLETED", "CLOSED"]:
                    continue
                
                if action == "YES":
                    deal["admin_action"] = "APPROVED"
                    deal["final_status"] = "COMPLETED"
                    
                    remove_stone_from_supplier_and_combined(deal["stone_id"])
                    
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
                    
                else:
                    deal["admin_action"] = "REJECTED"
                    deal["final_status"] = "CLOSED"
                    
                    unlock_stone(deal["stone_id"])
                    
                    save_notification(
                        deal["client_username"],
                        "client",
                        f"❌ Deal {deal_id} rejected for Stone {deal['stone_id']}"
                    )
                
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
        
    except Exception as e:
        logger.error(f"❌ Error in handle_admin_deal_approvals: {e}")
        await message.reply("❌ Failed to process deal approvals.")

async def handle_supplier_deal_responses(message: types.Message, user: Dict, df: pd.DataFrame, file_path: str):
    """Handle supplier deal response Excel"""
    try:
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
                if not s3:
                    continue
                    
                deal_key = f"{DEALS_FOLDER}{deal_id}.json"
                deal_data = s3.get_object(Bucket=CONFIG["AWS_BUCKET"], Key=deal_key)
                deal = json.loads(deal_data["Body"].read())
                
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
                    
                    admin_df = load_accounts()
                    admins = admin_df[admin_df["ROLE"].str.lower() == "admin"]["USERNAME"].tolist()
                    for admin in admins:
                        save_notification(
                            admin,
                            "admin",
                            f"📝 Deal {deal_id} awaiting admin approval"
                        )
                        
                else:
                    deal["supplier_action"] = "REJECTED"
                    deal["admin_action"] = "REJECTED"
                    deal["final_status"] = "CLOSED"
                    
                    unlock_stone(deal["stone_id"])
                    
                    save_notification(
                        deal["client_username"],
                        "client",
                        f"❌ Supplier rejected deal {deal_id} for Stone {deal['stone_id']}"
                    )
                
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
        
    except Exception as e:
        logger.error(f"❌ Error in handle_supplier_deal_responses: {e}")
        await message.reply("❌ Failed to process deal responses.")

# -------- MAIN ENTRY POINT --------
if __name__ == "__main__":
    logger.info(f"🚀 Starting Diamond Trading Bot v1.0")
    logger.info(f"📊 Python: {CONFIG['PYTHON_VERSION']}")
    logger.info(f"🌐 Port: {CONFIG['PORT']}")
    logger.info(f"🔗 Webhook URL: {CONFIG['WEBHOOK_URL']}")
    logger.info(f"🤖 Bot Token: {'Set' if CONFIG['BOT_TOKEN'] else 'Not Set'}")
    logger.info(f"📦 S3 Bucket: {CONFIG['AWS_BUCKET']}")
    
    # Check if all required environment variables are set
    required_vars = ["BOT_TOKEN", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_BUCKET"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        logger.error("Please set these in your Render environment variables.")
    else:
        logger.info("✅ All required environment variables are set")
    
    # Preload data for faster startup
    preload_data()
    
    # Register cleanup function
    atexit.register(lambda: logger.info("👋 Bot shutting down"))
    
    # IMPORTANT: Setup instructions for 24/7 operation
    logger.info("\n" + "="*80)
    logger.info("🔄 FOR 24/7 OPERATION:")
    logger.info("="*80)
    logger.info("✅ INTERNAL: Keep-alive pinger started (pings every 5 minutes)")
    logger.info("📋 EXTERNAL: Set up uptime monitoring:")
    logger.info("   1. Go to https://uptimerobot.com/")
    logger.info("   2. Create free account")
    logger.info("   3. Add new monitor:")
    logger.info("      - Monitor Type: HTTP(s)")
    logger.info("      - Friendly Name: Diamond Trading Bot")
    logger.info("      - URL: https://telegram-bot-6iil.onrender.com/keep-alive")
    logger.info("      - Monitoring Interval: 5 minutes")
    logger.info("="*80)
    logger.info("📞 Monitoring URLs:")
    logger.info(f"   • Health Check: {CONFIG['RENDER_EXTERNAL_URL']}/health")
    logger.info(f"   • Keep-Alive: {CONFIG['RENDER_EXTERNAL_URL']}/keep-alive")
    logger.info(f"   • Status: {CONFIG['RENDER_EXTERNAL_URL']}/status")
    logger.info("="*80 + "\n")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=CONFIG["PORT"],
        reload=False,
        log_level="info"
    )
