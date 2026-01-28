import asyncio
import nest_asyncio
import pandas as pd
import boto3
import re
from io import BytesIO
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from fastapi import FastAPI
import uvicorn
import os
import json
import pytz
import uuid
import time

IST = pytz.timezone("Asia/Kolkata")

def safe_excel(val):
    if isinstance(val, str) and val.startswith(("=", "+", "-", "@")):
        return "'" + val
    return val

YES = "YES"
NO = "NO"

STATUS_PENDING = "PENDING"
STATUS_ACCEPTED = "ACCEPTED"
STATUS_REJECTED = "REJECTED"
STATUS_COMPLETED = "COMPLETED"
STATUS_CLOSED = "CLOSED"


# ---------------- FASTAPI SERVER ----------------

app = FastAPI()

@app.get("/")
def home():
    return {"status": "ok"}

@app.get("/diamonds")
def diamonds():
    return {"message": "Supplier API integration coming soon 💎"}


# ---------------- CONFIG ----------------

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not set")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_BUCKET = os.getenv("AWS_BUCKET")

ACCOUNTS_KEY = "users/accounts.xlsx"
STOCK_KEY = "stock/diamonds.xlsx"

SUPPLIER_STOCK_FOLDER = "stock/suppliers/"
COMBINED_STOCK_KEY = "stock/combined/all_suppliers_stock.xlsx"
ACTIVITY_LOG_FOLDER = "activity_logs/"
DEALS_FOLDER = "deals/"
DEAL_HISTORY_KEY = "deals/deal_history.xlsx"
NOTIFICATIONS_FOLDER = "notifications/"


# ---------------- BOT INIT ----------------

bot = Bot(token=TOKEN)
dp = Dispatcher()

# ---------------- AWS ----------------

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# ---------------- KEYBOARDS ----------------
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

# ---------------- HELPERS ----------------

def generate_activity_excel():
    try:
        objs = s3.list_objects_v2(
            Bucket=AWS_BUCKET,
            Prefix=ACTIVITY_LOG_FOLDER
        )

        if "Contents" not in objs or not objs["Contents"]:
            return None

        rows = []

        # ✅ LOOP THROUGH ALL ACTIVITY FILES
        for obj in objs["Contents"]:
            if not obj["Key"].endswith(".json"):
                continue

            try:
                raw = s3.get_object(
                    Bucket=AWS_BUCKET,
                    Key=obj["Key"]
                )["Body"].read().decode("utf-8")

                data = json.loads(raw)

            except Exception as e:
                print("Failed to read activity file:", obj["Key"], e)
                continue

            # ✅ COLLECT ENTRIES
            for entry in data:
                rows.append({
                    "Date": entry.get("date"),
                    "Time": entry.get("time"),
                    "Login ID": entry.get("login_id"),
                    "Role": entry.get("role"),
                    "Action": entry.get("action"),
                    "Details": json.dumps(entry.get("details", {}))
                })

        if not rows:
            return None

        df = pd.DataFrame(rows)
        path = "/tmp/user_activity_report.xlsx"
        df.to_excel(path, index=False)

        return path

    except Exception as e:
        print("Activity report error:", e)
        return None

def log_deal_history(deal):
    try:
        s3.download_file(AWS_BUCKET, DEAL_HISTORY_KEY, "/tmp/deal_history.xlsx")
        df = pd.read_excel("/tmp/deal_history.xlsx")
    except:
        df = pd.DataFrame(columns=[
            "Deal ID",
            "Stone ID",
            "Supplier",
            "Client",
            "Actual Price",
            "Offer Price",
            "Supplier Action",
            "Admin Action",
            "Final Status",
            "Created At"
        ])



        df = pd.concat(
            [
                df,
                pd.DataFrame([{
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
            ],
            ignore_index=True
        )

    df.to_excel("/tmp/deal_history.xlsx", index=False)
    s3.upload_file("/tmp/deal_history.xlsx", AWS_BUCKET, DEAL_HISTORY_KEY)

def log_activity(user, action, details=None):
    if not user:
        return
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    log_entry = {
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "login_id": user.get("USERNAME"),
        "role": user.get("ROLE"),
        "action": action,
        "details": details or {}
    }
    key = f"{ACTIVITY_LOG_FOLDER}{log_entry['date']}/{log_entry['login_id']}.json"
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
    except:
        data = []
    data.append(log_entry)
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )

def save_notification(username, role, message):
    key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
    except:
        data = []
    data.append({
        "message": message,
        "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
        "read": False
    })
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )

def fetch_unread_notifications(username, role):
    key = f"{NOTIFICATIONS_FOLDER}{role}_{username}.json"
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
    except:
        return []
    unread = [n for n in data if not n.get("read")]
    for n in data:
        n["read"] = True
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2),
        ContentType="application/json"
    )
    return unread

def load_accounts():
    try:
        s3.download_file(AWS_BUCKET, ACCOUNTS_KEY, "/tmp/accounts.xlsx")
        return pd.read_excel("/tmp/accounts.xlsx", dtype=str)
    except:
        return pd.DataFrame(columns=["USERNAME","PASSWORD","ROLE","APPROVED"])

def save_accounts(df):
    df.to_excel("/tmp/accounts.xlsx", index=False)
    s3.upload_file("/tmp/accounts.xlsx", AWS_BUCKET, ACCOUNTS_KEY)


SESSION_KEY = "sessions/logged_in_users.json"

logged_in_users = {}
user_state = {}

SESSION_TIMEOUT = 3600  # 1 hour

def touch_session(uid):
    if uid in logged_in_users:
        logged_in_users[uid]["last_active"] = time.time()
        save_sessions()

def get_logged_user(uid):
    user = logged_in_users.get(uid)
    if not user:
        return None

    # ⏳ Auto logout inactive users
    if time.time() - user.get("last_active", 0) > SESSION_TIMEOUT:
        logged_in_users.pop(uid, None)
        save_sessions()
        return None

    return user 

def is_admin(user):
    return user is not None and user.get("ROLE") == "admin"

def rebuild_combined_stock():
    objs = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=SUPPLIER_STOCK_FOLDER)
    if "Contents" not in objs:
        return

    dfs = []
    for obj in objs.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".xlsx"):
            continue
        local_path = f"/tmp/{key.split('/')[-1]}"
        s3.download_file(AWS_BUCKET, key, local_path)
        df = pd.read_excel(local_path)
        df["SUPPLIER"] = key.split("/")[-1].replace(".xlsx","").lower()
        dfs.append(df)

    if not dfs:
        return

    final_df = pd.concat(dfs, ignore_index=True)
    
    # Required columns
    desired_columns = [
        "Stock #","Availability","Shape","Weight","Color","Clarity","Cut","Polish","Symmetry",
        "Fluorescence Color","Measurements","Shade","Milky","Eye Clean","Lab","Report #","Location",
        "Treatment","Discount","Price Per Carat","Final Price","Depth %","Table %","Girdle Thin",
        "Girdle Thick","Girdle %","Girdle Condition","Culet Size","Culet Condition","Crown Height",
        "Crown Angle","Pavilion Depth","Pavilion Angle","Inscription","Cert comment","KeyToSymbols",
        "White Inclusion","Black Inclusion","Open Inclusion","Fancy Color","Fancy Color Intensity",
        "Fancy Color Overtone","Country","State","City","CertFile","Diamond Video","Diamond Image",
        "SUPPLIER","LOCKED","Diamond Type"
    ]

    # Add missing columns
    for col in desired_columns:
        if col not in final_df.columns:
            final_df[col] = ""  # safeguard

    if "Diamond Type" not in final_df.columns:
        final_df["Diamond Type"] = "Unknown"

    final_df["LOCKED"] = final_df.get("LOCKED", "NO")
    final_df = final_df[desired_columns]

    final_df.to_excel("/tmp/all_suppliers_stock.xlsx", index=False)
    s3.upload_file("/tmp/all_suppliers_stock.xlsx", AWS_BUCKET, COMBINED_STOCK_KEY)

def load_stock():
    try:
        s3.download_file(AWS_BUCKET, COMBINED_STOCK_KEY, "/tmp/all_suppliers_stock.xlsx")
        return pd.read_excel("/tmp/all_suppliers_stock.xlsx")
    except:
        return pd.DataFrame()

def remove_stone_from_supplier_and_combined(stone_id):
    # Remove from combined stock
    df = load_stock()
    if not df.empty and "Stock #" in df.columns:
        df = df[df["Stock #"] != stone_id]
        df.to_excel("/tmp/all_suppliers_stock.xlsx", index=False)
        s3.upload_file(
            "/tmp/all_suppliers_stock.xlsx",
            AWS_BUCKET,
            COMBINED_STOCK_KEY
        )

    # Remove from supplier stock
    objs = s3.list_objects_v2(
        Bucket=AWS_BUCKET,
        Prefix=SUPPLIER_STOCK_FOLDER
    )

    for obj in objs.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".xlsx"):
            continue

        local = "/tmp/tmp_supplier.xlsx"
        s3.download_file(AWS_BUCKET, key, local)
        sdf = pd.read_excel(local)

        if "Stock #" in sdf.columns and stone_id in sdf["Stock #"].values:
            sdf = sdf[sdf["Stock #"] != stone_id]
            sdf.to_excel(local, index=False)
            s3.upload_file(local, AWS_BUCKET, key)
            break
# ---------------- STATE ----------------

def save_sessions():
    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=SESSION_KEY,
        Body=json.dumps(logged_in_users, default=str),
        ContentType="application/json"
    )

def load_sessions():
    global logged_in_users
    try:
        obj = s3.get_object(Bucket=AWS_BUCKET, Key=SESSION_KEY)
        raw = json.loads(obj["Body"].read())
        logged_in_users = {int(k): v for k, v in raw.items()}
    except:
        logged_in_users = {}

# ---------------- START ----------------

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.reply(
        "💎 Welcome\n/login or /createaccount",
        reply_markup=types.ReplyKeyboardRemove()
    )

# ---------------- CREATE / LOGIN ----------------

@dp.message(Command("createaccount"))
async def create_account(message: types.Message):
    user_state[message.from_user.id] = {"step": "username"}
    await message.reply("Enter Username:")

@dp.message(Command("login"))
async def login(message: types.Message):
    uid = message.from_user.id

    # ✅ Prevent restarting login if already in flow
    if uid in user_state and user_state[uid].get("step") in ["login_username", "login_password"]:
        await message.reply("⚠️ Login already in progress. Please enter username or password.")
        return

    user_state[uid] = {"step": "login_username"}
    await message.reply("👤 Enter Username:")


# ---------------- ACCOUNT FLOW HANDLER ----------------

@dp.message()
async def account_flow_handler(message: types.Message):
    uid = message.from_user.id

    # Ignore commands
    if message.text.startswith("/"):
        return

    if uid not in user_state:
        return

    step = user_state[uid].get("step")
    text = message.text.strip()

    # -------- CREATE ACCOUNT FLOW --------
    if step == "username":
        if len(text) < 3:
            await message.reply("❌ Username must be at least 3 characters.")
            return

        user_state[uid]["username"] = text.lower()
        user_state[uid]["step"] = "password"

        await message.reply("🔐 Enter Password:")
        return

    if step == "password":
        if len(text) < 4:
            await message.reply("❌ Password must be at least 4 characters.")
            return

        username = user_state[uid]["username"]
        password = text

        df = load_accounts()

        # Prevent duplicate user
        if not df[df["USERNAME"] == username].empty:
            await message.reply("❌ Username already exists.")
            user_state.pop(uid, None)
            return

        # Default role = client (you can change)
        new_row = {
            "USERNAME": username,
            "PASSWORD": password,
            "ROLE": "client",
            "APPROVED": "NO"
        }

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        save_accounts(df)

        user_state.pop(uid, None)

        await message.reply(
            "✅ Account created successfully!\n"
            "⏳ Wait for admin approval.\n"
            "Use /login after approval."
        )
        return

    # -------- LOGIN FLOW --------
    if step == "login_username":
        user_state[uid]["login_username"] = text.lower()
        user_state[uid]["step"] = "login_password"
        await message.reply("🔐 Enter Password:")
        return

        df = load_accounts()

        username_clean = str(username).strip().lower()
        password_clean = str(password).strip()

        print("🔍 USER INPUT:", username_clean, password_clean)
        print("📄 DATAFRAME:")
        print(df)

        row = df[
            (df["USERNAME"].astype(str).str.strip().str.lower() == username_clean) &
            (df["PASSWORD"].astype(str).str.strip() == password_clean) &
            (df["APPROVED"].astype(str).str.strip().str.upper() == "YES")
        ]

        print("✅ LOGIN MATCH ROWS:", len(row))
        print(row)

        if row.empty:
            await message.reply("❌ Invalid login or not approved by admin.")
            user_state.pop(uid, None)
            return



    if step == "login_password":
        username = user_state[uid].get("login_username")
        password = text

        user = row.iloc[0].to_dict()

        # ✅ Save session
        logged_in_users[uid] = {
            **user,
            "last_active": time.time()
        }
        save_sessions()

        user_state.pop(uid, None)

        # 🎯 Keyboard based on role
        role = user["ROLE"].lower()
        if role == "admin":
            kb = admin_kb
        elif role == "supplier":
            kb = supplier_kb
        else:
            kb = client_kb

        await message.reply(
            f"✅ Login successful!\nWelcome {username}",
            reply_markup=kb
        )

        log_activity(user, "LOGIN")
        return


# ---------------- LOGOUT ----------------

@dp.message(F.text == "🚪 Logout")
async def logout(message: types.Message):
    uid = message.from_user.id

    if uid not in logged_in_users:
        await message.reply("ℹ️ You are already logged out.")
        return

    log_activity(
        logged_in_users[uid],
        "LOGOUT"
    )

    logged_in_users.pop(uid, None)
    user_state.pop(uid, None)   # ✅ clear state also
    save_sessions()

    await message.reply(
        "✅ Logged out successfully.\n/login to continue.",
        reply_markup=types.ReplyKeyboardRemove()
    )

# ---------------- Supplier Button Logic ----------------

# 1️⃣ Pending Accounts
@dp.message(F.text == "⏳ Pending Accounts")
async def pending_accounts(message: types.Message):
    user = get_logged_user(message.from_user.id)

    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    df = load_accounts()

    if df.empty:
        await message.reply("ℹ️ No users found")
        return

    # ✅ normalize APPROVED column
    df["APPROVED"] = (
        df["APPROVED"]
        .fillna("NO")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    pending_df = df[df["APPROVED"] != "YES"]

    if pending_df.empty:
        await message.reply("✅ No pending accounts")
        return

    for _, row in pending_df.iterrows():
        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(
                    text="✅ Approve",
                    callback_data=f"approve:{row['USERNAME']}"
                ),
                InlineKeyboardButton(
                    text="❌ Reject",
                    callback_data=f"reject:{row['USERNAME']}"
                )
            ]]
        )

        await message.reply(
            f"👤 Username: {row['USERNAME']}\n"
            f"🔑 Role: {row['ROLE']}\n"
            f"⏳ Status: Pending Approval",
            reply_markup=kb
        )


#  user activity
@dp.message(F.text == "📑 User Activity Report")
async def user_activity_report(message: types.Message):
    user = get_logged_user(message.from_user.id)

    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    path = generate_activity_excel()

    if not path:
        await message.reply("❌ No activity logs found")
        return

    await message.reply_document(
        types.FSInputFile(path),
        caption="📑 User Activity Report (All Users)"
    )

    log_activity(
        user,
        "DOWNLOAD_ACTIVITY_REPORT"
    )

# ---------------- SMART DEALS ----------------
@dp.message(F.text == "🔥 Smart Deals")
async def smart_deals(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user:
        return

    # 🔒 Client only
    if user["ROLE"].lower() != "client":
        await message.reply("❌ Smart Deals are available for clients only.")
        return

    df = load_stock()
    if df.empty:
        await message.reply("❌ No stock available.")
        return

    # Normalize numeric fields
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")

    df = df.dropna(subset=["Price Per Carat", "Weight"])

    # Market median calculation
    group_cols = ["Shape", "Color", "Clarity", "Diamond Type"]
    df["MARKET_MEDIAN"] = df.groupby(group_cols)["Price Per Carat"].transform("median")

    # 🔐 Safety: remove zero / invalid medians
    df = df[df["MARKET_MEDIAN"] > 0]

    # Discount %
    df["DISCOUNT_%"] = (
        (df["MARKET_MEDIAN"] - df["Price Per Carat"]) / df["MARKET_MEDIAN"] * 100
    ).round(2)

    # Filter strong deals (10%+)
    deals = df[df["DISCOUNT_%"] >= 10].sort_values(
        "DISCOUNT_%", ascending=False
    )

    if deals.empty:
        await message.reply("😔 No strong deals right now.")
        return

    # 📦 Many deals → Excel
    if len(deals) > 5:
        out = "/tmp/smart_deals.xlsx"

        # Hide supplier column
        client_df = deals.drop(columns=["SUPPLIER"], errors="ignore")
        client_df.to_excel(out, index=False)

        await message.reply_document(
            types.FSInputFile(out),
            caption=f"🔥 {len(deals)} Smart Deals Found (10%+ below market)"
        )
        return

    # 💎 Few deals → Message + Button
    for _, r in deals.iterrows():
        price = int(r["Price Per Carat"])

        msg = (
            f"💎 {r['Weight']} ct | {r['Shape']} | {r['Color']} | {r['Clarity']}\n"
            f"💰 ${r.get('Price Per Carat', 'N/A')} / ct\n"
            f"🏛 Lab: {r.get('Lab', 'N/A')} | 🔒 Locked: {r.get('LOCKED', 'N/A')}\n"
        )
        await message.reply(msg)


# ---------------- ADMIN HANDLERS ----------------

@dp.callback_query(F.data.startswith("approve:"))
async def approve_user(callback: types.CallbackQuery):
    admin = get_logged_user(callback.from_user.id)

    if not is_admin(admin):
        await callback.answer("❌ Admin only", show_alert=True)
        return

    username = callback.data.split(":")[1]

    df = load_accounts()
    row = df[df["USERNAME"] == username]

    if row.empty:
        await callback.answer("⚠️ User not found", show_alert=True)
        return

    if row.iloc[0]["APPROVED"] == "YES":
        await callback.answer("ℹ️ Already approved")
        return

    df.loc[df["USERNAME"] == username, "APPROVED"] = "YES"
    save_accounts(df)

    log_activity(
        admin,
        "APPROVE_USER",
        {"approved_login_id": username}
    )

    await callback.message.edit_text(
        f"✅ {username} approved",
        reply_markup=None
    )

    await callback.answer("Approved ✅")

# ---------------- ADMIN HANDLERS ----------------
#Deal Approval

@dp.callback_query(F.data.startswith("deal_admin_approve:"))
async def admin_approve_deal(callback: types.CallbackQuery):
    admin = get_logged_user(callback.from_user.id)
    if not is_admin(admin):
        await callback.answer("Admin only", show_alert=True)
        return

    deal_id = callback.data.split(":")[1]
    key = f"{DEALS_FOLDER}{deal_id}.json"

    deal = json.loads(
        s3.get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read()
    )

    if deal["supplier_action"] != "ACCEPTED" or deal["admin_action"] != "PENDING":
        await callback.answer("⚠️ Invalid deal state", show_alert=True)
        return

    # ✅ FINAL APPROVAL
    deal["admin_action"] = "APPROVED"
    deal["final_status"] = "COMPLETED"

    remove_stone_from_supplier_and_combined(deal["stone_id"])
    log_deal_history(deal)

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(deal, indent=2),
        ContentType="application/json"
    )

    # 🔔 Notifications
    save_notification(
        deal["client_username"],
        "client",
        f"🎉 Deal APPROVED for Stone {deal['stone_id']}"
    )

    save_notification(
        deal["supplier_username"],
        "supplier",
        f"✅ Deal APPROVED for Stone {deal['stone_id']}"
    )

    save_notification(
       deal["supplier_username"],
       "supplier",
       "📦 Please deliver the approved stone to the admin office at the earliest. "
       "(મંજૂર થયેલ હીરા કૃપા કરીને વહેલી તકે એડમિન ઓફિસે પહોંચાડશો.)"
    )


    await callback.message.edit_text("✅ Deal approved successfully")
    await callback.answer()


#Deal Accpet

@dp.callback_query(F.data.startswith("deal_accept:"))
async def deal_accept(callback: types.CallbackQuery):
    user = get_logged_user(callback.from_user.id)

    if not user or user["ROLE"] != "supplier":
        await callback.answer("❌ Supplier only", show_alert=True)
        return

    deal_id = callback.data.split(":")[1]
    key = f"{DEALS_FOLDER}{deal_id}.json"

    deal = json.loads(s3.get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read())

    if deal["supplier_action"] != "PENDING":
        await callback.answer("⚠️ Deal already processed", show_alert=True)
        return

    if deal["supplier_username"] != user["USERNAME"].lower():
        await callback.answer("❌ Not your deal", show_alert=True)
        return

    deal["supplier_action"] = "ACCEPTED"
    deal["admin_action"] = "PENDING"

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(deal, indent=2),
        ContentType="application/json"
    )

    save_notification(
        username=deal["client_username"],
        role="client",
        message=f"⏳ Supplier accepted your offer for Stone {deal['stone_id']}"
    )

    # Notify all admins
    accounts = load_accounts()
    admins = accounts[accounts["ROLE"] == "admin"]["USERNAME"].tolist()

    for admin_user in admins:
        save_notification(
            username=admin_user,
            role="admin",
            message=f"📝 Deal {deal_id} awaiting admin approval"
        )

    await callback.message.edit_text("✅ Deal accepted and sent to admin")
    await callback.answer()


#Reject Deal

@dp.callback_query(F.data.startswith("deal_reject:"))
async def deal_reject(callback: types.CallbackQuery):
    deal_id = callback.data.split(":")[1]
    key = f"{DEALS_FOLDER}{deal_id}.json"

    user = get_logged_user(callback.from_user.id)
    if not user or user["ROLE"] != "supplier":
        await callback.answer("❌ Supplier only", show_alert=True)
        return

    deal = json.loads(
        s3.get_object(
            Bucket=AWS_BUCKET,
            Key=key
        )["Body"].read()
    )

    deal["supplier_action"] = "REJECTED"
    deal["admin_action"] = "REJECTED"
    deal["final_status"] = "CLOSED"
    unlock_stone(deal["stone_id"])

    log_deal_history(deal)

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(deal, indent=2),
        ContentType="application/json"
    )

    save_notification(
        deal["client_username"],
        "client",
        f"❌ Supplier rejected your offer for Stone {deal['stone_id']}"
    )

    await callback.message.edit_text("❌ Deal rejected.")
    await callback.answer()


@dp.callback_query(F.data.startswith("deal_admin_reject:"))
async def admin_reject_deal(callback: types.CallbackQuery):
    admin = get_logged_user(callback.from_user.id)
    if not is_admin(admin):
        await callback.answer("Admin only", show_alert=True)
        return

    deal_id = callback.data.split(":")[1]
    key = f"{DEALS_FOLDER}{deal_id}.json"

    deal = json.loads(
        s3.get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read()
    )

    deal["admin_action"] = "REJECTED"
    deal["final_status"] = "CLOSED"

    log_deal_history(deal)

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=json.dumps(deal, indent=2),
        ContentType="application/json"
    )

    save_notification(
        deal["client_username"],
        "client",
        f"❌ Deal rejected by admin for Stone {deal['stone_id']}"
    )

    supplier_user = deal["supplier_username"]
    save_notification(
        supplier_user,
        "supplier",
        f"❌ Deal rejected by admin for Stone {deal['stone_id']}"
    )

    await callback.message.edit_text("❌ Deal rejected by admin")
    await callback.answer()



# 2️⃣ View All Stock (Admin)
@dp.message(F.text == "💎 View All Stock")
async def view_all_stock(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    df = load_stock()
    if df.empty:
        await message.reply("❌ No stock available")
        return

    total_diamonds = len(df)
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce").fillna(0)
    total_carats = round(df["Weight"].sum(), 2)

    msg = f"💎 Total Diamonds: {total_diamonds}\n📊 Total Carats: {total_carats}\n"

    if "Shape" in df.columns:
        shape_counts = df["Shape"].str.lower().value_counts()
        msg += "📌 Shapes Distribution:\n"
        for shape, count in shape_counts.items():
            msg += f"- {shape.capitalize()}: {count}\n"

    await message.reply(msg)

    out_path = "/tmp/all_suppliers_stock.xlsx"
    df.to_excel(out_path, index=False)
    await message.reply_document(
        types.FSInputFile(out_path),
        caption=f"📊 Combined Stock Excel ({total_diamonds} diamonds)"
    )

# 3️⃣ View Users
@dp.message(F.text == "👥 View Users")
async def view_users(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    df = load_accounts()
    msg = "👥 Users List\n\n"
    for _, r in df.iterrows():
        msg += f"{r['USERNAME']} | {r['ROLE']} | {r['APPROVED']}\n"

    await message.reply(msg)

# ---------------- SUPPLIER LEADERBOARD ----------------
@dp.message(F.text == "🏆 Supplier Leaderboard")
async def supplier_leaderboard(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    df = load_stock()
    if df.empty:
        await message.reply("❌ No stock available")
        return

    # ✅ FIX: convert price to numeric
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
    df = df.dropna(subset=["Price Per Carat", "SUPPLIER"])

    if df.empty:
        await message.reply("❌ No valid pricing data")
        return

    leaderboard = (
        df.groupby("SUPPLIER")
        .agg(
            Stones=("SUPPLIER", "count"),
            Avg_Price=("Price Per Carat", "mean")
        )
        .sort_values("Stones", ascending=False)
    )

    msg = "🏆 Supplier Leaderboard\n\n"
    for i, (supplier, row) in enumerate(leaderboard.iterrows(), 1):
        msg += (
            f"{i}. {supplier}\n"
            f"   💎 Stones: {row['Stones']}\n"
            f"   💰 Avg $/ct: {round(row['Avg_Price'], 2)}\n\n"
        )

    await message.reply(msg)


# ---------------- Delete Supplier Stock (Admin) ----------------
@dp.message(F.text == "🗑 Delete Supplier Stock")
async def delete_supplier_stock(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "admin":
        await message.reply("❌ Admin only")
        return

    objs = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=SUPPLIER_STOCK_FOLDER)

    if "Contents" in objs:
        for obj in objs.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".xlsx"):
                s3.delete_object(Bucket=AWS_BUCKET, Key=key)

    try:
        s3.delete_object(Bucket=AWS_BUCKET, Key=COMBINED_STOCK_KEY)
    except:
        pass

    await message.reply("🗑 All supplier stock deleted successfully")

@dp.message(F.text == "📦 My Stock")
async def supplier_my_stock(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "supplier":
        return

    supplier_key = user.get("SUPPLIER_KEY")
    if not supplier_key:
        await message.reply("❌ Supplier key missing. Contact admin.")
        return

    key = f"{SUPPLIER_STOCK_FOLDER}{supplier_key}.xlsx"
    local = "/tmp/my_stock.xlsx"

    try:
        s3.download_file(AWS_BUCKET, key, local)
        await message.reply_document(
            types.FSInputFile(local),
            caption="📦 Your Uploaded Stock"
        )
    except:
        await message.reply("❌ You have not uploaded any stock yet")


# ---------------- My Analytics ------------------

@dp.message(F.text == "📊 My Analytics")
async def supplier_price_excel_analytics(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "supplier":
        return

    supplier_name = user.get("SUPPLIER_KEY")
    df = load_stock()
    if df.empty:
        await message.reply("❌ No market stock available.")
        return

    # ---------- NORMALIZE ----------
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce").round(2)
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")

    needed = ["Shape", "Color", "Clarity", "Weight", "Diamond Type", "SUPPLIER", "Price Per Carat"]
    missing_cols = [c for c in needed if c not in df.columns]
    if missing_cols:
        await message.reply(f"❌ Missing columns: {', '.join(missing_cols)}")
        return

    df = df.dropna(subset=["Price Per Carat", "Weight", "Diamond Type"])

    df["MATCH_KEY"] = (
        df["Weight"].astype(str) + "|" +
        df["Shape"].str.lower() + "|" +
        df["Color"].str.lower() + "|" +
        df["Clarity"].str.lower() + "|" +
        df["Diamond Type"].str.lower()
    )

    my_df = df[df["SUPPLIER"].str.lower() == supplier_name.lower()]
    if my_df.empty:
        await message.reply("❌ You have no stones uploaded.")
        return

    result_rows = []
    for _, row in my_df.iterrows():
        key = row["MATCH_KEY"]
        my_price = row["Price Per Carat"]
        market = df[df["MATCH_KEY"] == key]
        best_price = market["Price Per Carat"].min()
        diff = round(my_price - best_price, 2)
        status = "BEST PRICE" if diff == 0 else "OVERPRICED" if diff > 0 else "UNDERPRICED"

        result_rows.append({
            "Stock #": row["Stock #"],
            "Weight": row["Weight"],
            "Shape": row["Shape"],
            "Color": row["Color"],
            "Clarity": row["Clarity"],
            "Diamond Type": row["Diamond Type"],
            "Your Price Per Carat": my_price,
            "Best Market Price Per Carat": best_price,
            "Difference": diff,
            "Price Status": status
        })

    result_df = pd.DataFrame(result_rows)
    out_path = f"/tmp/{supplier_name}_price_analytics.xlsx"
    result_df.to_excel(out_path, index=False)

    await message.reply_document(
        types.FSInputFile(out_path),
        caption="📊 Your Full Market Price Comparison (All Stones)"
    )


# ---------------- View Deals ------------------

@dp.message(F.text == "🤝 View Deals")
async def view_deals(message: types.Message):
    user = get_logged_user(message.from_user.id)

    if not user:
        await message.reply("🔒 Please login first.")
        return

    if user["ROLE"] not in ["admin", "supplier", "client"]:
        await message.reply("❌ Unauthorized access.")
        return

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=AWS_BUCKET, Prefix=DEALS_FOLDER)

    deals = []
    found_any = False

    for page in pages:
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue

            found_any = True
            try:
                deal = json.loads(
                    s3.get_object(Bucket=AWS_BUCKET, Key=obj["Key"])["Body"].read()
                )
                deals.append(deal)
            except Exception as e:
                print("Deal load error:", e)

    deals.sort(key=lambda d: d.get("created_at", ""), reverse=True)

    if not found_any:
        await message.reply("ℹ️ No deals available.")
        return


    # ---------------- SUPPLIER VIEW ----------------
    if user["ROLE"].lower() == "supplier":
        supplier = user["USERNAME"].strip().lower()

        rows = [
            {
                "Deal ID": d["deal_id"],
                "Stock #": d["stone_id"],
                "Client": d["client_username"],
                "Actual Price ($/ct)": d.get("actual_stock_price", 0),
                "Client Offer ($/ct)": d.get("client_offer_price", 0),
                "Supplier Action (ACCEPT / REJECT)": "",
                "Admin Action": d.get("admin_action"),
                "Final Status": d.get("final_status"),
            }
            for d in deals
            if d.get("supplier_username", "").strip().lower() == supplier
        ]

        if not rows:
            await message.reply("ℹ️ No deals found.")
            return

        df = pd.DataFrame(rows)
        path = f"/tmp/{supplier}_deals.xlsx"

        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)

        df.to_excel(path, index=False)

        await message.reply_document(types.FSInputFile(path), caption="📊 Your Deals")

        if os.path.exists(path):
            os.remove(path)
            
        return

    # ---------------- ADMIN VIEW ----------------
    if user["ROLE"].lower() == "admin":
        rows = []

        for d in deals:
            if d.get("supplier_action") == "ACCEPTED" and d.get("admin_action") == "PENDING":
                actual = float(d.get("actual_stock_price") or 0)
                offer = float(d.get("client_offer_price") or 0)
                profit = round(offer - actual, 2)

                rows.append({
                    "Deal ID": d["deal_id"],
                    "Stock #": d["stone_id"],
                    "Supplier": d["supplier_username"],
                    "Client": d["client_username"],
                    "Actual Price ($/ct)": actual,
                    "Offer Price ($/ct)": offer,
                    "Profit / Loss ($/ct)": profit,
                    "Supplier Action (ACCEPT / REJECT)": "",
                    "Admin Action (YES / NO)": "",
                })

        if not rows:
            await message.reply("ℹ️ No deals pending admin approval.")
            return

        df = pd.DataFrame(rows)
        path = "/tmp/admin_pending_deals.xlsx"

        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(safe_excel)
            
        try:
            df.to_excel(path, index=False)
            await message.reply_document(types.FSInputFile(path))
        finally:
            if os.path.exists(path):
                os.remove(path)
            
        return


# ---------------- START DEAL REQUEST ----------------

@dp.message(F.text == "🤝 Request Deal")
async def request_deal_start(message: types.Message):
    user = get_logged_user(message.from_user.id)

    if not user:
        await message.reply("🔒 Please login first.")
        return

    if user["ROLE"] != "client":
        await message.reply("❌ Only clients can request deals.")
        return

    df = load_stock()
    if df.empty:
        await message.reply("❌ No stock available.")
        return

    total_stones = len(df)

    # 🔹 SMALL FLOW (≤ 5 stones) → chat based
    if total_stones <= 5:
        user_state[message.from_user.id] = {"step": "deal_stone"}
        await message.reply("🆔 Enter Stock # you want to make an offer on:")
        return

    # 🔹 BULK FLOW (> 5 stones) → BLANK TEMPLATE
    bulk_df = pd.DataFrame(
        columns=[
            "Stock #",
            "Offer Price ($/ct)"
        ]
    )

    out = "/tmp/request_deal_bulk.xlsx"

    bulk_df.to_excel(out, index=False)

    await message.reply_document(
        types.FSInputFile(out),
        caption=(
            "📊 Bulk Deal Request\n\n"
            "➡️ Fill STONE ID manually\n"
            "➡️ Enter your Offer Price ($/ct)\n"
            "➡️ Upload the file back"
        )
    )
    
    if os.path.exists(out):
        os.remove(out)

    user_state[message.from_user.id] = {"step": "bulk_deal_excel"}

# ---------------- LOGIN BUTTON ----------------

@dp.message(F.text.in_(["🔐 login", "login", "/login"]))
async def start_login(message: types.Message):
    uid = message.from_user.id

    # ✅ If already in login flow, do not reset
    if uid in user_state and user_state[uid].get("step") in ["login_username", "login_password"]:
        await message.reply("⚠️ Login already in progress. Please enter username or password.")
        return

    user_state[uid] = {"step": "login_username"}
    await message.reply("👤 Enter Username:")
    return

# ---------------- TEXT HANDLER ----------------

@dp.message()
async def handle_text(message: types.Message):
    uid = message.from_user.id

    # ✅ Safety: ignore non-text messages
    if not message.text:
        return

    text = message.text.strip()
    state = user_state.get(uid)

    # 🚫 Ignore commands so they don't break state flow
    if text.startswith("/") and not state:
        return

    # 🔄 Update last activity for logged-in users
    if uid in logged_in_users:
        logged_in_users[uid]["last_active"] = time.time()
        save_sessions()

    

    # ================= LOGIN FLOW =================
    if state and state.get("step") == "login_username":
        user_state[uid] = {
            "step": "login_password",
            "login_username": text.strip()
        }
        await message.reply("🔐 Enter Password:")
        return
        
    if state and state.get("step") == "login_password":
        username = user_state[uid].get("login_username")
        password = text

        df = load_accounts()

        print("===== LOGIN DEBUG =====")
        print("INPUT USERNAME:", username)
        print("INPUT PASSWORD:", password)
        print(df[["USERNAME", "PASSWORD", "APPROVED", "ROLE"]].head(20))
        print("=======================")

        # ✅ Normalize columns safely
        df["USERNAME"] = df["USERNAME"].astype(str).str.strip().str.lower()
        df["PASSWORD"] = df["PASSWORD"].astype(str).str.strip()
        df["APPROVED"] = df["APPROVED"].astype(str).str.strip().str.upper()
        df["ROLE"] = df["ROLE"].astype(str).str.strip()

        username_clean = username.strip().lower()
        password_clean = password.strip()

        r = df[
            (df["USERNAME"] == username_clean) &
            (df["PASSWORD"] == password_clean) &
            (df["APPROVED"] == "YES")
        ]

        print("LOGIN MATCH ROWS:", len(r))

        if r.empty:
            await message.reply("❌ Invalid username / password or not approved.")
            user_state.pop(uid, None)
            return

        # ---------------- ROLE FIX ----------------
        role = str(r.iloc[0]["ROLE"]).strip().lower()

        ADMIN_USERS = [
            u.strip()
            for u in os.getenv("ADMIN_USERS", "").lower().split(",")
            if u.strip()
        ]

        if r.iloc[0]["USERNAME"].strip().lower() in ADMIN_USERS:
            role = "admin"
        # ------------------------------------------

        logged_in_users[uid] = {
            "USERNAME": r.iloc[0]["USERNAME"],
            "ROLE": role,
            "SUPPLIER_KEY": (
                f"supplier_{r.iloc[0]['USERNAME'].lower()}"
                if role == "supplier" else None
            ),
            "last_active": time.time(),
        }

        save_sessions()
        log_activity(logged_in_users[uid], "LOGIN")

        # 🎯 Assign keyboard
        if role == "admin":
            kb = admin_kb
        elif role == "client":
            kb = client_kb
        elif role == "supplier":
            kb = supplier_kb
        else:
            kb = types.ReplyKeyboardRemove()

        username_disp = r.iloc[0]["USERNAME"].capitalize()

        if role == "admin":
            welcome_msg = f"👑 Welcome Admin {username_disp}"
        elif role == "supplier":
            welcome_msg = f"💎 Welcome Supplier {username_disp}"
        elif role == "client":
            welcome_msg = f"🥂 Welcome {username_disp}"
        else:
            welcome_msg = f"Welcome {username_disp}"

        await message.reply(welcome_msg, reply_markup=kb)

        # 🔔 Notifications
        notifications = fetch_unread_notifications(
            logged_in_users[uid]["USERNAME"],
            logged_in_users[uid]["ROLE"]
        )

        if notifications:
            note_msg = "🔔 Notifications\n\n"
            for n in notifications:
                note_msg += f"{n['message']}\n🕒 {n['time']}\n\n"
            await message.reply(note_msg)

        # ✅ CLEAR STATE AFTER LOGIN
        user_state.pop(uid, None)
        return



    # ================= DEAL REQUEST FLOW =================
    if state and state.get("step") in ["deal_stone", "deal_price"]:
        step = state.get("step")

        if step == "deal_stone":
            state["stone_id"] = text
            state["step"] = "deal_price"
            await message.reply("💰 Enter your offer price ($/ct):")
            return

        if step == "deal_price":
            try:
                offer_price = float(text)
                if offer_price <= 0:
                    await message.reply("❌ Price must be greater than zero.")
                    return
            except:
                await message.reply("❌ Enter a valid numeric price (e.g. 9500)")
                return

            user = get_logged_user(uid)
            if not user:
                await message.reply("❌ Session expired. Please login again.")
                user_state.pop(uid, None)
                return

            stone_id = state["stone_id"]
            df = load_stock()

            if df.empty:
                await message.reply("❌ No stock available.")
                user_state.pop(uid, None)
                return

            row = df[
                (df["Stock #"] == stone_id) &
                (df["LOCKED"] != "YES")
            ]
            
            if row.empty:
                await message.reply("❌ Stone not available or already locked.")
                user_state.pop(uid, None)
                return

            # 🔒 Reload stock before locking (race safety)
            latest_df = load_stock()
            latest_row = latest_df[
                (latest_df["Stock #"] == stone_id) &
                (latest_df["LOCKED"] != "YES")
            ]

            if latest_row.empty:
                await message.reply("🔒 Stone just got locked by another user.")
                user_state.pop(uid, None)
                return

            r = row.iloc[0]

            if r.get("LOCKED") == "YES":
                await message.reply("🔒 This stone is already locked in another deal.")
                user_state.pop(uid, None)
                return

            deal_id = f"DEAL-{uuid.uuid4().hex[:10]}"

            actual_price = pd.to_numeric(
                r.get("Price Per Carat", 0),
                errors="coerce"
            ) 
            if pd.isna(actual_price):
                actual_price = 0

            admin_profit_value = round(offer_price - actual_price, 2)

            deal = {
                "deal_id": deal_id,
                "stone_id": stone_id,
                "supplier_username": r["SUPPLIER"].replace("supplier_", "").lower(),
                "client_username": user["USERNAME"],
                "actual_stock_price": actual_price,
                "client_offer_price": offer_price,
                "admin_profit_value": admin_profit_value,
                "supplier_action": "PENDING",
                "admin_action": "PENDING",
                "final_status": "OPEN",
                "created_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            }

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=f"{DEALS_FOLDER}{deal_id}.json",
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

            if not lock_stone(stone_id):
                await message.reply("🔒 Stone already locked.")
                user_state.pop(uid, None)
                return

            save_notification(
                username=r["SUPPLIER"].replace("supplier_", "").lower(),
                role="supplier",
                message=(
                    "📩 New deal offer received\n\n"
                    f"💎 Stone ID: {stone_id}\n"
                    f"💰 Offer Price: ${offer_price} / ct"
                )
            )

            log_activity(
                user,
                "REQUEST_DEAL",
                {
                    "stone_id": stone_id,
                    "offer_price": offer_price
                }
            )

            await message.reply(
                f"✅ Deal request sent successfully!\n\n"
                f"💎 Stone ID: {stone_id}\n"
                f"💰 Your Offer: ${offer_price} / ct\n"
                f"⏳ Waiting for supplier response."
            )

            # ✅ Clear deal state AFTER completion
            user_state.pop(uid, None)
            return

    # -------- BUTTON HANDLING --------
    user = get_logged_user(uid)
    if not user:
        await message.reply("🔒 Please login first using /login")
        return

    # 🔄 Refresh user activity timestamp
    logged_in_users[uid]["last_active"] = time.time()
    save_sessions()

    if text == "💎 Search Diamonds":
        user_state[uid] = {"step": "search_carat", "search": {}}
        await message.reply("Enter Weight (e.g., 1 or 1-1.5, or 'any'):")
        return

    if text == "📤 Upload Excel":
        if user["ROLE"] != "supplier":
            await message.reply("❌ Only suppliers can upload diamonds.")
            return

        await message.reply("Send Excel file 📊")
        return

    if text == "📥 Download Sample Excel":
        # Create sample Excel in memory
        df = pd.DataFrame({
            "Stock #": ["D001", "D002", "D003"],
            "Location": ["Mumbai", "Delhi", "Bangalore"],
            "Shape": ["Round", "Oval", "Princess"],
            "Weight": [1.0, 1.5, 2.0],
            "Color": ["White", "Yellow", "Pink"],
            "Clarity": ["VVS", "VS", "SI"],
            "Cut": ["Excellent", "Very Good", "Good"],
            "Polish": ["PO123", "PO124", "PO125"],
            "Symmetry": ["Excellent", "Very Good", "Good"],
            "FLS": ["Yes", "No", "Yes"],
            "Price Per Carat": [10000, 15000, 20000],
            "Total Price": [10000, 15000, 20000],
            "Measurement": ["6.5x6.5x4.0", "7.0x5.5x3.5", "8.0x6.0x4.0"],
            "Table %": [57, 58, 59],
            "Depth %": [61, 62, 63],
            "Video": ["link1", "link2", "link3"],
            "Report #": ["R001", "R002", "R003"],
            "Lab": ["GIA", "IGI", "HRD"],
            "Company Comment": ["Good quality", "Premium", "Rare cut"],
            "Image": ["img1.jpg", "img2.jpg", "img3.jpg"],
            "Stock Status": ["Available", "Reserved", "Sold"],
            "Contact Number": ["1234567890", "0987654321", "1122334455"],
            "Diamond Type": ["Natural", "LGD", "HPHT"],
            "Description": ["Nice stone", "Premium quality", "Best cut"]
        })
        buffer = BytesIO()
        df.to_excel(buffer, index=False)
        buffer.seek(0)
        await message.reply_document(
            BufferedInputFile(buffer.read(), filename="sample_diamond_upload.xlsx"),
            caption="📥 Sample Diamond Upload Excel"
        )
        return

    # -------- CLIENT SEARCH --------
    if user["ROLE"] == "client" and uid in user_state and user_state[uid].get("step","").startswith("search_"):
        state = user_state[uid]
        search = state["search"]

        if state["step"] == "search_carat":
            search["carat"] = text
            state["step"] = "search_shape"
            await message.reply("Enter Shape(s) or 'any':")
            return

        if state["step"] == "search_shape":
            search["shape"] = text
            state["step"] = "search_color"
            await message.reply("Enter Color(s) or 'any':")
            return

        if state["step"] == "search_color":
            search["color"] = text
            state["step"] = "search_clarity"
            await message.reply("Enter Clarity(ies) or 'any':")
            return

        if state["step"] == "search_clarity":
            search["clarity"] = text

            df = load_stock()
            if df.empty:
                await message.reply("❌ No diamonds available")
                user_state.pop(uid)
                return

            # ---------------- NORMALIZE ----------------
            df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
            df["Shape"] = df["Shape"].astype(str)
            df["Color"] = df["Color"].astype(str)
            df["Clarity"] = df["Clarity"].astype(str)

            # ---------------- CARAT FILTER ----------------
            if search["carat"] != "any":
                carat_input = search["carat"].replace(" ", "")
                ranges = carat_input.split(",")

                mask = pd.Series(False, index=df.index)

                for r in ranges:
                    if "-" in r:
                        try:
                            s, e = map(float, r.split("-"))
                            mask |= (df["Weight"] >= s) & (df["Weight"] <= e)
                        except Exception as e:
                            print("ERROR:", e)
                            continue
                    else:
                        try:
                            carat = float(r)
                            mask |= (df["Weight"] >= carat) & (df["Weight"] <= carat + 0.2)
                        except Exception as e:
                            print("ERROR:", e)
                            continue
                df = df[mask]


            # ---------------- SHAPE FILTER ----------------
            if search["shape"] != "any":
                shapes = search["shape"].lower().split()
                df = df[df["Shape"].str.lower().apply(
                    lambda x: any(s in x for s in shapes)
                )]


            # ---------------- COLOR FILTER ----------------
            if search["color"] != "any":

                user_inputs = [
                    c.strip().lower()
                    for c in re.split(r"[,\s]+", search["color"])
                ]

                def normalize(text):
                    return str(text).strip().lower()

                def is_white_letter(c):
                    return len(c) == 1 and c.isalpha()

                def color_match(stock_color):
                    stock = normalize(stock_color)

                    for uc in user_inputs:

                        # 1️⃣ Strict white single letter (D, E, F…)
                        if is_white_letter(uc):
                            if stock == uc:
                                return True
                            # ❌ if searching letter like D, do NOT match fancy
                            continue

                        # 2️⃣ White letter range like D-E
                        if "-" in uc and all(is_white_letter(x) for x in uc.split("-")):
                            try:
                                start, end = uc.split("-")
                                if len(stock) == 1 and start <= stock <= end:
                                    return True
                            except:
                                pass
                            continue

                        # 3️⃣ Fancy with intensity: must match exactly
                        if uc.startswith("fancy"):
                            if stock == uc:
                                return True
                            continue

                        # 4️⃣ Normal colors like yellow, pink
                        if stock == uc or stock.endswith(" " + uc):
                            return True

                    return False

                df = df[df["Color"].apply(color_match)]




            # ---------------- CLARITY FILTER ----------------
            if search["clarity"] != "any":

                user_inputs = [
                    c.strip().lower()
                    for c in re.split(r"[,\s]+", search["clarity"])
                ]

                def normalize(text):
                    return str(text).strip().lower()

                def clarity_match(stock_clarity):
                    stock = normalize(stock_clarity)

                    for uc in user_inputs:

                        # 1️⃣ Exact clarity like vs1, si2, vvs2 exactly
                        if stock == uc:
                            return True

                        # 2️⃣ Group clarity
                        # vs -> vs1, vs2
                        # vvs -> vvs1, vvs2
                        # si -> si1, si2
                        # if is alone matches only IF
                        if uc in ["vs", "vvs", "si", "if"]:
                            # For ‘vs’ we match only vs1 & vs2
                            # stock.startswith(uc) ensures vs1, vs2
                            if stock.startswith(uc) and stock != "vvs" and stock != "si":
                                return True

                    return False

                df = df[df["Clarity"].apply(clarity_match)]



            # ---------------- NO RESULT ----------------
            if df.empty:
                await message.reply("❌ No diamonds match your search criteria.")
                user_state.pop(uid)
                return

            # ---------------- FORMAT OUTPUT ----------------
            shape_summary = ", ".join(
                f"{k.capitalize()}:{v}" for k, v in df["Shape"].value_counts().items()
            )

            if len(df) > 5:
                out = "/tmp/results.xlsx"

                # 🔒 REMOVE SUPPLIER COLUMN FOR CLIENT VIEW ONLY
                excel_df = df.drop(columns=["SUPPLIER"], errors="ignore")
                
                for col in excel_df.select_dtypes(include="object"):
                    excel_df[col] = excel_df[col].map(safe_excel)
                excel_df.to_excel(out, index=False)

                await message.reply_document(
                    types.FSInputFile(out),
                    caption=f"💎 {len(df)} diamonds found\nShapes: {shape_summary}"
                )
                if os.path.exists(out):
                    os.remove(out)
            else:
                for _, r in df.iterrows():
                    msg = (
                        f"💎 {r['Weight']} ct | {r['Shape']} | {r['Color']} | {r['Clarity']}\n"
                        f"💰 ${r.get('Price Per Carat', 'N/A')} / ct\n"
                        f"🏛 Lab: {r.get('Lab', 'N/A')} | 🔒 Locked: {r.get('LOCKED', 'N/A')}\n"
                    )

                    await message.reply(msg)

            log_activity(
                user,
                "SEARCH",
                {
                    "carat": search["carat"],
                    "shape": search["shape"],
                    "color": search["color"],
                    "clarity": search["clarity"],
                    "results": len(df)
                }
            )

            user_state.pop(uid)
            return

def load_accounts():
    try:
        s3.download_file(AWS_BUCKET, ACCOUNTS_KEY, "/tmp/accounts.xlsx")
        df = pd.read_excel("/tmp/accounts.xlsx", dtype=str)

        required = ["USERNAME", "PASSWORD", "ROLE", "APPROVED"]
        for col in required:
            if col not in df.columns:
                raise Exception(f"Missing column: {col}")

            df[col] = (
                df[col]
                .fillna("")
                .astype(str)
                .str.strip()
            )

        # ✅ Normalize
        df["USERNAME"] = df["USERNAME"].str.lower()
        df["APPROVED"] = df["APPROVED"].str.upper()
        df["ROLE"] = df["ROLE"].str.lower()

        print("✅ ACCOUNTS LOADED:")
        print(df.head(10))

        return df

    except Exception as e:
        print("❌ LOAD ACCOUNT ERROR:", e)
        return pd.DataFrame(columns=["USERNAME","PASSWORD","ROLE","APPROVED"])

# ---------------- SAFE STOCK LOCK ----------------

def lock_stone(stone_id: str) -> bool:
    df = load_stock()
    if df.empty:
        return False

    mask = (df["Stock #"] == stone_id) & (df["LOCKED"] != "YES")

    if not mask.any():
        return False   # already locked

    df.loc[mask, "LOCKED"] = "YES"

    temp = "/tmp/all_suppliers_stock.xlsx"
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(safe_excel)

    df.to_excel(temp, index=False)
    s3.upload_file(temp, AWS_BUCKET, COMBINED_STOCK_KEY)
    return True


def unlock_stone(stone_id: str):
    df = load_stock()
    if df.empty:
        return

    df.loc[df["Stock #"] == stone_id, "LOCKED"] = "NO"
    temp = "/tmp/all_suppliers_stock.xlsx"
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(safe_excel)
    df.to_excel(temp, index=False)
    s3.upload_file(temp, AWS_BUCKET, COMBINED_STOCK_KEY)


# ---------------- DOCUMENT HANDLER ----------------

@dp.message(F.document)
async def handle_doc(message: types.Message):

    # ❗ FILE SIZE LIMIT (10 MB) — CHECK FIRST
    if message.document.file_size > 10 * 1024 * 1024:
        await message.reply("❌ File too large. Max allowed size is 10 MB.")
        return
    allowed_ext = (".xls", ".xlsx")
    if not message.document.file_name.lower().endswith(allowed_ext):
        await message.reply("❌ Only Excel files allowed.")
        return
    uid = message.from_user.id
    user = get_logged_user(uid)

    if not user:
        await message.reply("🔒 Please login first.")
        return

    # 🚫 Block unauthorized file uploads early
    if user["ROLE"] not in ["client", "supplier", "admin"]:
        await message.reply("❌ Unauthorized upload attempt.")
        return

    # ==========================================================
    # ✅ CLIENT BULK DEAL REQUEST (FIRST & RETURN)
    # ==========================================================

    if (
        user["ROLE"] == "client"
        and user_state.get(uid, {}).get("step") == "bulk_deal_excel"
    ):
        file = await bot.get_file(message.document.file_id)
        path = f"/tmp/{uid}_{int(time.time())}_{message.document.file_name}"

        await bot.download_file(file.file_path, path)

        try:
            df = pd.read_excel(path)
        except Exception:
            await message.reply("❌ Invalid Excel file.")
            return


        stock_df = load_stock()

        supplier_rows = {}

        processed_stones = set()

        # ✅ Load stock once for performance
        latest_df_cache = load_stock()

        for _, row in df.iterrows():
            if pd.isna(row.get("Stock #")) or pd.isna(row.get("Offer Price ($/ct)")):
                continue

            stone_id = str(row["Stock #"]).strip()

            if not stone_id:
                continue

            # ✅ Prevent duplicate stone in same Excel upload
            if stone_id in processed_stones:
                continue
            processed_stones.add(stone_id)

            try:
                offer_price = float(row["Offer Price ($/ct)"])
                if offer_price <= 0:
                    continue
            except:
                continue

            stock_row = latest_df_cache[
                (latest_df_cache["Stock #"] == stone_id) &
                (latest_df_cache["LOCKED"] != "YES")
            ]

            if stock_row.empty:
                continue

            r = stock_row.iloc[0]

            actual_price = pd.to_numeric(
                r.get("Price Per Carat", 0),
                errors="coerce"
            ) 
            if pd.isna(actual_price):
                actual_price = 0

            supplier = str(r.get("SUPPLIER","")).replace("supplier_", "").lower()

            deal_id = f"DEAL-{uuid.uuid4().hex[:12]}"

            deal = {
                "deal_id": deal_id,
                "stone_id": stone_id,
                "supplier_username": supplier,
                "client_username": user["USERNAME"],
                "actual_stock_price": actual_price,
                "client_offer_price": offer_price,
                "admin_profit_value": round(offer_price - actual_price, 2),
                "supplier_action": "PENDING",
                "admin_action": "PENDING",
                "final_status": "OPEN",

                "created_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M"),
            }

            # 🔒 Final safety check before lock (REAL race protection)
            latest_df = load_stock()
            latest_row = latest_df[
                (latest_df["Stock #"] == stone_id) &
                (latest_df["LOCKED"] != "YES")
            ]

            if latest_row.empty:
                continue
            
            # 🔒 Lock stone safely
            if not lock_stone(stone_id):
                continue

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=f"{DEALS_FOLDER}{deal_id}.json",
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

            save_notification(
                supplier,
                "supplier",
                f"📩 New deal offer for Stone {stone_id}"
            )

            supplier_rows.setdefault(supplier, []).append({
                "Deal ID": deal_id,
                "Stone #": stone_id,
                "Actual Price ($/ct)": actual_price,
                "Offer Price ($/ct)": offer_price,
                "Profit/Loss ($/ct)": round(offer_price - actual_price, 2)
            })

        # SEND EXCEL ONLY IF STONES > 5
        for supplier, rows in supplier_rows.items():
            if len(rows) <= 5:
                continue

            df_excel = pd.DataFrame(rows)
            excel_path = f"/tmp/{supplier}_{int(time.time())}_bulk_deals.xlsx"

            for col in df_excel.select_dtypes(include="object"):
                df_excel[col] = df_excel[col].map(safe_excel)
            df_excel.to_excel(excel_path, index=False)

            supplier_user = get_user_by_username(supplier) 

            if supplier_user:
                with open(excel_path, "rb") as f:
                    await bot.send_document(
                        chat_id=supplier_user["TELEGRAM_ID"],
                        document=f
                    )


            if os.path.exists(excel_path):
                os.remove(excel_path)

            save_notification(
                supplier,
                "supplier",
                f"📊 You received {len(rows)} bulk deal offers. Please check Excel."
            )

        await message.reply("✅ Bulk deal requests sent successfully.")
        user_state.pop(uid, None)

        if os.path.exists(path):
            os.remove(path)

        return


    # ==========================================================
    # ✅ ADMIN DEAL APPROVAL EXCEL (MUST BE BEFORE SUPPLIER CHECK)
    # ==========================================================
    if user["ROLE"] == "admin" and message.document.file_name.lower().endswith(".xlsx"):

        file = await bot.get_file(message.document.file_id)
        try:
            path = f"/tmp/{uid}_{int(time.time())}_{message.document.file_name}"
            await bot.download_file(file.file_path, path)
        except Exception:
            await message.reply("❌ Invalid Excel file.")
            return
    
        df = pd.read_excel(path)

        required_cols = [
            "Deal ID",
            "Supplier Action (ACCEPT / REJECT)",
            "Admin Action (YES / NO)"
        ]
        for col in required_cols:
            if col not in df.columns:
                await message.reply("❌ Invalid admin approval Excel format.")
                if os.path.exists(path):
                    os.remove(path)
                return

        for _, row in df.iterrows():

            if pd.isna(row.get("Deal ID")):
                continue

            deal_id = str(row["Deal ID"]).strip()

            # ✅ Validate Deal ID
            if not deal_id.startswith("DEAL-"):
                continue

            supplier_decision = str(
                row.get("Supplier Action (ACCEPT / REJECT)", "")
            ).strip().upper()


            admin_decision = str(
                row.get("Admin Action (YES / NO)", "")
            ).strip().upper()

            if admin_decision not in ["YES", "NO", ""]:
                continue

            key = f"{DEALS_FOLDER}{deal_id}.json"

            try:
                deal = json.loads(
                    s3.get_object(
                        Bucket=AWS_BUCKET,
                        Key=key
                    )["Body"].read()
                )
            except:
                continue

            # 🚫 Prevent editing closed deals
            if deal.get("final_status") in [STATUS_COMPLETED, STATUS_CLOSED]:
                continue

            # ---------------- SUPPLIER ACTION ----------------
            if supplier_decision == "ACCEPT":
                deal["supplier_action"] = "ACCEPTED"

            elif supplier_decision == "REJECT":
                deal["supplier_action"] = "REJECTED"
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"
                unlock_stone(deal["stone_id"])


            # ---------------- ADMIN ACTION ----------------
            if admin_decision == "YES" and deal.get("supplier_action") == "ACCEPTED":
                deal["admin_action"] = "APPROVED"
                deal["final_status"] = "COMPLETED"

                try:
                    remove_stone_from_supplier_and_combined(deal["stone_id"])
                except Exception as e:
                    print("Remove stone failed:", e)
    
                save_notification(
                    deal["client_username"],
                    "client",
                    f"🎉 Deal APPROVED for Stone {deal['stone_id']}"
                )

                save_notification(
                    deal["supplier_username"],
                    "supplier",
                    f"✅ Deal APPROVED for Stone {deal['stone_id']}"
                )

            elif admin_decision == "NO":
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"

                # 🔓 Unlock stone
                unlock_stone(deal["stone_id"])

                save_notification(
                    deal["client_username"],
                    "client",
                    f"❌ Deal rejected by admin for Stone {deal['stone_id']}"
                )


                save_notification(
                    deal["supplier_username"],
                    "supplier",
                    f"❌ Deal rejected by admin for Stone {deal['stone_id']}"
                )

            # ---------------- SAVE DEAL ----------------
            log_deal_history(deal)

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

        await message.reply("✅ Admin deal decisions processed successfully.")

        if os.path.exists(path):
            os.remove(path)
            
        return

    # ==========================================================
    # ✅ SUPPLIER DEAL APPROVAL EXCEL
    # ==========================================================
    if (
        user["ROLE"] == "supplier"
        and message.document.file_name.lower().endswith(".xlsx")
    ):

        file = await bot.get_file(message.document.file_id)
        path = f"/tmp/{uid}_{int(time.time())}_{message.document.file_name}"
        await bot.download_file(file.file_path, path)
        try:
            df = pd.read_excel(path)
        except Exception:
            await message.reply("❌ Invalid Excel file.")
            return

        required_cols = [
            "Deal ID",
            "Supplier Action (ACCEPT / REJECT)"
        ]
        for col in required_cols:
            if col not in df.columns:
                await message.reply("❌ Invalid supplier approval Excel format.")
                if os.path.exists(path):
                    os.remove(path)
                return

        processed = 0

        for _, row in df.iterrows():

            if pd.isna(row.get("Deal ID")):
                continue

            deal_id = str(row["Deal ID"]).strip()
            decision = str(
                row.get("Supplier Action (ACCEPT / REJECT)", "")
            ).strip().upper()


            if not deal_id.startswith("DEAL-"):
                continue

            key = f"{DEALS_FOLDER}{deal_id}.json"

            try:
                deal = json.loads(
                    s3.get_object(
                        Bucket=AWS_BUCKET,
                        Key=key
                    )["Body"].read()
                )
            except:
                continue

            # 🔐 Only supplier who owns the deal can update
            if deal.get("supplier_username","").strip().lower() != user["USERNAME"].strip().lower():
                continue

            # 🚫 Prevent editing closed deals
            if deal.get("final_status") in [STATUS_COMPLETED, STATUS_CLOSED]:
                continue

            # ---------------- SUPPLIER DECISION ----------------
            if decision == "ACCEPT":
                deal["supplier_action"] = "ACCEPTED"

                save_notification(
                    deal["client_username"],
                    "client",
                    f"✅ Supplier accepted deal for Stone {deal['stone_id']}"
                )

            elif decision == "REJECT":
                deal["supplier_action"] = "REJECTED"
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"

                # 🔓 Unlock stone
                unlock_stone(deal["stone_id"])

                save_notification(
                    deal["client_username"],
                    "client",
                    f"❌ Supplier rejected deal for Stone {deal['stone_id']}"
                )
            else:
                continue

            # ---------------- SAVE DEAL ----------------
            log_deal_history(deal)

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

            processed += 1

        await message.reply(f"✅ Supplier deal decisions processed successfully. ({processed} deals)")
        return   # ✅ IMPORTANT: stop further processing

    if user["ROLE"].lower() != "supplier":
        await message.reply("❌ Only suppliers can upload diamonds")
        return


    file = await bot.get_file(message.document.file_id)
    path = f"/tmp/{uid}_{int(time.time())}_{message.document.file_name}"

    await bot.download_file(file.file_path, path)

    try:
        df = pd.read_excel(path)
    except Exception:
        await message.reply("❌ Invalid Excel file.")
        return

    required_cols = [
        "Stock #","Shape","Weight","Color","Clarity",
        "Price Per Carat","Total Price","Lab","Report #"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        await message.reply(
            f"❌ Missing required columns:\n" + ", ".join(missing)
        )
        return


    if df["Stock #"].isnull().any():
        await message.reply("❌ Stock # cannot be empty")
        return


    # ---------- DATA VALIDATION ----------
    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")

    if (df["Weight"] <= 0).any():
        await message.reply("❌ Weight must be greater than 0")
        return

    if "Price Per Carat" in df.columns and (df["Price Per Carat"] <= 0).any():
        await message.reply("❌ Price must be greater than 0")
        return

    invalid_shapes = df["Shape"].astype(str).str.len() < 3
    if invalid_shapes.any():
        await message.reply("❌ Invalid shape format")
        return

    mandatory_cols = ["Shape", "Color", "Clarity", "Weight", "Contact Number", "Diamond Type", "Description"]
    missing_cols = [c for c in mandatory_cols if c not in df.columns]
    if missing_cols:
        await message.reply(f"❌ Missing mandatory columns: {', '.join(missing_cols)}")
        return
    empty_cols = [c for c in mandatory_cols if df[c].isnull().any()]
    if empty_cols:
        await message.reply(f"❌ Empty values in columns: {', '.join(empty_cols)}")
        return

    supplier_key_name = user.get("SUPPLIER_KEY")

    supplier_key = f"{SUPPLIER_STOCK_FOLDER}{supplier_key_name}.xlsx"
    local_path = f"/tmp/{supplier_key_name}.xlsx"

    df["SUPPLIER"] = supplier_key_name

    existing = load_stock()
    if "LOCKED" not in df.columns:
        df["LOCKED"] = "NO"

    # ✅ Preserve already locked stones
    if not existing.empty and "LOCKED" in existing.columns:
        locked_map = dict(
            zip(existing["Stock #"], existing["LOCKED"])
        )
        df["LOCKED"] = df["Stock #"].map(locked_map).fillna(df["LOCKED"])


    for col in df.select_dtypes(include="object"):
        df[col] = df[col].map(safe_excel)
    df.to_excel(local_path, index=False)

    s3.upload_file(
        local_path,
        AWS_BUCKET,
        supplier_key
    )


    # rebuild combined stock

    df["Weight"] = pd.to_numeric(df["Weight"], errors="coerce")
    df["Price Per Carat"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
    rebuild_combined_stock()

    total_stones = len(df)
    total_weight = df["Weight"].sum()
    average_weight = df["Weight"].mean()
    total_price = (
        df["Weight"] * df["Price Per Carat"]
    ).sum()

    shape_counts = df["Shape"].dropna().str.lower().value_counts()
    shape_table = "Shape | Stones\n----------------------\n"
    for shape, count in shape_counts.items():
        shape_table += f"{shape.capitalize()} | {count}\n"

    # ✅ Supplier summary
    supplier_counts = df["SUPPLIER"].str.replace("supplier_", "", regex=False).value_counts()
    supplier_table = "Supplier | Stones\n----------------------\n"
    for supplier, count in supplier_counts.items():
        supplier_table += f"{supplier.capitalize()} | {count}\n"

    summary_msg = (
        f"💎 FlowAI Summary\n"
        f"- Total diamonds: {total_stones}\n"
        f"- Total weight: {round(total_weight,2)} ct\n"
        f"- Average weight: {round(average_weight,2)} ct\n"
        f"- Total price: {round(total_price,2)}\n\n"
        f"📊 Shape Distribution\n{shape_table}\n"
        f"📊 Supplier Distribution\n{supplier_table}\n"
        "💡 Insight: Review high-value diamonds and rare shapes."
    )
    log_activity(
        user,
        "UPLOAD_EXCEL",
        {
            "file_name": message.document.file_name,
            "stones": total_stones,
            "total_weight": round(total_weight, 2)
        }
    )

    await message.reply(summary_msg)

    if os.path.exists(local_path):
        os.remove(local_path)

# ---------------- START BOT ON SERVER START ----------------

@app.on_event("startup")
async def startup_event():
    import asyncio
    print("🤖 Telegram Bot starting...")

    load_sessions()

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        print("Webhook cleanup failed:", e)

    # ✅ HARD LOCK — prevent duplicate polling
    if not hasattr(startup_event, "started"):
        startup_event.started = True
        asyncio.create_task(dp.start_polling(bot))
        print("✅ Bot polling started")
    else:
        print("⚠️ Bot already running — skipping duplicate polling")

# ---------------- RUN FASTAPI SERVER ----------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
