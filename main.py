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
from aiogram import types
import os
import json
import pytz
import uuid
from openai import OpenAI



# ---------------- DEAL STATE VALIDATION ----------------

def is_valid_deal_state(deal: dict) -> bool:
    supplier_action = deal.get("supplier_action")
    admin_action = deal.get("admin_action")
    final_status = deal.get("final_status")

    valid_states = {
        ("PENDING", "PENDING", "OPEN"),
        ("ACCEPTED", "PENDING", "OPEN"),
        ("REJECTED", "REJECTED", "CLOSED"),
        ("ACCEPTED", "APPROVED", "COMPLETED"),
        ("ACCEPTED", "REJECTED", "CLOSED"),
    }

    return (supplier_action, admin_action, final_status) in valid_states



# ---------------- CONFIG ----------------

TOKEN = os.getenv("BOT_TOKEN")

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET = os.getenv("AWS_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# ---------------- OPENAI ----------------

openai_client = OpenAI(
    api_key=os.getenv("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")
)

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

def log_deal_history(deal):
    try:
        s3.download_file(AWS_BUCKET, DEAL_HISTORY_KEY, "/tmp/deal_history.xlsx")
        df = pd.read_excel("/tmp/deal_history.xlsx")
    except:
        df = pd.DataFrame(columns=[
            "deal_id",
            "stone_id",
            "supplier",
            "client",
            "actual_price",
            "offer_price",
            "supplier_action",
            "admin_action",
            "final_status",
            "created_at"
        ])

    df.loc[len(df)] = [
        deal["deal_id"],
        deal["stone_id"],
        deal["supplier_username"],
        deal["client_username"],
        deal["actual_stock_price"],
        deal["client_offer_price"],
        deal["supplier_action"],
        deal["admin_action"],
        deal["final_status"],
        deal["created_at"]
    ]

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
        "time": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M"),
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

# ---------------- OPENAI HELPER ----------------

def ask_openai(system_prompt, user_prompt, temperature=0.2, telegram_id=None):

    # ---------- AI RATE LIMIT ----------
    if telegram_id is not None:
        ai_usage[telegram_id] = ai_usage.get(telegram_id, 0) + 1
        if ai_usage[telegram_id] > AI_LIMIT_PER_SESSION:
            return "⚠️ AI limit reached. Try later."

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature
        )

        return response.choices[0].message.content.strip()

    except Exception:
        return "⚠️ AI service unavailable."


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

ai_usage = {}  # {telegram_id: ai_calls_count}
AI_LIMIT_PER_SESSION = 5


def get_logged_user(uid):
    return logged_in_users.get(uid)

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

    if "LOCKED" not in final_df.columns:
        final_df["LOCKED"] = "NO"

    final_df["LOCKED"] = final_df["LOCKED"].fillna("NO")

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

def lock_stone(stone_id):
    df = load_stock()
    if df.empty:
        return False

    if stone_id not in df["Stock #"].values:
        return False

    if df.loc[df["Stock #"] == stone_id, "LOCKED"].values[0] == "YES":
        return False

    df.loc[df["Stock #"] == stone_id, "LOCKED"] = "YES"
    df.to_excel("/tmp/all_suppliers_stock.xlsx", index=False)
    s3.upload_file("/tmp/all_suppliers_stock.xlsx", AWS_BUCKET, COMBINED_STOCK_KEY)
    return True


def unlock_stone(stone_id):
    df = load_stock()
    if df.empty:
        return

    df.loc[df["Stock #"] == stone_id, "LOCKED"] = "NO"
    df.to_excel("/tmp/all_suppliers_stock.xlsx", index=False)
    s3.upload_file("/tmp/all_suppliers_stock.xlsx", AWS_BUCKET, COMBINED_STOCK_KEY)


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
    user_state[message.from_user.id] = {"step": "login_username"}
    await message.reply("Enter Username:")

# ---------------- LOGOUT ----------------

@dp.message(Command("logout"))
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
    save_sessions()
    user_state.pop(uid, None)

    await message.reply(
        "✅ Logged out successfully.\n/login to continue.",
        reply_markup=types.ReplyKeyboardRemove()
    )

    ai_usage.pop(message.from_user.id, None)

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

# ---------------- SMART DEALS ----------------
@dp.message(F.text == "🔥 Smart Deals")
async def smart_deals(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user:
        return

    # 🔒 Client only
    if user["ROLE"] != "client":
        await message.reply("❌ Smart Deals are available for clients only.")
        return

    df = load_stock()
    df = df[df["LOCKED"] != "YES"]
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

        ai_explanation = ask_openai(
            system_prompt=(
                "You are a professional diamond market analyst. "
                "Explain deals clearly in 2 short sentences."
            ),
            user_prompt=f"""
            Shape: {r['Shape']}
            Weight: {r['Weight']} ct
            Color: {r['Color']}
            Clarity: {r['Clarity']}
            Price Per Carat: ${price}
            Market Median: ${int(r['MARKET_MEDIAN'])}
            """
        )


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

    save_notification(
        username="prince",
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
    total_carats = round(df["Weight"].sum(), 2)

    msg = f"💎 Total Diamonds: {total_diamonds}\n📊 Total Carats: {total_carats}\n"

    if "Shape" in df.columns:
        shape_counts = df["Shape"].str.lower().value_counts()
        msg += "📌 Shapes Distribution:\n"
        for shape, count in shape_counts.items():
            msg += f"- {shape.capitalize()}: {count}\n"

    await message.reply(msg + f"\n🧠 AI Insight:\n{ai_explanation}")

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

    await message.reply(msg + f"\n🧠 AI Insight:\n{ai_explanation}")

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
    df["CT/PR $"] = pd.to_numeric(df["Price Per Carat"], errors="coerce")
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

    await message.reply(msg + f"\n🧠 AI Insight:\n{ai_explanation}")


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

    key = f"{SUPPLIER_STOCK_FOLDER}{user.get('SUPPLIER_KEY')}.xlsx"
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

    my_df = df[df["SUPPLIER"] == supplier_name]
    if my_df.empty:
        await message.reply("❌ You have no stones uploaded.")
        return

    result_rows = []
    for _, row in my_df.iterrows():
        key = row["MATCH_KEY"]
        my_price = row["Price Per Carat"]
        market = df[df["MATCH_KEY"] == key]
        best_price = market["Price Per Carat"].min() if len(market) > 1 else my_price
        diff = round(my_price - best_price, 2)
        status = "BEST PRICE" if diff == 0 else "OVERPRICED" if diff > 0 else "UNDERPRICED"

        ai_advice = ask_openai(
           system_prompt=system_prompt,
           user_prompt=user_prompt,
           telegram_id=message.from_user.id
        )
            system_prompt=(
                "You are a diamond pricing consultant. "
                "Give clear pricing advice in 2 short sentences."
            ),
            user_prompt=f"""
            My price: ${my_price}
            Best market price: ${best_price}
            Difference: ${diff}
            Shape: {row['Shape']}
            Weight: {row['Weight']}
            Color: {row['Color']}
            Clarity: {row['Clarity']}
            Diamond Type: {row['Diamond Type']}
            """
        )

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
            "Price Status": status,
            "AI Advice": ai_advice
        })

    result_df = pd.DataFrame(result_rows)
    out_path = f"/tmp/{supplier_name}_price_analytics.xlsx"
    result_df.to_excel(out_path, index=False)

    await message.reply_document(
        types.FSInputFile(out_path),
        caption="📊 Your Full Market Price Comparison (All Stones)"
    )

# ---------------- View Deals ------------------

# ---------------- Supplier VIEW DEALS ------------------

@dp.message(F.text == "🤝 View Deals")
async def supplier_view_deals(message: types.Message):
    user = get_logged_user(message.from_user.id)

    if not user or user["ROLE"].lower() != "supplier":
        return

    supplier_username = user["USERNAME"].lower()

    response = s3.list_objects_v2(
        Bucket=AWS_BUCKET,
        Prefix=DEALS_FOLDER
    )

    rows = []

    for obj in response.get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue

        deal = json.loads(
            s3.get_object(
                Bucket=AWS_BUCKET,
                Key=obj["Key"]
            )["Body"].read()
        )

        if deal.get("supplier_username") != supplier_username:
            continue

        rows.append({
            "Deal ID": deal["deal_id"],
            "Stone ID": deal["stone_id"],
            "Client": deal["client_username"],
            "Actual Price ($/ct)": deal["actual_stock_price"],
            "Offer Price ($/ct)": deal["client_offer_price"],
            "Supplier Action (ACCEPT / REJECT)": deal["supplier_action"]
        })

    if not rows:
        await message.reply("ℹ️ No deals available.")
        return

    df = pd.DataFrame(rows)
    file_path = f"/tmp/{supplier_username}_deals.xlsx"
    df.to_excel(file_path, index=False)

    await message.reply_document(
        types.FSInputFile(file_path),
        caption="📊 Your deals — Fill ACCEPT or REJECT and upload back"
    )
# ---------------- ADMIN VIEW DEALS ------------------

@dp.message(F.text == "🤝 View Deals")
async def admin_view_deals(message: types.Message):
    user = get_logged_user(message.from_user.id)

    # ✅ Only admins can access
    if not is_admin(user):
        await message.reply("❌ Admin only")
        return

    # Fetch all deal JSONs from S3
    try:
        response = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=DEALS_FOLDER)
        objects = response.get("Contents", [])
        if not objects:
            await message.reply("ℹ️ No deals found.")
            return
    except Exception as e:
        await message.reply(f"❌ Error fetching deals: {str(e)}")
        return

    rows = []
    for obj in objects:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        try:
            deal_obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
            deal = json.loads(deal_obj["Body"].read())
        except Exception as e:
            continue  # skip corrupt files

        rows.append({
            "Deal ID": deal.get("deal_id", ""),
            "Stock #": deal.get("stone_id", ""),
            "Supplier": deal.get("supplier_username", ""),
            "Client": deal.get("client_username", ""),
            "Actual Price ($/ct)": deal.get("actual_stock_price", ""),
            "Offer Price ($/ct)": deal.get("client_offer_price", ""),
            "Supplier Action": deal.get("supplier_action", ""),
            "Admin Action": deal.get("admin_action", ""),
            "Final Status": deal.get("final_status", ""),
            "Created At": deal.get("created_at", "")
        })

    if not rows:
        await message.reply("ℹ️ No deals found.")
        return

    # Convert to Excel
    df = pd.DataFrame(rows)
    out_path = "/tmp/admin_all_deals.xlsx"
    df.to_excel(out_path, index=False)

    await message.reply_document(
        types.FSInputFile(out_path),
        caption=f"📊 All Deals (Admin View) | Total: {len(df)}"
    )

# ---------------- START DEAL REQUEST ----------------

@dp.message(F.text == "🤝 Request Deal")
async def request_deal_start(message: types.Message):
    user = get_logged_user(message.from_user.id)
    if not user or user["ROLE"] != "client":
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

    user_state[message.from_user.id] = {"step": "bulk_deal_excel"}



# user activity
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

    log_activity(user, "DOWNLOAD_ACTIVITY_REPORT")

def generate_activity_excel():
    objs = s3.list_objects_v2(Bucket=AWS_BUCKET, Prefix=ACTIVITY_LOG_FOLDER)
    rows = []

    for obj in objs.get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue

        data = json.loads(
            s3.get_object(Bucket=AWS_BUCKET, Key=obj["Key"])["Body"].read()
        )

        for r in data:
            rows.append(r)

    if not rows:
        return None

    df = pd.DataFrame(rows)
    path = "/tmp/user_activity_report.xlsx"
    df.to_excel(path, index=False)
    return path

# ---------------- TEXT HANDLER ----------------

@dp.message(F.text)
async def handle_text(message: types.Message):
    uid = message.from_user.id
    text = message.text.strip()

    # -------- LOGIN / CREATE FLOW --------
    if uid in user_state:
        state = user_state[uid]

        # ---------- DEAL REQUEST FLOW ----------
        if state.get("step") == "deal_stone":
            state["stone_id"] = text
            state["step"] = "deal_price"
            await message.reply("💰 Enter your offer price ($/ct):")
            return

        if state.get("step") == "deal_price":
            try:
                offer_price = float(text)
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

            row = df[df["Stock #"] == stone_id]
            if row.empty:
                await message.reply("❌ Stone not found.")
                user_state.pop(uid, None)
                return

            r = row.iloc[0]

            deal_id = f"DEAL-{uuid.uuid4().hex[:10]}"

            actual_price = float(r["Price Per Carat"])

            deal = {
                "deal_id": deal_id,
                "stone_id": stone_id,
                "supplier_username": r["SUPPLIER"].replace("supplier_", "").lower(),
                "client_username": user["USERNAME"],

                "actual_stock_price": actual_price,
                "client_offer_price": offer_price,

                "supplier_action": "PENDING",
                "admin_action": "PENDING",
                "final_status": "OPEN",

                "created_at": datetime.now(
                    pytz.timezone("Asia/Kolkata")
                ).strftime("%Y-%m-%d %H:%M")
            }

            # 🔒 LOCK STONE FIRST
            if not lock_stone(stone_id):
                await message.reply("❌ This stone is already under negotiation.")
                user_state.pop(uid, None)
                return

            # 💾 SAVE DEAL AFTER LOCK
            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=f"{DEALS_FOLDER}{deal_id}.json",
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

            # Notify supplier
            save_notification(
                username=r["SUPPLIER"].replace("supplier_", "").lower(),
                role="supplier",
                message=(
                     "📩 New deal offer received\n\n"
                     f"💎 Stone ID: {stone_id}\n"
                     f"🔷 Shape: {r.get('Shape','N/A')}\n"
                     f"⚖️ Weight: {r.get('Weight','N/A')}\n"
                     f"🎨 Color: {r.get('Color','N/A')}\n"
                     f"🔍 Clarity: {r.get('Clarity','N/A')}\n\n"
                     f"💰 Actual Price: ${actual_price} / ct\n"
                     f"📈 Offer Price: ${offer_price} / ct"
               )
            )

            # Log activity
            log_activity(
                user,
                "REQUEST_DEAL",
                {
                    "stone_id": stone_id,
                    "offer_price": offer_price
                }
            )

            # Confirmation message
            await message.reply(
                f"✅ Deal request sent successfully!\n\n"
                f"💎 Stone ID: {stone_id}\n"
                f"💰 Your Offer: ${offer_price} / ct\n"
                f"⏳ Waiting for supplier response."
            )

            user_state.pop(uid, None)
            return


        # ---- LOGIN FLOW ----
        if state["step"] == "login_username":
            state["username"] = message.text.strip()
            state["step"] = "login_password"
            await message.reply("Enter Password:")
            return

        if state["step"] == "login_password":
            df = load_accounts()
            r = df[(df["USERNAME"] == state["username"]) & (df["PASSWORD"] == message.text)]

            if r.empty:
                await message.reply("❌ Login failed. Invalid username or password.")
                user_state.pop(uid)
                return

            if r.iloc[0]["APPROVED"] != "YES":
                await message.reply("❌ Your account is not approved yet.")
                user_state.pop(uid)
                return

            # ---------------- FIX FOR PRINCE ----------------
            role = r.iloc[0]["ROLE"]
            if r.iloc[0]["USERNAME"].lower() == "prince":
                role = "admin"  # Force Prince to be admin
            # -----------------------------------------------

            # Save logged in Telegram ID
            ist = pytz.timezone("Asia/Kolkata")

            logged_in_users[uid] = {
                "USERNAME": r.iloc[0]["USERNAME"],
                "ROLE": role,
                "SUPPLIER_KEY": f"supplier_{r.iloc[0]['USERNAME'].lower()}" if role == "supplier" else None,
            }

            save_sessions()

            log_activity(
                logged_in_users[uid],
                "LOGIN"
            )

            # Assign keyboard
            if role == "admin":
                kb = admin_kb
            elif role == "client":
                kb = client_kb
            elif role == "supplier":
                kb = supplier_kb
            else:
                kb = types.ReplyKeyboardRemove()

            username = r.iloc[0]["USERNAME"].capitalize()

            if role == "admin":
                welcome_msg = (
                    f"👑 Welcome back, Admin {username} — command, control, excellence."
                )

            elif role == "supplier":
                welcome_msg = (
                    f"💎 Welcome, Supplier {username} — your brilliance drives the market."
                )

            elif role == "client":
                welcome_msg = (
                    f"🥂 Welcome, {username} — discover diamonds beyond ordinary."
                )

            else:
                welcome_msg = f"Welcome, {username}."

            await message.reply(
                welcome_msg,
                reply_markup=kb
            )

            # 🔔 SHOW SAVED NOTIFICATIONS
            notifications = fetch_unread_notifications(
                logged_in_users[uid]["USERNAME"],
                logged_in_users[uid]["ROLE"]
            )

            if notifications:
                note_msg = "🔔 Notifications\n\n"
                for n in notifications:
                    note_msg += f"{n['message']}\n🕒 {n['time']}\n\n"
                await message.reply(note_msg)

            user_state.pop(uid, None)




    # -------- BUTTON HANDLING --------
    user = get_logged_user(uid)
    if not user:
        return

    if text == "💎 Search Diamonds":
        user_state[uid] = {"step": "search_carat", "search": {}}
        await message.reply("Enter Weight (e.g., 1 or 1-1.5, or 'any'):")
        return

    if text == "📤 Upload Excel":
        await message.reply("Send Excel file 📊")
        return

    if text == "📥 Download Sample Excel":
        # Create sample Excel in memory
        df = pd.DataFrame({
            "STONE ID": ["D001", "D002", "D003"],
            "LOCATION": ["Mumbai", "Delhi", "Bangalore"],
            "Shape": ["Round", "Oval", "Princess"],
            "Carat": [1.0, 1.5, 2.0],
            "Color": ["White", "Yellow", "Pink"],
            "Clarity": ["VVS", "VS", "SI"],
            "CUT": ["Excellent", "Very Good", "Good"],
            "PO": ["PO123", "PO124", "PO125"],
            "Symmetry": ["Excellent", "Very Good", "Good"],
            "FLS": ["Yes", "No", "Yes"],
            "CT/PR $": [10000, 15000, 20000],
            "Total Price": [10000, 15000, 20000],
            "MEASURMENT": ["6.5x6.5x4.0", "7.0x5.5x3.5", "8.0x6.0x4.0"],
            "TABLE %": [57, 58, 59],
            "DEPTH %": [61, 62, 63],
            "VIDEO": ["link1", "link2", "link3"],
            "REPORT NO": ["R001", "R002", "R003"],
            "LAB": ["GIA", "IGI", "HRD"],
            "COMPANY COMMENT": ["Good quality", "Premium", "Rare cut"],
            "IMAGE": ["img1.jpg", "img2.jpg", "img3.jpg"],
            "STOCK STATUS": ["Available", "Reserved", "Sold"],
            "CERTIFICATE LINK": ["cert1.pdf", "cert2.pdf", "cert3.pdf"],
            "Contact Number": ["1234567890", "0987654321", "1122334455"],
            "Diamond Type": ["Natural", "LGD", "HPHT"]
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
                        except:
                            continue
                    else:
                        try:
                            carat = float(r)
                            mask |= (df["Weight"] >= carat) & (df["Weight"] <= carat + 0.2)
                        except:
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
                excel_df = df.rename(columns={"SUPPLIER": "Supplier"})
                excel_df.to_excel(out, index=False)

                await message.reply_document(
                    types.FSInputFile(out),
                    caption=f"💎 {len(df)} diamonds found\nShapes: {shape_summary}"
                )
            else:
                for _, r in df.iterrows():
                    msg = (
                        f"💎 {r['Weight']} ct | {r['Shape']} | {r['Color']} | {r['Clarity']}\n"
                        f"💰 ${r.get('Price Per Carat', 'N/A')} / ct\n"
                        f"🏛 Lab: {r.get('Lab', 'N/A')} | 📦 {r.get('Availability', 'N/A')}\n"
                    )
                    await message.reply(msg + f"\n🧠 AI Insight:\n{ai_explanation}")

            log_activity(
                user,
                "SEARCH",
                {
                    "Weight": search["carat"],
                    "shape": search["shape"],
                    "color": search["color"],
                    "clarity": search["clarity"],
                    "results": len(df)
                }
            )

            user_state.pop(uid)
            return

# ---------------- DOCUMENT HANDLER (FIXED & SAFE) ----------------
@dp.message(F.document)
async def handle_doc(message: types.Message):
    uid = message.from_user.id
    user = get_logged_user(uid)

    if not user:
        await message.reply("🔒 Please login first.")
        return

    filename = message.document.file_name.lower()

    # ❗ FILE SIZE LIMIT (10 MB)
    if message.document.file_size > 10 * 1024 * 1024:
        await message.reply("❌ File too large. Max allowed size is 10 MB.")
        return

    # ----------------------------------------------------------
    # 1️⃣ CLIENT BULK DEAL REQUEST
    # ----------------------------------------------------------
    if user["ROLE"] == "client" and user_state.get(uid, {}).get("step") == "bulk_deal_excel":
        file = await bot.get_file(message.document.file_id)
        path = f"/tmp/{message.document.file_name}"
        await bot.download_file(file.file_path, path)

        df = pd.read_excel(path)
        stock_df = load_stock()

        for _, row in df.iterrows():
            stone_id = str(row.get("Stock #", "")).strip()
            try:
                offer_price = float(row.get("Offer Price ($/ct)", 0))
            except:
                continue

            stock_row = stock_df[stock_df["Stock #"] == stone_id]
            if stock_row.empty:
                continue

            if not lock_stone(stone_id):
                continue

            r = stock_row.iloc[0]
            deal_id = f"DEAL-{uuid.uuid4().hex[:10]}"

            deal = {
                "deal_id": deal_id,
                "stone_id": stone_id,
                "supplier_username": r["SUPPLIER"].replace("supplier_", "").lower(),
                "client_username": user["USERNAME"],
                "actual_stock_price": float(r["Price Per Carat"]),
                "client_offer_price": offer_price,
                "supplier_action": "PENDING",
                "admin_action": "PENDING",
                "final_status": "OPEN",
                "created_at": datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M")
            }

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=f"{DEALS_FOLDER}{deal_id}.json",
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

            save_notification(
                deal["supplier_username"],
                "supplier",
                f"📩 New bulk deal offer for Stone {stone_id}"
            )

        user_state.pop(uid, None)
        await message.reply("✅ Bulk deal requests submitted successfully.")
        return

    # ----------------------------------------------------------
    # 2️⃣ ADMIN DEAL APPROVAL EXCEL
    # ----------------------------------------------------------
    if user["ROLE"] == "admin" and "admin_pending_deals" in filename:
        file = await bot.get_file(message.document.file_id)
        path = f"/tmp/{message.document.file_name}"
        await bot.download_file(file.file_path, path)

        df = pd.read_excel(path)

        for _, row in df.iterrows():
            deal_id = str(row.get("Deal ID", "")).strip()
            admin_decision = str(row.get("Admin Action (YES / NO)", "")).strip().upper()

            key = f"{DEALS_FOLDER}{deal_id}.json"
            try:
                deal = json.loads(s3.get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read())
            except:
                continue

            if admin_decision == "YES" and deal["supplier_action"] == "ACCEPTED":
                deal["admin_action"] = "APPROVED"
                deal["final_status"] = "COMPLETED"
                remove_stone_from_supplier_and_combined(deal["stone_id"])

            elif admin_decision == "NO":
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"

                # 🔓 UNLOCK STONE
                unlock_stone(deal["stone_id"])

            if not is_valid_deal_state(deal):
                continue

            log_deal_history(deal)

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

        await message.reply("✅ Admin deal approvals processed.")
        return

    # ----------------------------------------------------------
    # 3️⃣ SUPPLIER DEAL DECISION EXCEL
    # ----------------------------------------------------------
    if user["ROLE"] == "supplier" and "deals" in filename:
        file = await bot.get_file(message.document.file_id)
        path = f"/tmp/{message.document.file_name}"
        await bot.download_file(file.file_path, path)

        df = pd.read_excel(path)

        for _, row in df.iterrows():
            deal_id = str(row.get("Deal ID", "")).strip()
            decision = str(row.get("Supplier Action (ACCEPT / REJECT)", "")).upper()

            key = f"{DEALS_FOLDER}{deal_id}.json"
            try:
                deal = json.loads(s3.get_object(Bucket=AWS_BUCKET, Key=key)["Body"].read())
            except:
                continue

            if deal["supplier_username"] != user["USERNAME"].lower():
                continue

            if deal["supplier_action"] != "PENDING":
                continue

            if decision == "ACCEPT":
                deal["supplier_action"] = "ACCEPTED"
                deal["admin_action"] = "PENDING"
                deal["final_status"] = "OPEN"

            elif decision == "REJECT":
                deal["supplier_action"] = "REJECTED"
                deal["admin_action"] = "REJECTED"
                deal["final_status"] = "CLOSED"
                unlock_stone(deal["stone_id"])

                # 🔓 UNLOCK STONE
                unlock_stone(deal["stone_id"])


            if not is_valid_deal_state(deal):
                continue

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=key,
                Body=json.dumps(deal, indent=2),
                ContentType="application/json"
            )

        await message.reply("✅ Supplier deal actions processed.")
        return

    # ----------------------------------------------------------
    # 4️⃣ SUPPLIER STOCK UPLOAD (LAST)
    # ----------------------------------------------------------
    if user["ROLE"] != "supplier":
        await message.reply("❌ Only suppliers can upload stock.")
        return

    file = await bot.get_file(message.document.file_id)
    path = f"/tmp/{message.document.file_name}"
    await bot.download_file(file.file_path, path)

    df = pd.read_excel(path)

    required_cols = [
        "Stock #","Shape","Weight","Color","Clarity",
        "Price Per Carat","Final Price","Lab","Report #"
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

    mandatory_cols = ["Shape", "Color", "Clarity", "Weight", "Contact Number", "Diamond Type"]
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

# ---------------- MAIN ----------------

async def main():
    print("💎 Bot is starting...")
    load_sessions()
    await dp.start_polling(bot)

# ---------------- RUN ----------------
nest_asyncio.apply()
asyncio.run(main())
