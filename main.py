import os
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Read token from environment variable
TOKEN = os.getenv("8438406844:AAFkwfViMdsri3LktvSDGRYPd68391EJ-AM")

if not TOKEN:
    raise ValueError("❌ BOT_TOKEN environment variable not found!")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("✅ Bot is running successfully!")

def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print("🤖 Bot started...")
    app.run_polling()

if __name__ == "__main__":
    main()
