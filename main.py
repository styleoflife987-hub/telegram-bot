import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes

BOT_TOKEN = os.getenv("8438406844:AAFNnS_fFpOuWxg_9yLfdQkvgO07saVDs4Y")
ADMIN_USERNAME = os.getenv("Prince")  # without @

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Welcome! Send your diamond requirement.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user.username
    text = update.message.text

    # If admin sends message
    if user == ADMIN_USERNAME:
        await update.message.reply_text("Admin message received.")
    else:
        # Forward client message to admin
        admin_message = f"New Request from @{user}:\n{text}"
        await context.bot.send_message(chat_id=f"@{ADMIN_USERNAME}", text=admin_message)
        await update.message.reply_text("Sent to admin for approval.")

async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT, handle_message))
    await app.run_polling()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
