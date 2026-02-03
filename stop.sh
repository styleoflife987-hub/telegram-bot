#!/bin/bash
# stop.sh - Stop the Diamond Trading Bot

echo "🛑 Stopping Diamond Trading Bot..."

# Find and kill bot processes
BOT_PID=$(pgrep -f "python.*diamond_bot.py")

if [ -z "$BOT_PID" ]; then
    echo "ℹ️ Bot is not running."
else
    echo "⏳ Stopping PID: $BOT_PID"
    kill -9 $BOT_PID
    sleep 2
    
    if pgrep -f "python.*diamond_bot.py" > /dev/null; then
        echo "❌ Failed to stop bot. Trying force kill..."
        pkill -9 -f "python.*diamond_bot.py"
    fi
    
    echo "✅ Bot stopped successfully."
fi

# Also stop monitor if running
MONITOR_PID=$(pgrep -f "monitor.sh")
if [ ! -z "$MONITOR_PID" ]; then
    kill $MONITOR_PID 2>/dev/null
    echo "✅ Monitor stopped."
fi
