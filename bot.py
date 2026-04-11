import os
import logging
import asyncio
import random
import aiohttp
import asyncpg
from datetime import datetime
from flask import Flask, request, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# -------------------- Configuration --------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

# Flask app (for webhook)
app = Flask(__name__)

# Scheduler for quiz intervals
scheduler = AsyncIOScheduler()

# Store active quiz messages per chat
active_quizzes = {}

# Database pool
db_pool = None

# -------------------- Database Functions --------------------
async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS groups (
                chat_id BIGINT PRIMARY KEY,
                chat_title TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS quiz_history (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT,
                question TEXT,
                correct_answer TEXT,
                options TEXT[],
                asked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_scores (
                user_id BIGINT,
                chat_id BIGINT,
                username TEXT,
                correct_answers INTEGER DEFAULT 0,
                total_attempts INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, chat_id)
            )
        """)
    logger.info("Database initialized")

async def add_group(chat_id, chat_title):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO groups (chat_id, chat_title) 
            VALUES ($1, $2) 
            ON CONFLICT (chat_id) DO UPDATE 
            SET is_active = TRUE
        """, chat_id, chat_title)

async def remove_group(chat_id):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE groups SET is_active = FALSE WHERE chat_id = $1", chat_id)

async def get_active_groups():
    async with db_pool.acquire() as conn:
        return await conn.fetch("SELECT chat_id, chat_title FROM groups WHERE is_active = TRUE")

async def save_quiz_history(chat_id, question, correct_answer, options):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO quiz_history (chat_id, question, correct_answer, options)
            VALUES ($1, $2, $3, $4)
        """, chat_id, question, correct_answer, options)

async def update_score(user_id, chat_id, username, is_correct):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_scores (user_id, chat_id, username, correct_answers, total_attempts)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (user_id, chat_id) DO UPDATE
            SET correct_answers = user_scores.correct_answers + $4,
                total_attempts = user_scores.total_attempts + 1,
                username = $3
        """, user_id, chat_id, username, 1 if is_correct else 0)

async def get_leaderboard(chat_id, limit=10):
    async with db_pool.acquire() as conn:
        return await conn.fetch("""
            SELECT username, correct_answers, total_attempts,
                   ROUND(correct_answers * 100.0 / total_attempts, 1) AS accuracy
            FROM user_scores
            WHERE chat_id = $1
            ORDER BY correct_answers DESC
            LIMIT $2
        """, chat_id, limit)

async def get_last_quiz(chat_id):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT question, correct_answer, options 
            FROM quiz_history 
            WHERE chat_id = $1 
            ORDER BY asked_at DESC 
            LIMIT 1
        """, chat_id)

# -------------------- Gemini Quiz Generator --------------------
async def generate_quiz():
    """Generate a random quiz question using Gemini 2.5 Flash API"""
    categories = [
        "General Knowledge", "Science", "Technology", "World News",
        "Telegram", "Current Affairs", "History", "Geography",
        "Entertainment", "Sports"
    ]
    category = random.choice(categories)
    
    prompt = f"""Generate a multiple choice quiz question about {category}. 
    Format EXACTLY as:
    QUESTION: [the question text]
    OPTIONS: [option A] | [option B] | [option C] | [option D]
    ANSWER: [the correct letter A, B, C, or D]
    
    Make it interesting and educational. Difficulty: medium.
    """
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json={
                "contents": [{
                    "parts": [{"text": prompt}]
                }]
            }
        ) as response:
            data = await response.json()
            
            try:
                text = data['candidates'][0]['content']['parts'][0]['text']
                
                # Parse response
                lines = text.strip().split('\n')
                question = ""
                options = []
                answer = ""
                
                for line in lines:
                    if line.startswith("QUESTION:"):
                        question = line.replace("QUESTION:", "").strip()
                    elif line.startswith("OPTIONS:"):
                        options_raw = line.replace("OPTIONS:", "").strip()
                        options = [opt.strip() for opt in options_raw.split("|")]
                    elif line.startswith("ANSWER:"):
                        answer = line.replace("ANSWER:", "").strip().upper()
                
                # Ensure we have 4 options
                if len(options) != 4:
                    options = ["Option A", "Option B", "Option C", "Option D"]
                
                return {
                    'question': question or f"Quiz about {category}",
                    'options': options,
                    'correct': answer if answer in ['A', 'B', 'C', 'D'] else 'A',
                    'category': category
                }
            except Exception as e:
                logger.error(f"Gemini parse error: {e}, response: {data}")
                # Fallback quiz
                return {
                    'question': "What is the capital of France?",
                    'options': ["London", "Berlin", "Paris", "Madrid"],
                    'correct': 'C',
                    'category': "Geography"
                }

# -------------------- Quiz Sender --------------------
async def send_quiz_to_chat(chat_id, chat_title, context):
    """Send a quiz to a specific chat"""
    try:
        # Delete previous quiz if exists
        if chat_id in active_quizzes:
            try:
                await context.bot.delete_message(
                    chat_id=chat_id,
                    message_id=active_quizzes[chat_id]
                )
            except:
                pass
        
        # Generate new quiz
        quiz = await generate_quiz()
        
        # Create keyboard with options
        keyboard = [
            [
                InlineKeyboardButton("🔴 A", callback_data=f"quiz_{chat_id}_A"),
                InlineKeyboardButton("🔵 B", callback_data=f"quiz_{chat_id}_B"),
                InlineKeyboardButton("🟢 C", callback_data=f"quiz_{chat_id}_C"),
                InlineKeyboardButton("🟡 D", callback_data=f"quiz_{chat_id}_D")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Build options text
        options_text = ""
        for i, opt in enumerate(quiz['options']):
            letter = ['A', 'B', 'C', 'D'][i]
            emoji = ["🔴", "🔵", "🟢", "🟡"][i]
            options_text += f"{emoji} {letter}: {opt}\n"
        
        # Send quiz message
        message_text = (
            f"📊 **{quiz['category']} Quiz** 📊\n\n"
            f"❓ {quiz['question']}\n\n"
            f"{options_text}\n"
            f"⏰ New quiz in 30 minutes!"
        )
        
        sent_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        # Store active quiz
        active_quizzes[chat_id] = sent_msg.message_id
        
        # Save to history
        await save_quiz_history(chat_id, quiz['question'], quiz['correct'], quiz['options'])
        
        logger.info(f"Quiz sent to {chat_title} ({chat_id})")
        
    except Exception as e:
        logger.error(f"Failed to send quiz to {chat_id}: {e}")

async def send_quiz_to_all():
    """Send quiz to all active groups"""
    groups = await get_active_groups()
    for group in groups:
        await send_quiz_to_chat(group['chat_id'], group['chat_title'], application)

# -------------------- Handlers --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"🤖 **Quiz Bot**\n\n"
        f"Welcome, {user.first_name}!\n\n"
        f"Add me to any group or channel, and I'll send a quiz every 30 minutes!\n\n"
        f"**Features:**\n"
        f"✅ Automatic quizzes every 30 minutes\n"
        f"✅ Categories: GK, Science, News, Tech, and more\n"
        f"✅ Instant feedback – green for correct, red for wrong\n"
        f"✅ Leaderboard to track scores\n\n"
        f"**Commands:**\n"
        f"/leaderboard - Show top scorers\n"
        f"/stats - Group statistics\n"
        f"/help - Show this message",
        parse_mode='Markdown'
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"📖 **Help**\n\n"
        f"Add me to a group and make me an admin.\n"
        f"I'll send a quiz every 30 minutes automatically!\n\n"
        f"**Commands:**\n"
        f"/start - Start the bot\n"
        f"/leaderboard - Show top 10 scorers\n"
        f"/stats - Show group quiz statistics\n\n"
        f"Each quiz has 4 options. Click on A, B, C, or D to answer.\n"
        f"Correct answers are shown in green, wrong in red.",
        parse_mode='Markdown'
    )

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    leaderboard = await get_leaderboard(chat_id)
    
    if not leaderboard:
        await update.message.reply_text("📊 No scores yet! Be the first to answer a quiz!")
        return
    
    message = "🏆 **Leaderboard** 🏆\n\n"
    for i, user in enumerate(leaderboard, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        username = user['username'] or "Anonymous"
        message += f"{medal} {username}: {user['correct_answers']} correct ({user['accuracy']}%)\n"
    
    await update.message.reply_text(message, parse_mode='Markdown')

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "Private Chat"
    
    async with db_pool.acquire() as conn:
        total_quizzes = await conn.fetchval("SELECT COUNT(*) FROM quiz_history WHERE chat_id = $1", chat_id)
        total_users = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM user_scores WHERE chat_id = $1", chat_id)
        total_attempts = await conn.fetchval("SELECT COALESCE(SUM(total_attempts), 0) FROM user_scores WHERE chat_id = $1", chat_id)
    
    await update.message.reply_text(
        f"📊 **{chat_title} Statistics**\n\n"
        f"📝 Total quizzes sent: {total_quizzes}\n"
        f"👥 Active participants: {total_users}\n"
        f"🎯 Total answers given: {total_attempts}\n\n"
        f"Use /leaderboard to see top scorers!",
        parse_mode='Markdown'
    )

async def quiz_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    # Parse callback data
    parts = query.data.split('_')
    selected = parts[2] if len(parts) > 2 else None
    
    if not selected:
        return
    
    # Get the question from history
    quiz = await get_last_quiz(chat_id)
    
    if not quiz:
        await query.edit_message_text("Quiz expired! Waiting for next quiz...")
        return
    
    correct = quiz['correct_answer']
    is_correct = (selected == correct)
    options = quiz['options']
    
    # Update score
    await update_score(user.id, chat_id, user.username or user.first_name, is_correct)
    
    # Create result message
    result_text = ""
    for i, opt in enumerate(options):
        letter = ['A', 'B', 'C', 'D'][i]
        if letter == correct:
            result_text += f"✅ {letter}: {opt} (Correct)\n"
        elif letter == selected and not is_correct:
            result_text += f"❌ {letter}: {opt} (Your answer - Wrong)\n"
        else:
            result_text += f"⚪ {letter}: {opt}\n"
    
    await query.edit_message_text(
        f"📊 **Quiz Result**\n\n"
        f"❓ {quiz['question']}\n\n"
        f"{result_text}\n"
        f"{'🎉 Correct!' if is_correct else '😢 Wrong answer!'}\n\n"
        f"Next quiz in 30 minutes!",
        parse_mode='Markdown'
    )
    
    # Remove from active quizzes to allow new one
    if chat_id in active_quizzes:
        del active_quizzes[chat_id]

async def group_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When bot is added to a group"""
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            chat_id = update.effective_chat.id
            chat_title = update.effective_chat.title or "Group"
            await add_group(chat_id, chat_title)
            await update.message.reply_text(
                f"✅ Thanks for adding me to {chat_title}!\n\n"
                f"I'll start sending quizzes every 30 minutes!\n"
                f"Make sure I have admin permissions to send messages."
            )
            # Send first quiz immediately
            await asyncio.create_task(send_quiz_to_chat(chat_id, chat_title, context))
            break

async def group_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """When bot is removed from a group"""
    chat_id = update.effective_chat.id
    await remove_group(chat_id)
    if chat_id in active_quizzes:
        del active_quizzes[chat_id]

# -------------------- Application --------------------
application = Application.builder().token(BOT_TOKEN).updater(None).build()

# Register handlers
application.add_handler(CommandHandler("start", start))
application.add_handler(CommandHandler("help", help_command))
application.add_handler(CommandHandler("leaderboard", leaderboard))
application.add_handler(CommandHandler("stats", stats_command))
application.add_handler(CallbackQueryHandler(quiz_callback, pattern="^quiz_"))
application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add_handler))
application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, group_remove_handler))

# -------------------- Flask Webhook --------------------
@app.route("/webhook", methods=["POST"])
async def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    await application.process_update(update)
    return jsonify({"ok": True})

@app.route("/", methods=["GET"])
def index():
    return "Quiz Bot is running!"

# -------------------- Main Entry --------------------
if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def main():
        await init_db_pool()
        
        await application.initialize()
        await application.start()
        
        # Start the quiz scheduler
        scheduler.add_job(send_quiz_to_all, 'interval', minutes=30)
        scheduler.start()
        
        # Set webhook or polling
        render_url = os.getenv("RENDER_EXTERNAL_URL")
        if render_url:
            webhook_url = f"{render_url}/webhook"
            await application.bot.set_webhook(url=webhook_url)
            logger.info(f"Webhook set to: {webhook_url}")
            
            port = int(os.environ.get("PORT", 5000))
            app.run(host="0.0.0.0", port=port)
        else:
            # Polling for local testing
            await application.updater.start_polling()
            await asyncio.Event().wait()
    
    loop.run_until_complete(main())
