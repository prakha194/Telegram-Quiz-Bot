import os
import logging
import asyncio
import random
from flask import Flask, request, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)
import aiohttp
import asyncpg
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

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

app = Flask(__name__)
scheduler = AsyncIOScheduler()
active_quizzes = {}
db_pool = None
application = None
bot_loop = None

# -------------------- Database Functions (asyncpg) --------------------
async def init_db_pool():
    global db_pool
    try:
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
                    first_name TEXT,
                    correct_answers INTEGER DEFAULT 0,
                    wrong_answers INTEGER DEFAULT 0,
                    total_attempts INTEGER DEFAULT 0,
                    PRIMARY KEY (user_id, chat_id)
                )
            """)
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database init error: {e}")
        raise

async def add_group(chat_id, chat_title):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO groups (chat_id, chat_title) 
                VALUES ($1, $2) 
                ON CONFLICT (chat_id) DO UPDATE 
                SET is_active = TRUE, chat_title = EXCLUDED.chat_title
            """, chat_id, chat_title)
    except Exception as e:
        logger.error(f"Add group error: {e}")

async def remove_group(chat_id):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE groups SET is_active = FALSE WHERE chat_id = $1", chat_id)
    except Exception as e:
        logger.error(f"Remove group error: {e}")

async def get_active_groups():
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetch("SELECT chat_id, chat_title FROM groups WHERE is_active = TRUE")
    except Exception as e:
        logger.error(f"Get active groups error: {e}")
        return []

async def save_quiz_history(chat_id, question, correct_answer, options):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO quiz_history (chat_id, question, correct_answer, options)
                VALUES ($1, $2, $3, $4)
            """, chat_id, question, correct_answer, options)
    except Exception as e:
        logger.error(f"Save quiz history error: {e}")

async def update_score(user_id, chat_id, username, first_name, is_correct):
    try:
        async with db_pool.acquire() as conn:
            if is_correct:
                await conn.execute("""
                    INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
                    VALUES ($1, $2, $3, $4, 1, 0, 1)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET correct_answers = user_scores.correct_answers + 1,
                        total_attempts = user_scores.total_attempts + 1,
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name
                """, user_id, chat_id, username, first_name)
            else:
                await conn.execute("""
                    INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
                    VALUES ($1, $2, $3, $4, 0, 1, 1)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET wrong_answers = user_scores.wrong_answers + 1,
                        total_attempts = user_scores.total_attempts + 1,
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name
                """, user_id, chat_id, username, first_name)
    except Exception as e:
        logger.error(f"Update score error: {e}")

async def get_user_stats(user_id, chat_id):
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT correct_answers, wrong_answers, total_attempts
                FROM user_scores
                WHERE user_id = $1 AND chat_id = $2
            """, user_id, chat_id)
    except Exception as e:
        logger.error(f"Get user stats error: {e}")
        return None

async def get_group_stats(chat_id):
    try:
        async with db_pool.acquire() as conn:
            total_participants = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM user_scores WHERE chat_id = $1", chat_id) or 0
            total_quizzes = await conn.fetchval("SELECT COUNT(*) FROM quiz_history WHERE chat_id = $1", chat_id) or 0
            total_answers = await conn.fetchval("SELECT COALESCE(SUM(total_attempts), 0) FROM user_scores WHERE chat_id = $1", chat_id) or 0
            return total_participants, total_quizzes, total_answers
    except Exception as e:
        logger.error(f"Get group stats error: {e}")
        return 0, 0, 0

async def get_leaderboard(chat_id, limit=10):
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetch("""
                SELECT username, first_name, correct_answers, wrong_answers, total_attempts,
                       ROUND(correct_answers * 100.0 / NULLIF(total_attempts, 0), 1) AS accuracy
                FROM user_scores
                WHERE chat_id = $1 AND total_attempts > 0
                ORDER BY correct_answers DESC, accuracy DESC
                LIMIT $2
            """, chat_id, limit)
    except Exception as e:
        logger.error(f"Get leaderboard error: {e}")
        return []

async def get_last_quiz(chat_id):
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT question, correct_answer, options 
                FROM quiz_history 
                WHERE chat_id = $1 
                ORDER BY asked_at DESC 
                LIMIT 1
            """, chat_id)
    except Exception as e:
        logger.error(f"Get last quiz error: {e}")
        return None

# -------------------- Gemini Quiz Generator --------------------
async def generate_quiz():
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
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                json={
                    "contents": [{
                        "parts": [{"text": prompt}]
                    }]
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                text = data['candidates'][0]['content']['parts'][0]['text']
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
                
                if len(options) != 4:
                    options = ["Option A", "Option B", "Option C", "Option D"]
                
                return {
                    'question': question or f"Quiz about {category}",
                    'options': options,
                    'correct': answer if answer in ['A', 'B', 'C', 'D'] else 'A',
                    'category': category
                }
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return {
            'question': "What is the capital of France?",
            'options': ["London", "Berlin", "Paris", "Madrid"],
            'correct': 'C',
            'category': "Geography"
        }

# -------------------- Quiz Sender --------------------
async def send_quiz_to_chat(chat_id, chat_title, bot):
    try:
        if chat_id in active_quizzes:
            try:
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=active_quizzes[chat_id]
                )
            except:
                pass
        
        quiz = await generate_quiz()
        
        keyboard = [
            [
                InlineKeyboardButton("📊 Show Results", callback_data=f"show_results_{chat_id}")
            ],
            [
                InlineKeyboardButton("🔴 A", callback_data=f"quiz_{chat_id}_A"),
                InlineKeyboardButton("🔵 B", callback_data=f"quiz_{chat_id}_B"),
                InlineKeyboardButton("🟢 C", callback_data=f"quiz_{chat_id}_C"),
                InlineKeyboardButton("🟡 D", callback_data=f"quiz_{chat_id}_D")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        options_text = ""
        for i, opt in enumerate(quiz['options']):
            letter = ['A', 'B', 'C', 'D'][i]
            emoji = ["🔴", "🔵", "🟢", "🟡"][i]
            options_text += f"{emoji} {letter}: {opt}\n"
        
        message_text = (
            f"**#Question**\n"
            f"{quiz['question']}\n\n"
            f"*Anonymous Quiz*\n\n"
            f"{options_text}\n"
            f"0% Ans 1 | 0% Ans 2 | 0% Ans 3 | 0% Ans 4"
        )
        
        sent_msg = await bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        active_quizzes[chat_id] = sent_msg.message_id
        await save_quiz_history(chat_id, quiz['question'], quiz['correct'], quiz['options'])
        logger.info(f"Quiz sent to {chat_title} ({chat_id})")
        
    except Exception as e:
        logger.error(f"Failed to send quiz to {chat_id}: {e}")

async def send_quiz_to_all(bot):
    groups = await get_active_groups()
    for group in groups:
        await send_quiz_to_chat(group['chat_id'], group['chat_title'], bot)

# -------------------- Auto-delete Helper --------------------
async def delete_message_after_delay(bot, chat_id, message_id, delay=30):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except:
        pass

# -------------------- Handlers --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    bot_username = context.bot.username
    
    keyboard = [
        [InlineKeyboardButton("➕ Add me to your chat", url=f"https://t.me/{bot_username}?startgroup=true")],
        [InlineKeyboardButton("📊 View your stats", callback_data="view_stats")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"Hey there! My name is **Albert**. I am a multi-language quiz bot that sends random quizzes in groups.\n\n"
        f"Send the /settings command in groups to configure me.\n\n"
        f"*Powered by Sybotik.*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_title = update.effective_chat.title or "This group"
    
    keyboard = [
        [InlineKeyboardButton("📊 View Group Stats", callback_data="group_stats")],
        [InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard")],
        [InlineKeyboardButton("❌ Close", callback_data="close_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"⚙️ **Settings for {chat_title}**\n\n"
        f"Configure how Albert behaves in this chat.\n\n"
        f"Current settings:\n"
        f"• Quiz interval: 30 minutes",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "This group"
    bot = context.bot
    
    total_participants, total_quizzes, total_answers = await get_group_stats(chat_id)
    
    sent_msg = await update.effective_message.reply_text(
        f"📊 **Group Statistics for {chat_title}**\n\n"
        f"👥 Total participants: {total_participants}\n"
        f"📝 Total quizzes sent: {total_quizzes}\n"
        f"🎯 Total answers given: {total_answers}\n\n"
        f"Keep participating to improve your score! 💪",
        parse_mode='Markdown'
    )
    
    if update.effective_chat.type != "private":
        asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
        asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "this group"
    bot = context.bot
    
    stats = await get_user_stats(user.id, chat_id)
    
    if stats:
        correct = stats['correct_answers']
        wrong = stats['wrong_answers']
        total = stats['total_attempts']
        accuracy = round((correct * 100.0 / total), 1) if total > 0 else 0
        
        if accuracy >= 80:
            motivation = "🌟 Excellent! You're a quiz master! Keep shining!"
        elif accuracy >= 60:
            motivation = "🎉 Great job! You're doing really well!"
        elif accuracy >= 40:
            motivation = "👍 Good effort! Practice makes perfect!"
        elif accuracy >= 20:
            motivation = "💪 Keep going! Every quiz makes you smarter!"
        else:
            motivation = "🌱 You're just getting started! Try more quizzes to improve!"
        
        sent_msg = await update.effective_message.reply_text(
            f"📊 **Your Stats in {chat_title}**\n\n"
            f"👤 Name: {user.first_name}\n"
            f"🆔 ID: {user.id}\n\n"
            f"✅ Correct answers: {correct}\n"
            f"❌ Wrong answers: {wrong}\n"
            f"📊 Total attempts: {total}\n"
            f"🎯 Accuracy: {accuracy}%\n\n"
            f"💬 {motivation}\n\n"
            f"Keep participating! You can do better! 💪",
            parse_mode='Markdown'
        )
    else:
        sent_msg = await update.effective_message.reply_text(
            f"📊 **Your Stats in {chat_title}**\n\n"
            f"👤 Name: {user.first_name}\n"
            f"🆔 ID: {user.id}\n\n"
            f"✅ Correct answers: 0\n"
            f"❌ Wrong answers: 0\n"
            f"📊 Total attempts: 0\n"
            f"🎯 Accuracy: 0%\n\n"
            f"🌱 You haven't participated in any quiz yet!\n"
            f"💬 Answer the next quiz to get started! 🚀",
            parse_mode='Markdown'
        )
    
    if update.effective_chat.type != "private":
        asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
        asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    bot = context.bot
    
    leaderboard_data = await get_leaderboard(chat_id)
    
    if not leaderboard_data:
        sent_msg = await update.effective_message.reply_text(
            "🏆 **Leaderboard**\n\n"
            "No scores yet! Be the first to answer a quiz! 🚀",
            parse_mode='Markdown'
        )
    else:
        message = "🏆 **Leaderboard** 🏆\n\n"
        for i, user in enumerate(leaderboard_data, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            name = user['first_name'] or user['username'] or "Anonymous"
            accuracy = user['accuracy'] or 0
            message += f"{medal} {name}: {user['correct_answers']} ✅ / {user['wrong_answers']} ❌ (Accuracy: {accuracy}%)\n"
        
        message += f"\n💬 {random.choice(['Keep going!', 'You can do better!', 'Next time you will win!', 'Practice makes perfect!'])}"
        
        sent_msg = await update.effective_message.reply_text(message, parse_mode='Markdown')
    
    if update.effective_chat.type != "private":
        asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
        asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def quiz_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    chat_id = update.effective_chat.id
    
    # Show results button
    if query.data.startswith("show_results_"):
        quiz = await get_last_quiz(chat_id)
        if quiz:
            options = quiz['options']
            result_text = f"**📊 Quiz Results**\n\n"
            result_text += f"❓ {quiz['question']}\n\n"
            for i, opt in enumerate(options):
                letter = ['A', 'B', 'C', 'D'][i]
                result_text += f"✅ {letter}: {opt}\n"
            result_text += f"\n🔑 Correct answer: {quiz['correct_answer']}"
            await query.edit_message_text(result_text, parse_mode='Markdown')
        else:
            await query.edit_message_text("No quiz results available yet!")
        return
    
    # View stats from private chat
    if query.data == "view_stats":
        await my_stats(update, context)
        return
    
    # Group stats callback
    if query.data == "group_stats":
        await group_stats(update, context)
        return
    
    # Leaderboard callback
    if query.data == "leaderboard":
        await leaderboard(update, context)
        return
    
    # Close settings
    if query.data == "close_settings":
        await query.edit_message_text("Settings closed. Use /settings to reopen.")
        return
    
    # Handle quiz answer
    parts = query.data.split('_')
    if len(parts) >= 3:
        selected = parts[2]
        quiz = await get_last_quiz(chat_id)
        
        if not quiz:
            await query.edit_message_text("Quiz expired! Waiting for next quiz...")
            return
        
        correct = quiz['correct_answer']
        is_correct = (selected == correct)
        options = quiz['options']
        
        await update_score(user.id, chat_id, user.username or user.first_name, user.first_name, is_correct)
        
        result_text = f"**#Question**\n{quiz['question']}\n\n"
        result_text += f"*Anonymous Quiz*\n\n"
        
        for i, opt in enumerate(options):
            letter = ['A', 'B', 'C', 'D'][i]
            if letter == correct:
                result_text += f"✅ {letter}: {opt} (Correct)\n"
            elif letter == selected and not is_correct:
                result_text += f"❌ {letter}: {opt} (Your answer - Wrong)\n"
            else:
                result_text += f"⚪ {letter}: {opt}\n"
        
        result_text += f"\n📊 Results: 25% | 25% | 25% | 25%\n"
        result_text += f"\n{'🎉 Correct answer!' if is_correct else '😢 Wrong answer!'}"
        result_text += f"\n🔑 Correct answer was: {correct}"
        
        await query.edit_message_text(result_text, parse_mode='Markdown')
        
        if chat_id in active_quizzes:
            del active_quizzes[chat_id]

async def group_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            chat_id = update.effective_chat.id
            chat_title = update.effective_chat.title or "Group"
            await add_group(chat_id, chat_title)
            await update.message.reply_text(
                f"✅ Hey everyone! I'm **Albert**! 🎉\n\n"
                f"I'll send random quizzes every 30 minutes!\n\n"
                f"Use /settings to configure me.\n"
                f"Use /stats to see group statistics.\n"
                f"Use /leaderboard to see top scorers.\n\n"
                f"Let's have some fun learning together! 📚",
                parse_mode='Markdown'
            )
            await asyncio.create_task(send_quiz_to_chat(chat_id, chat_title, context.bot))
            break

async def group_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await remove_group(chat_id)
    if chat_id in active_quizzes:
        del active_quizzes[chat_id]

# -------------------- Create Application --------------------
def create_application():
    return Application.builder().token(BOT_TOKEN).updater(None).build()

# -------------------- Flask Webhook (SYNC - no async) --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    global application

    try:
        if application is None:
            print("Application not ready")
            return jsonify({"error": "app not ready"}), 500

        data = request.get_json(force=True)
        print("Incoming update:", data)

        update = Update.de_json(data, application.bot)

        loop = asyncio.get_event_loop()
        loop.create_task(application.process_update(update))

        return jsonify({"ok": True})

    except Exception as e:
        print("Webhook crash:", str(e))
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def index():
    return "Quiz Bot is running!"

# -------------------- Main Entry --------------------
async def main():
    global application, bot_loop
    
    bot_loop = asyncio.get_running_loop()
    
    # Initialize database
    await init_db_pool()
    
    # Create application
    application = create_application()
    
    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(CommandHandler("stats", group_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("mystats", my_stats))
    application.add_handler(CallbackQueryHandler(quiz_callback, pattern="^(show_results_|view_stats|group_stats|leaderboard|close_settings|quiz_)"))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add_handler))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, group_remove_handler))
    
    # Initialize application
    await application.initialize()
    await application.start()
    logger.info("Application initialized and started")
    
    # Start scheduler
    async def scheduled_quiz():
        await send_quiz_to_all(application.bot)
    
    scheduler.add_job(scheduled_quiz, 'interval', minutes=30)
    scheduler.start()
    logger.info("Scheduler started - quizzes every 30 minutes")
    
    # Set webhook
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_url:
        webhook_url = f"{render_url}/webhook"
        result = await application.bot.set_webhook(url=webhook_url)
        if result:
            logger.info(f"Webhook set successfully to: {webhook_url}")
        else:
            logger.error(f"Failed to set webhook to: {webhook_url}")
    else:
        logger.warning("RENDER_EXTERNAL_URL not found, webhook not set")

def start_flask():
    """Start Flask in a separate thread"""
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)

if __name__ == "__main__":
    # Create event loop for async operations
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run main async setup
    loop.run_until_complete(main())
    
    # Start Flask in the main thread (it blocks)
    start_flask()