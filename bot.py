import os
import logging
import asyncio
import random
import threading
import time
from flask import Flask, request, jsonify
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Poll
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    PollAnswerHandler,
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

# Updated Gemini URL to gemini-2.5-flash
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

app = Flask(__name__)
active_polls = {}   # chat_id -> {'poll_message_id': int, 'poll_id': str, 'quiz_id': int, 'correct_option_id': int}
db_pool = None
application = None
main_loop = None
scheduler = None

# -------------------- Database (unchanged, but we store options as text array) --------------------
async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, command_timeout=60)
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
                    correct_answer TEXT,      -- stores the correct option letter (A, B, C, D)
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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS quiz_answers (
                    chat_id BIGINT,
                    quiz_id INTEGER,
                    user_id BIGINT,
                    PRIMARY KEY (chat_id, quiz_id, user_id)
                )
            """)
        logger.info("Database initialized")
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
            quiz_id = await conn.fetchval("""
                INSERT INTO quiz_history (chat_id, question, correct_answer, options)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            """, chat_id, question, correct_answer, options)
            return quiz_id
    except Exception as e:
        logger.error(f"Save quiz history error: {e}")
        return None

async def has_user_answered(chat_id, quiz_id, user_id):
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM quiz_answers WHERE chat_id=$1 AND quiz_id=$2 AND user_id=$3",
                chat_id, quiz_id, user_id
            )
            return row is not None
    except Exception as e:
        logger.error(f"has_user_answered error: {e}")
        return False

async def mark_user_answered(chat_id, quiz_id, user_id):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO quiz_answers (chat_id, quiz_id, user_id)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
            """, chat_id, quiz_id, user_id)
    except Exception as e:
        logger.error(f"mark_user_answered error: {e}")

async def update_score(user_id, chat_id, username, first_name, is_correct):
    try:
        async with db_pool.acquire() as conn:
            if is_correct:
                await conn.execute("""
                    INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
                    VALUES ($1, $2, $3, $4, 1, 0, 1)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET correct_answers = user_scores.correct_answers + 1,
                        total_attempts  = user_scores.total_attempts + 1,
                        username        = EXCLUDED.username,
                        first_name      = EXCLUDED.first_name
                """, user_id, chat_id, username, first_name)
            else:
                await conn.execute("""
                    INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
                    VALUES ($1, $2, $3, $4, 0, 1, 1)
                    ON CONFLICT (user_id, chat_id) DO UPDATE
                    SET wrong_answers = user_scores.wrong_answers + 1,
                        total_attempts = user_scores.total_attempts + 1,
                        username       = EXCLUDED.username,
                        first_name     = EXCLUDED.first_name
                """, user_id, chat_id, username, first_name)
    except Exception as e:
        logger.error(f"Update score error: {e}")

async def get_user_stats(user_id, chat_id):
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT correct_answers, wrong_answers, total_attempts
                FROM user_scores WHERE user_id=$1 AND chat_id=$2
            """, user_id, chat_id)
    except Exception as e:
        logger.error(f"Get user stats error: {e}")
        return None

async def get_group_stats(chat_id):
    try:
        async with db_pool.acquire() as conn:
            total_participants = await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM user_scores WHERE chat_id=$1", chat_id) or 0
            total_quizzes = await conn.fetchval(
                "SELECT COUNT(*) FROM quiz_history WHERE chat_id=$1", chat_id) or 0
            total_answers = await conn.fetchval(
                "SELECT COALESCE(SUM(total_attempts),0) FROM user_scores WHERE chat_id=$1", chat_id) or 0
            return total_participants, total_quizzes, total_answers
    except Exception as e:
        logger.error(f"Get group stats error: {e}")
        return 0, 0, 0

async def get_leaderboard(chat_id, limit=10):
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetch("""
                SELECT username, first_name, correct_answers, wrong_answers, total_attempts,
                       ROUND(correct_answers * 100.0 / NULLIF(total_attempts,0), 1) AS accuracy
                FROM user_scores
                WHERE chat_id=$1 AND total_attempts > 0
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
                SELECT id, question, correct_answer, options
                FROM quiz_history WHERE chat_id=$1
                ORDER BY asked_at DESC LIMIT 1
            """, chat_id)
    except Exception as e:
        logger.error(f"Get last quiz error: {e}")
        return None

# -------------------- Gemini (no hardcoded fallback) --------------------
async def generate_quiz(retries=2):
    categories = [
        "General Knowledge", "Science", "Technology", "World News",
        "Telegram", "Current Affairs", "History", "Geography",
        "Entertainment", "Sports", "Art", "Music", "Movies"
    ]
    category = random.choice(categories)
    # Force uniqueness with timestamp + random salt
    unique_salt = f"{int(time.time())}_{random.randint(1, 999999)}"
    prompt = f"""Generate a multiple choice quiz question about {category}.
This is a unique request (salt: {unique_salt}). Do NOT repeat any question you've asked before.
Format EXACTLY as:
QUESTION: [the question text]
OPTIONS: [option A] | [option B] | [option C] | [option D]
ANSWER: [the correct letter A, B, C, or D]

Make it interesting and educational. Difficulty: medium."""

    for attempt in range(retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    data = await response.json()
                    text = data['candidates'][0]['content']['parts'][0]['text']
                    lines = text.strip().split('\n')
                    question, options, answer = "", [], ""
                    for line in lines:
                        if line.startswith("QUESTION:"):
                            question = line.replace("QUESTION:", "").strip()
                        elif line.startswith("OPTIONS:"):
                            options = [o.strip() for o in line.replace("OPTIONS:", "").strip().split("|")]
                        elif line.startswith("ANSWER:"):
                            answer = line.replace("ANSWER:", "").strip().upper()
                    if question and len(options) == 4 and answer in ['A','B','C','D']:
                        # Convert answer letter to 0-based index for poll correct_option_id
                        correct_index = ord(answer) - ord('A')
                        return {
                            'question': question,
                            'options': options,
                            'correct_letter': answer,
                            'correct_index': correct_index,
                            'category': category
                        }
                    else:
                        logger.warning(f"Gemini returned malformed data, attempt {attempt+1}")
        except Exception as e:
            logger.error(f"Gemini error attempt {attempt+1}: {e}")
        await asyncio.sleep(1)
    # No fallback – raise exception
    raise Exception("Gemini API failed to generate a valid quiz after multiple attempts")

# -------------------- Helpers --------------------
async def delete_message_after_delay(bot, chat_id, message_id, delay=30):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"Failed to delete message {message_id}: {e}")

def escape_md(text: str) -> str:
    special = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in special else c for c in str(text))

# -------------------- Poll Quiz Sender (deletes old poll message) --------------------
async def send_quiz_to_chat(chat_id, chat_title, bot):
    try:
        # Delete previous poll message if exists
        if chat_id in active_polls:
            old_msg_id = active_polls[chat_id]['poll_message_id']
            try:
                await bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
                logger.info(f"Deleted old poll message {old_msg_id} in {chat_id}")
            except Exception as e:
                logger.warning(f"Could not delete old poll message {old_msg_id}: {e}")
            del active_polls[chat_id]

        quiz = await generate_quiz()  # raises exception if fails

        # Send anonymous poll quiz
        sent_poll = await bot.send_poll(
            chat_id=chat_id,
            question=quiz['question'],
            options=quiz['options'],
            type=Poll.QUIZ,
            correct_option_id=quiz['correct_index'],
            is_anonymous=True,
            explanation=f"Correct answer: {quiz['correct_letter']} - {quiz['options'][quiz['correct_index']]}",
            explanation_parse_mode=None,
            open_period=300  # 5 minutes
        )

        quiz_id = await save_quiz_history(chat_id, quiz['question'], quiz['correct_letter'], quiz['options'])

        active_polls[chat_id] = {
            'poll_message_id': sent_poll.message_id,
            'poll_id': sent_poll.poll.id,
            'quiz_id': quiz_id,
            'correct_option_id': quiz['correct_index']
        }
        logger.info(f"Poll quiz sent to {chat_title} ({chat_id}), quiz_id={quiz_id}")

        # Schedule auto-delete of the poll message after 5 minutes + 10 seconds
        async def delete_poll_message():
            await asyncio.sleep(310)
            try:
                await bot.delete_message(chat_id=chat_id, message_id=sent_poll.message_id)
            except:
                pass
        asyncio.create_task(delete_poll_message())

    except Exception as e:
        logger.error(f"Failed to send quiz to {chat_id}: {e}")

async def send_quiz_to_all(bot):
    groups = await get_active_groups()
    logger.info(f"Scheduled quiz: sending to {len(groups)} active groups")
    for group in groups:
        await send_quiz_to_chat(group['chat_id'], group['chat_title'], bot)

# -------------------- Poll Answer Handler --------------------
async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    poll_answer = update.poll_answer
    user_id = poll_answer.user.id
    poll_id = poll_answer.poll_id
    option_ids = poll_answer.option_ids

    # Find which chat this poll belongs to
    chat_id = None
    quiz_id = None
    correct_option_id = None
    for cid, data in active_polls.items():
        if data['poll_id'] == poll_id:
            chat_id = cid
            quiz_id = data['quiz_id']
            correct_option_id = data['correct_option_id']
            break

    if not chat_id:
        logger.warning(f"Poll {poll_id} not found in active polls")
        return

    # Get user details
    user = poll_answer.user
    username = user.username or user.first_name
    first_name = user.first_name

    # Check if already answered
    if await has_user_answered(chat_id, quiz_id, user_id):
        return

    is_correct = (len(option_ids) == 1 and option_ids[0] == correct_option_id)
    await mark_user_answered(chat_id, quiz_id, user_id)
    await update_score(user_id, chat_id, username, first_name, is_correct)

    logger.info(f"User {user_id} answered poll {poll_id}: correct={is_correct}")

    # Optionally send a short confirmation message (auto-deleted)
    if is_correct:
        result_text = f"✅ {first_name} answered correctly!"
    else:
        # Get correct answer letter from DB
        quiz = await get_last_quiz(chat_id)
        correct_letter = quiz['correct_answer'] if quiz else "?"
        result_text = f"❌ {first_name} answered wrong. Correct answer: {correct_letter}"
    try:
        sent_msg = await context.bot.send_message(chat_id=chat_id, text=result_text, parse_mode='Markdown')
        asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 10))
    except:
        pass

# -------------------- Handlers (same as before, but group commands work) --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_type = update.effective_chat.type
    if chat_type == "private":
        bot_username = context.bot.username
        keyboard = [
            [InlineKeyboardButton("➕ Add me to your chat", url=f"https://t.me/{bot_username}?startgroup=true")],
            [InlineKeyboardButton("📊 View your stats", callback_data="view_stats")]
        ]
        await update.message.reply_text(
            "Hey there\\! My name is *Albert*\\. I am a multi\\-language quiz bot that sends random quizzes in groups\\.\n\n"
            "*Powered by Sybotik\\.*",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='MarkdownV2'
        )
    else:
        sent_msg = await update.message.reply_text(
            "Hey\\! I'm *Albert*, your quiz bot\\! 🎉\n\n"
            "I'll send quizzes every 5 minutes using anonymous polls\\!\n\n"
            "Use /stats to see group statistics\\.\n"
            "Use /leaderboard to see top scorers\\.\n"
            "Use /mystats to see your personal stats\\.",
            parse_mode='MarkdownV2'
        )
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, sent_msg.message_id, 30))
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, update.effective_message.message_id, 30))

async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            "ℹ️ *Group stats are only available inside a group.*\n\n"
            "Go to a group where I am added and use /stats there.",
            parse_mode='MarkdownV2'
        )
        return
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "This group"
    total_participants, total_quizzes, total_answers = await get_group_stats(chat_id)
    sent_msg = await update.effective_message.reply_text(
        f"📊 *Group Statistics for {escape_md(chat_title)}*\n\n"
        f"👥 Total participants: {total_participants}\n"
        f"📝 Total quizzes sent: {total_quizzes}\n"
        f"🎯 Total answers given: {total_answers}\n\n"
        f"Keep participating to improve your score\\! 💪",
        parse_mode='MarkdownV2'
    )
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, update.effective_message.message_id, 30))

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            "ℹ️ *Your quiz stats are tied to each group.*\n\n"
            "Go to a group where I am added and use /mystats there.",
            parse_mode='MarkdownV2'
        )
        return
    user = update.effective_user
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or "this group"
    stats = await get_user_stats(user.id, chat_id)
    if stats and stats['total_attempts'] > 0:
        correct = stats['correct_answers']
        wrong = stats['wrong_answers']
        total = stats['total_attempts']
        accuracy = round((correct * 100.0 / total), 1)
        motivation = (
            "🌟 Excellent\\! You're a quiz master\\!" if accuracy >= 80 else
            "🎉 Great job\\! You're doing really well\\!" if accuracy >= 60 else
            "👍 Good effort\\! Practice makes perfect\\!" if accuracy >= 40 else
            "💪 Keep going\\! Every quiz makes you smarter\\!" if accuracy >= 20 else
            "🌱 Keep trying\\! You're improving\\!"
        )
        sent_msg = await update.effective_message.reply_text(
            f"📊 *Your Stats in {escape_md(chat_title)}*\n\n"
            f"👤 Name: {escape_md(user.first_name)}\n"
            f"🆔 ID: `{user.id}`\n\n"
            f"✅ Correct answers: {correct}\n"
            f"❌ Wrong answers: {wrong}\n"
            f"📊 Total attempts: {total}\n"
            f"🎯 Accuracy: {accuracy}%\n\n"
            f"💬 {motivation}",
            parse_mode='MarkdownV2'
        )
    else:
        sent_msg = await update.effective_message.reply_text(
            f"📊 *Your Stats in {escape_md(chat_title)}*\n\n"
            f"👤 Name: {escape_md(user.first_name)}\n"
            f"🆔 ID: `{user.id}`\n\n"
            f"You haven't answered any quiz yet\\!\n"
            f"Answer the next quiz to get started\\! 🚀",
            parse_mode='MarkdownV2'
        )
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, update.effective_message.message_id, 30))

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text(
            "ℹ️ *Leaderboard is only available inside a group.*\n\n"
            "Go to a group where I am added and use /leaderboard there.",
            parse_mode='MarkdownV2'
        )
        return
    chat_id = update.effective_chat.id
    leaderboard_data = await get_leaderboard(chat_id)
    if not leaderboard_data:
        sent_msg = await update.effective_message.reply_text(
            "🏆 *Leaderboard*\n\nNo scores yet\\! Be the first to answer a quiz\\! 🚀",
            parse_mode='MarkdownV2'
        )
    else:
        message = "🏆 *Leaderboard* 🏆\n\n"
        for i, row in enumerate(leaderboard_data, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}\\."
            name = escape_md(row['first_name'] or row['username'] or "Anonymous")
            accuracy = row['accuracy'] or 0
            message += f"{medal} {name}: {row['correct_answers']} ✅ / {row['wrong_answers']} ❌ \\(Accuracy: {accuracy}%\\)\n"
        endings = ['Keep going\\!', 'You can do better\\!', 'Next time you will win\\!', 'Practice makes perfect\\!']
        message += f"\n💬 {random.choice(endings)}"
        sent_msg = await update.effective_message.reply_text(message, parse_mode='MarkdownV2')
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(context.bot, chat_id, update.effective_message.message_id, 30))

# Callback for the "View your stats" button in private chat
async def view_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "ℹ️ *Your quiz stats are tied to each group.*\n\n"
        "Go to a group where I am added and use /mystats there.",
        parse_mode='MarkdownV2'
    )

async def group_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            chat_id = update.effective_chat.id
            chat_title = update.effective_chat.title or "Group"
            await add_group(chat_id, chat_title)
            sent_msg = await update.message.reply_text(
                "✅ Hey everyone\\! I'm *Albert*\\! 🎉\n\n"
                "I'll send random quizzes every 5 minutes using anonymous polls\\!\n\n"
                "Use /stats to see group statistics\\.\n"
                "Use /leaderboard to see top scorers\\.\n"
                "Use /mystats to see your personal stats\\.\n\n"
                "Let's have some fun learning together\\! 📚",
                parse_mode='MarkdownV2'
            )
            asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 30))
            await send_quiz_to_chat(chat_id, chat_title, context.bot)
            break

async def group_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await remove_group(chat_id)
    active_polls.pop(chat_id, None)

# -------------------- Flask Webhook --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    global application, main_loop
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        future = asyncio.run_coroutine_threadsafe(application.process_update(update), main_loop)
        future.result(timeout=30)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/", methods=["GET"])
def index():
    return "Quiz Bot is running!"

# -------------------- Main --------------------
async def main():
    global application, main_loop, scheduler
    main_loop = asyncio.get_running_loop()
    await init_db_pool()

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", group_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("mystats", my_stats))
    application.add_handler(CallbackQueryHandler(view_stats_callback, pattern="view_stats"))
    application.add_handler(PollAnswerHandler(handle_poll_answer))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add_handler))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, group_remove_handler))

    await application.initialize()
    await application.start()
    logger.info("Application started")

    scheduler = AsyncIOScheduler(event_loop=main_loop)

    async def scheduled_quiz():
        logger.info("Scheduler triggered — sending quizzes now")
        await send_quiz_to_all(application.bot)

    scheduler.add_job(scheduled_quiz, 'interval', minutes=5)
    scheduler.start()
    logger.info("Scheduler started — quizzes every 5 minutes")

    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if not render_url:
        raise Exception("RENDER_EXTERNAL_URL not set")
    await application.bot.set_webhook(url=f"{render_url}/webhook")
    logger.info("Webhook set")

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        if scheduler:
            scheduler.shutdown()
        loop.run_until_complete(application.stop())