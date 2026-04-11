import os
import logging
import asyncio
import random
import threading
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
active_quizzes = {}   # chat_id -> { message_id, quiz_id }
db_pool = None
application = None
main_loop = None
scheduler = None      # created after loop is ready

# -------------------- Database --------------------
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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS quiz_answers (
                    chat_id BIGINT,
                    quiz_id INTEGER,
                    user_id BIGINT,
                    PRIMARY KEY (chat_id, quiz_id, user_id)
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

# -------------------- Gemini --------------------
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

Make it interesting and educational. Difficulty: medium."""

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

# -------------------- Helpers --------------------
async def delete_message_after_delay(bot, chat_id, message_id, delay=30):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"Failed to delete message {message_id}: {e}")

def escape_md(text: str) -> str:
    """Escape special characters for MarkdownV2."""
    special = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{c}' if c in special else c for c in str(text))

# -------------------- Quiz Sender --------------------
async def send_quiz_to_chat(chat_id, chat_title, bot):
    try:
        # Delete previous quiz — quiz stays in chat until new one arrives
        if chat_id in active_quizzes:
            try:
                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=active_quizzes[chat_id]['message_id']
                )
                logger.info(f"Deleted old quiz in {chat_id}")
            except Exception:
                pass
            del active_quizzes[chat_id]

        quiz = await generate_quiz()

        keyboard = [
            [InlineKeyboardButton("📊 Show Results", callback_data=f"show_results_{chat_id}")],
            [
                InlineKeyboardButton("🔴 A", callback_data=f"quiz_{chat_id}_A"),
                InlineKeyboardButton("🔵 B", callback_data=f"quiz_{chat_id}_B"),
                InlineKeyboardButton("🟢 C", callback_data=f"quiz_{chat_id}_C"),
                InlineKeyboardButton("🟡 D", callback_data=f"quiz_{chat_id}_D"),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        options_text = ""
        for i, opt in enumerate(quiz['options']):
            letter = ['A', 'B', 'C', 'D'][i]
            emoji  = ["🔴", "🔵", "🟢", "🟡"][i]
            options_text += f"{emoji} {letter}: {escape_md(opt)}\n"

        message_text = (
            f"*\\#Question*\n"
            f"{escape_md(quiz['question'])}\n\n"
            f"_Anonymous Quiz_\n\n"
            f"{options_text}"
        )

        sent_msg = await bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='MarkdownV2'
        )

        quiz_id = await save_quiz_history(chat_id, quiz['question'], quiz['correct'], quiz['options'])
        # Quiz stays until next one — no auto-delete timer
        active_quizzes[chat_id] = {
            'message_id': sent_msg.message_id,
            'quiz_id': quiz_id
        }
        logger.info(f"Quiz sent to {chat_title} ({chat_id}), quiz_id={quiz_id}")

    except Exception as e:
        logger.error(f"Failed to send quiz to {chat_id}: {e}")

async def send_quiz_to_all(bot):
    groups = await get_active_groups()
    logger.info(f"Scheduled quiz: sending to {len(groups)} active groups")
    for group in groups:
        await send_quiz_to_chat(group['chat_id'], group['chat_title'], bot)

# -------------------- Handlers --------------------
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
            "I'll send quizzes every 5 minutes\\!\n\n"
            "Use /stats to see group statistics\\.\n"
            "Use /leaderboard to see top scorers\\.\n"
            "Use /mystats to see your personal stats\\.",
            parse_mode='MarkdownV2'
        )
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, sent_msg.message_id, 30))
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, update.effective_message.message_id, 30))

async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id   = update.effective_chat.id
    chat_type = update.effective_chat.type
    bot       = context.bot

    # In private chat: redirect to group
    if chat_type == "private":
        await update.effective_message.reply_text(
            "ℹ️ *Group stats are only available inside a group\\.*\n\n"
            "Go to a group where I am added and use /stats there\\.",
            parse_mode='MarkdownV2'
        )
        return

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
    asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def my_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    chat_id   = update.effective_chat.id
    chat_type = update.effective_chat.type
    bot       = context.bot

    # In private chat: redirect to group
    if chat_type == "private":
        await update.effective_message.reply_text(
            "ℹ️ *Your quiz stats are tied to each group\\.*\n\n"
            "Go to a group where I am added and use /mystats there to see your stats\\.",
            parse_mode='MarkdownV2'
        )
        return

    chat_title = update.effective_chat.title or "this group"
    stats = await get_user_stats(user.id, chat_id)

    if stats and stats['total_attempts'] > 0:
        correct  = stats['correct_answers']
        wrong    = stats['wrong_answers']
        total    = stats['total_attempts']
        accuracy = round((correct * 100.0 / total), 1)

        if accuracy >= 80:
            motivation = "🌟 Excellent\\! You're a quiz master\\!"
        elif accuracy >= 60:
            motivation = "🎉 Great job\\! You're doing really well\\!"
        elif accuracy >= 40:
            motivation = "👍 Good effort\\! Practice makes perfect\\!"
        elif accuracy >= 20:
            motivation = "💪 Keep going\\! Every quiz makes you smarter\\!"
        else:
            motivation = "🌱 Keep trying\\! You're improving\\!"

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

    asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id   = update.effective_chat.id
    chat_type = update.effective_chat.type
    bot       = context.bot

    if chat_type == "private":
        await update.effective_message.reply_text(
            "ℹ️ *Leaderboard is only available inside a group\\.*\n\n"
            "Go to a group where I am added and use /leaderboard there\\.",
            parse_mode='MarkdownV2'
        )
        return

    leaderboard_data = await get_leaderboard(chat_id)

    if not leaderboard_data:
        sent_msg = await update.effective_message.reply_text(
            "🏆 *Leaderboard*\n\nNo scores yet\\! Be the first to answer a quiz\\! 🚀",
            parse_mode='MarkdownV2'
        )
    else:
        message = "🏆 *Leaderboard* 🏆\n\n"
        for i, row in enumerate(leaderboard_data, 1):
            medal    = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}\\."
            name     = escape_md(row['first_name'] or row['username'] or "Anonymous")
            accuracy = row['accuracy'] or 0
            message += f"{medal} {name}: {row['correct_answers']} ✅ / {row['wrong_answers']} ❌ \\(Accuracy: {accuracy}%\\)\n"

        endings = ['Keep going\\!', 'You can do better\\!', 'Next time you will win\\!', 'Practice makes perfect\\!']
        message += f"\n💬 {random.choice(endings)}"
        sent_msg = await update.effective_message.reply_text(message, parse_mode='MarkdownV2')

    asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 30))
    asyncio.create_task(delete_message_after_delay(bot, chat_id, update.effective_message.message_id, 30))

async def quiz_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user    = update.effective_user
    chat_id = update.effective_chat.id
    bot     = context.bot

    await query.answer()

    # ---------- Show Results ----------
    if query.data.startswith("show_results_"):
        quiz = await get_last_quiz(chat_id)
        if quiz:
            options     = quiz['options']
            result_text = f"*📊 Quiz Results*\n\n❓ {escape_md(quiz['question'])}\n\n"
            for i, opt in enumerate(options):
                letter = ['A', 'B', 'C', 'D'][i]
                mark   = "✅" if letter == quiz['correct_answer'] else "⚪"
                result_text += f"{mark} {letter}: {escape_md(opt)}\n"
            result_text += f"\n🔑 Correct answer: *{quiz['correct_answer']}*"
            try:
                await query.edit_message_text(result_text, parse_mode='MarkdownV2')
            except Exception as e:
                logger.warning(f"show_results edit error: {e}")
        else:
            try:
                await query.edit_message_text("No quiz results available yet\\!", parse_mode='MarkdownV2')
            except Exception:
                pass
        return

    # ---------- Private-chat "View your stats" button ----------
    if query.data == "view_stats":
        await query.message.reply_text(
            "ℹ️ *Your quiz stats are tied to each group\\.*\n\n"
            "Go to a group where I am added and use /mystats there to see your stats\\.",
            parse_mode='MarkdownV2'
        )
        return

    if query.data == "group_stats":
        await group_stats(update, context)
        return

    if query.data == "leaderboard":
        await leaderboard(update, context)
        return

    # ---------- Quiz Answer: quiz_{chat_id}_{letter} ----------
    parts = query.data.split('_')
    if len(parts) >= 3 and parts[0] == "quiz":
        selected = parts[2]
        quiz     = await get_last_quiz(chat_id)

        if not quiz:
            await query.answer("Quiz expired! Waiting for next quiz...", show_alert=True)
            return

        quiz_id = quiz['id']

        if await has_user_answered(chat_id, quiz_id, user.id):
            await query.answer("You already answered this quiz!", show_alert=True)
            return

        await mark_user_answered(chat_id, quiz_id, user.id)

        correct    = quiz['correct_answer']
        is_correct = (selected == correct)

        await update_score(
            user.id, chat_id,
            user.username or user.first_name,
            user.first_name,
            is_correct
        )

        result_line = "🎉 Correct answer!" if is_correct else f"😢 Wrong! Correct was: {correct}"
        await query.answer(result_line, show_alert=True)

        verb = "✅ Correct\\!" if is_correct else f"❌ Wrong\\! Correct: *{correct}*"
        try:
            sent_msg = await bot.send_message(
                chat_id=chat_id,
                text=f"👤 *{escape_md(user.first_name)}* answered *{selected}* — {verb}",
                parse_mode='MarkdownV2'
            )
            asyncio.create_task(delete_message_after_delay(bot, chat_id, sent_msg.message_id, 15))
        except Exception as e:
            logger.warning(f"Could not send answer result: {e}")

async def group_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            chat_id    = update.effective_chat.id
            chat_title = update.effective_chat.title or "Group"
            await add_group(chat_id, chat_title)
            sent_msg = await update.message.reply_text(
                "✅ Hey everyone\\! I'm *Albert*\\! 🎉\n\n"
                "I'll send random quizzes every 5 minutes\\!\n\n"
                "Use /stats to see group statistics\\.\n"
                "Use /leaderboard to see top scorers\\.\n\n"
                "Let's have some fun learning together\\! 📚",
                parse_mode='MarkdownV2'
            )
            asyncio.create_task(delete_message_after_delay(context.bot, chat_id, sent_msg.message_id, 30))
            asyncio.create_task(send_quiz_to_chat(chat_id, chat_title, context.bot))
            break

async def group_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await remove_group(chat_id)
    active_quizzes.pop(chat_id, None)

# -------------------- Flask Webhook --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    global application, main_loop
    try:
        data   = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        future = asyncio.run_coroutine_threadsafe(
            application.process_update(update),
            main_loop
        )
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

    # Capture running loop FIRST so Flask thread can use it
    main_loop = asyncio.get_running_loop()

    await init_db_pool()

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start",       start))
    application.add_handler(CommandHandler("stats",       group_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CommandHandler("mystats",     my_stats))
    application.add_handler(CallbackQueryHandler(
        quiz_callback,
        pattern="^(show_results_|view_stats|group_stats|leaderboard|quiz_)"
    ))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add_handler))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER,  group_remove_handler))

    await application.initialize()
    await application.start()
    logger.info("Application started")

    # Create scheduler AFTER the loop is running, passing the loop explicitly
    scheduler = AsyncIOScheduler(event_loop=main_loop)

    async def scheduled_quiz():
        logger.info("Scheduler triggered — sending quizzes now")
        await send_quiz_to_all(application.bot)

    scheduler.add_job(scheduled_quiz, 'interval', minutes=5, id='quiz_job')
    scheduler.start()
    logger.info("Scheduler started — quizzes every 5 minutes")

    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if not render_url:
        raise Exception("RENDER_EXTERNAL_URL not set")

    webhook_url = f"{render_url}/webhook"
    await application.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook set to: {webhook_url}")

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