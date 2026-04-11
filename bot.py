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

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"

app = Flask(__name__)
active_polls = {}
db_pool = None
application = None
main_loop = None
scheduler = None

# -------------------- Database (same as before) --------------------
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
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"DB init error: {e}")
        raise

async def add_group(chat_id, chat_title):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO groups (chat_id, chat_title) VALUES ($1, $2)
            ON CONFLICT (chat_id) DO UPDATE SET is_active = TRUE, chat_title = EXCLUDED.chat_title
        """, chat_id, chat_title)

async def remove_group(chat_id):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE groups SET is_active = FALSE WHERE chat_id = $1", chat_id)

async def get_active_groups():
    return await db_pool.fetch("SELECT chat_id, chat_title FROM groups WHERE is_active = TRUE")

async def save_quiz_history(chat_id, question, correct_answer, options):
    return await db_pool.fetchval("""
        INSERT INTO quiz_history (chat_id, question, correct_answer, options)
        VALUES ($1, $2, $3, $4) RETURNING id
    """, chat_id, question, correct_answer, options)

async def has_user_answered(chat_id, quiz_id, user_id):
    row = await db_pool.fetchrow("SELECT 1 FROM quiz_answers WHERE chat_id=$1 AND quiz_id=$2 AND user_id=$3", chat_id, quiz_id, user_id)
    return row is not None

async def mark_user_answered(chat_id, quiz_id, user_id):
    await db_pool.execute("INSERT INTO quiz_answers (chat_id, quiz_id, user_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING", chat_id, quiz_id, user_id)

async def update_score(user_id, chat_id, username, first_name, is_correct):
    if is_correct:
        await db_pool.execute("""
            INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
            VALUES ($1, $2, $3, $4, 1, 0, 1)
            ON CONFLICT (user_id, chat_id) DO UPDATE
            SET correct_answers = user_scores.correct_answers + 1,
                total_attempts = user_scores.total_attempts + 1,
                username = EXCLUDED.username, first_name = EXCLUDED.first_name
        """, user_id, chat_id, username, first_name)
    else:
        await db_pool.execute("""
            INSERT INTO user_scores (user_id, chat_id, username, first_name, correct_answers, wrong_answers, total_attempts)
            VALUES ($1, $2, $3, $4, 0, 1, 1)
            ON CONFLICT (user_id, chat_id) DO UPDATE
            SET wrong_answers = user_scores.wrong_answers + 1,
                total_attempts = user_scores.total_attempts + 1,
                username = EXCLUDED.username, first_name = EXCLUDED.first_name
        """, user_id, chat_id, username, first_name)

async def get_user_global_stats(user_id):
    return await db_pool.fetchrow("""
        SELECT SUM(correct_answers) AS correct, SUM(wrong_answers) AS wrong, SUM(total_attempts) AS total
        FROM user_scores WHERE user_id = $1
    """, user_id)

async def get_global_user_rank(user_id):
    user_total = await db_pool.fetchval("SELECT COALESCE(SUM(correct_answers),0) FROM user_scores WHERE user_id = $1", user_id)
    rank = await db_pool.fetchval("""
        SELECT COUNT(DISTINCT user_id) + 1 FROM user_scores
        WHERE (SELECT COALESCE(SUM(correct_answers),0) FROM user_scores sub WHERE sub.user_id = user_scores.user_id) > $1
    """, user_total)
    return rank or 1

async def get_group_leaderboard(chat_id, limit=5):
    return await db_pool.fetch("""
        SELECT first_name, correct_answers, wrong_answers, total_attempts,
               ROUND(correct_answers * 100.0 / NULLIF(total_attempts,0), 1) AS accuracy
        FROM user_scores WHERE chat_id=$1 AND total_attempts > 0
        ORDER BY correct_answers DESC, accuracy DESC LIMIT $2
    """, chat_id, limit)

async def get_global_group_rankings():
    return await db_pool.fetch("""
        SELECT g.chat_id, g.chat_title, COALESCE(SUM(us.correct_answers),0) AS total_correct
        FROM groups g LEFT JOIN user_scores us ON g.chat_id = us.chat_id
        WHERE g.is_active = TRUE GROUP BY g.chat_id, g.chat_title ORDER BY total_correct DESC
    """)

async def get_last_quiz(chat_id):
    return await db_pool.fetchrow("SELECT id, question, correct_answer, options FROM quiz_history WHERE chat_id=$1 ORDER BY asked_at DESC LIMIT 1", chat_id)

# -------------------- Gemini (short quizzes) --------------------
async def generate_quiz():
    categories = ["Brainstorming", "News", "General Knowledge", "Riddle", "Science", "Technology", "World News", "Telegram", "Current Affairs", "History", "Geography", "Entertainment", "Sports"]
    category = random.choice(categories)
    unique_salt = f"{int(time.time())}_{random.randint(1,999999)}"
    prompt = f"""Generate a very short multiple choice quiz question about {category}.
Unique id: {unique_salt}. Do NOT repeat previous questions.
Format EXACTLY:
QUESTION: (short, max 15 words)
OPTIONS: A) ... | B) ... | C) ... | D) ...
ANSWER: A, B, C or D
Make it interesting, fun, and concise."""

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{GEMINI_URL}?key={GEMINI_API_KEY}", json={"contents":[{"parts":[{"text":prompt}]}]}, timeout=30) as resp:
                    data = await resp.json()
                    text = data['candidates'][0]['content']['parts'][0]['text']
                    lines = text.strip().split('\n')
                    q, opts, ans = "", [], ""
                    for line in lines:
                        if line.startswith("QUESTION:"):
                            q = line.replace("QUESTION:","").strip()
                        elif line.startswith("OPTIONS:"):
                            opts = [o.strip() for o in line.replace("OPTIONS:","").strip().split("|")]
                        elif line.startswith("ANSWER:"):
                            ans = line.replace("ANSWER:","").strip().upper()
                    if q and len(opts)==4 and ans in 'ABCD':
                        return {'question': q, 'options': opts, 'correct_letter': ans, 'correct_index': ord(ans)-ord('A')}
        except Exception as e:
            logger.error(f"Gemini attempt {attempt+1}: {e}")
        await asyncio.sleep(1)
    raise Exception("Gemini failed")

# -------------------- Helpers --------------------
async def delete_msg(bot, chat_id, msg_id, delay=30):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass

def escape_md(s):
    for c in r'_*[]()~`>#+-=|{}.!':
        s = s.replace(c, f'\\{c}')
    return s

# -------------------- Quiz Sender --------------------
async def send_quiz_to_chat(chat_id, chat_title, bot):
    if chat_id in active_polls:
        try:
            await bot.delete_message(chat_id, active_polls[chat_id]['poll_message_id'])
        except:
            pass
        del active_polls[chat_id]
    quiz = await generate_quiz()
    sent = await bot.send_poll(
        chat_id=chat_id,
        question=quiz['question'],
        options=quiz['options'],
        type=Poll.QUIZ,
        correct_option_id=quiz['correct_index'],
        is_anonymous=True,
        explanation=f"Correct: {quiz['correct_letter']} - {quiz['options'][quiz['correct_index']]}",
        open_period=300
    )
    quiz_id = await save_quiz_history(chat_id, quiz['question'], quiz['correct_letter'], quiz['options'])
    active_polls[chat_id] = {'poll_message_id': sent.message_id, 'poll_id': sent.poll.id, 'quiz_id': quiz_id, 'correct_option_id': quiz['correct_index']}
    asyncio.create_task(delete_msg(bot, chat_id, sent.message_id, 310))

async def send_quiz_to_all():
    for g in await get_active_groups():
        await send_quiz_to_chat(g['chat_id'], g['chat_title'], application.bot)

# -------------------- Poll Answer Handler --------------------
async def handle_poll_answer(update, context):
    pa = update.poll_answer
    user_id = pa.user.id
    poll_id = pa.poll_id
    opt_ids = pa.option_ids
    for cid, data in active_polls.items():
        if data['poll_id'] == poll_id:
            chat_id, quiz_id, correct_id = cid, data['quiz_id'], data['correct_option_id']
            if await has_user_answered(chat_id, quiz_id, user_id):
                return
            await mark_user_answered(chat_id, quiz_id, user_id)
            is_correct = (len(opt_ids)==1 and opt_ids[0]==correct_id)
            await update_score(user_id, chat_id, pa.user.username or pa.user.first_name, pa.user.first_name, is_correct)
            break

# -------------------- Handlers --------------------
async def start(update, context):
    if update.effective_chat.type == "private":
        kb = [[InlineKeyboardButton("➕ Add me to a group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
              [InlineKeyboardButton("📊 My Global Stats", callback_data="view_stats")]]
        await update.message.reply_text("Hey! I'm *Albert*. I send short quizzes in groups.\n\nClick below to add me or see your stats.", reply_markup=InlineKeyboardMarkup(kb), parse_mode='MarkdownV2')
    else:
        msg = await update.message.reply_text("Hey! I'll send short quizzes every 5 minutes.\nUse /stats and /leaderboard.", parse_mode='MarkdownV2')
        asyncio.create_task(delete_msg(context.bot, update.effective_chat.id, msg.message_id, 30))

async def group_stats(update, context):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Use /stats inside a group where I am added.")
        return
    chat_id = update.effective_chat.id
    title = update.effective_chat.title or "This group"
    participants = await db_pool.fetchval("SELECT COUNT(DISTINCT user_id) FROM user_scores WHERE chat_id=$1", chat_id) or 0
    quizzes = await db_pool.fetchval("SELECT COUNT(*) FROM quiz_history WHERE chat_id=$1", chat_id) or 0
    answers = await db_pool.fetchval("SELECT COALESCE(SUM(total_attempts),0) FROM user_scores WHERE chat_id=$1", chat_id) or 0
    msg = await update.effective_message.reply_text(f"📊 *{escape_md(title)}* Stats\n👥 Participants: {participants}\n📝 Quizzes: {quizzes}\n🎯 Answers: {answers}", parse_mode='MarkdownV2')
    asyncio.create_task(delete_msg(context.bot, chat_id, msg.message_id, 30))
    asyncio.create_task(delete_msg(context.bot, chat_id, update.effective_message.message_id, 30))

async def leaderboard(update, context):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Use /leaderboard inside a group where I am added.")
        return
    chat_id = update.effective_chat.id
    title = update.effective_chat.title or "This group"

    # Global group ranks
    global_ranks = await get_global_group_rankings()
    out = "🌍 *Global Group Ranks*\n"
    my_rank = None
    for i, row in enumerate(global_ranks, 1):
        gtitle = row['chat_title'] or f"Group {row['chat_id']}"
        medal = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}."
        out += f"{medal} {escape_md(gtitle)}: {row['total_correct']} ✅\n"
        if row['chat_id'] == chat_id:
            my_rank = i
    if my_rank:
        out += f"\n📌 *{escape_md(title)}* is #{my_rank} globally.\n"

    # Top 5 members in this group
    members = await get_group_leaderboard(chat_id, 5)
    if members:
        out += "\n👥 *Top 5 Members*\n"
        for i, m in enumerate(members, 1):
            medal = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else f"{i}."
            name = escape_md(m['first_name'] or "Anonymous")
            acc = m['accuracy'] or 0
            out += f"{medal} {name}: {m['correct_answers']}✅ / {m['wrong_answers']}❌ ({acc}%)\n"
    else:
        out += "\n👥 No members have answered yet."

    msg = await update.effective_message.reply_text(out, parse_mode='MarkdownV2')
    asyncio.create_task(delete_msg(context.bot, chat_id, msg.message_id, 60))
    asyncio.create_task(delete_msg(context.bot, chat_id, update.effective_message.message_id, 60))

async def view_stats_callback(update, context):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    stats = await get_user_global_stats(user.id)
    if stats and stats['total'] and stats['total'] > 0:
        correct = stats['correct']
        wrong = stats['wrong']
        total = stats['total']
        acc = round(correct * 100.0 / total, 1)
        rank = await get_global_user_rank(user.id)
        medal = "🥇" if rank==1 else "🥈" if rank==2 else "🥉" if rank==3 else f"#{rank}"
        text = f"📊 *Your Global Stats*\n\n👤 {escape_md(user.first_name)}\n✅ Correct: {correct}\n❌ Wrong: {wrong}\n📊 Attempts: {total}\n🎯 Accuracy: {acc}%\n🏆 Global Rank: {medal}"
    else:
        text = f"📊 *Your Global Stats*\n\n👤 {escape_md(user.first_name)}\nNo quizzes answered yet. Join a group where I am added and answer!"
    await query.message.reply_text(text, parse_mode='MarkdownV2')

async def group_add(update, context):
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            cid = update.effective_chat.id
            title = update.effective_chat.title or "Group"
            await add_group(cid, title)
            msg = await update.message.reply_text("✅ I'm *Albert*! I'll send short quizzes every 5 minutes.\nUse /stats and /leaderboard.", parse_mode='MarkdownV2')
            asyncio.create_task(delete_msg(context.bot, cid, msg.message_id, 30))
            await send_quiz_to_chat(cid, title, context.bot)
            break

async def group_remove(update, context):
    cid = update.effective_chat.id
    await remove_group(cid)
    active_polls.pop(cid, None)

# -------------------- Flask Webhook --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    global application, main_loop
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        asyncio.run_coroutine_threadsafe(application.process_update(update), main_loop).result(timeout=30)
        return jsonify({"ok": True})
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/")
def index():
    return "Quiz Bot running"

# -------------------- Main --------------------
async def main():
    global application, main_loop, scheduler
    main_loop = asyncio.get_running_loop()
    await init_db_pool()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", group_stats))
    application.add_handler(CommandHandler("leaderboard", leaderboard))
    application.add_handler(CallbackQueryHandler(view_stats_callback, pattern="view_stats"))
    application.add_handler(PollAnswerHandler(handle_poll_answer))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, group_remove))
    await application.initialize()
    await application.start()
    scheduler = AsyncIOScheduler(event_loop=main_loop)
    scheduler.add_job(send_quiz_to_all, 'interval', minutes=5)
    scheduler.start()
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_url:
        await application.bot.set_webhook(url=f"{render_url}/webhook")
        logger.info("Webhook set")
    else:
        raise Exception("RENDER_EXTERNAL_URL missing")

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",5000)), use_reloader=False)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        if scheduler:
            scheduler.shutdown()
        loop.run_until_complete(application.stop())