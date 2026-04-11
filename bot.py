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

logging.basicConfig(level=logging.INFO)
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

# -------------------- Database --------------------
async def init_db():
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
    logger.info("Database ready")

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

async def save_quiz(chat_id, question, correct, options):
    return await db_pool.fetchval("""
        INSERT INTO quiz_history (chat_id, question, correct_answer, options)
        VALUES ($1, $2, $3, $4) RETURNING id
    """, chat_id, question, correct, options)

async def already_answered(chat_id, quiz_id, user_id):
    return await db_pool.fetchval(
        "SELECT 1 FROM quiz_answers WHERE chat_id=$1 AND quiz_id=$2 AND user_id=$3",
        chat_id, quiz_id, user_id
    ) is not None

async def mark_answered(chat_id, quiz_id, user_id):
    await db_pool.execute(
        "INSERT INTO quiz_answers (chat_id, quiz_id, user_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        chat_id, quiz_id, user_id
    )

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

async def get_global_stats(user_id):
    return await db_pool.fetchrow(
        """SELECT COALESCE(SUM(correct_answers),0) as correct,
                  COALESCE(SUM(wrong_answers),0) as wrong,
                  COALESCE(SUM(total_attempts),0) as total
           FROM user_scores WHERE user_id = $1""",
        user_id
    )

async def get_global_rank(user_id):
    user_correct = await db_pool.fetchval(
        "SELECT COALESCE(SUM(correct_answers),0) FROM user_scores WHERE user_id = $1",
        user_id
    )
    rank = await db_pool.fetchval(
        """SELECT COUNT(DISTINCT user_id) + 1 FROM user_scores
           WHERE (SELECT COALESCE(SUM(correct_answers),0) FROM user_scores sub
                  WHERE sub.user_id = user_scores.user_id) > $1""",
        user_correct
    )
    return int(rank) if rank else 1

async def get_top_members(chat_id, limit=5):
    return await db_pool.fetch("""
        SELECT first_name, correct_answers, wrong_answers, total_attempts,
               ROUND(correct_answers * 100.0 / NULLIF(total_attempts,0), 1) as acc
        FROM user_scores WHERE chat_id = $1 AND total_attempts > 0
        ORDER BY correct_answers DESC, acc DESC LIMIT $2
    """, chat_id, limit)

async def get_group_global_rank(chat_id):
    rows = await db_pool.fetch("""
        SELECT g.chat_id, COALESCE(SUM(us.correct_answers),0) as total_correct
        FROM groups g LEFT JOIN user_scores us ON g.chat_id = us.chat_id
        WHERE g.is_active = TRUE
        GROUP BY g.chat_id
        ORDER BY total_correct DESC
    """)
    for i, row in enumerate(rows, 1):
        if row['chat_id'] == chat_id:
            return i
    return None

# -------------------- Gemini --------------------
async def generate_quiz():
    categories = ["Brainstorming", "News", "GK", "Riddle", "Science", "Tech", "World News",
                  "Telegram", "History", "Geography", "Sports"]
    cat = random.choice(categories)
    seed = f"{int(time.time())}_{random.randint(1, 999999)}"
    prompt = f"""Generate a very short multiple choice quiz about {cat}. Seed {seed}.
Format exactly:
QUESTION: (max 15 words)
OPTIONS: A) ... | B) ... | C) ... | D) ...
ANSWER: A, B, C or D
Make it fun and concise."""
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=30
                ) as resp:
                    data = await resp.json()
                    logger.info(f"Gemini raw response: {data}")  # See exactly what's returned

                    # Handle blocked/error responses
                    if 'candidates' not in data:
                        reason = data.get('promptFeedback', {}).get('blockReason') or data.get('error', {}).get('message') or str(data)
                        logger.error(f"Gemini no candidates (attempt {attempt+1}): {reason}")
                        await asyncio.sleep(2)
                        continue

                    candidate = data['candidates'][0]

                    # Handle safety-blocked candidates
                    if candidate.get('finishReason') not in ('STOP', None, 'MAX_TOKENS'):
                        logger.error(f"Gemini bad finishReason: {candidate.get('finishReason')}")
                        await asyncio.sleep(2)
                        continue

                    text = candidate['content']['parts'][0]['text']
                    q, opts, ans = "", [], ""
                    for line in text.split('\n'):
                        line = line.strip()
                        if line.startswith("QUESTION:"):
                            q = line.replace("QUESTION:", "").strip()
                        elif line.startswith("OPTIONS:"):
                            opts = [o.strip() for o in line.replace("OPTIONS:", "").strip().split("|")]
                        elif line.startswith("ANSWER:"):
                            ans = line.replace("ANSWER:", "").strip().upper()
                            if ans and ans[0] in 'ABCD':
                                ans = ans[0]  # Take just the letter in case of "A)" or "A."

                    if q and len(opts) == 4 and ans in 'ABCD':
                        return {'question': q, 'options': opts, 'correct_letter': ans, 'correct_index': ord(ans) - 65}
                    else:
                        logger.error(f"Gemini bad parse — q={q!r} opts={opts} ans={ans!r}\nRaw:\n{text}")

        except Exception as e:
            logger.error(f"Gemini exception (attempt {attempt+1}): {e}")
        await asyncio.sleep(2)
    raise Exception("Gemini failed after 3 attempts")

# -------------------- Helpers --------------------
async def delete_later(bot, chat_id, msg_id, delay=30):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass

def format_rank(rank):
    """Medal for top 3, #N for everyone else."""
    if rank == 1: return "🥇"
    if rank == 2: return "🥈"
    if rank == 3: return "🥉"
    return f"#{rank}"

# -------------------- Quiz Sender --------------------
async def send_quiz(chat_id, title, bot):
    if chat_id in active_polls:
        try:
            await bot.delete_message(chat_id, active_polls[chat_id]['msg_id'])
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
        is_anonymous=False,  # FIX: must be False so PollAnswerHandler fires
        explanation=f"Correct: {quiz['correct_letter']} - {quiz['options'][quiz['correct_index']]}",
        open_period=300
    )
    qid = await save_quiz(chat_id, quiz['question'], quiz['correct_letter'], quiz['options'])
    active_polls[chat_id] = {
        'msg_id': sent.message_id,
        'poll_id': sent.poll.id,
        'quiz_id': qid,
        'correct': quiz['correct_index']
    }
    asyncio.create_task(delete_later(bot, chat_id, sent.message_id, 310))
    logger.info(f"Quiz sent to {chat_id}: {quiz['question']}")

async def send_quizzes_to_all():
    logger.info("Scheduler: sending quizzes to all groups")
    groups = await get_active_groups()
    for g in groups:
        try:
            await send_quiz(g['chat_id'], g['chat_title'], application.bot)
        except Exception as e:
            logger.error(f"Failed to send quiz to {g['chat_id']}: {e}")

# -------------------- Poll Answer --------------------
async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pa = update.poll_answer
    uid = pa.user.id
    pid = pa.poll_id
    opt = pa.option_ids

    if not opt:
        # User retracted their answer
        return

    for cid, data in list(active_polls.items()):
        if data['poll_id'] == pid:
            if await already_answered(cid, data['quiz_id'], uid):
                return
            await mark_answered(cid, data['quiz_id'], uid)
            is_correct = (len(opt) == 1 and opt[0] == data['correct'])
            await update_score(
                uid, cid,
                pa.user.username or pa.user.first_name,
                pa.user.first_name,
                is_correct
            )
            logger.info(f"User {uid} answered in {cid}: correct={is_correct}")
            break

# -------------------- Command Handlers --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Start from {update.effective_chat.id} type={update.effective_chat.type}")
    if update.effective_chat.type == "private":
        kb = [
            [InlineKeyboardButton("Add me to a group", url=f"https://t.me/{context.bot.username}?startgroup=true")],
            [InlineKeyboardButton("My Global Stats", callback_data="my_stats")]
        ]
        await update.message.reply_text(
            "Hey! I'm Albert. I send short quizzes in groups.\n\nClick below to add me or check your stats.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    else:
        msg = await update.message.reply_text(
            "Hey! I'll send short quizzes every 5 minutes.\nUse /stats and /leaderboard."
        )
        asyncio.create_task(delete_later(context.bot, update.effective_chat.id, msg.message_id, 30))

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Use /stats inside a group where I am added.")
        return
    cid = update.effective_chat.id
    title = update.effective_chat.title or "This group"
    participants = await db_pool.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM user_scores WHERE chat_id=$1", cid) or 0
    quizzes = await db_pool.fetchval(
        "SELECT COUNT(*) FROM quiz_history WHERE chat_id=$1", cid) or 0
    answers = await db_pool.fetchval(
        "SELECT COALESCE(SUM(total_attempts),0) FROM user_scores WHERE chat_id=$1", cid) or 0
    text = (
        f"{title} Stats\n"
        f"Participants: {participants}\n"
        f"Quizzes sent: {quizzes}\n"
        f"Total answers: {answers}"
    )
    msg = await update.effective_message.reply_text(text)
    asyncio.create_task(delete_later(context.bot, cid, msg.message_id, 30))
    asyncio.create_task(delete_later(context.bot, cid, update.effective_message.message_id, 30))

async def leaderboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("Use /leaderboard inside a group where I am added.")
        return
    cid = update.effective_chat.id
    title = update.effective_chat.title or "This group"

    # This group's global rank only
    group_rank = await get_group_global_rank(cid)
    lines = [f"Leaderboard - {title}"]
    if group_rank:
        lines.append(f"Group global rank: #{group_rank}")
    lines.append("")

    # Top 5 members of this group only
    members = await get_top_members(cid, 5)
    if members:
        lines.append("Top Members")
        for i, m in enumerate(members, 1):
            name = m['first_name'] or "Anonymous"
            lines.append(f"#{i} {name} - {m['correct_answers']} correct / {m['wrong_answers']} wrong")
    else:
        lines.append("No answers yet. Answer a quiz to appear here!")

    out = "\n".join(lines)
    msg = await update.effective_message.reply_text(out)
    asyncio.create_task(delete_later(context.bot, cid, msg.message_id, 60))
    asyncio.create_task(delete_later(context.bot, cid, update.effective_message.message_id, 60))

async def my_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    stats = await get_global_stats(user.id)
    if stats and int(stats['total']) > 0:
        correct = int(stats['correct'])
        wrong = int(stats['wrong'])
        total = int(stats['total'])
        acc = round(correct * 100.0 / total, 1)
        rank = await get_global_rank(user.id)
        rank_display = format_rank(rank)
        text = (
            f"Your Global Stats\n\n"
            f"Name: {user.first_name}\n"
            f"Correct: {correct}\n"
            f"Wrong: {wrong}\n"
            f"Attempts: {total}\n"
            f"Accuracy: {acc}%\n"
            f"Global Rank: {rank_display}"
        )
    else:
        text = (
            f"Your Global Stats\n\n"
            f"Name: {user.first_name}\n"
            f"No quizzes answered yet. Join a group and answer a quiz!"
        )
    await context.bot.send_message(chat_id=user.id, text=text)

async def group_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            cid = update.effective_chat.id
            title = update.effective_chat.title or "Group"
            await add_group(cid, title)
            msg = await update.message.reply_text(
                "I'm Albert! I'll send short quizzes every 5 minutes.\nUse /stats and /leaderboard."
            )
            asyncio.create_task(delete_later(context.bot, cid, msg.message_id, 30))
            await send_quiz(cid, title, context.bot)
            break

async def group_leave(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    await remove_group(cid)
    active_polls.pop(cid, None)

# -------------------- Flask Webhook --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    asyncio.run_coroutine_threadsafe(
        application.process_update(update), main_loop
    ).result(timeout=30)
    return jsonify({"ok": True})

@app.route("/")
def index():
    return "Bot running"

# -------------------- Main --------------------
async def main():
    global application, main_loop, scheduler
    main_loop = asyncio.get_running_loop()
    await init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("leaderboard", leaderboard_cmd))
    application.add_handler(CallbackQueryHandler(my_stats_callback, pattern="my_stats"))
    application.add_handler(PollAnswerHandler(handle_poll_answer))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, group_add))
    application.add_handler(MessageHandler(filters.StatusUpdate.LEFT_CHAT_MEMBER, group_leave))
    await application.initialize()
    await application.start()
    scheduler = AsyncIOScheduler(event_loop=main_loop)
    scheduler.add_job(send_quizzes_to_all, 'interval', minutes=5)
    scheduler.start()
    render_url = os.getenv("RENDER_EXTERNAL_URL")
    if render_url:
        await application.bot.set_webhook(url=f"{render_url}/webhook")
        logger.info("Webhook set")
    else:
        raise Exception("RENDER_EXTERNAL_URL missing")

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), use_reloader=False)

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