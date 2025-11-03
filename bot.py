import os
import sys
import re
import hmac
import asyncio
import tempfile
from pathlib import Path
from uuid import uuid4
from dotenv import load_dotenv

from telegram import Update, InputFile, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler

from filter_instagram import filter_csv
from uploader import upload_to_supabase
from auth_db import ensure_schema, get_permission, set_permission


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    require_auth = context.application.bot_data.get("REQUIRE_AUTH", False)
    db_path: Path = context.application.bot_data.get("AUTH_DB_PATH")
    user_id = update.effective_user.id if update.effective_user else None

    if not require_auth or not user_id:
        await update.message.reply_text("Send me a CSV file; I will return the filtered CSV.")
        return

    allowed = await asyncio.to_thread(get_permission, db_path, int(user_id))
    if allowed and allowed[0] == 1:
        await update.message.reply_text("You are authorized. Send me a CSV file; I will return the filtered CSV.")
    else:
        context.user_data["await_password"] = True
        await update.message.reply_text("Please send the access password to continue.")


def _is_csv_document(update: Update) -> bool:
    doc = update.message.document
    if not doc:
        return False
    name = (doc.file_name or "").lower()
    mime = (doc.mime_type or "").lower()
    return name.endswith(".csv") or mime == "text/csv"


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return

    # Authorization gate
    require_auth = context.application.bot_data.get("REQUIRE_AUTH", False)
    if require_auth:
        db_path: Path = context.application.bot_data.get("AUTH_DB_PATH")
        user_id = update.effective_user.id if update.effective_user else None
        if not user_id:
            await update.message.reply_text("Cannot verify user. Please try /start.")
            return
        allowed = await asyncio.to_thread(get_permission, db_path, int(user_id))
        if not (allowed and allowed[0] == 1):
            await update.message.reply_text("You are not authorized. Run /start and send your email to request access.")
            return

    if not _is_csv_document(update):
        await update.message.reply_text("Please upload a CSV file ('.csv').")
        return

    doc = update.message.document
    await update.message.reply_text("File received. Preparing confirmation…")

    try:
        tg_file = await doc.get_file()
        uploads_dir: Path = context.application.bot_data.get("UPLOADS_DIR")
        uploads_dir.mkdir(parents=True, exist_ok=True)
        staged_path = uploads_dir / f"{uuid4()}_{doc.file_name or 'input.csv'}"
        await tg_file.download_to_drive(staged_path.as_posix())

        # remember pending file for this user
        context.user_data["pending_file"] = str(staged_path)

        # Ask for confirmation
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Process", callback_data="process"),
             InlineKeyboardButton("Cancel", callback_data="cancel")]
        ])
        await update.message.reply_text(
            f"Ready to process: {doc.file_name}. Proceed?", reply_markup=kb
        )
    except Exception as e:
        await update.message.reply_text(f"Failed to process the file: {e}")


async def confirm_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    action = query.data
    path_str = context.user_data.get("pending_file")
    if not path_str:
        await query.edit_message_text("No pending file to process.")
        return

    staged_path = Path(path_str)

    if action == "cancel":
        try:
            if staged_path.exists():
                staged_path.unlink()
        finally:
            context.user_data.pop("pending_file", None)
        await query.edit_message_text("Cancelled. Upload a CSV to try again.")
        return

    if action != "process":
        await query.edit_message_text("Unknown action.")
        return

    await query.edit_message_text("Processing your file…")

    try:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            out_path = td_path / f"filtered_{staged_path.name}"

            # Run CPU-bound filtering in a worker thread
            stats = await asyncio.to_thread(filter_csv, staged_path, out_path)

            # Optional: upload to Supabase if enabled via env vars
            try:
                uploaded = await asyncio.to_thread(upload_to_supabase, out_path)
                if uploaded:
                    await query.message.reply_text(f"Uploaded {uploaded} rows to Supabase.")
            except Exception as e:
                await query.message.reply_text(f"Supabase upload skipped/failed: {e}")

            # Send stats summary
            if isinstance(stats, dict):
                summary = (
                    "✅ Filtering complete.\n"
                    f"   Total profiles processed: {stats.get('total_processed', 0)}\n"
                    f"   Profiles removed: {stats.get('removed', 0)}\n"
                    f"   Remaining profiles: {stats.get('remaining', 0)}\n"
                    "   Filtered CSV attached."
                )
                await query.message.reply_text(summary)

            # Send file
            with out_path.open("rb") as f:
                await query.message.reply_document(document=InputFile(f, filename=out_path.name))

    except Exception as e:
        await query.message.reply_text(f"Failed to process the file: {e}")
    finally:
        # cleanup staged file and state
        try:
            if staged_path.exists():
                staged_path.unlink()
        finally:
            context.user_data.pop("pending_file", None)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Capture email if we're awaiting it
    if not update.message or not update.message.text:
        return
    if not context.user_data.get("await_password"):
        return

    supplied = update.message.text.strip()
    password = os.getenv("UPLOAD_PASSWORD", "")
    if not password:
        await update.message.reply_text("Server not configured: missing UPLOAD_PASSWORD. Please contact admin.")
        return

    if hmac.compare_digest(supplied, password):
        db_path: Path = context.application.bot_data.get("AUTH_DB_PATH")
        user_id = update.effective_user.id if update.effective_user else None
        if user_id:
            # store placeholder label instead of the password
            await asyncio.to_thread(set_permission, db_path, int(user_id), "password_auth", 1)
        context.user_data.pop("await_password", None)
        await update.message.reply_text("Access granted. You can now upload CSV files.")
    else:
        await update.message.reply_text("Incorrect password. Please try again.")


def main() -> None:
    # Load variables from a local .env file if present
    # This allows setting BOT_TOKEN in C:\Users\yaros\OneDrive\Документы\class\.env
    load_dotenv(dotenv_path=Path(__file__).with_name('.env'))

    token = os.environ.get("BOT_TOKEN")
    if not token:
        print("Error: BOT_TOKEN environment variable is not set. Set it and rerun.\n"
              "PowerShell example:  $env:BOT_TOKEN = '123:ABC'", file=sys.stderr)
        sys.exit(1)

    app = ApplicationBuilder().token(token).build()

    # Auth config
    require_auth = os.getenv("REQUIRE_AUTH", "false").lower() == "true"
    auth_db_path = Path(os.getenv("AUTH_DB_PATH", "auth.db")).resolve()
    if require_auth:
        ensure_schema(auth_db_path)
    app.bot_data["REQUIRE_AUTH"] = require_auth
    app.bot_data["AUTH_DB_PATH"] = auth_db_path

    # Uploads staging directory
    uploads_dir = Path(os.getenv("UPLOADS_DIR", "uploads")).resolve()
    uploads_dir.mkdir(parents=True, exist_ok=True)
    app.bot_data["UPLOADS_DIR"] = uploads_dir

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(confirm_process, pattern=r"^(process|cancel)$"))

    try:
        app.run_polling()
    except (KeyboardInterrupt, SystemExit):
        # Graceful shutdown without verbose traceback on Ctrl+C
        print("Bot stopped by user.")


if __name__ == "__main__":
    main()
