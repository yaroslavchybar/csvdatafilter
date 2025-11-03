import os
import sys
import asyncio
import tempfile
from pathlib import Path
from dotenv import load_dotenv

from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

from filter_instagram import filter_csv


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Send me a CSV file; I will return the filtered CSV.")


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

    if not _is_csv_document(update):
        await update.message.reply_text("Please upload a CSV file ('.csv').")
        return

    doc = update.message.document
    await update.message.reply_text("Downloading and processing your file…")

    try:
        tg_file = await doc.get_file()
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            in_path = td_path / (doc.file_name or "input.csv")
            out_path = td_path / f"filtered_{in_path.name}"

            await tg_file.download_to_drive(in_path.as_posix())

            # Run CPU-bound filtering in a worker thread
            await asyncio.to_thread(filter_csv, in_path, out_path)

            with out_path.open("rb") as f:
                await update.message.reply_document(
                    document=InputFile(f, filename=out_path.name)
                )
    except Exception as e:
        await update.message.reply_text(f"Failed to process the file: {e}")


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

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    app.run_polling()


if __name__ == "__main__":
    main()
