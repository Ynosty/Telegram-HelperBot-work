import asyncio
import logging
from aiogram import Bot, Dispatcher, exceptions

from app.heandlers import router
import schedule
import db
bot = Bot(token="")
dp = Dispatcher()
chat_id = ""

async def send_messages_periodically():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)

async def send_message_to_chat(chat_id, text):
        max_message_length = 4096
        delay_between_messages = 12 
        for i in range(0, len(text), max_message_length):
            part = text[i:i + max_message_length]

            part = f"*{part}* "
            try:
                await bot.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
                await asyncio.sleep(delay_between_messages)
            except Exception as e:
                print(e)

async def job():
    try:
        # Create an instance of the parser_db class
        db_handler = db.parser_db()

        # Get unsent data from 'rabota' and 'djini' tables (or any table as needed)
        tables = ["rabota", "djini"]
        for table_name in tables:
            data = db_handler.get_unsent_data(table_name)
            for row in data:
                if table_name == "rabota":
                    rabota_url = f"https://robota.ua/company719438/vacancy{row[0]}?ref=search&cre=search_new&pos=dkp_search_new"
                    message_text = f"{row[1]}:\n[Link]({rabota_url})"
                # Example for 'djini':
                    
                elif table_name == "djini":
                    djini_url = f"https://djinni.co{row[0]}"
                    message_text = f"{row[1]}:\n[Link]({djini_url})"
                else:
                    # Handle other tables (optional)
                    message_text = "Неизвестная таблица"

                # Send the message
                await send_message_to_chat(chat_id, message_text)

                # Update sent flag after successful message sending
                message_id = row[0]  # Assuming 'id' is the first column
                db_handler.update_sent_flag(message_id, table_name)

    except Exception as e:
        logging.error(f"Error in job: {e}")


schedule.every(10).minutes.do(lambda: asyncio.create_task(job()))

async def main():
    dp.include_router(router)  # Include router if needed
    asyncio.create_task(send_messages_periodically())
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('Exit')
