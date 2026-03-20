import os
import asyncio
import logging
from telegram import Bot
import re

logger = logging.getLogger(__name__)

def escape_markdown(text):
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(r'([{}])'.format(re.escape(escape_chars)), r'\\\1', text)

class TelegramManager:
    """Manages Telegram bots and sending alerts."""
    _bots = {}

    @classmethod
    def get_bot(cls, token: str) -> Bot:
        if token not in cls._bots:
            cls._bots[token] = Bot(token=token)
        return cls._bots[token]

    @classmethod
    async def send_message(cls, token: str, chat_ids: list, message: str, use_markdown=True, parse_mode=None):
        if not token:
            logger.warning("Telegram token is missing. Alert not sent.")
            return

        bot = cls.get_bot(token)
        if use_markdown and not parse_mode:
            message = escape_markdown(message)

        for chat_id in chat_ids:
            if not chat_id: continue
            try:
                await bot.send_message(chat_id=chat_id.strip(), text=message, parse_mode=parse_mode)
                logger.info(f"Telegram alert sent to {chat_id}")
            except Exception as e:
                logger.error(f"Failed to send Telegram alert to {chat_id}: {e}")
