import asyncio
import datetime
import os
import re
import calendar
import io
import cv2
import numpy as np
from pyzbar import pyzbar
import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv


from aiogram import Bot, Dispatcher, F, types

from aiogram.types import InlineQuery, InlineQueryResultArticle, InputTextMessageContent, ChosenInlineResult, BotCommand

from aiogram.filters import Command, StateFilter
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from motor.motor_asyncio import AsyncIOMotorClient

from pymongo.errors import DuplicateKeyError

from bson import ObjectId
import aiocron
from loguru import logger
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.chat_action import ChatActionSender


# -----------------------------
# Configuration and Setup
# -----------------------------
MAX_CALLBACK_DATA_LEN = 64
MEDS_PER_PAGE = 5

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
BOT_USERNAME = os.getenv("BOT_USERNAME")

logger.add("bot.log", rotation="1 MB", level="INFO")
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=storage)
client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
med_collection = db[COLLECTION_NAME]



# --- CallbackData Factories ---
class MedAction(CallbackData, prefix="med"):
    action: str
    item_id: str | None = None # ID лекарства (основной идентификатор)
    item_name: str | None = None
    # -------------------------------------------------
    field: str | None = None
    page: int | None = None
    confirm: bool | None = None

class CalendarNav(CallbackData, prefix="cal"):
    action: str
    year: int
    month: int
    day: int | None = None

# --- FSM States ---
class AddMedicine(StatesGroup):
    waiting_for_name = State()
    waiting_for_quantity = State()
    waiting_for_notes = State()
    waiting_for_exp_date = State()
    # Данные в state
    prompt_chat_id: int | None = None
    prompt_message_id: int | None = None
    calendar_message_id: int | None = None
    calendar_year: int | None = None
    calendar_month: int | None = None
    name: str | None = None
    name_lower: str | None = None
    quantity: str | None = None
    notes: str | None = None
    exp_date: str | None = None

class EditMedicine(StatesGroup):
    waiting_for_new_value = State()
    med_id: str | None = None
    med_name: str | None = None
    field_to_edit: str | None = None
    user_id: int | None = None
    # ID сообщения для редактирования (одно из)
    prompt_chat_id: int | None = None
    prompt_message_id: int | None = None # Обычное сообщение
    inline_message_id: str | None = None # Inline сообщение
    # ID сообщения с календарем (может совпадать с одним из верхних)
    calendar_message_id: int | str | None = None # Может быть int или str
    calendar_year: int | None = None
    calendar_month: int | None = None


# -----------------------------
# Keyboards
# -----------------------------

def get_confirm_barcode_update_keyboard(med_name: str) -> InlineKeyboardMarkup:
    """Предлагает обновить существующее лекарство или добавить другое."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            # Передаем item_name для этого конкретного случая
            InlineKeyboardButton(text="🔄 Да, обновить", callback_data=MedAction(action="confirm_barcode_update", item_name=med_name).pack()),
            InlineKeyboardButton(text="➕ Добавить другое", callback_data=MedAction(action="add_different_barcode").pack())
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")]
    ])
    return kb


def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Создает клавиатуру главного меню (БЕЗ КНОПКИ ПОИСКА)."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💊 Список лекарств", callback_data=MedAction(action="list", page=1).pack())],
        [InlineKeyboardButton(text="➕ Добавить лекарство", callback_data=MedAction(action="add").pack())],
        [InlineKeyboardButton(
            text=f"🔍 Начать поиск",
            switch_inline_query_current_chat=" " # Вставляем только @имя_бота
            # ------------------------------------------
        )],
    ])
    return kb

def get_medicine_list_keyboard(medicines: list, current_page: int = 1, page_size: int = MEDS_PER_PAGE) -> InlineKeyboardMarkup:
    """Создает клавиатуру со списком лекарств с пагинацией (использует item_id)."""
    buttons = []
    total_items = len(medicines)
    total_pages = (total_items + page_size - 1) // page_size

    start_index = (current_page - 1) * page_size
    end_index = start_index + page_size
    paginated_medicines = medicines[start_index:end_index]

    if paginated_medicines:
        buttons.extend([
            [InlineKeyboardButton(
                text=f"{med['name']} ({med['quantity']}) | Срок: {med.get('exp_date', 'N/A')}",
                # Убираем context, всегда передаем item_id
                callback_data=MedAction(action="view", item_id=str(med['_id'])).pack()
             )] for med in paginated_medicines
        ])
    else:
        buttons.append([InlineKeyboardButton(text="Список пуст на этой странице", callback_data="dummy")])

    # Кнопки пагинации
    page_buttons = []
    if current_page > 1:
        page_buttons.append(
            # Убираем context
            InlineKeyboardButton(text="◀️ Пред.", callback_data=MedAction(action="page", page=current_page - 1).pack())
        )
    if total_pages > 1:
         page_buttons.append(
             InlineKeyboardButton(text=f"📄 {current_page}/{total_pages}", callback_data="dummy_page_info")
         )
    if current_page < total_pages:
        page_buttons.append(
            # Убираем context
            InlineKeyboardButton(text="След. ▶️", callback_data=MedAction(action="page", page=current_page + 1).pack())
        )
    if page_buttons:
        buttons.append(page_buttons)

    # Кнопки действий (только для списка)
    buttons.append([InlineKeyboardButton(text="➕ Добавить лекарство", callback_data=MedAction(action="add").pack())])
    # Используем простую строку для callback_data, чтобы не смешивать с MedAction
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main_menu")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- ИЗМЕНЕНИЕ: Добавили параметр context ---
def get_medicine_details_keyboard(med_id: str, is_inline: bool = False) -> InlineKeyboardMarkup:
    """Создает клавиатуру для просмотра/редактирования деталей лекарства (использует item_id)."""
    kb_rows = [
        # Убираем context
        [InlineKeyboardButton(text="✏️ Изменить Название", callback_data=MedAction(action="edit", item_id=med_id, field="name").pack())],
        [InlineKeyboardButton(text="✏️ Изменить Количество", callback_data=MedAction(action="edit", item_id=med_id, field="quantity").pack())],
        [InlineKeyboardButton(text="✏️ Изменить Заметки", callback_data=MedAction(action="edit", item_id=med_id, field="notes").pack())],
        [InlineKeyboardButton(text="✏️ Изменить Срок годности", callback_data=MedAction(action="edit", item_id=med_id, field="exp_date").pack())],
        [InlineKeyboardButton(text="🗑️ Удалить лекарство", callback_data=MedAction(action="delete", item_id=med_id).pack())]
    ]
    # Кнопка "Назад" зависит от is_inline
    if not is_inline:
        # Для обычных сообщений возвращаемся к списку
        kb_rows.append([InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=MedAction(action="list", page=1).pack())])
    else:
        kb_rows.append([InlineKeyboardButton(text="☑️ Скрыть информацию", callback_data=MedAction(action="hide_inline_info").pack())])

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


# --- ИЗМЕНЕНИЕ: Добавили параметр context ---
def get_confirm_delete_keyboard(med_id: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления (использует item_id)."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            # Убираем context
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=MedAction(action="confirm_delete", item_id=med_id, confirm=True).pack()),
            # Кнопка отмены возвращает к просмотру деталей (тоже без context)
            InlineKeyboardButton(text="❌ Отмена", callback_data=MedAction(action="view", item_id=med_id).pack())
        ]
    ])
    return kb


def get_cancel_keyboard(context: str | None = None) -> InlineKeyboardMarkup:
    """Кнопка отмены для FSM, возвращает в зависимости от контекста."""
    # Пока простой вариант - всегда в главное меню
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_action")] # Общий обработчик отмены
    ])
    return kb

# -----------------------------
# Calendar Keyboard Generation
# -----------------------------
async def create_calendar(year: int | None = None, month: int | None = None) -> InlineKeyboardMarkup:
    """Создает inline-клавиатуру с календарем."""
    now = datetime.datetime.now()
    if year is None: year = now.year
    if month is None: month = now.month

    kb_builder = []

    # Header: << Year < Month > Year >>
    kb_builder.append([
        InlineKeyboardButton(text="<<", callback_data=CalendarNav(action="prev_year", year=year, month=month).pack()),
        InlineKeyboardButton(text="<", callback_data=CalendarNav(action="prev_month", year=year, month=month).pack()),
        InlineKeyboardButton(text=f"{calendar.month_name[month]} {year}", callback_data=CalendarNav(action="ignore", year=year, month=month).pack()), # Кнопка без действия
        InlineKeyboardButton(text=">", callback_data=CalendarNav(action="next_month", year=year, month=month).pack()),
        InlineKeyboardButton(text=">>", callback_data=CalendarNav(action="next_year", year=year, month=month).pack()),
    ])

    # Weekdays
    kb_builder.append([
        InlineKeyboardButton(text=day, callback_data=CalendarNav(action="ignore", year=year, month=month).pack()) for day in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    ])

    # Days
    month_calendar = calendar.monthcalendar(year, month)
    for week in month_calendar:
        week_buttons = []
        for day in week:
            if day == 0:
                week_buttons.append(InlineKeyboardButton(text=" ", callback_data=CalendarNav(action="ignore", year=year, month=month).pack()))
            else:
                week_buttons.append(InlineKeyboardButton(
                    text=str(day),
                    callback_data=CalendarNav(action="select_day", year=year, month=month, day=day).pack()
                ))
        kb_builder.append(week_buttons)

     # Cancel button (using the generic cancel callback)
    kb_builder.append([InlineKeyboardButton(text="❌ Отмена ввода даты", callback_data="cancel_action")])


    return InlineKeyboardMarkup(inline_keyboard=kb_builder)


# -----------------------------
# Helper Functions
# -----------------------------
# --- ИЗМЕНЕНИЕ: Добавлен inline_message_id ---
async def safe_edit_message(text: str,
                           chat_id: int | None = None,
                           message_id: int | None = None,
                           inline_message_id: str | None = None,
                           reply_markup: InlineKeyboardMarkup | None = None,
                           parse_mode: str | None = None):
    """Безопасно редактирует сообщение (обычное или inline)."""
    if not (chat_id and message_id) and not inline_message_id:
        logger.error("safe_edit_message called without chat_id/message_id or inline_message_id")
        return False

    identifier = f"message {message_id} in chat {chat_id}" if chat_id and message_id else f"inline message {inline_message_id}"

    try:
        if inline_message_id:
            await bot.edit_message_text(
                text=text,
                inline_message_id=inline_message_id,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
            logger.debug(f"Successfully edited {identifier}")
        else:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
            logger.debug(f"Successfully edited {identifier}")
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
             logger.debug(f"{identifier} was not modified: {e}")
             return True
        elif "message to edit not found" in str(e) or "message can't be edited" in str(e) or "MESSAGE_ID_INVALID" in str(e) or "inline message ID is invalid" in str(e):
             logger.warning(f"Could not edit {identifier} (not found, too old, or invalid ID): {e}")
             # Для inline сообщений отправка нового невозможна из safe_edit_message
             if chat_id:
                 try:
                     await bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode, disable_web_page_preview=True)
                     logger.info(f"Sent new message to chat {chat_id} after edit failed for {identifier}")
                     return True # Успешно отправили новое
                 except Exception as send_e:
                     logger.error(f"Failed to send new message after edit failed for {identifier} in chat {chat_id}: {send_e}")
                     return False
             else:
                 # Не можем отправить новое сообщение, если у нас только inline_message_id
                 return False
        else:
             logger.warning(f"Could not edit {identifier}: {e}.")
             return False
    except Exception as e:
        logger.error(f"Unexpected error editing {identifier}: {e}")
        return False

async def _get_user_medicines(user_id: int) -> list:
    """Вспомогательная функция для получения лекарств конкретного пользователя."""
    medicines = await med_collection.find(
        {"added_by": user_id}
    ).sort("name_lower").to_list(length=None)
    return medicines

async def _show_user_medicine_list(user_id: int, chat_id: int, message_id: int, page: int = 1):
    """Обновляет сообщение, показывая список лекарств пользователя с пагинацией."""
    medicines = await _get_user_medicines(user_id)
    # Вызываем обновленную get_medicine_list_keyboard (без context)
    kb = get_medicine_list_keyboard(medicines, current_page=page)
    text = "Ваши лекарства:" if medicines else "Список лекарств пуст."
    total_pages = (len(medicines) + MEDS_PER_PAGE - 1) // MEDS_PER_PAGE
    if total_pages > 1:
        text = f"Ваши лекарства (Страница {page}/{total_pages}):"
    await safe_edit_message(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)


# --- ИЗМЕНЕНИЕ: Добавлен inline_message_id ---
async def show_main_menu_message(chat_id: int | None = None, message_id: int | None = None, inline_message_id: str | None = None):
    """Обновляет сообщение (обычное или inline), показывая главное меню."""
    text = "🏠 Домашняя аптечка\n\nУправляйте списком ваших лекарств или воспользуйтесь поиском."
    kb = get_main_menu_keyboard()
    # --- ИЗМЕНЕНИЕ: Передаем все параметры в safe_edit_message ---
    await safe_edit_message(text, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, reply_markup=kb)

async def create_db_indexes():
    """Создает необходимые индексы в MongoDB."""
    try:
        # --- РЕКОМЕНДОВАННЫЙ ИНДЕКС ---
        # Составной уникальный индекс: уникальность имени в рамках ОДНОГО пользователя
        await med_collection.create_index([("added_by", 1), ("name_lower", 1)], unique=True)
        logger.info("Ensured unique compound index on ['added_by', 'name_lower'] (PER-USER uniqueness).")
        # -------------------------------

    except Exception as e:
        logger.error(f"Error creating/ensuring unique index: {e}")

    try:
        # Индекс по пользователю для быстрого поиска лекарств пользователя
        await med_collection.create_index("added_by")
        logger.info("Ensured index on 'added_by'.")
    except Exception as e:
        logger.error(f"Error creating/ensuring index on 'added_by': {e}")

    try:
        # Индекс по дате для напоминаний
        await med_collection.create_index("exp_date")
        logger.info("Ensured index on 'exp_date'.")
    except Exception as e:
        logger.error(f"Error creating/ensuring index on 'exp_date': {e}")

    # Попытка удалить старые индексы (если были)
    try:
        index_info = await med_collection.index_information()
        # Удаляем старый глобальный name_lower_1, если используется составной
        if "name_lower_1" in index_info and "added_by_1_name_lower_1" in index_info:
            await med_collection.drop_index("name_lower_1")
            logger.info("Dropped old global unique index 'name_lower_1'.")
        # Удаляем старый name_1
        if "name_1" in index_info:
             await med_collection.drop_index("name_1")
             logger.info("Dropped potentially old index 'name_1'.")
    except Exception as e:
        logger.warning(f"Could not check/drop old indexes: {e}")

    logger.info("MongoDB indexes check completed.")



# --- Barcode/Web Scraping Helpers ---

async def download_photo(message: types.Message) -> io.BytesIO | None:
    """Скачивает фото (наибольшего размера) и возвращает его как BytesIO."""
    if not message.photo:
        return None
    try:
        # Берем фото наибольшего размера
        photo = message.photo[-1]
        file_info = await bot.get_file(photo.file_id)
        file_content = await bot.download_file(file_info.file_path)
        return file_content # Возвращает BytesIO
    except Exception as e:
        logger.error(f"Failed to download photo: {e}")
        return None

def decode_barcode(image_bytes_io: io.BytesIO) -> str | None:
    """Декодирует штрихкод из изображения."""
    try:
        # Читаем байты в numpy массив
        image_bytes_io.seek(0) # Перемещаем указатель в начало BytesIO
        file_bytes = np.asarray(bytearray(image_bytes_io.read()), dtype=np.uint8)
        # Декодируем изображение с помощью OpenCV
        img = cv2.imdecode(file_bytes, cv2.IMREAD_COLOR)
        if img is None:
            logger.warning("Failed to decode image with OpenCV.")
            return None

        logger.debug("Attempting barcode decoding on original image...")
        barcodes = pyzbar.decode(img)

        if barcodes:
            barcode_data = barcodes[0].data.decode('utf-8')
            logger.info(f"Barcode decoded (original): {barcode_data}")
            return barcode_data

        # Если не нашли, пробуем с grayscale
        logger.debug("Attempting barcode decoding on grayscale image...")
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        barcodes_gray = pyzbar.decode(gray)
        if barcodes_gray:
            barcode_data = barcodes_gray[0].data.decode('utf-8')
            logger.info(f"Barcode decoded (grayscale): {barcode_data}")
            return barcode_data

        logger.info("Barcode not found in the image.")
        return None

    except Exception as e:
        logger.error(f"Error during barcode decoding: {e}")
        return None

async def fetch_barcode_info(barcode: str) -> str | None:
    """Получает HTML страницу с информацией о штрихкоде."""
    url = f"https://barcode-list.ru/barcode/RU/barcode-{barcode}/%D0%9F%D0%BE%D0%B8%D1%81%D0%BA.htm"
    logger.info(f"Fetching barcode info from: {url}")
    try:
        async with aiohttp.ClientSession() as session:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            async with session.get(url, headers=headers, timeout=15) as response:
                response.raise_for_status()
                html_content = await response.text()
                logger.info(f"Successfully fetched HTML for barcode {barcode}")
                return html_content
    except aiohttp.ClientError as e:
        logger.error(f"HTTP Client Error fetching barcode {barcode} info: {e}")
        return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching barcode {barcode} info from {url}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching barcode {barcode} info: {e}")
        return None

def parse_barcode_html(html_content: str) -> list[str]:
    """Парсит HTML и извлекает список наименований товаров."""
    names = []
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', class_='randomBarcodes')
        if not table:
            logger.warning("Table 'randomBarcodes' not found in HTML.")
            if "Штрих-код не найден в базе данных" in html_content:
                 logger.info("Barcode not found message detected on site.")
            return []

        rows = table.find_all('tr')
        if len(rows) <= 1:
             logger.warning("No data rows found in 'randomBarcodes' table.")
             return []

        for row in rows[1:]:
            cols = row.find_all('td')
            if len(cols) >= 3:
                name = cols[2].get_text(strip=True)
                if name:
                    names.append(name)

        unique_names = sorted(list(set(names)))
        logger.info(f"Parsed names from HTML: {unique_names}")
        return unique_names

    except Exception as e:
        logger.error(f"Error parsing barcode HTML: {e}")
        return []

def transliterate(text: str) -> str:
    """Простая функция транслитерации RU <-> EN."""
    ru_en = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
        'з': 'z', 'и': 'i', 'й': 'j', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
        'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
        'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
        'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo', 'Ж': 'Zh',
        'З': 'Z', 'И': 'I', 'Й': 'J', 'К': 'K', 'Л': 'L', 'М': 'M', 'Н': 'N', 'О': 'O',
        'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U', 'Ф': 'F', 'Х': 'H', 'Ц': 'Ts',
        'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Shch', 'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu',
        'Я': 'Ya'
    }
    en_ru = {
        'a': 'а', 'b': 'б', 'v': 'в', 'g': 'г', 'd': 'д', 'e': 'е', 'yo': 'ё', 'zh': 'ж',
        'z': 'з', 'i': 'и', 'j': 'й', 'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о',
        'p': 'п', 'r': 'р', 's': 'с', 't': 'т', 'u': 'у', 'f': 'ф', 'h': 'х', 'ts': 'ц',
        'ch': 'ч', 'sh': 'ш', 'shch': 'щ', 'y': 'ы', 'yu': 'ю', 'ya': 'я',
        'A': 'А', 'B': 'Б', 'V': 'В', 'G': 'Г', 'D': 'Д', 'E': 'Е', 'Yo': 'Ё', 'Zh': 'Ж',
        'Z': 'З', 'I': 'И', 'J': 'Й', 'K': 'К', 'L': 'Л', 'M': 'М', 'N': 'Н', 'O': 'О',
        'P': 'П', 'R': 'Р', 'S': 'С', 'T': 'Т', 'U': 'У', 'F': 'Ф', 'H': 'Х', 'Ts': 'Ц',
        'Ch': 'Ч', 'Sh': 'Ш', 'Shch': 'Щ', 'Y': 'Ы', 'Yu': 'Ю', 'Ya': 'Я'
    }

    result = ""
    is_ru = any(c in ru_en for c in text)
    is_en = any(c in en_ru for c in text)

    if is_ru and not is_en:
        dic = ru_en
    elif is_en and not is_ru:
        text = text.replace('shch', 'щ').replace('Shch', 'Щ')
        text = text.replace('yo', 'ё').replace('Yo', 'Ё')
        text = text.replace('zh', 'ж').replace('Zh', 'Ж')
        text = text.replace('ts', 'ц').replace('Ts', 'Ц')
        text = text.replace('ch', 'ч').replace('Ch', 'Ч')
        text = text.replace('sh', 'ш').replace('Sh', 'Ш')
        text = text.replace('yu', 'ю').replace('Yu', 'Ю')
        text = text.replace('ya', 'я').replace('Ya', 'Я')
        dic = en_ru
    else:
        return text

    for char in text:
        result += dic.get(char, char)

    return result



# -----------------------------
# Command Handlers
# -----------------------------
@dp.message(Command("start"), StateFilter(None))
async def start_handler(message: types.Message):
    """Обработчик команды /start. Показывает приветствие и главное меню."""
    text = (
        "👋 Привет! Я бот для учета лекарств в домашней аптечке.\n\n"
        "Я помогу тебе вести список лекарств, отслеживать сроки годности и быстро находить нужные препараты.\n\n"
        "Используй кнопки ниже для навигации или команду /help для получения инструкции."
        )
    kb = get_main_menu_keyboard()
    await message.answer(text, reply_markup=kb)

@dp.message(Command("help"), StateFilter(None))
async def help_command(message: types.Message):
    """Выводит инструкцию по использованию бота."""
    help_text = """
    📖 *Как пользоваться ботом "Домашняя аптечка"*

    Этот бот помогает вести список ваших лекарств. Вот что он умеет:

    1️⃣ *Добавить лекарство*
    Нажмите "➕ Добавить лекарство" или введите команду /add.
    Вы можете написать название лекарства или отправить фото с его штрихкодом.
    Бот попросит указать количество, срок годности и заметку (например, дозировку).

    2️⃣ *Посмотреть список*
    Нажмите "💊 Список лекарств" или введите команду /list.
    Используйте стрелки, чтобы листать страницы.

    3️⃣ *Посмотреть и изменить детали*
    Нажмите на нужное лекарство в списке.
    Вы сможете изменить название, количество, заметку или срок годности.
    Также можно удалить лекарство.

    4️⃣ *Удалить лекарство*
    Откройте нужное лекарство и нажмите "🗑️ Удалить лекарство".
    Бот спросит подтверждение.

    5️⃣ *Поиск через @*
    В любом чате напишите `@имя_вашего_бота` и название лекарства.
    Пример: `@medicount_bot аспирин`
    Бот покажет подходящие варианты из вашей аптечки. Нажмите на нужный — бот отправит его описание с кнопкой просмотра деталей. Нажмите на кнопку, чтобы отредактировать это лекарство.

    6️⃣ *Отмена действия*
    Если передумали — нажмите "❌ Отмена", введите "отмена" или команду /cancel.

    📅 *Напоминания*
    Бот сам проверяет сроки годности и может напоминать, если что-то скоро испортится.

    Если что-то пошло не так — начните с команды /start.
    """
    await message.answer(help_text, parse_mode="Markdown", disable_web_page_preview=True)

@dp.message(Command("list"), StateFilter(None))
async def list_medicines_command(message: types.Message):
    """Обработчик команды /list. Показывает лекарства ТЕКУЩЕГО пользователя (страница 1)."""
    user_id = message.from_user.id
    # Отправляем новое сообщение со списком, т.к. у нас нет message_id для редактирования
    medicines = await _get_user_medicines(user_id)
    kb = get_medicine_list_keyboard(medicines, current_page=1, context="list")
    text = "Ваши лекарства:" if medicines else "Список лекарств пуст."
    total_pages = (len(medicines) + MEDS_PER_PAGE - 1) // MEDS_PER_PAGE
    if total_pages > 1:
        text = f"Ваши лекарства (Страница 1/{total_pages}):"
    await message.answer(text, reply_markup=kb)


@dp.message(Command("add"), StateFilter(None))
async def add_medicine_command(message: types.Message, state: FSMContext):
    """Запускает процесс добавления лекарства через команду /add."""
    sent_message = await message.answer(
        "➕ Добавление нового препарата.\n\n"
        "Введите *точное название* или отправьте *фотографию штрихкода*:",
        parse_mode="Markdown",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AddMedicine.waiting_for_name)
    # Сохраняем ID сообщения бота для дальнейшего редактирования
    await state.update_data(prompt_chat_id=sent_message.chat.id, prompt_message_id=sent_message.message_id)


@dp.message(Command("cancel"))
@dp.message(F.text.casefold() == "отмена", StateFilter("*"))
async def cancel_handler(message: types.Message, state: FSMContext):
    """Отменяет текущее действие FSM (через команду /cancel или текст)."""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нет активного действия для отмены.", reply_markup=ReplyKeyboardRemove())
        return

    logger.info(f"Cancelling state {current_state} via command/text for user {message.from_user.id}")
    user_data = await state.get_data()
    # Пытаемся найти ID сообщения, которое редактировали (календарь, обычное или inline)
    message_id_to_edit = user_data.get("calendar_message_id") or user_data.get("prompt_message_id")
    inline_message_id_to_edit = user_data.get("inline_message_id")
    chat_id_to_edit = user_data.get("prompt_chat_id") # chat_id нужен только для обычных сообщений

    await state.clear()

    try:
        await message.delete() # Удаляем сообщение пользователя (/cancel или "отмена")
    except Exception as e:
        logger.warning(f"Could not delete user cancel message: {e}")

    if inline_message_id_to_edit:
        # Если отменяли действие с inline-сообщением, показываем на нем главное меню
        await show_main_menu_message(inline_message_id=inline_message_id_to_edit)
    elif chat_id_to_edit and message_id_to_edit:
        # Если было сообщение FSM (приглашение или календарь), возвращаем его в главное меню
        await show_main_menu_message(chat_id=chat_id_to_edit, message_id=message_id_to_edit)
    else:
        # Если не нашли ID (например, отмена сразу после /add), просто говорим об отмене
        # и показываем меню новым сообщением
         await message.answer("Действие отменено.", reply_markup=ReplyKeyboardRemove())
         await start_handler(message) # Показываем главное меню новым сообщением

# --- FSM Handlers for Adding Medicine ---

@dp.message(AddMedicine.waiting_for_name, F.text)
async def process_medicine_name_text(message: types.Message, state: FSMContext):
    """Обрабатывает название, введенное текстом."""
    name = message.text.strip()
    user_id = message.from_user.id
    state_data = await state.get_data()
    prompt_msg_id = state_data.get("prompt_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")
    logger.debug(f"State (text name input): {state_data}")

    # --- ПРОВЕРКА НАЛИЧИЯ ID СООБЩЕНИЯ БОТА ---
    if not prompt_chat_id or not prompt_msg_id:
        logger.error("process_medicine_name_text: Missing prompt_chat_id or prompt_message_id in state.")
        # Отправляем новое сообщение, так как не можем отредактировать старое
        await message.reply("Произошла ошибка состояния. Пожалуйста, попробуйте добавить заново /add")
        await state.clear()
        return
    # -----------------------------------------

    if not name:
        # Редактируем сообщение бота, просим ввести снова
        await safe_edit_message(
            text="Название не может быть пустым.\n\nВведите *название* или отправьте *фото штрихкода*:",
            chat_id=prompt_chat_id,
            message_id=prompt_msg_id,
            reply_markup=get_cancel_keyboard(),
            parse_mode="Markdown"
        )
        try: await message.delete() # Удаляем сообщение пользователя
        except Exception as e: logger.warning(f"Could not delete user message (empty name): {e}")
        return # Остаемся в том же состоянии

    name_lower = name.lower()
    existing_med_user = await med_collection.find_one({"name_lower": name_lower, "added_by": user_id})

    if existing_med_user:
        logger.info(f"Duplicate medicine name '{name}' entered by user {user_id}.")
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ: Редактируем сообщение бота ---
        await safe_edit_message(
            text=f"⚠️ Препарат '{existing_med_user['name']}' уже есть в вашей аптечке.\n\n"
                 f"Введите другое название или отправьте фото штрихкода:", # Убрал "или отмените", т.к. кнопка Отмена уже есть
            chat_id=prompt_chat_id,
            message_id=prompt_msg_id,
            reply_markup=get_cancel_keyboard(), # Оставляем кнопку Отмена
            parse_mode="Markdown" # Используем Markdown для выделения
        )
        # --- Удаляем сообщение пользователя с дубликатом ---
        try: await message.delete()
        except Exception as e: logger.warning(f"Could not delete user message (duplicate name): {e}")
        # ----------------------------------------------------
        return # Остаемся в состоянии waiting_for_name
        # ----------------------------------------------------

    # --- Если имя уникальное, продолжаем как обычно ---
    await state.update_data(name=name, name_lower=name_lower)
    logger.debug(f"Setting state to AddMedicine.waiting_for_quantity")
    await state.set_state(AddMedicine.waiting_for_quantity)

    # Удаляем сообщение пользователя с принятым именем
    try: await message.delete()
    except Exception as e: logger.warning(f"Could not delete user message (accepted name): {e}")

    # Редактируем сообщение бота, запрашивая количество
    edit_successful = await safe_edit_message(
        text="Название принято.\n\nУкажите количество (например, '10 шт', '50 мл', '1 блистер'):",
        chat_id=prompt_chat_id,
        message_id=prompt_msg_id,
        reply_markup=get_cancel_keyboard()
    )
    if not edit_successful: # Если редактирование/отправка не удались
        logger.warning("Failed to edit prompt message to ask for quantity. Sending new.")
        new_msg = await bot.send_message(prompt_chat_id, "Название принято.\n\nУкажите количество:", reply_markup=get_cancel_keyboard())
        # Обновляем ID в state на случай, если старое сообщение стало недоступно
        await state.update_data(prompt_message_id=new_msg.message_id)
    # ----------------------------------------------------



@dp.message(AddMedicine.waiting_for_name, F.photo)
async def process_medicine_name_photo(message: types.Message, state: FSMContext):
    """Обрабатывает фото со штрихкодом, предлагает обновить, если дубликат."""
    user_id = message.from_user.id
    state_data = await state.get_data()
    prompt_msg_id = state_data.get("prompt_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")
    logger.debug(f"State (photo name input): {state_data}")

    if not prompt_chat_id or not prompt_msg_id:
        logger.error("Cannot find prompt message info in state for AddMedicine.waiting_for_name (photo)")
        await message.reply("Произошла ошибка состояния. Пожалуйста, начните добавление заново (/add).")
        await state.clear()
        return

    try:
        await message.delete()
    except Exception as e:
        logger.warning(f"Could not delete user photo message: {e}")

    async with ChatActionSender.typing(bot=bot, chat_id=prompt_chat_id):
        await safe_edit_message("📸 Получил фото, распознаю штрихкод...", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard())
        photo_bytes_io = await download_photo(message)
        if not photo_bytes_io:
             await safe_edit_message("Не удалось загрузить фото. Попробуйте еще раз или введите название вручную.", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard())
             return
        barcode = decode_barcode(photo_bytes_io)
        photo_bytes_io.close()
        if not barcode:
            await safe_edit_message("Не удалось распознать штрихкод на фото. Попробуйте еще раз или введите название вручную.", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard())
            return

        await safe_edit_message(f"🔍 Штрихкод: `{barcode}`. Ищу на barcode-list.ru...", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
        html_content = await fetch_barcode_info(barcode)
        if not html_content:
            await safe_edit_message(f"Не удалось получить инфо для `{barcode}`. Введите название вручную:", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
            return
        parsed_names = parse_barcode_html(html_content)
        if not parsed_names:
             await safe_edit_message(f"Не найдено наименований для `{barcode}`. Введите название вручную:", chat_id=prompt_chat_id, message_id=prompt_msg_id, reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
             return

        shortest_name = min(parsed_names, key=len)
        logger.info(f"Shortest name found: '{shortest_name}'")
        name_lower = shortest_name.lower()
        existing_med_user = await med_collection.find_one({"name_lower": name_lower, "added_by": user_id})

        if existing_med_user:
            logger.info(f"Medicine '{shortest_name}' already exists for user {user_id}. Offering update.")
            await safe_edit_message(
                text=f"⚠️ Лекарство *{shortest_name}* уже есть в вашей аптечке.\n\n"
                     f"Хотите обновить информацию о нем (например, количество)?",
                chat_id=prompt_chat_id,
                message_id=prompt_msg_id,
                reply_markup=get_confirm_barcode_update_keyboard(shortest_name),
                parse_mode="Markdown"
            )
            # Сохраняем ID для колбэков, состояние не меняем
            await state.update_data(prompt_chat_id=prompt_chat_id, prompt_message_id=prompt_msg_id)
        else:
            logger.info(f"Automatically selected unique shortest name: '{shortest_name}'")
            await state.update_data(name=shortest_name, name_lower=name_lower)
            logger.debug(f"Setting state to AddMedicine.waiting_for_quantity")
            await state.set_state(AddMedicine.waiting_for_quantity)

            edit_successful = await safe_edit_message(
                text=f"✅ Название по штрихкоду: *{shortest_name}*\n\n"
                     f"Укажите количество (например, '10 шт', '50 мл', '1 блистер'):",
                chat_id=prompt_chat_id,
                message_id=prompt_msg_id,
                reply_markup=get_cancel_keyboard(),
                parse_mode="Markdown"
            )
            # Обновляем ID в state, если было отправлено новое сообщение
            if not edit_successful:
                 new_msg = await bot.send_message(prompt_chat_id, f"✅ Название по штрихкоду: *{shortest_name}*\n\nУкажите количество:", reply_markup=get_cancel_keyboard(), parse_mode="Markdown")
                 await state.update_data(prompt_chat_id=new_msg.chat.id, prompt_message_id=new_msg.message_id)
            else:
                 await state.update_data(prompt_chat_id=prompt_chat_id, prompt_message_id=prompt_msg_id)



@dp.message(AddMedicine.waiting_for_quantity, F.text)
async def process_medicine_quantity(message: types.Message, state: FSMContext):
    logger.debug(f"Entered process_medicine_quantity. Current state: {await state.get_state()}")
    quantity_str = message.text.strip()
    logger.debug(f"Received quantity: '{quantity_str}'")
    state_data = await state.get_data()
    prompt_msg_id = state_data.get("prompt_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")
    logger.debug(f"State data before processing quantity: {state_data}")

    if not quantity_str:
         await message.reply("Количество не может быть пустым. Укажите количество:")
         return

    await state.update_data(quantity=quantity_str)
    logger.debug(f"Quantity '{quantity_str}' saved to state.")
    logger.debug(f"Setting state to AddMedicine.waiting_for_notes")
    await state.set_state(AddMedicine.waiting_for_notes)

    try:
        await message.delete()
    except Exception as e: logger.warning(f"Could not delete user message: {e}")

    if prompt_chat_id and prompt_msg_id:
        logger.debug(f"Attempting to edit message {prompt_msg_id} in chat {prompt_chat_id} to ask for notes.")
        edit_successful = await safe_edit_message(
            text="Количество принято.\n\nДобавьте примечания (дозировка, способ применения, '-' если нет):",
            chat_id=prompt_chat_id,
            message_id=prompt_msg_id,
            reply_markup=get_cancel_keyboard()
        )
        if not edit_successful:
             logger.error("Failed to edit or send new prompt message in AddMedicine.waiting_for_quantity after state change.")
             new_msg = await bot.send_message(prompt_chat_id, "Количество принято.\n\nДобавьте примечания:", reply_markup=get_cancel_keyboard())
             await state.update_data(prompt_chat_id=new_msg.chat.id, prompt_message_id=new_msg.message_id)
    else:
        logger.error("Cannot find prompt message info in state for AddMedicine.waiting_for_quantity")
        new_msg = await bot.send_message(message.chat.id, "Количество принято.\n\nДобавьте примечания:", reply_markup=get_cancel_keyboard())
        await state.update_data(prompt_chat_id=new_msg.chat.id, prompt_message_id=new_msg.message_id)



@dp.message(AddMedicine.waiting_for_notes, F.text)
async def process_medicine_notes(message: types.Message, state: FSMContext):
    notes = message.text.strip()
    state_data = await state.get_data()
    prompt_msg_id = state_data.get("prompt_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")

    if not notes:
         await message.reply("Заметки не могут быть пустыми. Введите хотя бы '-' или 'нет':")
         return

    await state.update_data(notes=notes)
    await state.set_state(AddMedicine.waiting_for_exp_date)
    try:
        await message.delete()
    except Exception as e: logger.warning(f"Could not delete user message: {e}")

    today = datetime.date.today()
    calendar_markup = await create_calendar(today.year, today.month)

    if prompt_chat_id and prompt_msg_id:
        edit_successful = await safe_edit_message(
            text="Примечания добавлены.\n\nУкажите срок годности (ГГГГ-ММ-ДД) или выберите дату:",
            chat_id=prompt_chat_id,
            message_id=prompt_msg_id,
            reply_markup=calendar_markup
        )
        if edit_successful:
             # --- ИЗМЕНЕНИЕ: Сохраняем calendar_message_id ---
             await state.update_data(calendar_message_id=prompt_msg_id, calendar_year=today.year, calendar_month=today.month)
        else:
             new_cal_msg = await bot.send_message(prompt_chat_id, "Примечания добавлены.\n\nУкажите срок годности:", reply_markup=calendar_markup)
             # --- ИЗМЕНЕНИЕ: Обновляем prompt_message_id и calendar_message_id ---
             await state.update_data(prompt_chat_id=new_cal_msg.chat.id, prompt_message_id=new_cal_msg.message_id, calendar_message_id=new_cal_msg.message_id, calendar_year=today.year, calendar_month=today.month)
    else:
        logger.error("Cannot find prompt message info in state for AddMedicine.waiting_for_notes")
        new_cal_msg = await bot.send_message(message.chat.id, "Примечания добавлены.\n\nУкажите срок годности:", reply_markup=calendar_markup)
        # --- ИЗМЕНЕНИЕ: Обновляем все ID ---
        await state.update_data(prompt_chat_id=new_cal_msg.chat.id, prompt_message_id=new_cal_msg.message_id, calendar_message_id=new_cal_msg.message_id, calendar_year=today.year, calendar_month=today.month)


@dp.message(AddMedicine.waiting_for_exp_date, F.text)
async def process_medicine_exp_date_text(message: types.Message, state: FSMContext):
    exp_date_str = message.text.strip()
    user_id = message.from_user.id
    state_data = await state.get_data()
    # --- ИЗМЕНЕНИЕ: Используем calendar_message_id ---
    calendar_msg_id = state_data.get("calendar_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")

    try:
        exp_date_obj = datetime.datetime.strptime(exp_date_str, "%Y-%m-%d").date()
        if exp_date_obj < datetime.date.today():
             await message.reply("Срок годности не может быть в прошлом. Введите корректную дату:")
             return
    except ValueError:
        await message.reply("Неверный формат даты. Используйте ГГГГ-ММ-ДД:")
        return

    await state.update_data(exp_date=exp_date_obj.isoformat())

    try:
        await message.delete()
    except Exception as e: logger.warning(f"Could not delete user message with date: {e}")

    # --- ИЗМЕНЕНИЕ: Передаем правильные ID ---
    await _save_new_medicine(user_id, state, prompt_chat_id, calendar_msg_id)




async def _save_new_medicine(user_id: int, state: FSMContext, chat_id: int | None, message_id: int | None):
    user_data = await state.get_data()
    name = user_data.get('name')
    name_lower = user_data.get('name_lower')
    quantity = user_data.get('quantity')
    notes = user_data.get('notes')
    exp_date_iso = user_data.get('exp_date')

    if not all([name, name_lower, quantity, notes, exp_date_iso]):
        logger.error("Incomplete medicine data before insertion: {}", user_data)
        await state.clear()
        error_text = "Произошла ошибка: не все данные собраны. Попробуйте добавить заново."
        if chat_id and message_id:
            await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id, reply_markup=get_main_menu_keyboard())
        # else: # Не можем отправить пользователю, если не знаем chat_id
        #     await bot.send_message(user_id, error_text, reply_markup=get_main_menu_keyboard())
        return

    med_doc = {
        "name": name, "name_lower": name_lower, "quantity": quantity,
        "notes": notes, "exp_date": exp_date_iso, "added_by": user_id
    }

    try:
        await med_collection.insert_one(med_doc)
        logger.info("Added medicine via FSM for user {}: {}", user_id, med_doc['name'])
        await state.clear()
        if chat_id and message_id:
            await _show_user_medicine_list(user_id, chat_id, message_id, page=1)
        # else:
        #      await bot.send_message(user_id, f"Лекарство '{name}' успешно добавлено!",
        #                            reply_markup=get_main_menu_keyboard())
    except DuplicateKeyError:
        logger.warning("Duplicate key error on insert for user {}: attempt to add '{}'", user_id, name)
        await state.clear()
        error_text = f"Ошибка: Препарат с названием '{name}' уже существует в вашей аптечке."
        if chat_id and message_id:
            await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id, reply_markup=get_main_menu_keyboard())
        # else:
        #     await bot.send_message(user_id, error_text, reply_markup=get_main_menu_keyboard())
    except Exception as e:
         logger.error("Failed to insert medicine for user {}: {}. Data: {}", user_id, e, med_doc)
         await state.clear()
         error_text = "Произошла ошибка при сохранении данных. Попробуйте позже."
         if chat_id and message_id:
             await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id, reply_markup=get_main_menu_keyboard())
         # else:
         #      await bot.send_message(user_id, error_text, reply_markup=get_main_menu_keyboard())


# --- FSM Handlers for Editing Medicine ---

@dp.message(EditMedicine.waiting_for_new_value, F.text)
async def process_new_value_text(message: types.Message, state: FSMContext):
    new_value_str = message.text.strip()
    user_id = message.from_user.id
    state_data = await state.get_data()
    # --- ИЗМЕНЕНИЕ: Получаем med_id и med_name из state ---
    med_id = state_data.get("med_id")
    med_name_original = state_data.get("med_name") # Имя нужно для проверок и логов
    field_to_edit = state_data.get("field_to_edit")
    prompt_msg_id = state_data.get("prompt_message_id")
    prompt_chat_id = state_data.get("prompt_chat_id")
    calendar_msg_id = state_data.get("calendar_message_id")
    inline_msg_id = state_data.get("inline_message_id")

    message_id_to_restore = calendar_msg_id or prompt_msg_id
    chat_id_to_restore = prompt_chat_id

    # --- ИЗМЕНЕНИЕ: Проверяем наличие med_id ---
    if not all([med_id, med_name_original, field_to_edit]) or not ( (chat_id_to_restore and message_id_to_restore) or inline_msg_id):
        logger.error("State data missing (med_id/name/field or IDs) in EditMedicine state: {}", state_data)
        await state.clear()
        await message.reply("Произошла ошибка состояния редактирования. Попробуйте снова.")
        if inline_msg_id: await show_main_menu_message(inline_message_id=inline_msg_id)
        elif chat_id_to_restore and message_id_to_restore: await show_main_menu_message(chat_id=chat_id_to_restore, message_id=message_id_to_restore)
        return

    update_data = {}
    error_message = None
    validated_value = None

    # Валидация
    if field_to_edit == "name":
         if not new_value_str: error_message = "Название не может быть пустым."
         else:
             new_value_lower = new_value_str.lower()
             # Ищем другое лекарство с таким же новым именем у этого пользователя
             existing_med_user = await med_collection.find_one({
                 "name_lower": new_value_lower,
                 "added_by": user_id,
                 # --- ИЗМЕНЕНИЕ: Убедимся, что это не то же самое лекарство ---
                 "_id": {"$ne": ObjectId(med_id)}
             })
             if existing_med_user:
                 error_message = f"Препарат '{new_value_str}' уже есть в аптечке."
             else:
                 update_data["name"] = new_value_str; update_data["name_lower"] = new_value_lower; validated_value = new_value_str
    # Остальные поля (quantity, notes, exp_date - ручной ввод) без изменений в логике валидации
    elif field_to_edit == "quantity":
        if not new_value_str: error_message = "Количество не может быть пустым."
        else: update_data[field_to_edit] = new_value_str; validated_value = new_value_str
    elif field_to_edit == "notes":
         if not new_value_str: error_message = "Заметки не могут быть пустыми (введите '-')."
         else: update_data[field_to_edit] = new_value_str; validated_value = new_value_str
    elif field_to_edit == "exp_date":
        try:
            dt = datetime.datetime.strptime(new_value_str, "%Y-%m-%d").date()
            if dt < datetime.date.today(): error_message = "Срок годности не может быть в прошлом."
            else: update_data[field_to_edit] = dt.isoformat(); validated_value = dt
        except ValueError: error_message = "Неверный формат даты. Используйте ГГГГ-ММ-ДД."

    if error_message:
        await message.reply(f"{error_message}\nПопробуйте ввести значение еще раз или отмените действие.")
        return

    try:
        await message.delete()
    except Exception as e: logger.warning(f"Could not delete user message with new value: {e}")

    # --- ИЗМЕНЕНИЕ: Передаем med_id в _save_edited_medicine ---
    await _save_edited_medicine(
        user_id=user_id, med_id=med_id, field_to_edit=field_to_edit,
        new_value=validated_value, update_data_dict=update_data, state=state,
        chat_id=chat_id_to_restore,
        message_id=message_id_to_restore,
        inline_message_id=inline_msg_id
    )


# --- ИЗМЕНЕНИЕ: Добавлен inline_message_id ---
async def _save_edited_medicine(user_id: int, med_id: str, field_to_edit: str, new_value: any, update_data_dict: dict, state: FSMContext,
                                chat_id: int | None, message_id: int | None, inline_message_id: str | None):
    """Сохраняет изменения и вызывает view_medicine_details с правильными ID."""
    try: object_id = ObjectId(med_id)
    except Exception as e: # ... обработка ошибки ID ...
        logger.error(f"Invalid ObjectId received in _save_edited_medicine: {med_id}. Error: {e}")
        await state.clear(); # ... сообщить об ошибке ...
        return

    find_query = {"_id": object_id, "added_by": user_id}
    current_med = await med_collection.find_one(find_query, {"name": 1})
    med_name_original = current_med.get("name", "???" if current_med else "Не найдено")

    if field_to_edit == "exp_date" and isinstance(new_value, datetime.date): update_data_dict[field_to_edit] = new_value.isoformat()
    elif field_to_edit not in update_data_dict and new_value is not None: update_data_dict[field_to_edit] = new_value

    if not update_data_dict: # ... обработка пустого обновления ...
        logger.warning("No data to update for medicine ID '{}', user {}", med_id, user_id)
        await state.clear()
        # Возвращаемся к просмотру деталей с правильными ID
        await view_medicine_details(med_id, user_id, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id)
        return

    result = await med_collection.update_one(find_query, {"$set": update_data_dict})
    await state.clear()
    new_name_after_update = update_data_dict.get("name", med_name_original)

    if result.matched_count == 0:
        # ... (обработка ошибки: лекарство не найдено при обновлении) ...
        logger.warning(f"Medicine ID '{med_id}' for user {user_id} not found for update.")
        error_text = "Лекарство для обновления не найдено."
        if inline_message_id:
            await safe_edit_message(error_text, inline_message_id=inline_message_id)
            await asyncio.sleep(2); await show_main_menu_message(inline_message_id=inline_message_id)
        elif chat_id and message_id:
            await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id)
            await asyncio.sleep(2); await _show_user_medicine_list(user_id, chat_id, message_id, page=1)
    elif result.modified_count:
        # ... (логирование успеха) ...
        logger.info("Updated medicine ID '{}' (Name: '{}'), field '{}' for user {}. New name: '{}'", med_id, med_name_original, field_to_edit, user_id, new_name_after_update)
        # Возвращаемся к просмотру деталей с правильными ID
        await view_medicine_details(med_id, user_id, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id)
    else:
        # ... (лекарство найдено, но не изменено) ...
        logger.info("Medicine ID '{}' (Name: '{}') for user {} matched but not modified.", med_id, med_name_original, user_id)
        # Возвращаемся к просмотру деталей с правильными ID
        await view_medicine_details(med_id, user_id, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id)




# --- Inline Query Handler ---
@dp.inline_query()
async def inline_search_handler(inline_query: InlineQuery):
    query = inline_query.query.strip()
    user_id = inline_query.from_user.id
    results = []
    offset = int(inline_query.offset) if inline_query.offset else 0
    limit = 20

    if len(query) < 1:
        # --- ИЗМЕНЕНИЕ: Поведение switch_pm_text ---
        # При нажатии на "Введите название..." пользователь перейдет в ЛС с ботом
        # и боту будет отправлена команда /start inline_help
        # Текущий обработчик /start просто покажет главное меню.
        # Если нужно спец. сообщение - нужно доработать /start хендлер.
        await inline_query.answer([], cache_time=5, is_personal=True,
                                  switch_pm_text="Введите название...",
                                  switch_pm_parameter="inline_help")
        return

    logger.info(f"Inline query from user {user_id}: '{query}' (Offset: {offset})")

    # Логика поиска (без изменений)
    query_lower = query.lower()
    translit_query_lower = transliterate(query_lower)
    search_conditions = []
    regex_options = "i"
    search_conditions.append({"name_lower": {"$regex": f"{re.escape(query_lower)}", "$options": regex_options}})
    if translit_query_lower != query_lower:
        search_conditions.append({"name_lower": {"$regex": f"{re.escape(translit_query_lower)}", "$options": regex_options}})
    db_query = {"added_by": user_id}
    if len(search_conditions) > 1: db_query["$or"] = search_conditions
    elif search_conditions: db_query.update(search_conditions[0])
    else:
        await inline_query.answer([], cache_time=5, is_personal=True)
        return

    try:
        found_medicines_cursor = med_collection.find(db_query).sort("name_lower").skip(offset).limit(limit)
        found_medicines = await found_medicines_cursor.to_list(length=limit)

        for med in found_medicines:
            # --- ИЗМЕНЕНИЕ: Используем item_id ---
            med_id = str(med['_id'])
            result_article_id = f"med_{user_id}_{med_id}" # Стабильный ID для результата
            med_name = med['name']
            med_quantity = med.get('quantity', 'N/A')
            med_exp = med.get('exp_date', 'N/A')
            med_notes = med.get('notes', '-')

            message_text = (
                f"💊 *{med_name}*\n\n"
                f"▫️ Количество: {med_quantity}\n"
                f"▫️ Срок годности: `{med_exp}`\n"
                f"📝 Примечания: {med_notes}"
            )

            # --- ИЗМЕНЕНИЕ: Клавиатура использует item_id ---
            inline_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="✏️ Посмотреть / Изменить",
                    callback_data=MedAction(action="view", item_id=med_id, context="inline_view").pack()
                )]
            ])

            results.append(
                InlineQueryResultArticle(
                    id=result_article_id, # Используем стабильный ID
                    title=med_name,
                    description=f"Кол-во: {med_quantity} | Срок: {med_exp}",
                    input_message_content=InputTextMessageContent(
                        message_text=message_text,
                        parse_mode="Markdown",
                        disable_web_page_preview=True
                    ),
                    reply_markup=inline_kb
                )
            )

        next_offset = str(offset + limit) if len(found_medicines) == limit else ""
        await inline_query.answer(
            results=results, cache_time=5, is_personal=True, next_offset=next_offset
        )

    except Exception as e:
        logger.error(f"Error processing inline query for user {user_id}: {e}")
        await inline_query.answer([], cache_time=1, is_personal=True, switch_pm_text="Ошибка поиска. Перейти в бот?", switch_pm_parameter="error")


# -----------------------------
# Callback Query Handlers
# -----------------------------

@dp.callback_query(MedAction.filter(F.action == "hide_inline_info"), StateFilter(None))
async def hide_inline_info_callback(callback: types.CallbackQuery):
    """Скрывает (редактирует на символ) сообщение, отправленное через inline-режим."""
    if callback.inline_message_id:
        logger.info(f"Hiding inline message {callback.inline_message_id} by user {callback.from_user.id}")
        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        # Редактируем на короткий символ без клавиатуры
        edit_success = await safe_edit_message(
            text="⚠️", # Просто символ
            inline_message_id=callback.inline_message_id,
            reply_markup=None, # Убираем клавиатуру
            parse_mode=None # Не нужен parse_mode для символа
        )
        # ----------------------
        if edit_success:
            await callback.answer("Информация скрыта.")
        else:
            # Если редактирование не удалось (например, сообщение слишком старое)
            await callback.answer("Не удалось скрыть информацию.", show_alert=True)
    else:
        logger.warning(f"hide_inline_info callback received without inline_message_id from user {callback.from_user.id}")
        await callback.answer("Это действие доступно только для сообщений из поиска.", show_alert=True)

# --- Main Menu and List Navigation ---
@dp.callback_query(MedAction.filter(F.action == "list"), StateFilter(None))
async def list_medicines_callback(callback: types.CallbackQuery, callback_data: MedAction):
    """Показывает список лекарств пользователя (с пагинацией)."""
    user_id = callback.from_user.id
    page = callback_data.page or 1
    # Убедимся, что колбэк пришел от обычного сообщения
    if callback.message:
        await _show_user_medicine_list(user_id, callback.message.chat.id, callback.message.message_id, page=page)
        await callback.answer()
    else:
        await callback.answer("Действие недоступно для этого сообщения.", show_alert=True)


@dp.callback_query(MedAction.filter(F.action == "page"), StateFilter(None))
async def handle_page_callback(callback: types.CallbackQuery, callback_data: MedAction):
    """Обрабатывает кнопки пагинации списка."""
    # Работает только для обычных сообщений
    if callback.message:
        user_id = callback.from_user.id
        page = callback_data.page or 1
        await _show_user_medicine_list(user_id, callback.message.chat.id, callback.message.message_id, page=page)
        await callback.answer()
    else:
        logger.warning("page callback received without message")
        await callback.answer("Пагинация здесь недоступна.", show_alert=True)


@dp.callback_query(F.data == "back_to_main_menu", StateFilter(None))
async def back_to_menu_callback_from_list(callback: types.CallbackQuery):
    """Возвращает в главное меню ИЗ СПИСКА ЛЕКАРСТВ."""
    # Работает только для обычных сообщений
    if callback.message:
        await show_main_menu_message(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
        await callback.answer()
    else:
        logger.warning("back_to_main_menu callback received without message")
        await callback.answer("Ошибка навигации.", show_alert=True)


@dp.callback_query(MedAction.filter(F.action == "back" and F.context == "list"), StateFilter(None))
async def back_to_list_callback(callback: types.CallbackQuery):
    """Возвращает к списку лекарств (страница 1)."""
    # Эта кнопка есть только у обычных сообщений
    if callback.message:
        user_id = callback.from_user.id
        await _show_user_medicine_list(user_id, callback.message.chat.id, callback.message.message_id, page=1)
        await callback.answer()
    else:
        await callback.answer("Действие недоступно.", show_alert=True)

# --- Add Medicine Flow ---
@dp.callback_query(MedAction.filter(F.action == "add"), StateFilter(None))
async def add_medicine_callback_start(callback: types.CallbackQuery, state: FSMContext):
    # Добавление возможно только из обычного сообщения
    if not callback.message:
        await callback.answer("Добавить лекарство можно только в личной переписке с ботом.", show_alert=True)
        return

    await callback.answer("Начинаем добавление...")
    chat_id=callback.message.chat.id; message_id=callback.message.message_id
    await safe_edit_message(
        text="➕ Добавление нового препарата.\n\nВведите *название* или отправьте *фото штрихкода*:",
        chat_id=chat_id, message_id=message_id, parse_mode="Markdown", reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AddMedicine.waiting_for_name)
    await state.update_data(prompt_chat_id=chat_id, prompt_message_id=message_id)


@dp.callback_query(MedAction.filter(F.action == "confirm_barcode_update"), AddMedicine.waiting_for_name)
async def handle_confirm_barcode_update(callback: types.CallbackQuery, callback_data: MedAction, state: FSMContext):
    """Находит лекарство по имени из callback и показывает его детали."""
    logger.debug(f"Entered handle_confirm_barcode_update for user {callback.from_user.id}")
    if not callback.message:
        logger.warning("handle_confirm_barcode_update called without callback.message")
        return await callback.answer("Ошибка: Действие недоступно.", show_alert=True)

    # Читаем item_name из callback_data
    med_name = callback_data.item_name
    user_id = callback.from_user.id
    # Получаем ID чата и сообщения из контекста колбэка
    prompt_chat_id = callback.message.chat.id
    prompt_message_id = callback.message.message_id
    logger.debug(f"Callback data item_name: {med_name}")
    logger.debug(f"Context: chat_id={prompt_chat_id}, message_id={prompt_message_id}")

    if not med_name: # Проверка med_name (на всякий случай)
        logger.error(f"Confirm barcode update missing data: Name is None. ChatID: {prompt_chat_id}, MsgID: {prompt_message_id}")
        await callback.answer("Произошла ошибка (нет имени)!", show_alert=True)
        # Попытка вернуть главное меню
        await state.clear()
        await show_main_menu_message(chat_id=prompt_chat_id, message_id=prompt_message_id)
        return

    # Находим ID лекарства по имени
    logger.debug(f"Searching for med with name_lower: '{med_name.lower()}' for user {user_id}")
    existing_med = await med_collection.find_one({"name_lower": med_name.lower(), "added_by": user_id})
    if not existing_med:
        logger.error(f"Cannot find med '{med_name}' for user {user_id} to get ID.")
        await callback.answer(f"Не удалось найти '{med_name}'.", show_alert=True)
        # Можно оставить пользователя в состоянии ввода имени или вернуть меню
        # Вернем меню для простоты
        await state.clear()
        await show_main_menu_message(chat_id=prompt_chat_id, message_id=prompt_message_id)
        return

    med_id = str(existing_med['_id'])
    logger.info(f"Found med_id: {med_id} for name: '{med_name}'. Showing details view instead of editing quantity.")

    # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ---
    # Очищаем состояние FSM (выходим из AddMedicine.waiting_for_name)
    await state.clear()

    # Вызываем функцию отображения деталей для найденного ID
    # Передаем chat_id и message_id из текущего колбэка
    await view_medicine_details(
        med_id=med_id,
        user_id=user_id,
        chat_id=prompt_chat_id,
        message_id=prompt_message_id
        # is_inline здесь явно False, так как мы работаем с callback.message
    )
    # --------------------------

    # Отвечаем на колбэк, чтобы убрать "часики"
    await callback.answer()

@dp.callback_query(MedAction.filter(F.action == "add_different_barcode"), AddMedicine.waiting_for_name)
async def handle_add_different_barcode(callback: types.CallbackQuery, state: FSMContext):
    # Логика без изменений, просто возвращает к вводу имени
    if not callback.message: return await callback.answer("Ошибка.", show_alert=True)
    state_data = await state.get_data()
    prompt_chat_id = state_data.get("prompt_chat_id")
    prompt_message_id = state_data.get("prompt_message_id")
    if not all([prompt_chat_id, prompt_message_id]):
         logger.error(f"Add different barcode missing prompt data.")
         await callback.answer("Произошла ошибка!", show_alert=True)
         await state.clear()
         await show_main_menu_message(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
         return
    await callback.answer("Введите другое название.")
    await safe_edit_message(
        text="➕ Хорошо, добавьте другое лекарство.\n\n"
             "Введите *точное название* или отправьте *фотографию штрихкода*:",
        chat_id=prompt_chat_id, message_id=prompt_message_id,
        parse_mode="Markdown", reply_markup=get_cancel_keyboard())
    await state.update_data(prompt_chat_id=prompt_chat_id, prompt_message_id=prompt_message_id)



# --- View Medicine Details ---
# --- ИЗМЕНЕНИЕ: Добавлены inline_message_id и context ---
async def view_medicine_details(med_id: str, user_id: int,
                                chat_id: int | None = None, message_id: int | None = None,
                                inline_message_id: str | None = None):
    """Отображает детали лекарства по ID (для обычного или inline сообщения)."""
    is_inline = bool(inline_message_id) # Определяем, inline это или нет

    if not (chat_id and message_id) and not inline_message_id:
        logger.error("view_medicine_details called without chat_id/message_id or inline_message_id")
        return

    try:
        object_id = ObjectId(med_id)
    except Exception as e:
        logger.error(f"Invalid ObjectId received in view_medicine_details: {med_id}. Error: {e}")
        error_text = "Произошла внутренняя ошибка (неверный ID лекарства)."
        await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id)
        return

    medicine = await med_collection.find_one({"_id": object_id, "added_by": user_id})

    if not medicine:
        logger.warning(f"Medicine ID '{med_id}' for user {user_id} not found when trying to view details.")
        error_text = "Не удалось найти информацию о лекарстве."
        if is_inline:
             # Для inline просто показываем ошибку и меню
             await safe_edit_message(error_text, inline_message_id=inline_message_id, reply_markup=get_main_menu_keyboard())
        elif chat_id and message_id:
             # Для обычного сообщения показываем ошибку, потом возвращаем список
             await safe_edit_message(error_text, chat_id=chat_id, message_id=message_id)
             await asyncio.sleep(2)
             await _show_user_medicine_list(user_id, chat_id, message_id, page=1)
        return

    med_name = medicine['name']
    text = (
        f"Препарат: *{med_name}*\n\n"
        f"*Количество:* {medicine['quantity']}\n"
        f"*Примечания:* {medicine.get('notes', '-')}\n"
        f"Срок годности: `{medicine.get('exp_date', 'Не указан')}`"
    )
    # Передаем is_inline в клавиатуру
    kb = get_medicine_details_keyboard(med_id, is_inline=is_inline)
    await safe_edit_message(text, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, reply_markup=kb, parse_mode="Markdown")


# --- ИЗМЕНЕНИЕ: Обрабатываем колбэки от обычных и inline сообщений ---
@dp.callback_query(MedAction.filter(F.action == "view"), StateFilter(None))
async def view_medicine_callback(callback: types.CallbackQuery, callback_data: MedAction):
    """Обрабатывает нажатие кнопки для просмотра деталей лекарства (из списка или inline)."""
    user_id = callback.from_user.id
    med_id = callback_data.item_id

    if not med_id: # ... обработка ошибки med_id ...
        logger.warning("View action called without item_id for user {}", user_id)
        await callback.answer("Ошибка: Не указан ID лекарства.", show_alert=True); return
    try: ObjectId(med_id) # Проверка
    except Exception: # ... обработка ошибки med_id ...
        logger.warning(f"View action called with invalid item_id '{med_id}' for user {user_id}")
        await callback.answer("Ошибка: Некорректный ID лекарства.", show_alert=True); return

    # Вызываем универсальную view_medicine_details, передавая нужные ID
    if callback.inline_message_id:
        await view_medicine_details(med_id, user_id, inline_message_id=callback.inline_message_id)
    elif callback.message:
        await view_medicine_details(med_id, user_id, chat_id=callback.message.chat.id, message_id=callback.message.message_id)
    else: # Не должно возникать
        logger.error(f"View callback without message or inline_message_id. Data: {callback_data}")
        await callback.answer("Неизвестная ошибка.", show_alert=True)

    await callback.answer()


# --- Delete Medicine Flow ---
# --- ИЗМЕНЕНИЕ: Обрабатываем колбэки от обычных и inline сообщений ---
@dp.callback_query(MedAction.filter(F.action == "delete"), StateFilter(None))
async def delete_medicine_request(callback: types.CallbackQuery, callback_data: MedAction):
    """Запрашивает подтверждение перед удалением (из списка или inline)."""
    med_id = callback_data.item_id
    user_id = callback.from_user.id
    chat_id = callback.message.chat.id if callback.message else None
    message_id = callback.message.message_id if callback.message else None
    inline_message_id = callback.inline_message_id

    if not med_id: return await callback.answer("Ошибка: Не указан ID лекарства.", show_alert=True)
    if not ( (chat_id and message_id) or inline_message_id ): # ... обработка ошибки ID сообщения ...
        logger.error(f"Missing ID for delete request. C/M ID: {chat_id}/{message_id}, Inline ID: {inline_message_id}")
        return await callback.answer("Ошибка запроса удаления.", show_alert=True)

    # Получаем имя для подтверждения
    try:
        object_id = ObjectId(med_id)
        medicine = await med_collection.find_one({"_id": object_id, "added_by": user_id}, {"name": 1})
        if not medicine: raise ValueError("Medicine not found")
        med_name = medicine.get("name", "Неизвестное лекарство")
    except Exception as e: # ... обработка ошибки получения имени ...
        logger.error(f"Failed to get medicine name for delete confirmation. ID: {med_id}, User: {user_id}, Error: {e}")
        await callback.answer("Не удалось найти лекарство для удаления.", show_alert=True); return

    text = f"🗑️ Вы уверены, что хотите удалить препарат *{med_name}*?"
    kb = get_confirm_delete_keyboard(med_id) # Универсальная клавиатура
    await safe_edit_message(text, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, reply_markup=kb, parse_mode="Markdown")
    await callback.answer()



# --- ИЗМЕНЕНИЕ: Обрабатываем колбэки от обычных и inline сообщений ---
@dp.callback_query(MedAction.filter( (F.action == "confirm_delete") & (F.confirm == True) ), StateFilter(None))
async def delete_medicine_confirm(callback: types.CallbackQuery, callback_data: MedAction):
    """Выполняет удаление после подтверждения (из списка или inline)."""
    user_id = callback.from_user.id
    med_id = callback_data.item_id
    chat_id = callback.message.chat.id if callback.message else None
    message_id = callback.message.message_id if callback.message else None
    inline_message_id = callback.inline_message_id

    # ... (проверки med_id и ID сообщения) ...
    if not med_id: return await callback.answer("Ошибка: Не указан ID лекарства.", show_alert=True)
    if not ( (chat_id and message_id) or inline_message_id ):
         logger.error(f"Missing ID for confirm delete. C/M ID: {chat_id}/{message_id}, Inline ID: {inline_message_id}")
         return await callback.answer("Ошибка подтверждения удаления.", show_alert=True)

    # ... (получение имени перед удалением) ...
    try: # ... получение med_name ...
        object_id = ObjectId(med_id)
        med_to_delete = await med_collection.find_one({"_id": object_id, "added_by": user_id}, {"name": 1})
        med_name = med_to_delete.get("name", "???" if med_to_delete else "Не найдено")
    except Exception as e: # ... обработка ошибки ...
         logger.error(f"Failed to get name before delete. ID: {med_id}, User: {user_id}, Error: {e}")
         med_name = "???" ; object_id = None

    # ... (удаление из БД) ...
    deleted_count = 0
    if object_id:
        result = await med_collection.delete_one({"_id": object_id, "added_by": user_id})
        deleted_count = result.deleted_count

    if deleted_count:
        logger.info("Confirmed delete medicine ID '{}' (Name: '{}') for user {}", med_id, med_name, user_id)
        await callback.answer("Препарат удален.", show_alert=False)

        # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
        if inline_message_id:
            # Для inline-сообщения редактируем его на символ "корзина"
            logger.info(f"Updating inline message {inline_message_id} to '🗑️' after deleting med {med_id}")
            await safe_edit_message(
                text="🗑️", # Просто символ корзины
                inline_message_id=inline_message_id,
                reply_markup=None, # Убираем клавиатуру
                parse_mode=None
            )
        # ----------------------
        elif chat_id and message_id:
            # Для обычного сообщения возвращаемся к списку
            await _show_user_medicine_list(user_id, chat_id, message_id, page=1)
        else:
            logger.warning("Deleted medicine but no message/inline_message_id found.")
    else:
        # ... (обработка ошибки удаления - лекарство не найдено) ...
        logger.warning(f"Medicine ID '{med_id}' (Name: '{med_name}') for user {user_id} not found for confirmed deletion (or invalid ID).")
        await callback.answer("Не удалось удалить. Препарат не найден.", show_alert=True)
        if inline_message_id:
             await view_medicine_details(med_id, user_id, inline_message_id=inline_message_id) # Покажет ошибку "не найдено"
        elif chat_id and message_id:
            await _show_user_medicine_list(user_id, chat_id, message_id, page=1)



# --- Edit Medicine Flow ---
# --- ИЗМЕНЕНИЕ: Обрабатываем колбэки от обычных и inline сообщений ---
@dp.callback_query(MedAction.filter(F.action == "edit"), StateFilter(None))
async def edit_medicine_field_start(callback: types.CallbackQuery, callback_data: MedAction, state: FSMContext):
    """Начинает FSM для редактирования выбранного поля (из списка или inline)."""
    user_id = callback.from_user.id
    med_id = callback_data.item_id
    field = callback_data.field
    chat_id = callback.message.chat.id if callback.message else None
    message_id = callback.message.message_id if callback.message else None
    inline_message_id = callback.inline_message_id

    if not med_id or not field: # ... обработка ошибки ...
         logger.error(f"Missing med_id or field in edit callback: {callback_data}"); await callback.answer("Ошибка данных!", show_alert=True); return
    if not ( (chat_id and message_id) or inline_message_id ): # ... обработка ошибки ID ...
         logger.error(f"Missing message/inline ID for edit request. C/M ID: {chat_id}/{message_id}, Inline ID: {inline_message_id}"); await callback.answer("Ошибка запроса!", show_alert=True); return

    # Получаем имя лекарства по ID
    try:
        object_id = ObjectId(med_id)
        medicine = await med_collection.find_one({"_id": object_id, "added_by": user_id}, {"name": 1})
        if not medicine: raise ValueError("Medicine not found")
        med_name = medicine.get("name", "???")
    except Exception as e: # ... обработка ошибки получения имени ...
        logger.error(f"Failed to get medicine name for edit start. ID: {med_id}, User: {user_id}, Error: {e}"); await callback.answer("Не удалось найти лекарство!", show_alert=True); return

    field_rus_map = { "name": "название", "quantity": "количество", "notes": "примечания", "exp_date": "срок годности" }
    prompt_text = f"✏️ Редактирование '{med_name}'.\n\nВведите новое значение для поля '{field_rus_map.get(field, field)}':"

    reply_markup = get_cancel_keyboard()

    # Сохраняем ID и тип сообщения в state
    state_update_data = {
        "med_id": med_id, "med_name": med_name, "field_to_edit": field, "user_id": user_id,
        "prompt_chat_id": chat_id, "prompt_message_id": message_id, "inline_message_id": inline_message_id,
        "calendar_message_id": message_id if message_id else inline_message_id # ID для календаря/отмены
    }

    if field == "exp_date":
        prompt_text = f"✏️ Редактирование '{med_name}'.\n\nУкажите новый срок годности (ГГГГ-ММ-ДД) или выберите дату:"
        today = datetime.date.today()
        try:
            calendar_markup = await create_calendar(today.year, today.month)
            reply_markup = calendar_markup
            state_update_data.update(calendar_year=today.year, calendar_month=today.month)
        except Exception as e: logger.error(f"Failed to create calendar for editing: {e}")

    # Редактируем исходное сообщение
    edit_success = await safe_edit_message(text=prompt_text, chat_id=chat_id, message_id=message_id, inline_message_id=inline_message_id, reply_markup=reply_markup)

    if edit_success:
        await state.set_state(EditMedicine.waiting_for_new_value)
        await state.update_data(**state_update_data)
        await callback.answer("Введите новое значение...")
    else: # ... обработка ошибки редактирования сообщения ...
         logger.error(f"Failed to prompt for new value when editing. C/M ID: {chat_id}/{message_id}, Inline ID: {inline_message_id}"); await callback.answer("Не удалось начать редактирование.", show_alert=True)


# --- Calendar Handlers ---
@dp.callback_query(CalendarNav.filter(F.action.in_({"prev_year", "prev_month", "next_month", "next_year"})), StateFilter("*")) # Работает в любом состоянии FSM
async def handle_calendar_change(callback: types.CallbackQuery, callback_data: CalendarNav, state: FSMContext):
    """Обрабатывает навигацию по календарю."""
    current_year = callback_data.year
    current_month = callback_data.month

    new_year, new_month = current_year, current_month

    if callback_data.action == "prev_year": new_year -= 1
    elif callback_data.action == "next_year": new_year += 1
    elif callback_data.action == "prev_month":
        new_month -= 1
        if new_month == 0: new_month = 12; new_year -= 1
    elif callback_data.action == "next_month":
        new_month += 1
        if new_month == 13: new_month = 1; new_year += 1

    today_year = datetime.date.today().year
    if not (today_year - 5 <= new_year <= today_year + 10):
         await callback.answer("Доступны только ближайшие годы.", show_alert=True)
         return

    # --- ИЗМЕНЕНИЕ: Определяем, какое сообщение редактировать ---
    state_data = await state.get_data()
    inline_message_id = state_data.get("inline_message_id")
    chat_id = state_data.get("prompt_chat_id")
    message_id = state_data.get("calendar_message_id") # ID сообщения с календарем

    if not inline_message_id and not (chat_id and message_id):
         logger.error(f"Cannot find message ID to update calendar in state: {state_data}")
         await callback.answer("Ошибка отображения календаря.", show_alert=True)
         return

    try:
        new_markup = await create_calendar(new_year, new_month)
        await safe_edit_message(
            text=callback.message.text if callback.message else "Выберите дату:", # Текст может отличаться для inline
            chat_id=chat_id,
            message_id=message_id,
            inline_message_id=inline_message_id,
            reply_markup=new_markup
        )
        if await state.get_state() is not None:
             await state.update_data(calendar_year=new_year, calendar_month=new_month)
        await callback.answer(f"Календарь: {calendar.month_name[new_month]} {new_year}")
    except Exception as e:
        logger.error(f"Error updating calendar view: {e}")
        await callback.answer("Ошибка обновления календаря.", show_alert=True)


@dp.callback_query(CalendarNav.filter(F.action == "select_day"), StateFilter("*"))
async def handle_date_select(callback: types.CallbackQuery, callback_data: CalendarNav, state: FSMContext):
    # ... (логика получения даты, проверка на прошлое) ...
    year=callback_data.year; month=callback_data.month; day=callback_data.day; user_id=callback.from_user.id
    if day is None: return await callback.answer("Ошибка даты.", show_alert=True)
    selected_date = datetime.date(year, month, day)
    if selected_date < datetime.date.today(): return await callback.answer("Срок в прошлом.", show_alert=True)
    await callback.answer(f"Выбрана дата: {selected_date.strftime('%Y-%m-%d')}")

    await state.update_data(exp_date=selected_date.isoformat())
    current_state_str = await state.get_state()
    state_data = await state.get_data()

    # Получаем все ID из state
    prompt_chat_id = state_data.get("prompt_chat_id")
    # ID сообщения с календарем (может быть int или str)
    calendar_message_id = state_data.get("calendar_message_id")
    inline_msg_id = state_data.get("inline_message_id") # ID inline, если редактирование было из inline
    # Определяем ID для редактирования сообщения/сохранения
    chat_id_to_use = prompt_chat_id # Может быть None
    # message_id_to_use будет либо ID обычного сообщения, либо None
    message_id_to_use = calendar_message_id if isinstance(calendar_message_id, int) else None
    # inline_id_to_use будет либо ID inline сообщения, либо None
    inline_id_to_use = inline_msg_id # Используем inline_message_id из state, если он есть

    if current_state_str == AddMedicine.waiting_for_exp_date.state:
        # Добавление (только обычные сообщения)
        if chat_id_to_use and message_id_to_use:
             await _save_new_medicine(user_id, state, chat_id_to_use, message_id_to_use)
        else: logger.error("Missing chat_id/message_id for saving new med: {}", state_data); await state.clear()

    elif current_state_str == EditMedicine.waiting_for_new_value.state:
        # Редактирование
        med_id = state_data.get("med_id")
        field_to_edit = state_data.get("field_to_edit")
        if not med_id: # ... обработка ошибки med_id ...
             logger.error("Missing med_id for calendar edit save: {}", state_data); await state.clear(); return

        if field_to_edit == "exp_date":
            await _save_edited_medicine(
                user_id=user_id, med_id=med_id, field_to_edit=field_to_edit,
                new_value=selected_date, update_data_dict={"exp_date": selected_date.isoformat()},
                state=state,
                chat_id=chat_id_to_use, # ID чата исходного сообщения
                message_id=message_id_to_use, # ID обычного исходного сообщения
                inline_message_id=inline_id_to_use # ID inline исходного сообщения
            )
        else: # ... обработка ошибки: не то поле редактируется ...
            logger.error(f"Calendar date selected while editing field '{field_to_edit}' for med_id '{med_id}'")
            await state.clear()
            # Попытка вернуть меню
            if inline_id_to_use: await show_main_menu_message(inline_message_id=inline_id_to_use)
            elif chat_id_to_use and message_id_to_use: await show_main_menu_message(chat_id=chat_id_to_use, message_id=message_id_to_use)

    else: # ... обработка неожиданного состояния ...
        logger.warning(f"Calendar date selected in unexpected state: {current_state_str}")
        await state.clear()
        # Попытка вернуть меню
        if inline_id_to_use: await show_main_menu_message(inline_message_id=inline_id_to_use)
        elif callback.message: await show_main_menu_message(chat_id=callback.message.chat.id, message_id=callback.message.message_id)


@dp.callback_query(CalendarNav.filter(F.action == "ignore"), StateFilter("*"))
async def handle_calendar_ignore(callback: types.CallbackQuery):
    """Обрабатывает нажатие на неактивные кнопки календаря (заголовок, дни недели)."""
    await callback.answer() # Просто отвечаем, ничего не делаем

# --- Cancel Action Button ---
@dp.callback_query(F.data == "cancel_action", StateFilter("*"))
async def cancel_action_callback(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает нажатие inline-кнопки Отмена в FSM."""
    current_state = await state.get_state()
    if current_state is None:
         # Если нет состояния, но кнопка есть, просто вернем в меню
         if callback.message:
             await show_main_menu_message(chat_id=callback.message.chat.id, message_id=callback.message.message_id)
         elif callback.inline_message_id:
             await show_main_menu_message(inline_message_id=callback.inline_message_id)
         await callback.answer()
         return

    logger.info(f"Cancelling state {current_state} via callback button for user {callback.from_user.id}")

    # --- ИЗМЕНЕНИЕ: Получаем ID из state, т.к. callback может быть от кнопки на календаре ---
    user_data = await state.get_data()
    chat_id = user_data.get("prompt_chat_id")
    # ID сообщения, где была кнопка (календарь или приглашение)
    message_id = user_data.get("calendar_message_id") or user_data.get("prompt_message_id")
    inline_message_id = user_data.get("inline_message_id")

    await state.clear()
    # Возвращаем пользователя в главное меню, редактируя сообщение с кнопкой Отмена
    if inline_message_id:
        await show_main_menu_message(inline_message_id=inline_message_id)
    elif chat_id and message_id:
        await show_main_menu_message(chat_id=chat_id, message_id=message_id)
    else:
         logger.warning("Could not determine which message to edit after cancel callback.")
         # В крайнем случае просто отвечаем на колбэк
         await callback.answer("Действие отменено.")
         return # Не вызываем answer() дважды

    await callback.answer("Действие отменено.")


# -----------------------------
# Reminder Task (без изменений)
# -----------------------------
@aiocron.crontab('0 9 * * *') # Каждый день в 9:00
async def daily_reminder():
    """Находит лекарства с истекающим/истекшим сроком для КАЖДОГО пользователя и отправляет ему напоминание."""
    days_threshold = 30
    today = datetime.date.today()
    upcoming_limit = today + datetime.timedelta(days=days_threshold)
    max_len = 4096

    try:
        distinct_user_ids = await med_collection.distinct("added_by")
        if not distinct_user_ids:
            logger.info("No users with medicines found for reminders.")
            return

        logger.info(f"Checking reminders for {len(distinct_user_ids)} users.")

        for user_id in distinct_user_ids:
            logger.debug(f"Checking reminders for user {user_id}...")
            reminder_texts = []

            # Expiring soon
            query_soon = {
                "added_by": user_id,
                "exp_date": {"$gte": today.isoformat(), "$lte": upcoming_limit.isoformat()}
            }
            expiring_soon = await med_collection.find(query_soon).sort("exp_date").to_list(length=None)
            if expiring_soon:
                reminder_text_soon = f"⚠️ Напоминание: Срок годности следующих ваших лекарств истекает в ближайшие {days_threshold} дней:\n"
                for med in expiring_soon:
                    try:
                        exp_date_obj = datetime.date.fromisoformat(med['exp_date'])
                        days_left = (exp_date_obj - today).days
                        days_str = f"(осталось {days_left} дн.)" if days_left >= 0 else ""
                        reminder_text_soon += f"- *{med['name']}*: `{med['exp_date']}` {days_str}\n"
                    except Exception:
                         reminder_text_soon += f"- *{med['name']}*: `{med['exp_date']}`\n"
                reminder_texts.append(reminder_text_soon)

            # Expired
            query_expired = {
                "added_by": user_id,
                "exp_date": {"$lt": today.isoformat()}
            }
            expired = await med_collection.find(query_expired).sort("exp_date").to_list(length=None)
            if expired:
                reminder_text_expired = f"🚨 Внимание: Срок годности следующих ваших лекарств истек:\n"
                for med in expired:
                    reminder_text_expired += f"- *{med['name']}*: `{med['exp_date']}`\n"
                reminder_texts.append(reminder_text_expired)

            # Send message
            if reminder_texts:
                full_message = "\n".join(reminder_texts)
                logger.info(f"Sending expiration reminders to user {user_id}.")
                try:
                    # Split message if too long
                    for i in range(0, len(full_message), max_len):
                         chunk = full_message[i:i+max_len]
                         # --- Добавлено: Проверка на пустой чанк ---
                         if chunk.strip(): # Отправляем только если чанк не пустой
                              await bot.send_message(user_id, chunk, parse_mode="Markdown")
                              await asyncio.sleep(0.2) # Небольшая пауза между чанками
                except Exception as e:
                    logger.error(f"Failed to send reminder to user {user_id}: {e}. Message chunk starts with: {chunk[:100] if 'chunk' in locals() else 'N/A'}...")
            else:
                logger.debug(f"No medicines expiring/expired found for user {user_id}.")

    except Exception as e:
        logger.error(f"An error occurred during the daily reminder task: {e}")


async def on_shutdown_global_client(bot: Bot):
     logger.warning('Shutting down..')
     logger.info("Closing MongoDB connection...")
     client.close()
     logger.info("MongoDB connection closed.")
     logger.warning('Bye!')

async def set_bot_commands(bot: Bot):
    """Устанавливает список команд для кнопки Menu."""
    commands = [
        BotCommand(command="/start", description="🏠 Перезапустить / Главное меню"),
        BotCommand(command="/help", description="❓ Помощь по боту"),
        BotCommand(command="/list", description="💊 Показать список лекарств"),
        BotCommand(command="/add", description="➕ Добавить новое лекарство"),
        BotCommand(command="/cancel", description="❌ Отменить текущее действие"),
    ]
    try:
        await bot.set_my_commands(commands)
        logger.info("Bot commands set successfully.")
    except Exception as e:
        logger.error(f"Failed to set bot commands: {e}")


async def main():
    dp.shutdown.register(on_shutdown_global_client)
    await create_db_indexes()
    await set_bot_commands(bot)
    logger.info("Deleting any existing webhook configuration...")
    
    await bot.delete_webhook(drop_pending_updates=True)
    logger.info("Webhook deleted.")
    logger.info("Starting Medicine Inventory Bot...")
    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Polling stopped.")


if __name__ == "__main__":
    import sys
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    # logger.add(sys.stderr, level="DEBUG", colorize=True, format="...") # Для отладки
    logger.add("bot.log", rotation="1 MB", level="INFO")

    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.warning("Bot stopped by user (KeyboardInterrupt/SystemExit)")