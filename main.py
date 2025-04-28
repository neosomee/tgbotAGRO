import asyncio
import math
import logging
import re
import pandas as pd
import chardet
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
import io
from aiogram.enums.parse_mode import ParseMode
import idna
from aiogram.types.input_file import BufferedInputFile
from db import Database
import logging
import sys

logging.basicConfig(level=logging.INFO)

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

API_TOKEN = '7626300396:AAHxkGqY2GnarCEoxVlm9IfS-MCAfvG6fSM'
ADMIN_USERNAME = '@lprost'

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

MAX_ROWS_PER_FILE = 1000

class OrderQuantity(StatesGroup):
    waiting_for_quantity = State()
    waiting_for_contact = State()
    waiting_for_address = State()

class UploadStates(StatesGroup):
    waiting_for_categories = State()
    waiting_for_products = State()

class UserStates(StatesGroup):
    waiting_for_article_request = State()
    waiting_for_multiple_articles_file = State()

class MultipleArticlesStates(StatesGroup):
    waiting_for_file = State()

class OrderStates(StatesGroup):
    waiting_for_contact = State()
    waiting_for_address = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast_content = State()
    waiting_for_categories = State()
    waiting_for_products = State()

db = Database()


admin_ids = [5056594883, 6521061663]

categories = []
products = []

user_carts = {}



def remove_keyboard():
    return ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)

def get_cart_confirmation_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🛒 Перейти в корзину")],
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )

def get_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🔍 Запрос одного артикула"),
                KeyboardButton(text="📊 Просчёт Excel с артикулами"),
            ],
            [
                KeyboardButton(text="🛒 Корзина"),
                KeyboardButton(text="👨‍💻 Связь с поддержкой")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие"
    )

def get_back_to_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )

def get_product_keyboard(product_id, quantity_available):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🛒 Добавить в корзину",
            callback_data=f"add_{product_id}_{quantity_available}"
        )]
    ])


def get_cart_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🗑 Очистить корзину"), KeyboardButton(text="✅ Оформить заказ")],
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )
    return keyboard


def split_message(text, max_length=4096):
    parts = []
    while len(text) > max_length:
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:]
    parts.append(text)
    return parts

import re

def normalize_article(article) -> str:
    """Нормализация артикула: удаление спецсимволов и приведение к верхнему регистру"""
    if not article:
        return ''
    
    article = str(article).strip().upper()
    
    # Удаление всех специальных символов (можно расширять список)
    for char in (' ', '-', '.', '/', '\\', '_', ':', ','):
        article = article.replace(char, '')
    
    return article

def find_product_by_article(article_query: str, products: list, use_cache=True):
    """Поиск товара с возможностью кэширования"""
    norm_query = normalize_article(article_query)
    
    if use_cache:
        if not hasattr(find_product_by_article, '_cache'):
            find_product_by_article._cache = {normalize_article(p['_SKU_']): p for p in products}
        return find_product_by_article._cache.get(norm_query)
    
    return next((p for p in products if normalize_article(p['_SKU_']) == norm_query), None)

def parse_price(price_str):
    try:
        price_clean = str(price_str).replace(' ', '').replace(',', '.')
        return float(price_clean)
    except:
        return 0.0

def normalize_sku(sku: str):
    return str(sku).replace('.', '').strip()

def format_product_info(product):
    return (
        f"🛠️ *Название:* {product.get('_NAME_', 'Нет названия')}\n"
        f"🔖 *Артикул:* {product.get('_SKU_', 'Нет артикула')}\n"
        f"💰 *Цена:* {product.get('_PRICE_', 'Нет цены')} ₽\n"
        f"📦 *В наличии:* {product.get('_QUANTITY_', '0')} шт."
    )

async def send_message_in_parts(message: types.Message, text: str, **kwargs):
    for part in split_message(text):
        await message.answer(part, **kwargs)

async def show_cart(message: types.Message):
    user_id = message.from_user.id
    await message.answer("⏳ Формирую файл с вашей корзиной...", reply_markup=get_back_to_main_menu_keyboard())

    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Корзина пуста", reply_markup=get_back_to_main_menu_keyboard())
        return

    rows = []
    total_sum = 0.0

    for product_id, item in user_carts[user_id].items():
        sku = ''
        product = next((p for p in products if str(p.get('_ID_')) == str(product_id)), None)
        if product:
            sku = product.get('_SKU_', '')
        name = item['name']
        quantity = item['quantity']
        price = item['price']
        sum_price = price * quantity
        total_sum += sum_price
        rows.append({
            "Артикул": sku,
            "Название": name,
            "Количество": quantity,
            "Цена за единицу (₽)": price,
            "Сумма (₽)": sum_price
        })

    df = pd.DataFrame(rows)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Корзина')

    output.seek(0)

    doc = BufferedInputFile(output.read(), filename="Корзина.xlsx")

    await bot.send_document(chat_id=user_id, document=doc, caption=f"🛒 Ваша корзина. Итого: {total_sum:.2f} ₽")

    await bot.send_message(chat_id=user_id, text="Выберите действие:", reply_markup=get_cart_keyboard())

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user = message.from_user
    # Добавляем пользователя в базу (если его там нет)
    await db.add_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
    )
    original_url = "https://агроснайпер.рф/image/catalog/logoagro3.png"
    punycode_domain = idna.encode("агроснайпер.рф").decode()
    photo_url = original_url.replace("агроснайпер.рф", punycode_domain)

    caption = (
        "👋 Привет! Добро пожаловать в наш Агроснайпер бот.\n"
        "Сайт: Агроснайпер.рф\n\n"
        "Вот что ты можешь сделать:\n"
        "1️⃣ *🔍 Запрос одного артикула* - введи артикул, чтобы получить информацию и фото товара.\n"
        "2️⃣ *📊 Просчёт Excel с артикулами* - отправь Excel-файл с артикулами и количеством, и я сразу добавлю товары в корзину.\n"
        "3️⃣ *🛒 Корзина* - здесь ты можешь посмотреть добавленные товары, изменить количество или оформить заказ.\n"
        "4️⃣ *👨‍💻 Связь с поддержкой* - контакты менеджера, если нужна помощь.\n\n"
        "🔹 После каждого действия у тебя будет кнопка *🏠 Основное меню* для быстрого возврата сюда.\n"
        "🔹 Чтобы добавить товар в корзину, после запроса артикула нажми на кнопку \"🛒 Добавить в корзину\" и укажи количество.\n"
        "🔹 Для оформления заказа перейди в корзину и следуй инструкциям.\n\n"
        "Если возникнут вопросы - пиши в раздел связи с поддержкой.\n\n"
        "Желаем приятных покупок! 🛍️"
    )

    await message.answer_photo(
        photo=photo_url,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(F.text == "🏠 Основное меню")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Главное меню. Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )

def get_support_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Написать менеджеру",
                url="https://t.me/lprost"  
            )
        ]
    ])

@dp.message(F.text == "👨‍💻 Связь с поддержкой")
async def contact_support(message: types.Message):
    text = (
        "📞 *Ваш менеджер:* Николаенко Александр\n"
        "📧 *Электронная почта:* hourtone@gmail.com\n"
        "📱 *Телефон:* +7 999 123-45-67\n\n"
        "Сайт: Агроснайпер.рф"
    )
    await send_message_in_parts(
        message,
        text,
        parse_mode="Markdown",
        reply_markup=get_support_inline_keyboard()
    )

def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📂 Загрузить категории")],
            [KeyboardButton(text="📦 Загрузить продукты")],
            [KeyboardButton(text="📊 Статистика")],
            [KeyboardButton(text="📢 Рассылка сообщений")],
            [KeyboardButton(text="🏠 Выход в основное меню")]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие"
    )

@dp.message(Command("admin"))
async def admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await state.set_state(None)  # Сбрасываем все состояния
        await message.answer(
            "🛠️ Админ-панель. Что хотите сделать?", 
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer("❌ У вас нет прав для доступа к админ-панели.")

@dp.message(F.text == "🏠 Выход в основное меню")
async def exit_admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await state.clear()
        await message.answer(
            "✅ Вы вышли из админ-панели",
            reply_markup=get_main_menu_keyboard()
        )
    else:
        await message.answer("❌ У вас нет прав для этого действия.")

# Модифицированные хэндлеры для админ-действий
@dp.message(F.text == "📢 Рассылка сообщений")
async def start_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.")
        return
    
    await message.answer(
        "✉️ Отправьте сообщение для рассылки...",
        reply_markup=remove_keyboard()  
    )
    await state.set_state(AdminStates.waiting_for_broadcast_content)

@dp.message(AdminStates.waiting_for_broadcast_content)
async def process_broadcast_content(message: types.Message, state: FSMContext):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.")
        await state.clear()
        return

    await message.answer("⏳ Начинаю рассылку...")

    users = await db.get_all_users()
    success_count = 0
    fail_count = 0

    # Определяем тип сообщения и параметры отправки
    if message.photo:
        photo = message.photo[-1].file_id
        caption = message.caption or ""
        send_func = bot.send_photo
        send_kwargs = {"photo": photo, "caption": caption}
    elif message.video:
        video = message.video.file_id
        caption = message.caption or ""
        send_func = bot.send_video
        send_kwargs = {"video": video, "caption": caption}
    elif message.text:
        send_func = bot.send_message
        send_kwargs = {"text": message.text}
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения. Пожалуйста, отправьте текст, фото или видео.")
        await state.clear()
        return

    # Вспомогательная функция для отправки одному пользователю с обработкой ошибок
    async def send_to_user(user_id):
        nonlocal success_count, fail_count
        try:
            await send_func(chat_id=user_id, **send_kwargs)
            success_count += 1
            await asyncio.sleep(0.05)  # Таймаут между отправками
        except Exception as e:
            print(f"Ошибка при отправке пользователю {user_id}: {e}")
            fail_count += 1

    # Создаем задачи для параллельной отправки
    tasks = [send_to_user(user_id) for user_id, _ in users]

    # Запускаем все задачи параллельно
    await asyncio.gather(*tasks)

    await message.answer(
        f"✅ Рассылка завершена!\nУспешно: {success_count}\nНе удалось: {fail_count}",
        reply_markup=get_admin_keyboard()
    )
    await state.set_state(None)



@dp.message(F.text == "📂 Загрузить категории")
async def load_categories(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer(
            "📁 Отправьте CSV-файл с категориями",
            reply_markup=get_admin_keyboard()
        )
        # Используем UploadStates вместо AdminStates
        await state.set_state(UploadStates.waiting_for_categories)
    else:
        await message.answer("❌ У вас нет прав для этого действия.")

@dp.message(F.text == "📦 Загрузить продукты")
async def load_products(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("📁 Отправьте CSV-файл с продуктами.", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(UploadStates.waiting_for_products)
    else:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "📊 Статистика")
async def show_stats(message: types.Message):
    if message.from_user.id not in admin_ids:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())
        return

    users = await db.get_all_users()
    users_count = len(users)

    await message.answer(
        f"📈 Количество пользователей в боте: {users_count}\n"
        f"📈 Загружено категорий: {len(categories)}\n"
        f"📈 Загружено продуктов: {len(products)}",
        reply_markup=get_admin_keyboard()
    )


@dp.message(UploadStates.waiting_for_categories, F.document)
async def process_categories_file(message: types.Message, state: FSMContext):
    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding'] or 'utf-8'

        df = pd.read_csv(io.BytesIO(raw_data), sep=';', encoding=encoding, header=0)
        df.columns = df.columns.str.strip('"').str.strip()

        global categories
        categories = df.to_dict('records')

        await message.answer(f"✅ Загружено {len(categories)} категорий.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла категорий: {e}", reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(UploadStates.waiting_for_products, F.document)
async def process_products_file(message: types.Message, state: FSMContext):
    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding'] or 'utf-8'

        df = pd.read_csv(io.BytesIO(raw_data), sep=';', encoding=encoding, header=0)
        df.columns = df.columns.str.strip('"').str.strip()

        global products
        products = df.to_dict('records')

        await message.answer(f"✅ Загружено {len(products)} продуктов",reply_markup=get_admin_keyboard())
        await state.set_state(None)
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла продуктов: {e}", reply_markup=get_admin_keyboard())
        await state.clear()

@dp.message(F.text == "🔍 Запрос одного артикула")
async def start_single_article(message: types.Message, state: FSMContext):
    await message.answer("✏️ Введите артикул для поиска информации и фото товара:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(UserStates.waiting_for_article_request)

class UserStates(StatesGroup):
    waiting_for_article_request = State()
    article_requested_once = State()  # новое состояние - после первого запроса

@dp.message(UserStates.waiting_for_article_request)
async def handle_article_request(message: types.Message, state: FSMContext):
    text = message.text.strip()

    # Спецкоманды
    if text == "🏠 Основное меню":
        await state.clear()
        await message.answer("Вы в основном меню.", reply_markup=get_main_menu_keyboard())
        return

    if text == "🛒 Перейти в корзину":
        await handle_cart_button(message)
        return

    if text == "🗑 Очистить корзину":
        user_id = message.from_user.id
        if user_id in user_carts:
            user_carts[user_id].clear()
            await message.answer("🗑 Корзина очищена.", reply_markup=get_main_menu_keyboard())
        else:
            await message.answer("🗑 Корзина уже пуста.", reply_markup=get_main_menu_keyboard())
        return
    
    if text == "✅ Оформить заказ":
        # Передаем управление в хэндлер оформления заказа
        await checkout(message, state)
        return

    # Дальше - поиск по артикулу
    raw_query = text
    norm_query = normalize_article(raw_query)
    
    product = next((p for p in products if normalize_article(p.get('_SKU_', '')) == norm_query), None)

    if product:
        # Отправка фото
        product_url = product.get('_URL_')
        photo_sent = False
        
        if product_url:
            try:
                response = requests.get(product_url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                img_tag = (soup.select_one('.product-image img') or 
                          soup.select_one('.product-page img') or
                          next((tag for tag in soup.find_all('img') 
                              if norm_query in normalize_article(tag.get('src', ''))), None))
                
                if img_tag and img_tag.get('src'):
                    img_url = img_tag['src']
                    if not img_url.startswith('http'):
                        img_url = urljoin(product_url, img_url)
                    
                    await message.answer_photo(
                        photo=img_url,
                        caption=f"🖼 {product.get('_NAME_', '')}"
                    )
                    photo_sent = True
            except Exception as e:
                await message.answer("⚠️ Не удалось загрузить фото товара")

        # Отправка информации о товаре
        text = format_product_info(product)
        quantity = int(product.get('_QUANTITY_', 0))
        product_id = product.get('_ID_')
        
        await send_message_in_parts(
            message,
            text,
            reply_markup=get_product_keyboard(product_id, quantity),
            parse_mode='Markdown'
        )
        
        # Предложение ввести следующий артикул
        await message.answer(
            "➡️ Введите следующий артикул или вернитесь в меню",
            reply_markup=get_back_to_main_menu_keyboard()
        )
    else:
        await message.answer(
            f"❌ Товар '{raw_query}' не найден. Проверьте артикул",
            reply_markup=get_cart_keyboard()
        )

  
@dp.callback_query(F.data.startswith("add_"))
async def add_to_cart(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("❌ Ошибка данных, попробуйте снова.")
        return
    product_id, quantity_available_str = parts[1], parts[2]
    try:
        quantity_available = int(quantity_available_str)
    except ValueError:
        await callback.answer("❌ Ошибка данных, попробуйте снова.")
        return

    product = next((p for p in products if str(p['_ID_']) == product_id), None)
    if product:
        price = parse_price(product.get('_PRICE_', '0'))
        product_data = {
            'product_id': product_id,
            'quantity_available': quantity_available,
            'price': price,
            'name': product.get('_NAME_', 'Без названия')
        }
        await state.update_data(**product_data)
        await callback.message.answer(f"✏️ Введите количество (макс. {quantity_available} шт.):", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(OrderQuantity.waiting_for_quantity)
    else:
        await callback.answer("❌ Товар не найден.")

@dp.message(OrderQuantity.waiting_for_quantity)
async def process_quantity(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_id = data.get('product_id')
    quantity_available = data.get('quantity_available')
    price = data.get('price')
    name = data.get('name')

    if product_id is None or quantity_available is None or price is None or name is None:
        await message.answer("❌ Произошла ошибка, попробуйте добавить товар заново.", 
                           reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
        return

    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("⚠️ Пожалуйста, введите число.", 
                           reply_markup=get_back_to_main_menu_keyboard())
        return

    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"⚠️ Некорректное количество. Введите от 1 до {quantity_available}:",
                           reply_markup=get_back_to_main_menu_keyboard())
        return

    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}

    user_carts[user_id][product_id] = {
        'quantity': quantity,
        'price': price,
        'name': name
    }

    # После добавления товара предлагаем ввести следующий артикул или перейти в корзину/меню
    await message.answer(
        f"✅ Добавлено {quantity} шт. в корзину!\n\n"
        "Введите следующий артикул для поиска или выберите действие ниже.",
        reply_markup=get_cart_keyboard()  # Клавиатура с кнопками
    )

    # Возвращаем в состояние ожидания артикула
    await state.set_state(UserStates.waiting_for_article_request)


@dp.message(F.text == "🛒 Перейти в корзину")
async def handle_cart_button(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Ваша корзина пуста", 
                           reply_markup=get_main_menu_keyboard())
        return
    
    # Здесь должна быть ваша логика отображения корзины
    cart_text = "🛒 Ваша корзина:\n\n"
    total = 0
    
    for product_id, item in user_carts[user_id].items():
        product_total = item['quantity'] * item['price']
        cart_text += f"▪ {item['name']}\n"
        cart_text += f"Количество: {item['quantity']} × {item['price']} ₽ = {product_total} ₽\n\n"
        total += product_total
    
    cart_text += f"Итого: {total} ₽"
    
    await message.answer(
        cart_text,
        reply_markup=get_main_menu_keyboard()  # Или другая клавиатура для корзины
    )

@dp.message(F.text == "📊 Просчёт Excel с артикулами")
async def start_multiple_articles(message: types.Message, state: FSMContext):
    await message.answer(
        "📤 Отправьте Excel-файл с артикулами, названием (можно оставить пустым) и количеством.\n\n"
        "Формат: в первом столбце артикул, во втором - название, в третьем - количество.",
        reply_markup=get_back_to_main_menu_keyboard()
    )
    await state.set_state(MultipleArticlesStates.waiting_for_file)

MAX_ROWS_PER_FILE = 1000

@dp.message(MultipleArticlesStates.waiting_for_file, F.document)
async def process_multiple_articles_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer("⏳ Обрабатываю файл, добавляю товары в корзину...", reply_markup=get_back_to_main_menu_keyboard())

    try:
        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()

        df = pd.read_excel(io.BytesIO(raw_data), dtype=str)

        if df.shape[1] < 3:
            await message.answer("❗ В файле должно быть минимум 3 столбца: Артикул, Название, Количество.", reply_markup=get_back_to_main_menu_keyboard())
            return

        rows = []
        total_sum = 0.0
        total_added_quantity = 0

        if user_id not in user_carts:
            user_carts[user_id] = {}

        for index, row in df.iterrows():
            try:
                sku = normalize_sku(str(row[0]))
                file_name = str(row[1]).strip() if not pd.isna(row[1]) else ''
                quantity_str = str(row[2]).strip()

                if not sku or not quantity_str.isdigit():
                    continue

                quantity = int(quantity_str)

                product = next((p for p in products if normalize_sku(p.get('_SKU_', '')) == sku), None)
                if product:
                    product_id = product.get('_ID_')
                    price = parse_price(product.get('_PRICE_', '0'))
                    available = int(product.get('_QUANTITY_', 0))
                    name = file_name if file_name else product.get('_NAME_', 'Без названия')

                    quantity_to_add = min(quantity, available)

                    if product_id in user_carts[user_id]:
                        user_carts[user_id][product_id]['quantity'] += quantity_to_add
                    else:
                        user_carts[user_id][product_id] = {
                            'quantity': quantity_to_add,
                            'price': price,
                            'name': name
                        }

                    sum_price = price * quantity_to_add
                    total_sum += sum_price
                    total_added_quantity += quantity_to_add

                    rows.append({
                        "Артикул": sku,
                        "Название": name,
                        "Количество (запрошено)": quantity,
                        "Количество (добавлено)": quantity_to_add,
                        "Цена": price,
                        "Доступно": available,
                        "Сумма": sum_price,
                        "Статус": "Добавлено"
                    })
                else:
                    rows.append({
                        "Артикул": sku,
                        "Название": file_name,
                        "Количество (запрошено)": quantity,
                        "Количество (добавлено)": 0,
                        "Цена": "Не найдено",
                        "Доступно": "Не найдено",
                        "Сумма": 0,
                        "Статус": "Не найден"
                    })

            except Exception as e:
                logging.exception("Ошибка при обработке строки")

        if not rows:
            await message.answer("⚠️ В файле не найдено ни одного артикула из базы.", reply_markup=get_back_to_main_menu_keyboard())
            await state.clear()
            return

        df_result = pd.DataFrame(rows)
        num_files = math.ceil(len(df_result) / MAX_ROWS_PER_FILE)

        for i in range(num_files):
            start = i * MAX_ROWS_PER_FILE
            end = min((i + 1) * MAX_ROWS_PER_FILE, len(df_result))
            part_df = df_result.iloc[start:end]

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                part_df.to_excel(writer, index=False, sheet_name='Результаты')
                worksheet = writer.sheets['Результаты']
                worksheet.set_column('A:A', 20)
                worksheet.set_column('B:B', 40)
                worksheet.set_column('C:D', 18)
                worksheet.set_column('E:G', 15)
                worksheet.set_column('H:H', 15)

            output.seek(0)
            filename = f"Результаты_поиска_часть_{i+1}.xlsx"
            doc = BufferedInputFile(output.read(), filename=filename)
            await bot.send_document(chat_id=user_id, document=doc, caption=f"Результаты поиска (часть {i+1}/{num_files})", reply_markup=get_back_to_main_menu_keyboard())

        await message.answer(f"✅ Добавлено товаров в корзину: {total_added_quantity} на сумму {total_sum:.2f} ₽", reply_markup=get_back_to_main_menu_keyboard())
        await show_cart(message)  # сразу показываем корзину
        await state.clear()

    except Exception as e:
        logging.exception("Ошибка при обработке файла")
        await message.answer(f"❌ Ошибка при обработке файла: {e}", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()

@dp.message(F.text == "🛒 Корзина")
async def show_cart(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_carts and user_carts[user_id]:
        cart_items = user_carts[user_id]

        await message.answer("⏳ Формирую файл с вашей корзиной…")

        data = []
        total_sum = 0.0
        for product_id, product_info in cart_items.items():
            row = {
                "Артикул": product_id,
                "Название": product_info['name'],
                "Количество": product_info['quantity'],
                "Цена за шт.": product_info['price'],
                "Сумма": product_info['price'] * product_info['quantity']
            }
            total_sum += row["Сумма"]
            data.append(row)

        df = pd.DataFrame(data)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Корзина', index=False)
            writer.close()
        output.seek(0)

        await bot.send_document(
            chat_id=message.chat.id,
            document=types.BufferedInputFile(output.read(), filename="Корзина.xlsx"),
            caption="Ваша корзина в файле"
        )

        await message.answer(f"🛒 Итого: {total_sum:.2f} ₽", reply_markup=get_cart_keyboard())
    else:
        await message.answer("🛒 Ваша корзина пуста.", reply_markup=get_main_menu_keyboard())

# Обработчик кнопки "Очистить корзину"
@dp.message(F.text == "🗑 Очистить корзину")
async def clear_cart(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_carts:
        user_carts[user_id].clear()
    await message.answer("🛒 Корзина очищена.", reply_markup=get_main_menu_keyboard())

# Обработчик кнопки "Оформить заказ"
@dp.message(F.text == "✅ Оформить заказ")
async def checkout(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Ваша корзина пуста. Добавьте товары перед оформлением заказа.", reply_markup=get_main_menu_keyboard())
        return
    await message.answer("📞 Введите ваш номер телефона для связи:", reply_markup=get_main_menu_keyboard())
    await state.set_state(OrderStates.waiting_for_contact)

# Обработчик ввода телефона
@dp.message(OrderStates.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    contact = message.text.strip()
    # Простейшая валидация телефона (можно улучшить)
    if len(contact) < 5:
        await message.answer("❌ Введите корректный номер телефона:")
        return
    await state.update_data(contact=contact)
    await message.answer("📍 Введите адрес доставки:")
    await state.set_state(OrderStates.waiting_for_address)

# Обработчик ввода адреса
@dp.message(OrderStates.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    address = message.text.strip()
    data = await state.get_data()
    contact = data.get("contact", "Не указан")
    user_id = message.from_user.id

    cart_items = user_carts.get(user_id, {})
    if not cart_items:
        await message.answer("🛒 Ваша корзина пуста.", reply_markup=get_main_menu_keyboard())
        await state.clear()
        return

    total_sum = sum(item['price'] * item['quantity'] for item in cart_items.values())

    # Отправляем краткое сообщение без списка товаров
    order_summary = (
        f"✅ *Заказ оформлен!*\n\n"
        f"📞 *Контакт:* {contact}\n"
        f"🏠 *Адрес:* {address}\n\n"
        f"💰 *Итого:* {total_sum:.2f} ₽\n\n"
        "📄 Подробности заказа в прикреплённом файле."
    )
    await message.answer(order_summary, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

    # Формируем Excel-файл с заказом
    data = []
    for product_id, product_info in cart_items.items():
        data.append({
            "Название": product_info['name'],
            "Количество": product_info['quantity'],
            "Цена за шт.": product_info['price'],
            "Сумма": product_info['price'] * product_info['quantity']
        })
    df = pd.DataFrame(data)
    total_row = pd.DataFrame([{
        "Название": "Итого",
        "Количество": "",
        "Цена за шт.": "",
        "Сумма": total_sum
    }])
    df = pd.concat([df, total_row], ignore_index=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Заказ', index=False)
        writer.close()
    output.seek(0)

    # Отправляем файл с заказом
    await bot.send_document(
        chat_id=message.chat.id,
        document=types.BufferedInputFile(output.read(), filename="Заказ.xlsx")
    )

    # Отправляем кнопку для связи с менеджером
    manager_username = "lprost"  # замени на нужный username
    contact_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="📩 Связаться с менеджером",
                url=f"https://t.me/{manager_username}"
            )
        ]
    ])
    await message.answer(
        "📢 Чтобы завершить оформление заказа, напишите менеджеру.",
        reply_markup=contact_keyboard
    )

    # Очищаем корзину и состояние
    user_carts[user_id].clear()
    await state.clear()



async def main():
    logging.info("Старт бота")
    await db.connect()  # Подключаемся к базе
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, skip_updates=True)
    except asyncio.CancelledError:
        logging.info("Polling отменён")
    finally:
        logging.info("Закрываем бота и базу данных")
        await bot.session.close()
        await db.close()  # Закрываем базу

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен вручную")
