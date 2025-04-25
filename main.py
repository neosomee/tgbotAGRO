import asyncio
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

API_TOKEN = '7834135496:AAFqhUG_mbgV_03-bmxzJGaNxQUgaQ1Olak'
ADMIN_USERNAME = '@lprost'  

logging.basicConfig(level=logging.INFO)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

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

admin_ids = [6521061663]  

categories = []
products = []

user_carts = {}  

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

def get_main_menu_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🔍 Запрос одного артикула"),
                KeyboardButton(text="📋 Запрос нескольких артикулов"),
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
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Оформить заказ"), KeyboardButton(text="Очистить корзину")],
            [KeyboardButton(text="🏠 Основное меню")]
        ],
        resize_keyboard=True
    )


def split_message(text, max_length=4096):
    """Разбивает текст на части, не превышающие max_length символов."""
    parts = []
    while len(text) > max_length:
        split_pos = text.rfind('\n', 0, max_length)
        if split_pos == -1:
            split_pos = max_length
        parts.append(text[:split_pos])
        text = text[split_pos:]
    parts.append(text)
    return parts

async def send_message_in_parts(message: types.Message, text: str, **kwargs):
    """Отправляет сообщение частями, если оно превышает лимит Telegram."""
    for part in split_message(text):
        await message.answer(part, **kwargs)
    
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    text = (
        "👋 Привет! Добро пожаловать в наш Агроснайпер бот.\n\n"
        "Вот что ты можешь сделать:\n"
        "1️⃣ *🔍 Запрос одного артикула* — введи артикул, чтобы получить информацию и фото товара.\n"
        "2️⃣ *📋 Запрос нескольких артикулов* — отправь Excel-файл с артикулами и количеством, и я сразу добавлю товары в корзину.\n"
        "3️⃣ *🛒 Корзина* — здесь ты можешь посмотреть добавленные товары, изменить количество или оформить заказ.\n"
        "4️⃣ *👨‍💻 Связь с поддержкой* — контакты менеджера, если нужна помощь.\n\n"
        "🔹 После каждого действия у тебя будет кнопка *🏠 Основное меню* для быстрого возврата сюда.\n"
        "🔹 Чтобы добавить товар в корзину, после запроса артикула нажми на кнопку \"🛒 Добавить в корзину\" и укажи количество.\n"
        "🔹 Для оформления заказа перейди в корзину и следуй инструкциям.\n\n"
        "Если возникнут вопросы — пиши в раздел связи с поддержкой.\n\n"
        "Желаем приятных покупок! 🛍️"
    )
    await send_message_in_parts(message, text, parse_mode="Markdown", reply_markup=get_main_menu_keyboard())

@dp.message(F.text == "🏠 Основное меню")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 Главное меню. Выберите действие:",
        reply_markup=get_main_menu_keyboard()
    )

@dp.message(F.text == "👨‍💻 Связь с поддержкой")
async def contact_support(message: types.Message):
    text = (
        "📞 *Ваш менеджер:* Николаенко Александр\n"
        "📧 *Электронная почта:* hourtone@gmail.com\n"
        "📱 *Телефон:* +7 999 123-45-67"
    )
    await send_message_in_parts(message, text, parse_mode="Markdown", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id in admin_ids:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📂 Загрузить категории")],
                [KeyboardButton(text="📦 Загрузить продукты")],
                [KeyboardButton(text="📊 Статистика")],
                [KeyboardButton(text="🏠 Основное меню")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие"
        )
        await message.answer("🛠️ Админ-панель. Что хотите сделать?", reply_markup=kb)
    else:
        await message.answer("❌ У вас нет прав для доступа к админ-панели.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "📂 Загрузить категории")
async def load_categories(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("📁 Отправьте CSV-файл с категориями.", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(UploadStates.waiting_for_categories)
    else:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "📦 Загрузить продукты")
async def load_products(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("📁 Отправьте CSV-файл с продуктами.", reply_markup=get_back_to_main_menu_keyboard())
        await state.set_state(UploadStates.waiting_for_products)
    else:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "📊 Статистика")
async def show_stats(message: types.Message):
    if message.from_user.id in admin_ids:
        await message.answer(f"📈 Загружено категорий: {len(categories)}\n📈 Загружено продуктов: {len(products)}", reply_markup=get_back_to_main_menu_keyboard())
    else:
        await message.answer("❌ У вас нет прав для этого действия.", reply_markup=get_back_to_main_menu_keyboard())

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

        await message.answer(f"✅ Загружено {len(categories)} категорий.", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла категорий: {e}", reply_markup=get_back_to_main_menu_keyboard())
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

        await message.answer(f"✅ Загружено {len(products)} продуктов.", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла продуктов: {e}", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()

@dp.message(F.text == "🔍 Запрос одного артикула")
async def start_single_article(message: types.Message, state: FSMContext):
    await message.answer("✏️ Введите артикул для поиска информации и фото товара:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(UserStates.waiting_for_article_request)

@dp.message(UserStates.waiting_for_article_request)
async def handle_article_request(message: types.Message, state: FSMContext):
    raw_query = message.text.strip()
    query_no_dots = raw_query.replace('.', '')

    product = next(
        (p for p in products if normalize_sku(p.get('_SKU_', '')) in [raw_query, query_no_dots]),
        None
    )

    if product:
        text = format_product_info(product)
        quantity_available = int(product.get('_QUANTITY_', 0))
        product_id = product.get('_ID_')
        await send_message_in_parts(message, text, reply_markup=get_product_keyboard(product_id, quantity_available), parse_mode='Markdown')

        product_url = product.get('_URL_')
        if product_url:
            try:
                response = requests.get(product_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                img_tag = soup.select_one('.product-image img') or soup.select_one('.product-page img')

                if not img_tag:
                    img_tags = soup.find_all('img')
                    for tag in img_tags:
                        src = tag.get('src', '')
                        if query_no_dots in src or 'product' in src.lower():
                            img_tag = tag
                            break

                if img_tag and img_tag.get('src'):
                    img_url = img_tag['src']
                    if not img_url.startswith('http'):
                        img_url = urljoin(product_url, img_url)

                    caption = f"🖼 Фото товара:\n{product.get('_NAME_', '')}\n🔖 Артикул: {product.get('_SKU_', '')}"
                    await message.answer_photo(photo=img_url, caption=caption, reply_markup=get_back_to_main_menu_keyboard())
                else:
                    await message.answer("⚠️ Фото для этого товара не найдено на странице.", reply_markup=get_back_to_main_menu_keyboard())
            except Exception as e:
                await message.answer(f"❌ Ошибка при загрузке фото: {str(e)[:50]}", reply_markup=get_back_to_main_menu_keyboard())
        else:
            await message.answer("⚠️ URL товара не найден в данных.", reply_markup=get_back_to_main_menu_keyboard())
    else:
        await message.answer(f"❌ Товар с артикулом '{raw_query}' не найден.", reply_markup=get_back_to_main_menu_keyboard())

    await state.clear()

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
        await message.answer("❌ Произошла ошибка, попробуйте добавить товар заново.", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
        return

    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("⚠️ Пожалуйста, введите число.", reply_markup=get_back_to_main_menu_keyboard())
        return

    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"⚠️ Некорректное количество. Введите от 1 до {quantity_available}:", reply_markup=get_back_to_main_menu_keyboard())
        return

    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}

    user_carts[user_id][product_id] = {
        'quantity': quantity,
        'price': price,
        'name': name
    }

    await message.answer(f"✅ Добавлено {quantity} шт. в корзину!", reply_markup=get_back_to_main_menu_keyboard())

    await state.clear()

@dp.message(F.text == "📋 Запрос нескольких артикулов")
async def start_multiple_articles(message: types.Message, state: FSMContext):
    await message.answer("📤 Отправьте Excel-файл с артикулами и количеством.\n\n"
                         "Формат: в первом столбце артикул, во втором — количество.", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(MultipleArticlesStates.waiting_for_file)

@dp.message(MultipleArticlesStates.waiting_for_file, F.document)
async def process_multiple_articles_file(message: types.Message, state: FSMContext):
    try:
        if not message.document.file_name.lower().endswith(('.xlsx', '.xls')):
            await message.answer("❗ Пожалуйста, отправьте файл в формате Excel (.xlsx или .xls).", reply_markup=get_back_to_main_menu_keyboard())
            return

        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()

        df = pd.read_excel(io.BytesIO(raw_data), dtype=str)

        if df.shape[1] < 3:
            await message.answer("❗ В файле должно быть минимум 3 столбца: Артикул, Название, Количество.", reply_markup=get_back_to_main_menu_keyboard())
            return

        user_id = message.from_user.id
        if user_id not in user_carts:
            user_carts[user_id] = {}

        added_items = []
        total_added = 0
        total_price = 0.0

        for index, row in df.iterrows():
            try:
                sku = normalize_sku(str(row[0]))
                file_name = str(row[1])
                quantity = int(row[2])

                product = next((p for p in products if normalize_sku(p.get('_SKU_', '')) == sku), None)
                if product:
                    price = parse_price(product.get('_PRICE_', '0'))
                    product_id = product.get('_ID_')
                    quantity_available = int(product.get('_QUANTITY_', 0))
                    add_qty = min(quantity, quantity_available)
                    if add_qty > 0:
                        user_carts[user_id][product_id] = {
                            'quantity': add_qty,
                            'price': price,
                            'name': product.get('_NAME_', file_name)
                        }
                        added_items.append(
                            f"✅ {file_name} (артикул: {sku}) — {add_qty} шт. по {price} ₽"
                        )
                        total_added += add_qty
                        total_price += add_qty * price
                    else:
                        added_items.append(
                            f"⚠️ {file_name} (артикул: {sku}) — нет в наличии"
                        )
                else:
                    added_items.append(
                        f"❌ {file_name} (артикул: {sku}) — не найден"
                    )
            except Exception as e:
                added_items.append(
                    f"❗ Ошибка в строке {index+2}: {e}"
                )

        summary_text = (
            "📝 *Результат обработки файла:*\n\n" +
            "\n".join(added_items) +
            f"\n\nИтого добавлено: {total_added} шт. на сумму {total_price:.2f} ₽"
        )

        # Отправляем результат частями, чтобы не превышать лимит Telegram
        await send_message_in_parts(message, summary_text, parse_mode="Markdown", reply_markup=get_back_to_main_menu_keyboard())

        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла: {e}", reply_markup=get_back_to_main_menu_keyboard())
        await state.clear()
    
@dp.message(F.text == "🛒 Корзина")
async def show_cart_handler(message: types.Message):
    await show_cart(message)

async def show_cart(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Ваша корзина пуста.", reply_markup=get_back_to_main_menu_keyboard())
        return

    cart = user_carts[user_id]
    total_price = 0.0
    cart_text_lines = ["🛒 *Содержимое вашей корзины:*\n"]

    for product_id, item in cart.items():
        name = item['name']
        quantity = item['quantity']
        price = item['price']
        item_price = quantity * price
        total_price += item_price
        cart_text_lines.append(f"• {name} — {quantity} шт. по {price:.2f} ₽ = {item_price:.2f} ₽")

    cart_text_lines.append(f"\n💰 *Итого:* {total_price:.2f} ₽")
    cart_text = "\n".join(cart_text_lines)
    kb = get_cart_keyboard()
    await send_message_in_parts(message, cart_text, reply_markup=kb, parse_mode="Markdown")

@dp.message(F.text == "Очистить корзину")
async def clear_cart_handler(message: types.Message):
    user_id = message.from_user.id
    if user_id in user_carts:
        user_carts[user_id] = {}
    await message.answer("🗑️ Ваша корзина была очищена.", reply_markup=get_back_to_main_menu_keyboard())

@dp.message(F.text == "Оформить заказ")
async def start_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_carts or not user_carts[user_id]:
        await message.answer("🛒 Ваша корзина пуста, невозможно оформить заказ.", reply_markup=get_back_to_main_menu_keyboard())
        return

    await message.answer("📞 Пожалуйста, укажите ваш контактный номер телефона для связи:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(OrderQuantity.waiting_for_contact)

@dp.message(OrderQuantity.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    contact = message.text
    await state.update_data(contact=contact)
    await message.answer("🚚 Теперь, пожалуйста, укажите ваш адрес доставки:", reply_markup=get_back_to_main_menu_keyboard())
    await state.set_state(OrderQuantity.waiting_for_address)

@dp.message(OrderQuantity.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    address = message.text
    user_data = await state.get_data()
    contact = user_data.get('contact', 'Не указан')

    user_id = message.from_user.id
    cart = user_carts[user_id]
    total_price = 0.0
    order_details = []

    for product_id, item in cart.items():
        name = item['name']
        quantity = item['quantity']
        price = item['price']
        item_price = quantity * price
        total_price += item_price
        order_details.append(f"• {name} — {quantity} шт. по {price:.2f} ₽ = {item_price:.2f} ₽")

    order_summary = "\n".join(order_details)
    order_text = (
        "🎉 *Новый заказ:*\n\n"
        f"{order_summary}\n\n"
        f"💰 *Общая сумма заказа:* {total_price:.2f} ₽\n\n"
        f"📞 *Контактный номер:* {contact}\n"
        f"🚚 *Адрес доставки:* {address}\n\n"
        f"Пользователь: {message.from_user.username} ({message.from_user.id})"
    )

    for admin_id in admin_ids:
        try:
            for part in split_message(order_text):
                await bot.send_message(admin_id, part, parse_mode="Markdown")
        except Exception as e:
            await message.answer(f"❌ Ошибка при отправке заказа администратору: {e}", reply_markup=get_back_to_main_menu_keyboard())
            await state.clear()
            return

    await message.answer(f"✅ Ваш заказ принят. Напишите менеджеру и перешлите это сообщение {ADMIN_USERNAME}", reply_markup=get_back_to_main_menu_keyboard())
    user_carts[user_id] = {}
    await state.clear()

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
