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

API_TOKEN = '7945899429:AAEvshSutPoVX5OaesH6FMUc24uOSfL1P8A'  # Замените на ваш токен
ADMIN_USERNAME = '@tutAdminnnIliZakazhik'  # Замените на Telegram username администратора для оплаты

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
    waiting_for_quantity = State()

admin_ids = [6521061663]  # Замените на свои ID

categories = []
products = []

user_carts = {}  # {user_id: {product_id: {quantity, price, name}}}

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
        f"🛠️ Название: {product.get('_NAME_', 'Нет названия')}\n"
        f"🔖 Артикул: {product.get('_SKU_', 'Нет артикула')}\n"
        f"💰 Цена: {product.get('_PRICE_', 'Нет цены')} ₽\n"
        f"📦 В наличии: {product.get('_QUANTITY_', '0')} шт."
    )

def get_product_keyboard(product_id, quantity_available):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🛒 Добавить в корзину",
            callback_data=f"add_{product_id}_{quantity_available}"
        )]
    ])

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="Запрос одного артикула"),
                KeyboardButton(text="Запрос нескольких артикулов"),
                KeyboardButton(text="🛒 Корзина")
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие"
    )
    await message.answer("Привет! Что хотите сделать?", reply_markup=kb)

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id in admin_ids:
        kb = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="Загрузить категории")],
                [KeyboardButton(text="Загрузить продукты")],
                [KeyboardButton(text="Статистика")]
            ],
            resize_keyboard=True,
            input_field_placeholder="Выберите действие"
        )
        await message.answer("Админ-панель. Что хотите сделать?", reply_markup=kb)
    else:
        await message.answer("У вас нет прав для доступа к админ-панели.")

@dp.message(F.text == "Загрузить категории")
async def load_categories(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("Отправьте CSV-файл с категориями.")
        await state.set_state(UploadStates.waiting_for_categories)
    else:
        await message.answer("У вас нет прав для этого действия.")

@dp.message(F.text == "Загрузить продукты")
async def load_products(message: types.Message, state: FSMContext):
    if message.from_user.id in admin_ids:
        await message.answer("Отправьте CSV-файл с продуктами.")
        await state.set_state(UploadStates.waiting_for_products)
    else:
        await message.answer("У вас нет прав для этого действия.")

@dp.message(F.text == "Статистика")
async def show_stats(message: types.Message):
    if message.from_user.id in admin_ids:
        await message.answer(f"Загружено категорий: {len(categories)}\nЗагружено продуктов: {len(products)}")
    else:
        await message.answer("У вас нет прав для этого действия.")

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

        await message.answer(f"Загружено {len(categories)} категорий.")
        await state.clear()
    except Exception as e:
        await message.answer(f"Ошибка при обработке файла категорий: {e}")
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

        await message.answer(f"Загружено {len(products)} продуктов.")
        await state.clear()
    except Exception as e:
        await message.answer(f"Ошибка при обработке файла продуктов: {e}")
        await state.clear()

@dp.message(F.text == "Запрос одного артикула")
async def start_single_article(message: types.Message, state: FSMContext):
    await message.answer("Введите артикул для поиска информации и фото товара:")
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
        await message.answer(text, reply_markup=get_product_keyboard(product_id, quantity_available))

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
                    await message.answer_photo(photo=img_url, caption=caption)
                else:
                    await message.answer("⚠️ Фото для этого товара не найдено на странице.")
            except Exception as e:
                await message.answer(f"❌ Ошибка при загрузке фото: {str(e)[:50]}")
        else:
            await message.answer("⚠️ URL товара не найден в данных.")
    else:
        await message.answer(f"❌ Товар с артикулом '{raw_query}' не найден.")

    await state.clear()

@dp.callback_query(F.data.startswith("add_"))
async def add_to_cart(callback: types.CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) != 3:
        await callback.answer("Ошибка данных, попробуйте снова.")
        return
    product_id, quantity_available_str = parts[1], parts[2]
    try:
        quantity_available = int(quantity_available_str)
    except ValueError:
        await callback.answer("Ошибка данных, попробуйте снова.")
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
        await callback.message.answer(f"Введите количество (макс. {quantity_available} шт.):")
        await state.set_state(OrderQuantity.waiting_for_quantity)
    else:
        await callback.answer("Товар не найден.")

@dp.message(OrderQuantity.waiting_for_quantity)
async def process_quantity(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_id = data.get('product_id')
    quantity_available = data.get('quantity_available')
    price = data.get('price')
    name = data.get('name')

    if product_id is None or quantity_available is None or price is None or name is None:
        await message.answer("Произошла ошибка, попробуйте добавить товар заново.")
        await state.clear()
        return

    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("Пожалуйста, введите число.")
        return

    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"Некорректное количество. Введите от 1 до {quantity_available}:")
        return

    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}

    user_carts[user_id][product_id] = {
        'quantity': quantity,
        'price': price,
        'name': name
    }
    await message.answer(f"Добавлено {quantity} шт. в корзину!")

    await state.clear()

@dp.message(F.text == "Запрос нескольких артикулов")
async def start_multiple_articles(message: types.Message, state: FSMContext):
    await message.answer("Отправьте Excel-файл с артикулами (один артикул в ячейке, в любом столбце):\nГлавное чтобы не повторялись артикулы, иначе не сработает")
    await state.set_state(MultipleArticlesStates.waiting_for_file)

@dp.message(MultipleArticlesStates.waiting_for_file, F.document)
async def process_multiple_articles_file(message: types.Message, state: FSMContext):
    try:
        if not message.document.file_name.lower().endswith(('.xlsx', '.xls')):
            await message.answer("Пожалуйста, отправьте файл в формате Excel (.xlsx или .xls).")
            return

        file_id = message.document.file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        file_content = await bot.download_file(file_path)
        raw_data = file_content.read()

        df = pd.read_excel(io.BytesIO(raw_data), dtype=str)
        skus_raw = df.values.flatten()
        skus = [normalize_sku(sku) for sku in skus_raw if str(sku).strip() != '']

        if not skus:
            await message.answer("В файле не найдено артикулов.")
            await state.clear()
            return

        await state.update_data(skus=skus, index=0)
        await process_next_article(message.chat.id, state)

    except Exception as e:
        await message.answer(f"Ошибка при обработке файла: {str(e)[:100]}")
        await state.clear()

async def process_next_article(chat_id, state: FSMContext):
    data = await state.get_data()
    skus = data.get('skus', [])
    index = data.get('index', 0)

    if index >= len(skus):
        await bot.send_message(chat_id, "Все товары обработаны. Вот содержимое вашей корзины:")
        user_id = (await bot.get_chat(chat_id)).id
        await show_cart_by_user_id(user_id)
        await state.clear()
        return

    sku = skus[index]
    product = next((p for p in products if normalize_sku(p.get('_SKU_', '')) == sku), None)
    if product:
        text = format_product_info(product)
        await bot.send_message(chat_id, f"Товар {index + 1} из {len(skus)}:\n\n{text}\n\nВведите количество для добавления в корзину (макс. {product.get('_QUANTITY_', 0)}):")
        await state.update_data(current_product_id=product.get('_ID_'), current_quantity_available=int(product.get('_QUANTITY_', 0)))
        await state.set_state(MultipleArticlesStates.waiting_for_quantity)
    else:
        await bot.send_message(chat_id, f"Товар с артикулом '{sku}' не найден. Пропускаем.")
        await state.update_data(index=index + 1)
        await process_next_article(chat_id, state)

@dp.message(MultipleArticlesStates.waiting_for_quantity)
async def process_quantity_multiple(message: types.Message, state: FSMContext):
    data = await state.get_data()
    product_id = data.get('current_product_id')
    quantity_available = data.get('current_quantity_available')
    index = data.get('index', 0)
    skus = data.get('skus', [])

    try:
        quantity = int(message.text)
    except ValueError:
        await message.answer("Пожалуйста, введите число.")
        return

    if quantity <= 0 or quantity > quantity_available:
        await message.answer(f"Некорректное количество. Введите число от 1 до {quantity_available}.")
        return

    user_id = message.from_user.id
    if user_id not in user_carts:
        user_carts[user_id] = {}

    product = next((p for p in products if str(p['_ID_']) == str(product_id)), None)
    if not product:
        await message.answer("Ошибка: товар не найден.")
        await state.clear()
        return

    price = parse_price(product.get('_PRICE_', '0'))
    name = product.get('_NAME_', 'Без названия')

    if product_id in user_carts[user_id]:
        user_carts[user_id][product_id]['quantity'] += quantity
    else:
        user_carts[user_id][product_id] = {
            'quantity': quantity,
            'price': price,
            'name': name
        }

    await message.answer(f"Добавлено {quantity} шт. в корзину!")

    await state.update_data(index=index + 1)
    await process_next_article(message.chat.id, state)

async def show_cart_by_user_id(user_id: int):
    if user_id not in user_carts or not user_carts[user_id]:
        await bot.send_message(user_id, "🛒 Корзина пуста")
        return

    total = 0
    cart_text = "🛒 *Ваша корзина:*\n\n"

    for product_id, item in user_carts[user_id].items():
        quantity = item['quantity']
        price = item['price']
        name = item['name']

        total += price * quantity
        cart_text += (
            f"🔖 {name}\n"
            f"📦 Количество: {quantity}\n"
            f"💰 Цена за 1 шт.: {price} ₽\n"
            f"💰 Сумма: {price * quantity} ₽\n\n"
        )

    cart_text += f"💵 *Итого к оплате:* {total} ₽\n\n"
    

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚖 Оформить заказ", callback_data="checkout")]
    ])

    await bot.send_message(user_id, cart_text, reply_markup=kb, parse_mode="Markdown")

@dp.message(F.text == "🛒 Корзина")
async def show_cart(message: types.Message):
    await show_cart_by_user_id(message.from_user.id)

@dp.callback_query(F.data == "clear_cart")
async def clear_cart(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in user_carts:
        del user_carts[user_id]
    await callback.message.edit_text("🛒 Корзина очищена")

@dp.callback_query(F.data == "checkout")
async def start_checkout(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите ваш номер телефона для связи:")
    await state.set_state(OrderQuantity.waiting_for_contact)

@dp.message(OrderQuantity.waiting_for_contact)
async def process_contact(message: types.Message, state: FSMContext):
    raw_contact = message.text.strip()
    if not raw_contact.startswith('+'):
        raw_contact = '+' + raw_contact
    if not re.fullmatch(r'\+\d{7,15}', raw_contact):
        await message.answer("❌ Неверный формат номера. Введите номер телефона цифрами, например: 9123456789 или +79123456789")
        return
    await state.update_data(contact=raw_contact)
    await message.answer("Введите полный адрес доставки (город, улица, дом, квартира):")
    await state.set_state(OrderQuantity.waiting_for_address)

@dp.message(OrderQuantity.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    address = message.text.strip()
    if len(address) < 10:
        await message.answer("Пожалуйста, введите полный адрес доставки (минимум 10 символов):")
        return
    data = await state.get_data()
    user_id = message.from_user.id
    order_text = (
        "📦 *Новый заказ!*\n\n"
        f"👤 Пользователь: @{message.from_user.username or message.from_user.full_name}\n"
        f"📞 Контакт: {data['contact']}\n"
        f"🏠 Адрес: {address}\n\n"
        "Список товаров:\n"
    )
    total = 0
    for product_id, item in user_carts.get(user_id, {}).items():
        quantity = item['quantity']
        price = item['price']
        name = item['name']
        total += price * quantity
        order_text += f"- {name} x{quantity} ({price} ₽)\n"
    order_text += f"\n💵 Итого: {total} ₽"
    for admin_id in admin_ids:
        await bot.send_message(
            chat_id=admin_id,
            text=order_text,
            parse_mode="Markdown"
        )
    if user_id in user_carts:
        del user_carts[user_id]
    await message.answer(f"✅ Заказ оформлен! С вами свяжутся для подтверждения.\nДля оформления и оплаты заказа напишите:\n{ADMIN_USERNAME}")
    await state.clear()

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, skip_updates=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен")
