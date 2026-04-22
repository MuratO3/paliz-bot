"""
Paliz Market Bot - Агро-маркетплейс для Нукуса
Полностью рабочий код для деплоя на Render.com
"""

import sqlite3
import asyncio
import logging
import os
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(os.getenv("ADMIN_ID"))] if os.getenv("ADMIN_ID") else []

# Проверка наличия токена
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден в переменных окружения!")

logging.basicConfig(level=logging.INFO)

# ==================== БАЗА ДАННЫХ ====================

DB_NAME = "paliz.db"

def init_db():
    """Создаёт все таблицы при первом запуске"""
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        
        # ========== ТАБЛИЦА ПОЛЬЗОВАТЕЛЕЙ ==========
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                phone TEXT,
                role TEXT DEFAULT 'buyer',
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица фермеров
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS farmers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                farm_name TEXT,
                address TEXT,
                latitude REAL,
                longitude REAL,
                phone TEXT,
                work_hours TEXT,
                is_approved BOOLEAN DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Таблица категорий
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        ''')
        
        # Таблица товаров
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                farmer_id INTEGER,
                category_id INTEGER,
                name TEXT,
                description TEXT,
                price INTEGER,
                unit TEXT,
                stock REAL,
                photo_id TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (farmer_id) REFERENCES farmers (id),
                FOREIGN KEY (category_id) REFERENCES categories (id)
            )
        ''')
        
        # Таблица заказов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                farmer_id INTEGER,
                product_id INTEGER,
                quantity REAL,
                total_price INTEGER,
                delivery_method TEXT,
                address TEXT,
                phone TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (farmer_id) REFERENCES farmers (id),
                FOREIGN KEY (product_id) REFERENCES products (id)
            )
        ''')
        
        # Таблица корзины
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cart (
                user_id INTEGER,
                product_id INTEGER,
                farmer_id INTEGER,
                quantity REAL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, product_id)
            )
        ''')
        
        # Добавляем тестовые категории
        cursor.execute("SELECT COUNT(*) FROM categories")
        if cursor.fetchone()[0] == 0:
            categories = ['Овощи', 'Фрукты', 'Зелень', 'Молочные продукты', 'Хлеб и выпечка']
            for cat in categories:
                cursor.execute("INSERT INTO categories (name) VALUES (?)", (cat,))
        
        # Добавляем тестового фермера
        cursor.execute("SELECT COUNT(*) FROM farmers")
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO farmers (user_id, farm_name, address, latitude, longitude, phone, work_hours, is_approved)
                VALUES (1, 'Фермерское хозяйство "Paliz"', 'г. Нукус, ул. Каракалпакская 15', 42.4647, 59.6163, '+998901234567', '09:00 - 18:00', 1)
            ''')
        
        # Добавляем тестовые товары
        cursor.execute("SELECT COUNT(*) FROM products")
        if cursor.fetchone()[0] == 0:
            test_products = [
                (1, 1, 'Помидоры', 'Свежие, сочные помидоры', 8000, 'кг', 100, None),
                (1, 2, 'Яблоки', 'Сладкие яблоки', 12000, 'кг', 50, None),
                (1, 3, 'Укроп', 'Свежая зелень', 2000, 'пучок', 200, None),
                (1, 4, 'Молоко', 'Домашнее молоко', 15000, 'литр', 30, None),
                (1, 5, 'Лепёшка', 'Свежая лепёшка', 5000, 'шт', 100, None),
            ]
            cursor.executemany('''
                INSERT INTO products (farmer_id, category_id, name, description, price, unit, stock, photo_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', test_products)
        
        conn.commit()
        print("✅ База данных инициализирована")

# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================

def get_all_categories() -> List[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM categories ORDER BY name")
        rows = cursor.fetchall()
        return [{'id': row[0], 'name': row[1]} for row in rows]

def get_products_by_category(category_id: int) -> List[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.id, p.name, p.price, p.unit, p.photo_id, p.stock, p.description, f.farm_name, f.id as farmer_id
            FROM products p
            JOIN farmers f ON p.farmer_id = f.id
            WHERE p.category_id = ? AND p.is_active = 1 AND f.is_approved = 1
        ''', (category_id,))
        rows = cursor.fetchall()
        return [{
            'id': row[0], 'name': row[1], 'price': row[2], 'unit': row[3],
            'photo_id': row[4], 'stock': row[5], 'description': row[6],
            'farm_name': row[7], 'farmer_id': row[8]
        } for row in rows]

def get_product_by_id(product_id: int) -> Optional[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT p.id, p.name, p.price, p.unit, p.photo_id, p.stock, p.description, p.farmer_id, f.farm_name, f.address, f.latitude, f.longitude
            FROM products p
            JOIN farmers f ON p.farmer_id = f.id
            WHERE p.id = ? AND p.is_active = 1
        ''', (product_id,))
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0], 'name': row[1], 'price': row[2], 'unit': row[3],
                'photo_id': row[4], 'stock': row[5], 'description': row[6],
                'farmer_id': row[7], 'farm_name': row[8], 'address': row[9],
                'latitude': row[10], 'longitude': row[11]
            }
        return None

def get_farmer_info(farmer_id: int) -> Optional[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT farm_name, address, latitude, longitude, phone, work_hours FROM farmers WHERE id = ?', (farmer_id,))
        row = cursor.fetchone()
        if row:
            return {
                'farm_name': row[0], 'address': row[1], 'latitude': row[2],
                'longitude': row[3], 'phone': row[4], 'work_hours': row[5]
            }
        return None

def add_to_cart(user_id: int, product_id: int, farmer_id: int, quantity: float = 1):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT quantity FROM cart WHERE user_id = ? AND product_id = ?", (user_id, product_id))
        row = cursor.fetchone()
        if row:
            new_qty = row[0] + quantity
            cursor.execute("UPDATE cart SET quantity = ?, farmer_id = ? WHERE user_id = ? AND product_id = ?", 
                          (new_qty, farmer_id, user_id, product_id))
        else:
            cursor.execute("INSERT INTO cart (user_id, product_id, farmer_id, quantity) VALUES (?, ?, ?, ?)", 
                          (user_id, product_id, farmer_id, quantity))
        conn.commit()

def get_cart(user_id: int) -> List[Tuple]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.product_id, p.name, p.price, c.quantity, (p.price * c.quantity) as total, p.unit, c.farmer_id
            FROM cart c
            JOIN products p ON c.product_id = p.id
            WHERE c.user_id = ?
        ''', (user_id,))
        return cursor.fetchall()

def is_in_cart(user_id: int, product_id: int) -> bool:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM cart WHERE user_id = ? AND product_id = ?", (user_id, product_id))
        return cursor.fetchone() is not None

def clear_cart(user_id: int):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cart WHERE user_id = ?", (user_id,))
        conn.commit()

def get_cart_total(user_id: int) -> int:
    cart_items = get_cart(user_id)
    return sum(item[4] for item in cart_items)

def get_cart_farmer_id(user_id: int) -> Optional[int]:
    cart_items = get_cart(user_id)
    if cart_items:
        return cart_items[0][6]
    return None

def create_order(user_id: int, farmer_id: int, delivery_method: str, address: str = None, phone: str = None) -> int:
    cart_items = get_cart(user_id)
    if not cart_items:
        return None
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        order_ids = []
        for item in cart_items:
            product_id, name, price, quantity, total, unit, f_id = item
            cursor.execute('''
                INSERT INTO orders (user_id, farmer_id, product_id, quantity, total_price, delivery_method, address, phone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, farmer_id, product_id, quantity, total, delivery_method, address, phone))
            order_ids.append(cursor.lastrowid)
        cursor.execute("DELETE FROM cart WHERE user_id = ?", (user_id,))
        conn.commit()
        return order_ids[0] if order_ids else None

def update_order_status(order_id: int, status: str):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))
        conn.commit()

def get_user_orders(user_id: int) -> List[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT o.id, p.name, o.quantity, o.total_price, o.status, o.created_at, o.delivery_method
            FROM orders o
            JOIN products p ON o.product_id = p.id
            WHERE o.user_id = ?
            ORDER BY o.created_at DESC
        ''', (user_id,))
        rows = cursor.fetchall()
        return [{
            'id': row[0], 'product_name': row[1], 'quantity': row[2],
            'total_price': row[3], 'status': row[4], 'created_at': row[5], 'delivery_method': row[6]
        } for row in rows]

def add_user(user_id: int, username: str = None, full_name: str = None):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)', 
                      (user_id, username, full_name))
        conn.commit()

def get_user(user_id: int) -> Optional[Dict]:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        if row:
            return {'user_id': row[0], 'username': row[1], 'full_name': row[2], 
                    'phone': row[3], 'role': row[4], 'registered_at': row[5]}
        return None

# ==================== КЛАВИАТУРЫ ====================

def get_main_keyboard() -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🛒 Каталог")],
        [KeyboardButton(text="🛍️ Корзина"), KeyboardButton(text="📦 Мои заказы")],
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_categories_keyboard(categories: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for cat in categories:
        builder.button(text=cat['name'], callback_data=f"cat_{cat['id']}")
    builder.adjust(1)
    return builder.as_markup()

def get_products_keyboard(products: List[Dict], page: int = 0, items_per_page: int = 5) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * items_per_page
    end = start + items_per_page
    for product in products[start:end]:
        builder.button(text=f"{product['name']} — {product['price']} сум / {product['unit']}", 
                      callback_data=f"product_{product['id']}")
    builder.adjust(1)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"page_{page-1}"))
    if end < len(products):
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"page_{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="🔙 Назад к категориям", callback_data="back_to_categories"))
    return builder.as_markup()

def get_product_detail_keyboard(product_id: int, in_cart: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if in_cart:
        builder.button(text="✅ В корзине", callback_data="already_in_cart")
    else:
        builder.button(text="🛒 Добавить в корзину", callback_data=f"add_to_cart_{product_id}")
    builder.button(text="🔙 Назад", callback_data="back_to_products")
    builder.button(text="📍 Показать на карте", callback_data=f"show_map_{product_id}")
    builder.adjust(1)
    return builder.as_markup()

def get_cart_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Оформить заказ", callback_data="checkout")
    builder.button(text="🗑️ Очистить корзину", callback_data="clear_cart")
    builder.button(text="🔙 Продолжить покупки", callback_data="back_to_catalog")
    builder.adjust(1)
    return builder.as_markup()

def get_delivery_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📍 Самовывоз (бесплатно)", callback_data="pickup")
    builder.button(text="🚛 Доставка (5000 сум)", callback_data="delivery")
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить заказ", callback_data="confirm_order")
    builder.button(text="❌ Отмена", callback_data="cancel_order")
    builder.adjust(2)
    return builder.as_markup()

def get_pickup_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить самовывоз", callback_data="confirm_pickup")
    builder.button(text="❌ Отмена", callback_data="cancel_order")
    builder.adjust(2)
    return builder.as_markup()

def get_quantity_keyboard(product_id: int, unit: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if unit == 'кг':
        builder.button(text="0.5 кг", callback_data=f"qty_{product_id}_0.5")
        builder.button(text="1 кг", callback_data=f"qty_{product_id}_1")
        builder.button(text="2 кг", callback_data=f"qty_{product_id}_2")
        builder.button(text="3 кг", callback_data=f"qty_{product_id}_3")
        builder.button(text="5 кг", callback_data=f"qty_{product_id}_5")
    elif unit == 'пучок':
        builder.button(text="1 пучок", callback_data=f"qty_{product_id}_1")
        builder.button(text="2 пучка", callback_data=f"qty_{product_id}_2")
        builder.button(text="3 пучка", callback_data=f"qty_{product_id}_3")
        builder.button(text="5 пучков", callback_data=f"qty_{product_id}_5")
    elif unit == 'литр':
        builder.button(text="1 л", callback_data=f"qty_{product_id}_1")
        builder.button(text="2 л", callback_data=f"qty_{product_id}_2")
        builder.button(text="3 л", callback_data=f"qty_{product_id}_3")
    else:
        builder.button(text="1 шт", callback_data=f"qty_{product_id}_1")
        builder.button(text="2 шт", callback_data=f"qty_{product_id}_2")
        builder.button(text="3 шт", callback_data=f"qty_{product_id}_3")
        builder.button(text="5 шт", callback_data=f"qty_{product_id}_5")
    
    builder.button(text="✏️ Своё значение", callback_data=f"custom_qty_{product_id}")
    builder.button(text="🔙 Назад", callback_data=f"back_to_product_{product_id}")
    builder.adjust(2)
    return builder.as_markup()

# ==================== СОСТОЯНИЯ (FSM) ====================

class OrderStates(StatesGroup):
    waiting_for_delivery_method = State()
    waiting_for_address = State()
    waiting_for_phone = State()
    waiting_for_confirmation = State()

class AddToCartStates(StatesGroup):
    waiting_for_custom_quantity = State()

# ==================== ОБРАБОТЧИКИ ====================

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    full_name = message.from_user.full_name
    add_user(user_id, username, full_name)
    await message.answer(
        f"👋 Привет, {message.from_user.first_name}!\n\n"
        "Добро пожаловать в **Paliz Market** — ваш гид в мире свежих продуктов от местных фермеров!\n\n"
        "🛒 Выбирайте товары в каталоге, оформляйте заказы и получайте их с доставкой или самовывозом.\n\n"
        "Хороших вам покупок!",
        reply_markup=get_main_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(Command("help"))
@dp.message(F.text == "❓ Помощь")
async def cmd_help(message: Message):
    await message.answer(
        "❓ *Как пользоваться ботом?*\n\n"
        "• 🛒 *Каталог* — просмотр и выбор товаров\n"
        "• 🛍️ *Корзина* — оформление заказа\n"
        "• 📦 *Мои заказы* — история покупок\n"
        "• 👤 *Профиль* — ваши данные\n\n"
        "📍 *Самовывоз*: Фермерское хозяйство Paliz, г. Нукус, ул. Каракалпакская 15\n"
        "🚛 *Доставка*: 5000 сум по городу\n\n"
        "По всем вопросам: @paliz_support",
        parse_mode="Markdown"
    )

@dp.message(F.text == "🛒 Каталог")
async def show_catalog(message: Message):
    categories = get_all_categories()
    if not categories:
        await message.answer("📭 Каталог пока пуст. Загляните позже!")
        return
    await message.answer("📋 *Выберите категорию товаров:*", 
                        reply_markup=get_categories_keyboard(categories), 
                        parse_mode="Markdown")

@dp.callback_query(F.data.startswith("cat_"))
async def show_products(callback: CallbackQuery):
    category_id = int(callback.data.split("_")[1])
    products = get_products_by_category(category_id)
    if not products:
        await callback.message.edit_text("📭 В этой категории пока нет товаров.")
        await callback.answer()
        return
    await callback.message.edit_text("📋 *Список товаров:*", 
                                    reply_markup=get_products_keyboard(products, 0), 
                                    parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("page_"))
async def paginate_products(callback: CallbackQuery):
    page = int(callback.data.split("_")[1])
    categories = get_all_categories()
    if categories:
        products = get_products_by_category(categories[0]['id'])
        await callback.message.edit_reply_markup(reply_markup=get_products_keyboard(products, page))
    await callback.answer()

@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: CallbackQuery):
    categories = get_all_categories()
    await callback.message.edit_text("📋 *Выберите категорию товаров:*", 
                                    reply_markup=get_categories_keyboard(categories), 
                                    parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "back_to_products")
async def back_to_products(callback: CallbackQuery):
    categories = get_all_categories()
    if categories:
        products = get_products_by_category(categories[0]['id'])
        await callback.message.edit_text("📋 *Список товаров:*", 
                                        reply_markup=get_products_keyboard(products, 0), 
                                        parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: CallbackQuery):
    categories = get_all_categories()
    await callback.message.edit_text("📋 *Выберите категорию товаров:*", 
                                    reply_markup=get_categories_keyboard(categories), 
                                    parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("back_to_product_"))
async def back_to_product(callback: CallbackQuery):
    product_id = int(callback.data.split("_")[3])
    product = get_product_by_id(product_id)
    if product:
        in_cart = is_in_cart(callback.from_user.id, product_id)
        text = f"🍅 *{product['name']}*\n\n💰 Цена: {product['price']} сум / {product['unit']}\n📦 В наличии: {product['stock']} {product['unit']}\n🌾 Продавец: {product['farm_name']}\n📍 {product['address']}\n\n📝 {product['description'] or 'Описание отсутствует'}"
        await callback.message.edit_text(text, reply_markup=get_product_detail_keyboard(product_id, in_cart), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("product_"))
async def show_product_detail(callback: CallbackQuery):
    product_id = int(callback.data.split("_")[1])
    product = get_product_by_id(product_id)
    if not product:
        await callback.message.edit_text("❌ Товар не найден.")
        await callback.answer()
        return
    in_cart = is_in_cart(callback.from_user.id, product_id)
    text = (f"🍅 *{product['name']}*\n\n"
            f"💰 Цена: {product['price']} сум / {product['unit']}\n"
            f"📦 В наличии: {product['stock']} {product['unit']}\n"
            f"🌾 Продавец: {product['farm_name']}\n"
            f"📍 Адрес: {product['address']}\n\n"
            f"📝 {product['description'] or 'Описание отсутствует'}")
    await callback.message.edit_text(text, reply_markup=get_product_detail_keyboard(product_id, in_cart), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("show_map_"))
async def show_map(callback: CallbackQuery):
    product_id = int(callback.data.split("_")[2])
    product = get_product_by_id(product_id)
    if product and product['latitude'] and product['longitude']:
        await callback.message.answer_location(latitude=product['latitude'], longitude=product['longitude'])
        await callback.message.answer(f"📍 {product['farm_name']}\n{product['address']}")
    else:
        await callback.message.answer("📍 Координаты не указаны. Адрес для самовывоза:\n" + (product['address'] if product else "Адрес не найден"))
    await callback.answer()

@dp.callback_query(F.data.startswith("add_to_cart_"))
async def add_to_cart_start(callback: CallbackQuery):
    product_id = int(callback.data.split("_")[3])
    product = get_product_by_id(product_id)
    if not product:
        await callback.answer("Товар не найден!", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"✏️ *Сколько {product['unit']} вы хотите купить?*\n\n"
        f"Товар: {product['name']}\n"
        f"Цена: {product['price']} сум / {product['unit']}\n"
        f"Доступно: {product['stock']} {product['unit']}\n\n"
        f"Выберите количество из предложенных или введите своё:",
        reply_markup=get_quantity_keyboard(product_id, product['unit']),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("qty_"))
async def process_quantity_button(callback: CallbackQuery):
    parts = callback.data.split("_")
    product_id = int(parts[1])
    quantity = float(parts[2])
    
    product = get_product_by_id(product_id)
    if product and quantity <= product['stock']:
        add_to_cart(callback.from_user.id, product_id, product['farmer_id'], quantity)
        await callback.message.edit_text(
            f"✅ *Добавлено в корзину!*\n\n"
            f"{quantity} {product['unit']} — {product['name']}\n"
            f"Сумма: {int(product['price'] * quantity)} сум\n\n"
            f"Можете продолжить покупки или перейти в корзину.",
            reply_markup=get_main_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            f"❌ Извините, запрошенное количество ({quantity} {product['unit']}) превышает доступное ({product['stock']} {product['unit']}).",
            reply_markup=get_quantity_keyboard(product_id, product['unit']),
            parse_mode="Markdown"
        )
    await callback.answer()

@dp.callback_query(F.data.startswith("custom_qty_"))
async def custom_quantity_start(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[2])
    await state.update_data(product_id=product_id)
    await state.set_state(AddToCartStates.waiting_for_custom_quantity)
    await callback.message.answer(
        "✏️ Введите нужное количество (например: 1.5, 2, 3):\n\n"
        "Допускаются дробные числа (для кг и литров).",
        reply_markup=types.ReplyKeyboardRemove()
    )
    await callback.answer()

@dp.message(AddToCartStates.waiting_for_custom_quantity)
async def process_custom_quantity(message: Message, state: FSMContext):
    try:
        quantity = float(message.text.replace(',', '.'))
        if quantity <= 0:
            raise ValueError
        
        data = await state.get_data()
        product_id = data['product_id']
        product = get_product_by_id(product_id)
        
        if product and quantity <= product['stock']:
            add_to_cart(message.from_user.id, product_id, product['farmer_id'], quantity)
            await message.answer(
                f"✅ *Добавлено в корзину!*\n\n"
                f"{quantity} {product['unit']} — {product['name']}\n"
                f"Сумма: {int(product['price'] * quantity)} сум\n\n"
                f"Можете продолжить покупки или перейти в корзину.",
                reply_markup=get_main_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await message.answer(
                f"❌ Извините, запрошенное количество ({quantity} {product['unit']}) превышает доступное ({product['stock']} {product['unit']}).\n\n"
                f"Попробуйте снова:",
                reply_markup=get_main_keyboard()
            )
        await state.clear()
    except ValueError:
        await message.answer("❌ Пожалуйста, введите корректное число (например: 1.5, 2, 3)")

@dp.message(F.text == "🛍️ Корзина")
async def show_cart(message: Message):
    user_id = message.from_user.id
    cart_items = get_cart(user_id)
    if not cart_items:
        await message.answer("🛒 Ваша корзина пуста. Добавьте товары через каталог!")
        return
    text = "🛒 *Ваша корзина:*\n\n"
    total = 0
    for item in cart_items:
        product_id, name, price, quantity, item_total, unit, farmer_id = item
        text += f"• {name} — {quantity} {unit} × {price} сум = {item_total} сум\n"
        total += item_total
    text += f"\n💵 *Итого: {total} сум*"
    
    farmers = set(item[6] for item in cart_items)
    if len(farmers) > 1:
        text += "\n\n⚠️ *Внимание!* В вашей корзине товары от разных фермеров. Придётся оформить несколько заказов."
    
    await message.answer(text, reply_markup=get_cart_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "clear_cart")
async def clear_cart_handler(callback: CallbackQuery):
    clear_cart(callback.from_user.id)
    await callback.message.edit_text("🛒 Корзина очищена!")
    await callback.answer()

@dp.callback_query(F.data == "checkout")
async def start_checkout(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    cart_items = get_cart(user_id)
    if not cart_items:
        await callback.answer("Ваша корзина пуста!", show_alert=True)
        return
    
    await state.update_data(cart_items=cart_items)
    await state.set_state(OrderStates.waiting_for_delivery_method)
    
    await callback.message.answer(
        "🚚 *Выберите способ получения заказа:*\n\n"
        "📍 Самовывоз — бесплатно, вы забираете товар сами\n"
        "🚛 Доставка — 5000 сум по городу",
        reply_markup=get_delivery_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(OrderStates.waiting_for_delivery_method)
async def process_delivery_method(callback: CallbackQuery, state: FSMContext):
    method = callback.data
    await state.update_data(delivery_method=method)
    
    user_id = callback.from_user.id
    farmer_id = get_cart_farmer_id(user_id)
    farmer_info = get_farmer_info(farmer_id) if farmer_id else None
    
    if method == "pickup":
        await state.update_data(address=farmer_info['address'] if farmer_info else "г. Нукус, ул. Каракалпакская 15")
        
        text = "📍 *Самовывоз*\n\n"
        if farmer_info:
            text += f"🏪 {farmer_info['farm_name']}\n"
            text += f"📍 {farmer_info['address']}\n"
            text += f"🕐 Часы работы: {farmer_info['work_hours']}\n"
            text += f"📞 Телефон: {farmer_info['phone']}\n\n"
        text += "✅ Подтвердите заказ, чтобы завершить оформление."
        
        await callback.message.answer(
            text,
            reply_markup=get_pickup_confirmation_keyboard(),
            parse_mode="Markdown"
        )
        await state.set_state(OrderStates.waiting_for_confirmation)
    else:
        await state.set_state(OrderStates.waiting_for_address)
        await callback.message.answer(
            "🚚 *Доставка*\n\n"
            "Пожалуйста, введите адрес доставки:",
            parse_mode="Markdown"
        )
    await callback.answer()

@dp.message(OrderStates.waiting_for_address)
async def process_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text)
    await state.set_state(OrderStates.waiting_for_phone)
    await message.answer("📞 Введите ваш номер телефона для связи:\n\nПример: +998 90 123 45 67")

@dp.message(OrderStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    data = await state.get_data()
    cart_items = data['cart_items']
    address = data['address']
    phone = data['phone']
    delivery_method = data['delivery_method']
    
    total = sum(item[4] for item in cart_items)
    if delivery_method == "delivery":
        total += 5000
    
    text = "📝 *Проверьте ваш заказ:*\n\n"
    for item in cart_items:
        product_id, name, price, quantity, item_total, unit, farmer_id = item
        text += f"• {name} — {quantity} {unit} × {price} сум = {item_total} сум\n"
    text += f"\n💵 *Итого: {total} сум*"
    if delivery_method == "delivery":
        text += "\n(включая доставку 5000 сум)"
    text += f"\n\n🚚 Способ: {'Доставка' if delivery_method == 'delivery' else 'Самовывоз'}"
    text += f"\n📍 Адрес: {address}"
    text += f"\n📞 Телефон: {phone}"
    text += "\n\n✅ Подтверждаете заказ?"
    
    farmer_id = get_cart_farmer_id(message.from_user.id)
    await state.update_data(farmer_id=farmer_id, total=total)
    
    await message.answer(text, reply_markup=get_confirmation_keyboard(), parse_mode="Markdown")
    await state.set_state(OrderStates.waiting_for_confirmation)

@dp.callback_query(F.data == "confirm_pickup", OrderStates.waiting_for_confirmation)
async def confirm_pickup_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cart_items = data['cart_items']
    delivery_method = data.get('delivery_method', 'pickup')
    address = data.get('address', 'Самовывоз')
    phone = data.get('phone', callback.from_user.username or 'Не указан')
    farmer_id = get_cart_farmer_id(callback.from_user.id)
    total = sum(item[4] for item in cart_items)
    
    order_id = create_order(callback.from_user.id, farmer_id, delivery_method, address, phone)
    
    if order_id:
        for admin_id in ADMIN_IDS:
            await callback.bot.send_message(
                admin_id,
                f"💰 *Новый заказ!*\n\n"
                f"Номер: #{order_id}\n"
                f"Пользователь: @{callback.from_user.username or callback.from_user.first_name}\n"
                f"Сумма: {total} сум\n"
                f"Способ: Самовывоз",
                parse_mode="Markdown"
            )
        
        farmer_info = get_farmer_info(farmer_id)
        address_text = farmer_info['address'] if farmer_info else "г. Нукус, ул. Каракалпакская 15"
        
        await callback.message.edit_text(
            f"✅ *Заказ подтверждён!*\n\n"
            f"Номер заказа: #{order_id}\n"
            f"Сумма: {total} сум\n\n"
            f"📍 *Адрес самовывоза:*\n{address_text}\n\n"
            f"🕐 Часы работы: {farmer_info['work_hours'] if farmer_info else '09:00 - 18:00'}\n\n"
            f"Статус заказа можно отслеживать в разделе «Мои заказы».",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text("❌ Ошибка при создании заказа. Попробуйте снова.")
    
    await state.clear()

@dp.callback_query(F.data == "confirm_order", OrderStates.waiting_for_confirmation)
async def confirm_order_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cart_items = data['cart_items']
    delivery_method = data.get('delivery_method', 'delivery')
    address = data.get('address')
    phone = data.get('phone')
    farmer_id = get_cart_farmer_id(callback.from_user.id)
    total = sum(item[4] for item in cart_items)
    if delivery_method == "delivery":
        total += 5000
    
    order_id = create_order(callback.from_user.id, farmer_id, delivery_method, address, phone)
    
    if order_id:
        for admin_id in ADMIN_IDS:
            await callback.bot.send_message(
                admin_id,
                f"💰 *Новый заказ!*\n\n"
                f"Номер: #{order_id}\n"
                f"Пользователь: @{callback.from_user.username or callback.from_user.first_name}\n"
                f"Сумма: {total} сум\n"
                f"Способ: Доставка\n"
                f"Адрес: {address}\n"
                f"Телефон: {phone}",
                parse_mode="Markdown"
            )
        
        await callback.message.edit_text(
            f"✅ *Заказ подтверждён!*\n\n"
            f"Номер заказа: #{order_id}\n"
            f"Сумма: {total} сум\n\n"
            f"Статус заказа можно отслеживать в разделе «Мои заказы».\n\n"
            f"Доставка будет осуществлена в ближайшее время.",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text("❌ Ошибка при создании заказа. Попробуйте снова.")
    
    await state.clear()

@dp.callback_query(F.data == "cancel_order", OrderStates.waiting_for_confirmation)
async def cancel_order_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Заказ отменён.\n\nВы можете продолжить покупки.", reply_markup=get_main_keyboard())
    await state.clear()

@dp.message(F.text == "📦 Мои заказы")
async def show_orders(message: Message):
    user_id = message.from_user.id
    orders = get_user_orders(user_id)
    if not orders:
        await message.answer("📭 У вас пока нет заказов.")
        return
    status_emoji = {'pending': '⏳', 'paid': '✅', 'delivered': '🚚', 'cancelled': '❌'}
    text = "📦 *Ваши заказы:*\n\n"
    for order in orders:
        status = status_emoji.get(order['status'], '📌')
        delivery_icon = "📍" if order['delivery_method'] == 'pickup' else "🚛"
        text += (f"{status} *Заказ #{order['id']}*\n"
                f"📦 {order['product_name']} — {order['quantity']} шт\n"
                f"💰 {order['total_price']} сум\n"
                f"{delivery_icon} {order['delivery_method']}\n"
                f"🕐 {order['created_at'][:16]}\n\n")
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    user = get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ошибка: пользователь не найден.")
        return
    role_name = {'buyer': 'Покупатель', 'farmer': 'Фермер', 'admin': 'Администратор'}.get(user['role'], user['role'])
    text = (f"👤 *Ваш профиль*\n\n"
            f"🆔 ID: {user['user_id']}\n"
            f"📝 Имя: {user['full_name'] or 'Не указано'}\n"
            f"🔑 Роль: {role_name}\n"
            f"📅 Зарегистрирован: {user['registered_at'][:16]}")
    await message.answer(text, parse_mode="Markdown")

@dp.message()
async def unknown_message(message: Message):
    await message.answer(
        "❓ Я не понимаю эту команду.\n"
        "Пожалуйста, воспользуйтесь кнопками меню или отправьте /help.",
        reply_markup=get_main_keyboard()
    )

# ==================== ЗАПУСК ДЛЯ RENDER ====================

async def main():
    init_db()  # <--- ЭТА СТРОКА ДОБАВЛЯЕТСЯ!
    
    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        await bot.set_webhook(webhook_url)
        logging.info(f"✅ Webhook установлен: {webhook_url}")
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Запуск в режиме polling (локальная разработка)")
        await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
