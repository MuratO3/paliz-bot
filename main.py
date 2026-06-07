"""
Paliz Market Bot - Полная версия
С ролями, геолокацией, сменой ролей, Supabase
"""

import asyncio
import logging
import os
import uuid
import math
from datetime import datetime
from typing import List, Dict, Optional, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(os.getenv("ADMIN_ID"))] if os.getenv("ADMIN_ID") else []

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден!")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL и SUPABASE_KEY обязательны!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(level=logging.INFO)

# ==================== КЭШ ГЕОЛОКАЦИИ ПОКУПАТЕЛЕЙ ====================
user_location_cache = {}

# ==================== ФУНКЦИИ РАССТОЯНИЯ ====================

def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние между точками в км (формула гаверсинуса)"""
    if not lat1 or not lon1 or not lat2 or not lon2:
        return float('inf')
    
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(min(1, math.sqrt(a)))
    
    return round(R * c, 1)

# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================

def get_user_by_telegram_id(telegram_id: int) -> Optional[Dict]:
    try:
        result = supabase.table("users").select("*").eq("user_id", telegram_id).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logging.error(f"get_user_by_telegram_id error: {e}")
        return None

def add_user(telegram_id: int, username: str = None, full_name: str = None, role: str = 'buyer') -> Optional[Dict]:
    try:
        data = {
            "user_id": telegram_id,
            "username": username,
            "full_name": full_name,
            "role": role,
            "created_at": datetime.now().isoformat()
        }
        result = supabase.table("users").upsert(data).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        logging.error(f"add_user error: {e}")
        return None

def update_user_role(telegram_id: int, role: str) -> bool:
    try:
        supabase.table("users").update({"role": role}).eq("user_id", telegram_id).execute()
        return True
    except Exception as e:
        logging.error(f"update_user_role error: {e}")
        return False

def get_all_managers() -> List[Dict]:
    try:
        result = supabase.table("users").select("user_id, username").eq("role", "manager").execute()
        return result.data
    except Exception as e:
        logging.error(f"get_all_managers error: {e}")
        return []

def add_farmer_request(user_id: int, farm_name: str, address: str, phone: str, latitude: float, longitude: float) -> bool:
    try:
        data = {
            "user_id": user_id,
            "farm_name": farm_name,
            "address": address,
            "phone": phone,
            "latitude": latitude,
            "longitude": longitude,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        supabase.table("farmer_requests").insert(data).execute()
        return True
    except Exception as e:
        logging.error(f"add_farmer_request error: {e}")
        return False

def add_delivery_request(user_id: int, full_name: str, phone: str, vehicle_type: str, latitude: float, longitude: float) -> bool:
    try:
        data = {
            "user_id": user_id,
            "full_name": full_name,
            "phone": phone,
            "vehicle_type": vehicle_type,
            "latitude": latitude,
            "longitude": longitude,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        supabase.table("delivery_requests").insert(data).execute()
        return True
    except Exception as e:
        logging.error(f"add_delivery_request error: {e}")
        return False

def add_gardener(user_id: int, garden_name: str, address: str, phone: str, latitude: float, longitude: float) -> bool:
    try:
        data = {
            "user_id": user_id,
            "garden_name": garden_name,
            "address": address,
            "phone": phone,
            "latitude": latitude,
            "longitude": longitude
        }
        supabase.table("gardeners").upsert(data).execute()
        update_user_role(user_id, "gardener")
        return True
    except Exception as e:
        logging.error(f"add_gardener error: {e}")
        return False

def get_pending_farmer_requests() -> List[Dict]:
    try:
        result = supabase.table("farmer_requests").select("*").eq("status", "pending").execute()
        return result.data
    except Exception as e:
        logging.error(f"get_pending_farmer_requests error: {e}")
        return []

def get_pending_delivery_requests() -> List[Dict]:
    try:
        result = supabase.table("delivery_requests").select("*").eq("status", "pending").execute()
        return result.data
    except Exception as e:
        logging.error(f"get_pending_delivery_requests error: {e}")
        return []

def approve_farmer_request(request_id: int, user_id: int, farm_name: str, address: str, phone: str, latitude: float, longitude: float) -> bool:
    try:
        supabase.table("farmer_requests").update({
            "status": "approved",
            "reviewed_at": datetime.now().isoformat()
        }).eq("id", request_id).execute()
        
        farmer_data = {
            "user_id": user_id,
            "farm_name": farm_name,
            "address": address,
            "phone": phone,
            "latitude": latitude,
            "longitude": longitude,
            "is_approved": True,
            "approved_at": datetime.now().isoformat()
        }
        supabase.table("farmers").upsert(farmer_data).execute()
        update_user_role(user_id, "farmer")
        return True
    except Exception as e:
        logging.error(f"approve_farmer_request error: {e}")
        return False

def approve_delivery_request(request_id: int, user_id: int, full_name: str, phone: str, vehicle_type: str, latitude: float, longitude: float) -> bool:
    try:
        supabase.table("delivery_requests").update({
            "status": "approved",
            "reviewed_at": datetime.now().isoformat()
        }).eq("id", request_id).execute()
        
        delivery_data = {
            "user_id": user_id,
            "full_name": full_name,
            "phone": phone,
            "vehicle_type": vehicle_type,
            "latitude": latitude,
            "longitude": longitude,
            "is_approved": True,
            "approved_at": datetime.now().isoformat()
        }
        supabase.table("delivery_profiles").upsert(delivery_data).execute()
        update_user_role(user_id, "delivery")
        return True
    except Exception as e:
        logging.error(f"approve_delivery_request error: {e}")
        return False

def reject_request(table: str, request_id: int) -> bool:
    try:
        supabase.table(table).update({
            "status": "rejected",
            "reviewed_at": datetime.now().isoformat()
        }).eq("id", request_id).execute()
        return True
    except Exception as e:
        logging.error(f"reject_request error: {e}")
        return False

def get_categories() -> List[Dict]:
    try:
        result = supabase.table("categories").select("*").order("sort_order").execute()
        return result.data
    except Exception as e:
        logging.error(f"get_categories error: {e}")
        return []

def get_all_products_with_sellers() -> List[Dict]:
    """Получить все товары с полной информацией о продавце"""
    try:
        farmer_products = supabase.table("products")\
            .select("*, farmers!inner(user_id, farm_name, address, latitude, longitude, phone)")\
            .eq("is_active", True)\
            .execute()
        
        gardener_products = supabase.table("products")\
            .select("*, gardeners!inner(user_id, garden_name, address, latitude, longitude, phone)")\
            .eq("is_active", True)\
            .execute()
        
        result = []
        
        for item in farmer_products.data:
            result.append({
                "id": item["id"],
                "name": item["name"],
                "price": item["price"],
                "unit": item["unit"],
                "stock": item["stock"],
                "description": item.get("description"),
                "photo_id": item.get("photo_id"),
                "category_id": item.get("category_id"),
                "seller_user_id": item["farmers"]["user_id"],
                "seller_name": item["farmers"]["farm_name"],
                "seller_type": "farmer",
                "seller_address": item["farmers"]["address"],
                "seller_lat": item["farmers"].get("latitude"),
                "seller_lon": item["farmers"].get("longitude"),
                "seller_phone": item["farmers"]["phone"],
                "badge": "🌾 Верифицированный фермер"
            })
        
        for item in gardener_products.data:
            result.append({
                "id": item["id"],
                "name": item["name"],
                "price": item["price"],
                "unit": item["unit"],
                "stock": item["stock"],
                "description": item.get("description"),
                "photo_id": item.get("photo_id"),
                "category_id": item.get("category_id"),
                "seller_user_id": item["gardeners"]["user_id"],
                "seller_name": item["gardeners"]["garden_name"],
                "seller_type": "gardener",
                "seller_address": item["gardeners"]["address"],
                "seller_lat": item["gardeners"].get("latitude"),
                "seller_lon": item["gardeners"].get("longitude"),
                "seller_phone": item["gardeners"]["phone"],
                "badge": "🏠 Садовод (частник)"
            })
        
        return result
    except Exception as e:
        logging.error(f"get_all_products_with_sellers error: {e}")
        return []

def get_products_by_category(category_id: int) -> List[Dict]:
    all_products = get_all_products_with_sellers()
    return [p for p in all_products if p.get("category_id") == category_id]

def get_product_by_id(product_id: int) -> Optional[Dict]:
    all_products = get_all_products_with_sellers()
    return next((p for p in all_products if p["id"] == product_id), None)

def add_to_cart(user_id: int, product_id: int, quantity: float = 1) -> bool:
    try:
        existing = supabase.table("cart").select("*").eq("user_id", user_id).eq("product_id", product_id).execute()
        if existing.data:
            new_qty = existing.data[0]["quantity"] + quantity
            supabase.table("cart").update({"quantity": new_qty}).eq("user_id", user_id).eq("product_id", product_id).execute()
        else:
            data = {
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "added_at": datetime.now().isoformat()
            }
            supabase.table("cart").insert(data).execute()
        return True
    except Exception as e:
        logging.error(f"add_to_cart error: {e}")
        return False

def get_cart(user_id: int) -> List[Dict]:
    try:
        result = supabase.table("cart").select("*, products(*)").eq("user_id", user_id).execute()
        return result.data
    except Exception as e:
        logging.error(f"get_cart error: {e}")
        return []

def clear_cart(user_id: int) -> bool:
    try:
        supabase.table("cart").delete().eq("user_id", user_id).execute()
        return True
    except Exception as e:
        logging.error(f"clear_cart error: {e}")
        return False

def create_order(buyer_id: int, cart_items: List[Dict], delivery_method: str, address: str = None, phone: str = None) -> Optional[int]:
    try:
        total = sum(item["quantity"] * item["products"]["price"] for item in cart_items)
        order_number = f"PALIZ-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        
        items_json = [{
            "product_id": item["product_id"],
            "name": item["products"]["name"],
            "quantity": item["quantity"],
            "price": item["products"]["price"]
        } for item in cart_items]
        
        order_data = {
            "order_number": order_number,
            "buyer_id": buyer_id,
            "seller_id": cart_items[0]["products"].get("seller_user_id") if cart_items else None,
            "seller_type": cart_items[0]["products"].get("seller_type") if cart_items else "farmer",
            "items": items_json,
            "total_amount": total + (5000 if delivery_method == "delivery" else 0),
            "delivery_method": delivery_method,
            "delivery_address": address,
            "delivery_phone": phone,
            "delivery_fee": 5000 if delivery_method == "delivery" else 0,
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        
        result = supabase.table("orders").insert(order_data).execute()
        
        if result.data:
            clear_cart(buyer_id)
            return result.data[0]["id"]
        return None
    except Exception as e:
        logging.error(f"create_order error: {e}")
        return None

def get_user_orders(user_id: int) -> List[Dict]:
    try:
        result = supabase.table("orders").select("*").eq("buyer_id", user_id).order("created_at", desc=True).execute()
        return result.data
    except Exception as e:
        logging.error(f"get_user_orders error: {e}")
        return []

def check_admin_password(password: str) -> bool:
    try:
        result = supabase.table("settings").select("value").eq("key", "admin_password").execute()
        if result.data:
            return result.data[0]["value"] == password
        return password == "Paliz20030303m"
    except Exception as e:
        logging.error(f"check_admin_password error: {e}")
        return password == "Paliz20030303m"

def set_admin_role(telegram_id: int) -> bool:
    return update_user_role(telegram_id, "admin")

# ==================== КЛАВИАТУРЫ ====================

def get_main_keyboard(role: str = "buyer") -> ReplyKeyboardMarkup:
    if role == "farmer":
        buttons = [
            [KeyboardButton(text="📦 Мои заказы")],
            [KeyboardButton(text="➕ Добавить товар"), KeyboardButton(text="📋 Мои товары")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    elif role == "gardener":
        buttons = [
            [KeyboardButton(text="📦 Мои заказы")],
            [KeyboardButton(text="➕ Добавить товар"), KeyboardButton(text="📋 Мои товары")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    elif role == "delivery":
        buttons = [
            [KeyboardButton(text="🚚 Заказы на доставку")],
            [KeyboardButton(text="✅ Мои доставки")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    elif role == "manager":
        buttons = [
            [KeyboardButton(text="🌾 Заявки фермеров"), KeyboardButton(text="🚚 Заявки доставщиков")],
            [KeyboardButton(text="📦 Все заказы")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    elif role == "admin":
        buttons = [
            [KeyboardButton(text="⚙️ Админ панель")],
            [KeyboardButton(text="👥 Пользователи"), KeyboardButton(text="📦 Заказы")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    else:
        buttons = [
            [KeyboardButton(text="🛒 Каталог")],
            [KeyboardButton(text="🛍️ Корзина"), KeyboardButton(text="📦 Мои заказы")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="❓ Помощь")]
        ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_role_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🛒 ПОКУПАТЕЛЬ", callback_data="role_buyer")
    builder.button(text="🏠 САДОВОД (частник)", callback_data="role_gardener")
    builder.button(text="🌾 ФЕРМЕР (с проверкой)", callback_data="role_farmer")
    builder.button(text="🚚 ДОСТАВЩИК (с проверкой)", callback_data="role_delivery")
    builder.adjust(1)
    return builder.as_markup()

def get_change_role_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора роли для смены"""
    builder = InlineKeyboardBuilder()
    builder.button(text="🛒 Покупатель", callback_data="change_to_buyer")
    builder.button(text="🏠 Садовод", callback_data="change_to_gardener")
    builder.button(text="🌾 Фермер (заявка)", callback_data="change_to_farmer")
    builder.button(text="🚚 Доставщик (заявка)", callback_data="change_to_delivery")
    builder.button(text="🔙 Назад", callback_data="back_to_profile")
    builder.adjust(1)
    return builder.as_markup()

def get_location_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True
    )
    return keyboard

def get_farmer_requests_keyboard(requests: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(text=f"🌾 {req['farm_name'][:25]}", callback_data=f"fr_{req['id']}")
    builder.adjust(1)
    return builder.as_markup()

def get_delivery_requests_keyboard(requests: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(text=f"🚚 {req['full_name'][:25]}", callback_data=f"dr_{req['id']}")
    builder.adjust(1)
    return builder.as_markup()

def get_request_action_keyboard(request_id: int, request_type: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Одобрить", callback_data=f"approve_{request_type}_{request_id}")
    builder.button(text="❌ Отклонить", callback_data=f"reject_{request_type}_{request_id}")
    builder.adjust(2)
    return builder.as_markup()

def get_categories_keyboard(categories: List[Dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for cat in categories:
        builder.button(text=f"{cat.get('icon', '📦')} {cat['name']}", callback_data=f"cat_{cat['id']}")
    builder.adjust(2)
    return builder.as_markup()

def get_products_with_sellers_keyboard(products: List[Dict], page: int = 0, items_per_page: int = 5, user_lat: float = None, user_lon: float = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * items_per_page
    end = start + items_per_page
    
    for product in products[start:end]:
        text = f"{product['name']} — {product['price']}₽ / {product['unit']}"
        
        if product['seller_type'] == 'farmer':
            text = f"🌾 {text}"
        else:
            text = f"🏠 {text}"
        
        if user_lat and user_lon and product.get('seller_lat') and product.get('seller_lon'):
            dist = calculate_distance(user_lat, user_lon, product['seller_lat'], product['seller_lon'])
            if dist != float('inf'):
                text = f"{text} 📍{dist}км"
        
        builder.button(text=text[:60], callback_data=f"product_{product['id']}")
    
    builder.adjust(1)
    
    if page > 0:
        builder.button(text="◀️ Назад", callback_data=f"products_page_{page-1}")
    if end < len(products):
        builder.button(text="Вперед ▶️", callback_data=f"products_page_{page+1}")
    
    builder.button(text="📊 Сортировка", callback_data="sort_menu")
    builder.button(text="🔙 Назад к категориям", callback_data="back_to_categories")
    
    return builder.as_markup()

def get_sort_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 По цене (возрастание)", callback_data="sort_price_asc")
    builder.button(text="💰 По цене (убывание)", callback_data="sort_price_desc")
    builder.button(text="📍 По расстоянию (ближе)", callback_data="sort_distance_asc")
    builder.button(text="🌾 Фермеры сначала", callback_data="sort_farmer_first")
    builder.button(text="🏠 Садоводы сначала", callback_data="sort_gardener_first")
    builder.button(text="📛 По названию", callback_data="sort_name")
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
    builder.button(text="📍 Самовывоз (бесплатно)", callback_data="delivery_pickup")
    builder.button(text="🚛 Доставка (5000₽)", callback_data="delivery_courier")
    builder.adjust(1)
    return builder.as_markup()

def get_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить заказ", callback_data="confirm_order")
    builder.button(text="❌ Отмена", callback_data="cancel_order")
    builder.adjust(2)
    return builder.as_markup()

# ==================== СОСТОЯНИЯ ====================

class RegistrationStates(StatesGroup):
    waiting_for_role = State()
    waiting_for_farmer_name = State()
    waiting_for_farmer_address = State()
    waiting_for_farmer_phone = State()
    waiting_for_farmer_location = State()
    waiting_for_gardener_name = State()
    waiting_for_gardener_address = State()
    waiting_for_gardener_phone = State()
    waiting_for_gardener_location = State()
    waiting_for_delivery_name = State()
    waiting_for_delivery_phone = State()
    waiting_for_delivery_vehicle = State()
    waiting_for_delivery_location = State()
    waiting_for_admin_password = State()

class OrderStates(StatesGroup):
    waiting_for_delivery_method = State()
    waiting_for_address = State()
    waiting_for_phone = State()

class AddToCartStates(StatesGroup):
    waiting_for_quantity = State()

class CatalogStates(StatesGroup):
    products = State()
    current_page = State()
    sort_type = State()

# ==================== ОБРАБОТЧИКИ ====================

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

async def send_notification_to_managers(message_text: str):
    managers = get_all_managers()
    for manager in managers:
        try:
            await bot.send_message(manager["user_id"], message_text, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Ошибка уведомления: {e}")

async def show_main_menu(message: Message, user: Dict):
    role = user.get("role", "buyer")
    role_names = {
        'buyer': '🛒 Покупатель',
        'gardener': '🏠 Садовод',
        'farmer': '🌾 Фермер',
        'delivery': '🚚 Доставщик',
        'manager': '👤 Менеджер',
        'admin': '⚙️ Администратор'
    }
    welcome = f"👋 Добро пожаловать, {user.get('full_name', user.get('username', 'Пользователь'))}!\n\nВаша роль: {role_names.get(role, role)}"
    await message.answer(welcome, reply_markup=get_main_keyboard(role), parse_mode="Markdown")

# ---------- /start ----------
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user = get_user_by_telegram_id(message.from_user.id)
    if not user:
        await state.set_state(RegistrationStates.waiting_for_role)
        await message.answer(
            "🌾 *Paliz Marketga xush kelibsiz!*\n\nKim bo'lib ro'yxatdan o'tmoqchisiz?",
            reply_markup=get_role_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await show_main_menu(message, user)

# ---------- /reset ----------
@dp.message(Command("reset"))
async def cmd_reset(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Удаляем пользователя из всех таблиц
    supabase.table("users").delete().eq("user_id", user_id).execute()
    supabase.table("gardeners").delete().eq("user_id", user_id).execute()
    supabase.table("farmers").delete().eq("user_id", user_id).execute()
    supabase.table("delivery_profiles").delete().eq("user_id", user_id).execute()
    supabase.table("farmer_requests").delete().eq("user_id", user_id).execute()
    supabase.table("delivery_requests").delete().eq("user_id", user_id).execute()
    supabase.table("cart").delete().eq("user_id", user_id).execute()
    
    await state.clear()
    await message.answer(
        "✅ *Ваш аккаунт полностью сброшен!*\n\n"
        "Напишите /start для повторной регистрации.",
        parse_mode="Markdown"
    )

# ---------- /admin ----------
@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.set_state(RegistrationStates.waiting_for_admin_password)
    await message.answer("🔐 *Введите пароль для входа в админ-панель:*", parse_mode="Markdown")

@dp.message(RegistrationStates.waiting_for_admin_password)
async def process_admin_password(message: Message, state: FSMContext):
    if check_admin_password(message.text):
        set_admin_role(message.from_user.id)
        await message.answer("✅ *Вы стали администратором!*", parse_mode="Markdown")
        user = get_user_by_telegram_id(message.from_user.id)
        await show_main_menu(message, user)
    else:
        await message.answer("❌ *Неверный пароль!*", parse_mode="Markdown")
    await state.clear()

# ---------- Выбор роли ----------
@dp.callback_query(RegistrationStates.waiting_for_role)
async def process_role_selection(callback: CallbackQuery, state: FSMContext):
    role = callback.data.split("_")[1]
    
    if role == "buyer":
        add_user(callback.from_user.id, callback.from_user.username, callback.from_user.full_name, "buyer")
        user = get_user_by_telegram_id(callback.from_user.id)
        await callback.message.edit_text("✅ Вы зарегистрированы как Покупатель!")
        await show_main_menu(callback.message, user)
        await callback.answer()
        await state.clear()
        
    elif role == "gardener":
        await state.update_data(role="gardener", is_change=False)
        await state.set_state(RegistrationStates.waiting_for_gardener_name)
        await callback.message.edit_text("🏠 *Регистрация садовода*\n\nВведите название вашего сада:", parse_mode="Markdown")
        await callback.answer()
        
    elif role == "farmer":
        await state.update_data(role="farmer", is_change=False)
        await state.set_state(RegistrationStates.waiting_for_farmer_name)
        await callback.message.edit_text("🌾 *Регистрация фермера*\n\nВведите название хозяйства:", parse_mode="Markdown")
        await callback.answer()
        
    elif role == "delivery":
        await state.update_data(role="delivery", is_change=False)
        await state.set_state(RegistrationStates.waiting_for_delivery_name)
        await callback.message.edit_text("🚚 *Регистрация доставщика*\n\nВведите ваше ФИО:", parse_mode="Markdown")
        await callback.answer()

# ---------- Садовод ----------
@dp.message(RegistrationStates.waiting_for_gardener_name)
async def process_gardener_name(message: Message, state: FSMContext):
    await state.update_data(garden_name=message.text)
    await state.set_state(RegistrationStates.waiting_for_gardener_address)
    await message.answer("🏠 Введите адрес вашего сада/огорода:")

@dp.message(RegistrationStates.waiting_for_gardener_address)
async def process_gardener_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text)
    await state.set_state(RegistrationStates.waiting_for_gardener_phone)
    await message.answer("📞 Введите номер телефона для связи:")

@dp.message(RegistrationStates.waiting_for_gardener_phone)
async def process_gardener_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await state.set_state(RegistrationStates.waiting_for_gardener_location)
    await message.answer(
        "📍 *Отправьте геолокацию вашего сада/огорода*\n\n"
        "Нажмите на кнопку ниже и отправьте местоположение на карте.",
        reply_markup=get_location_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(RegistrationStates.waiting_for_gardener_location, F.location)
async def process_gardener_location(message: Message, state: FSMContext):
    data = await state.get_data()
    latitude = message.location.latitude
    longitude = message.location.longitude
    is_change = data.get('is_change', False)
    
    if is_change:
        supabase.table("gardeners").upsert({
            "user_id": message.from_user.id,
            "garden_name": data['garden_name'],
            "address": data['address'],
            "phone": data['phone'],
            "latitude": latitude,
            "longitude": longitude
        }).execute()
        update_user_role(message.from_user.id, "gardener")
        await message.answer(
            "✅ *Роль успешно изменена на Садовод!*",
            reply_markup=get_main_keyboard("gardener"),
            parse_mode="Markdown"
        )
    else:
        add_user(message.from_user.id, message.from_user.username, message.from_user.full_name, "gardener")
        add_gardener(message.from_user.id, data['garden_name'], data['address'], data['phone'], latitude, longitude)
        await message.answer(
            "✅ *Вы успешно зарегистрированы как Садовод!*",
            reply_markup=get_main_keyboard("gardener"),
            parse_mode="Markdown"
        )
    await state.clear()

# ---------- Фермер ----------
@dp.message(RegistrationStates.waiting_for_farmer_name)
async def process_farmer_name(message: Message, state: FSMContext):
    await state.update_data(farm_name=message.text)
    await state.set_state(RegistrationStates.waiting_for_farmer_address)
    await message.answer("🌾 Введите адрес вашего хозяйства:")

@dp.message(RegistrationStates.waiting_for_farmer_address)
async def process_farmer_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text)
    await state.set_state(RegistrationStates.waiting_for_farmer_phone)
    await message.answer("📞 Введите номер телефона для связи:")

@dp.message(RegistrationStates.waiting_for_farmer_phone)
async def process_farmer_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await state.set_state(RegistrationStates.waiting_for_farmer_location)
    await message.answer(
        "📍 *Отправьте геолокацию вашего хозяйства*\n\n"
        "Нажмите на кнопку ниже и отправьте местоположение на карте.",
        reply_markup=get_location_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(RegistrationStates.waiting_for_farmer_location, F.location)
async def process_farmer_location(message: Message, state: FSMContext):
    data = await state.get_data()
    latitude = message.location.latitude
    longitude = message.location.longitude
    is_change = data.get('is_change', False)
    
    if is_change:
        add_farmer_request(message.from_user.id, data['farm_name'], data['address'], data['phone'], latitude, longitude)
        await message.answer(
            "✅ *Заявка на роль фермера отправлена!*\n\n"
            "Менеджер рассмотрит вашу заявку.",
            reply_markup=get_main_keyboard("buyer"),
            parse_mode="Markdown"
        )
    else:
        add_user(message.from_user.id, message.from_user.username, message.from_user.full_name, "buyer")
        add_farmer_request(message.from_user.id, data['farm_name'], data['address'], data['phone'], latitude, longitude)
        await message.answer(
            "✅ *Заявка отправлена менеджеру!*\n\n"
            "После одобрения вы сможете добавлять товары.",
            reply_markup=get_main_keyboard("buyer"),
            parse_mode="Markdown"
        )
    
    await send_notification_to_managers(
        f"🔔 *Новая заявка фермера!*\n\n"
        f"👤 Пользователь: @{message.from_user.username}\n"
        f"🏠 Хозяйство: {data['farm_name']}\n"
        f"📍 Адрес: {data['address']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"🗺️ Координаты: {latitude}, {longitude}"
    )
    await state.clear()

# ---------- Доставщик ----------
@dp.message(RegistrationStates.waiting_for_delivery_name)
async def process_delivery_name(message: Message, state: FSMContext):
    await state.update_data(full_name=message.text)
    await state.set_state(RegistrationStates.waiting_for_delivery_phone)
    await message.answer("📞 Введите номер телефона:")

@dp.message(RegistrationStates.waiting_for_delivery_phone)
async def process_delivery_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    await state.set_state(RegistrationStates.waiting_for_delivery_vehicle)
    await message.answer("🚗 Вид транспорта (машина/мотобайк/велосипед):")

@dp.message(RegistrationStates.waiting_for_delivery_vehicle)
async def process_delivery_vehicle(message: Message, state: FSMContext):
    await state.update_data(vehicle_type=message.text)
    await state.set_state(RegistrationStates.waiting_for_delivery_location)
    await message.answer(
        "📍 *Отправьте вашу текущую геолокацию*\n\n"
        "Это нужно для распределения заказов поблизости.",
        reply_markup=get_location_keyboard(),
        parse_mode="Markdown"
    )

@dp.message(RegistrationStates.waiting_for_delivery_location, F.location)
async def process_delivery_location(message: Message, state: FSMContext):
    data = await state.get_data()
    latitude = message.location.latitude
    longitude = message.location.longitude
    is_change = data.get('is_change', False)
    
    if is_change:
        add_delivery_request(message.from_user.id, data['full_name'], data['phone'], data['vehicle_type'], latitude, longitude)
        await message.answer(
            "✅ *Заявка на роль доставщика отправлена!*\n\n"
            "Менеджер рассмотрит вашу заявку.",
            reply_markup=get_main_keyboard("buyer"),
            parse_mode="Markdown"
        )
    else:
        add_user(message.from_user.id, message.from_user.username, message.from_user.full_name, "buyer")
        add_delivery_request(message.from_user.id, data['full_name'], data['phone'], data['vehicle_type'], latitude, longitude)
        await message.answer(
            "✅ *Заявка отправлена менеджеру!*",
            reply_markup=get_main_keyboard("buyer"),
            parse_mode="Markdown"
        )
    
    await send_notification_to_managers(
        f"🔔 *Новая заявка доставщика!*\n\n"
        f"👤 Пользователь: @{message.from_user.username}\n"
        f"📝 ФИО: {data['full_name']}\n"
        f"📞 Телефон: {data['phone']}\n"
        f"🚗 Транспорт: {data['vehicle_type']}\n"
        f"🗺️ Координаты: {latitude}, {longitude}"
    )
    await state.clear()

# ---------- Менеджер: заявки ----------
@dp.message(F.text == "🌾 Заявки фермеров")
async def show_farmer_requests(message: Message):
    user = get_user_by_telegram_id(message.from_user.id)
    if user.get("role") not in ["manager", "admin"]:
        await message.answer("⛔ Нет доступа.")
        return
    requests = get_pending_farmer_requests()
    if not requests:
        await message.answer("📭 Нет заявок.")
        return
    await message.answer(f"📋 Заявки фермеров ({len(requests)}):", reply_markup=get_farmer_requests_keyboard(requests))

@dp.message(F.text == "🚚 Заявки доставщиков")
async def show_delivery_requests(message: Message):
    user = get_user_by_telegram_id(message.from_user.id)
    if user.get("role") not in ["manager", "admin"]:
        await message.answer("⛔ Нет доступа.")
        return
    requests = get_pending_delivery_requests()
    if not requests:
        await message.answer("📭 Нет заявок.")
        return
    await message.answer(f"📋 Заявки доставщиков ({len(requests)}):", reply_markup=get_delivery_requests_keyboard(requests))

@dp.callback_query(F.data.startswith("fr_"))
async def view_farmer_request(callback: CallbackQuery):
    request_id = int(callback.data.split("_")[1])
    req = next((r for r in get_pending_farmer_requests() if r["id"] == request_id), None)
    if not req:
        await callback.message.edit_text("❌ Заявка не найдена.")
        await callback.answer()
        return
    text = f"🌾 *Заявка #{req['id']}*\n🏠 {req['farm_name']}\n📍 {req['address']}\n📞 {req['phone']}\n🗺️ Координаты: {req.get('latitude', 'нет')}, {req.get('longitude', 'нет')}"
    await callback.message.edit_text(text, reply_markup=get_request_action_keyboard(request_id, "farmer"))
    await callback.answer()

@dp.callback_query(F.data.startswith("dr_"))
async def view_delivery_request(callback: CallbackQuery):
    request_id = int(callback.data.split("_")[1])
    req = next((r for r in get_pending_delivery_requests() if r["id"] == request_id), None)
    if not req:
        await callback.message.edit_text("❌ Заявка не найдена.")
        await callback.answer()
        return
    text = f"🚚 *Заявка #{req['id']}*\n👤 {req['full_name']}\n📞 {req['phone']}\n🚗 {req['vehicle_type']}\n🗺️ Координаты: {req.get('latitude', 'нет')}, {req.get('longitude', 'нет')}"
    await callback.message.edit_text(text, reply_markup=get_request_action_keyboard(request_id, "delivery"))
    await callback.answer()

@dp.callback_query(F.data.startswith("approve_"))
async def approve_request(callback: CallbackQuery):
    _, req_type, req_id = callback.data.split("_")
    req_id = int(req_id)
    
    if req_type == "farmer":
        req = next((r for r in get_pending_farmer_requests() if r["id"] == req_id), None)
        if req:
            approve_farmer_request(req_id, req["user_id"], req["farm_name"], req.get("address", ""), req.get("phone", ""), req.get("latitude"), req.get("longitude"))
            await callback.message.edit_text("✅ Заявка фермера одобрена!")
            try:
                await bot.send_message(req["user_id"], "🌾 *Ваша заявка одобрена! Теперь вы фермер.*", parse_mode="Markdown")
            except: pass
    elif req_type == "delivery":
        req = next((r for r in get_pending_delivery_requests() if r["id"] == req_id), None)
        if req:
            approve_delivery_request(req_id, req["user_id"], req["full_name"], req["phone"], req["vehicle_type"], req.get("latitude"), req.get("longitude"))
            await callback.message.edit_text("✅ Заявка доставщика одобрена!")
            try:
                await bot.send_message(req["user_id"], "🚚 *Ваша заявка одобрена! Теперь вы доставщик.*", parse_mode="Markdown")
            except: pass
    await callback.answer()

@dp.callback_query(F.data.startswith("reject_"))
async def reject_request_cmd(callback: CallbackQuery):
    _, req_type, req_id = callback.data.split("_")
    req_id = int(req_id)
    reject_request(f"{req_type}_requests", req_id)
    await callback.message.edit_text("❌ Заявка отклонена.")
    await callback.answer()

# ---------- Каталог ----------
@dp.message(F.text == "🛒 Каталог")
async def show_catalog_location_request(message: Message):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Поделиться геолокацией", request_location=True)]],
        resize_keyboard=True
    )
    await message.answer(
        "📍 *Для показа ближайших продавцов*, поделитесь геолокацией.\nИли нажмите 'Пропустить'.",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )

@dp.message(F.location)
async def handle_location(message: Message, state: FSMContext):
    user_location_cache[message.from_user.id] = {"lat": message.location.latitude, "lon": message.location.longitude}
    categories = get_categories()
    await message.answer("📋 *Выберите категорию:*", reply_markup=get_categories_keyboard(categories), parse_mode="Markdown")

@dp.callback_query(F.data.startswith("cat_"))
async def show_products_by_category(callback: CallbackQuery, state: FSMContext):
    category_id = int(callback.data.split("_")[1])
    products = get_products_by_category(category_id)
    if not products:
        await callback.message.edit_text("📭 Товаров нет.")
        await callback.answer()
        return
    
    products.sort(key=lambda x: (0 if x['seller_type'] == 'farmer' else 1, x['price']))
    
    await state.update_data(products=products, current_page=0, sort_type="default")
    
    loc = user_location_cache.get(callback.from_user.id)
    await callback.message.edit_text(
        "📋 *Список товаров:*\n🌾 — фермер, 🏠 — садовод, 📍 — расстояние",
        reply_markup=get_products_with_sellers_keyboard(products, 0, 5, loc.get("lat") if loc else None, loc.get("lon") if loc else None),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("products_page_"))
async def paginate_products(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    data = await state.get_data()
    products = data.get("products", [])
    loc = user_location_cache.get(callback.from_user.id)
    await callback.message.edit_reply_markup(
        reply_markup=get_products_with_sellers_keyboard(products, page, 5, loc.get("lat") if loc else None, loc.get("lon") if loc else None)
    )
    await callback.answer()

@dp.callback_query(F.data == "sort_menu")
async def show_sort_menu(callback: CallbackQuery):
    await callback.message.edit_text("📊 *Сортировка:*", reply_markup=get_sort_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("sort_"))
async def apply_sort(callback: CallbackQuery, state: FSMContext):
    sort_type = callback.data.split("_")[1]
    data = await state.get_data()
    products = data.get("products", []).copy()
    loc = user_location_cache.get(callback.from_user.id)
    
    if sort_type == "price_asc":
        products.sort(key=lambda x: x['price'])
        sort_name = "По цене ↑"
    elif sort_type == "price_desc":
        products.sort(key=lambda x: x['price'], reverse=True)
        sort_name = "По цене ↓"
    elif sort_type == "distance_asc":
        if loc:
            for p in products:
                if p.get('seller_lat') and p.get('seller_lon'):
                    p['distance'] = calculate_distance(loc['lat'], loc['lon'], p['seller_lat'], p['seller_lon'])
                else:
                    p['distance'] = float('inf')
            products.sort(key=lambda x: x.get('distance', float('inf')))
            sort_name = "По расстоянию"
        else:
            await callback.answer("Поделитесь геолокацией!", show_alert=True)
            return
    elif sort_type == "farmer_first":
        products.sort(key=lambda x: (0 if x['seller_type'] == 'farmer' else 1, x['price']))
        sort_name = "Фермеры сначала"
    elif sort_type == "gardener_first":
        products.sort(key=lambda x: (0 if x['seller_type'] == 'gardener' else 1, x['price']))
        sort_name = "Садоводы сначала"
    elif sort_type == "name":
        products.sort(key=lambda x: x['name'])
        sort_name = "По названию"
    else:
        return
    
    await state.update_data(products=products, current_page=0, sort_type=sort_type)
    await callback.message.edit_text(
        f"📋 *Список товаров* (сортировка: {sort_name})",
        reply_markup=get_products_with_sellers_keyboard(products, 0, 5, loc.get("lat") if loc else None, loc.get("lon") if loc else None),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_categories")
async def back_to_categories(callback: CallbackQuery):
    categories = get_categories()
    await callback.message.edit_text("📋 *Выберите категорию:*", reply_markup=get_categories_keyboard(categories), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data.startswith("product_"))
async def show_product_detail(callback: CallbackQuery):
    product_id = int(callback.data.split("_")[1])
    product = get_product_by_id(product_id)
    if not product:
        await callback.message.edit_text("❌ Товар не найден.")
        await callback.answer()
        return
    
    loc = user_location_cache.get(callback.from_user.id)
    text = f"🍅 *{product['name']}*\n💰 {product['price']}₽ / {product['unit']}\n📦 {product['stock']} {product['unit']}\n\n{product['badge']}\n🏪 *{product['seller_name']}*\n📍 {product['seller_address']}"
    
    if loc and product.get('seller_lat') and product.get('seller_lon'):
        dist = calculate_distance(loc['lat'], loc['lon'], product['seller_lat'], product['seller_lon'])
        if dist != float('inf'):
            text += f"\n📏 Расстояние: {dist} км"
    
    text += f"\n📝 {product.get('description', 'Описание отсутствует')}"
    
    builder = InlineKeyboardBuilder()
    builder.button(text="🛒 Добавить в корзину", callback_data=f"add_to_cart_{product_id}")
    builder.button(text="🔙 Назад", callback_data="back_to_products")
    builder.adjust(1)
    
    if product.get('photo_id'):
        await callback.message.delete()
        await callback.message.answer_photo(photo=product['photo_id'], caption=text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    else:
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(F.data == "back_to_products")
async def back_to_products(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    products = data.get("products", [])
    loc = user_location_cache.get(callback.from_user.id)
    await callback.message.edit_text(
        "📋 *Список товаров:*",
        reply_markup=get_products_with_sellers_keyboard(products, 0, 5, loc.get("lat") if loc else None, loc.get("lon") if loc else None),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("add_to_cart_"))
async def add_to_cart_start(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[3])
    await state.update_data(product_id=product_id)
    await state.set_state(AddToCartStates.waiting_for_quantity)
    await callback.message.answer("✏️ *Введите количество:*", parse_mode="Markdown")
    await callback.answer()

@dp.message(AddToCartStates.waiting_for_quantity)
async def process_quantity(message: Message, state: FSMContext):
    try:
        quantity = float(message.text.replace(',', '.'))
        if quantity <= 0:
            raise ValueError
        data = await state.get_data()
        add_to_cart(message.from_user.id, data['product_id'], quantity)
        await message.answer(f"✅ *Добавлено {quantity} ед. в корзину!*", parse_mode="Markdown")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите корректное число.")

# ---------- Корзина ----------
@dp.message(F.text == "🛍️ Корзина")
async def show_cart(message: Message):
    cart_items = get_cart(message.from_user.id)
    if not cart_items:
        await message.answer("🛒 Корзина пуста.")
        return
    
    text = "🛒 *Ваша корзина:*\n\n"
    total = 0
    for item in cart_items:
        prod = item.get("products", {})
        price = prod.get("price", 0)
        qty = item.get("quantity", 0)
        total += price * qty
        text += f"• {prod.get('name', '?')} — {qty} × {price} = {price * qty}₽\n"
    text += f"\n💵 *Итого: {total}₽*"
    await message.answer(text, reply_markup=get_cart_keyboard(), parse_mode="Markdown")

@dp.callback_query(F.data == "clear_cart")
async def clear_cart_cmd(callback: CallbackQuery):
    clear_cart(callback.from_user.id)
    await callback.message.edit_text("🗑️ Корзина очищена.")
    await callback.answer()

@dp.callback_query(F.data == "checkout")
async def start_checkout(callback: CallbackQuery, state: FSMContext):
    cart_items = get_cart(callback.from_user.id)
    if not cart_items:
        await callback.answer("Корзина пуста!", show_alert=True)
        return
    await state.update_data(cart_items=cart_items)
    await state.set_state(OrderStates.waiting_for_delivery_method)
    await callback.message.answer("🚚 *Выберите способ получения:*", reply_markup=get_delivery_keyboard(), parse_mode="Markdown")
    await callback.answer()

@dp.callback_query(OrderStates.waiting_for_delivery_method)
async def process_delivery_method(callback: CallbackQuery, state: FSMContext):
    method = "pickup" if callback.data == "delivery_pickup" else "delivery"
    await state.update_data(delivery_method=method)
    if method == "delivery":
        await state.set_state(OrderStates.waiting_for_address)
        await callback.message.answer("🚚 *Введите адрес доставки:*", parse_mode="Markdown")
    else:
        await state.set_state(OrderStates.waiting_for_phone)
        await callback.message.answer("📞 *Введите номер телефона:*", parse_mode="Markdown")
    await callback.answer()

@dp.message(OrderStates.waiting_for_address)
async def process_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text)
    await state.set_state(OrderStates.waiting_for_phone)
    await message.answer("📞 *Введите номер телефона:*", parse_mode="Markdown")

@dp.message(OrderStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text)
    data = await state.get_data()
    
    order_id = create_order(
        message.from_user.id,
        data['cart_items'],
        data['delivery_method'],
        data.get('address'),
        data['phone']
    )
    
    if order_id:
        await message.answer(f"✅ *Заказ #{order_id} оформлен!*", parse_mode="Markdown")
    else:
        await message.answer("❌ Ошибка при оформлении заказа.")
    await state.clear()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: CallbackQuery):
    categories = get_categories()
    await callback.message.edit_text("📋 *Выберите категорию:*", reply_markup=get_categories_keyboard(categories), parse_mode="Markdown")
    await callback.answer()

# ---------- Профиль ----------
@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message):
    user = get_user_by_telegram_id(message.from_user.id)
    if not user:
        await message.answer("❌ Ошибка: пользователь не найден.")
        return
    
    role_names = {
        'buyer': '🛒 Покупатель',
        'gardener': '🏠 Садовод',
        'farmer': '🌾 Фермер',
        'delivery': '🚚 Доставщик',
        'manager': '👤 Менеджер',
        'admin': '⚙️ Администратор'
    }
    
    text = (
        f"👤 *Ваш профиль*\n\n"
        f"🆔 ID: {user['user_id']}\n"
        f"📝 Имя: {user.get('full_name', 'Не указано')}\n"
        f"🔑 Роль: {role_names.get(user['role'], user['role'])}\n"
        f"📅 Зарегистрирован: {user.get('created_at', 'Неизвестно')[:16] if user.get('created_at') else 'Неизвестно'}"
    )
    
    # Дополнительная информация для садовода/фермера
    if user['role'] == 'gardener':
        gardener = supabase.table("gardeners").select("*").eq("user_id", user['user_id']).execute()
        if gardener.data:
            text += f"\n\n🏠 Сад: {gardener.data[0].get('garden_name', '-')}\n📍 {gardener.data[0].get('address', '-')}"
    elif user['role'] == 'farmer':
        farmer = supabase.table("farmers").select("*").eq("user_id", user['user_id']).execute()
        if farmer.data:
            text += f"\n\n🌾 Хозяйство: {farmer.data[0].get('farm_name', '-')}\n📍 {farmer.data[0].get('address', '-')}"
    
    # Клавиатура с кнопкой смены роли
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Сменить роль", callback_data="change_role")
    builder.adjust(1)
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# ---------- Смена роли ----------
@dp.callback_query(F.data == "change_role")
async def change_role_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🔄 *Смена роли*\n\n"
        "Выберите новую роль:\n\n"
        "🛒 **Покупатель** — покупка товаров\n"
        "🏠 **Садовод** — продажа со своего сада (без проверки)\n"
        "🌾 **Фермер** — продажа с фермы (нужна проверка)\n"
        "🚚 **Доставщик** — доставка заказов (нужна проверка)",
        reply_markup=get_change_role_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "change_to_buyer")
async def change_to_buyer(callback: CallbackQuery):
    update_user_role(callback.from_user.id, "buyer")
    await callback.message.answer(
        "✅ *Вы сменили роль на Покупатель!*",
        reply_markup=get_main_keyboard("buyer"),
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "change_to_gardener")
async def change_to_gardener_start(callback: CallbackQuery, state: FSMContext):
    await state.update_data(role="gardener", is_change=True)
    await state.set_state(RegistrationStates.waiting_for_gardener_name)
    await callback.message.answer(
        "🏠 *Регистрация садовода*\n\n"
        "Введите название вашего сада или огорода:",
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "change_to_farmer")
async def change_to_farmer_start(callback: CallbackQuery, state: FSMContext):
    await state.update_data(role="farmer", is_change=True)
    await state.set_state(RegistrationStates.waiting_for_farmer_name)
    await callback.message.answer(
        "🌾 *Регистрация фермера*\n\n"
        "Введите название вашего хозяйства:",
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "change_to_delivery")
async def change_to_delivery_start(callback: CallbackQuery, state: FSMContext):
    await state.update_data(role="delivery", is_change=True)
    await state.set_state(RegistrationStates.waiting_for_delivery_name)
    await callback.message.answer(
        "🚚 *Регистрация доставщика*\n\n"
        "Введите ваше ФИО:",
        parse_mode="Markdown"
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_profile")
async def back_to_profile(callback: CallbackQuery):
    await callback.message.delete()
    await show_profile(callback.message)
    await callback.answer()

# ---------- Мои заказы ----------
@dp.message(F.text == "📦 Мои заказы")
async def show_orders(message: Message):
    orders = get_user_orders(message.from_user.id)
    if not orders:
        await message.answer("📭 У вас пока нет заказов.")
        return
    
    status_emoji = {
        'pending': '⏳ Ожидает',
        'paid': '✅ Оплачен',
        'preparing': '👨‍🍳 Готовится',
        'delivery': '🚚 В доставке',
        'delivered': '📦 Доставлен',
        'cancelled': '❌ Отменён'
    }
    
    text = "📦 *Ваши заказы:*\n\n"
    for order in orders:
        status = status_emoji.get(order.get('status', 'pending'), order.get('status', 'pending'))
        text += (
            f"🆔 *Заказ #{order.get('id', '?')}*\n"
            f"💵 Сумма: {order.get('total_amount', 0)}₽\n"
            f"📊 Статус: {status}\n"
            f"🕐 {order.get('created_at', '')[:16]}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")

# ---------- Помощь ----------
@dp.message(F.text == "❓ Помощь")
@dp.message(Command("help"))
async def show_help(message: Message):
    await message.answer(
        "❓ *Помощь*\n\n"
        "🛒 **Каталог** — просмотр товаров с сортировкой по цене и расстоянию\n"
        "🛍️ **Корзина** — оформление заказа\n"
        "📦 **Мои заказы** — история заказов\n"
        "👤 **Профиль** — ваши данные и смена роли\n\n"
        "📍 *Для сортировки по расстоянию* поделитесь геолокацией в каталоге.\n\n"
        "По вопросам: @paliz_support",
        parse_mode="Markdown"
    )

# ---------- Неизвестные команды ----------
@dp.message()
async def unknown_message(message: Message):
    user = get_user_by_telegram_id(message.from_user.id)
    role = user.get("role", "buyer") if user else "buyer"
    await message.answer(
        "❓ Я не понимаю эту команду.\n"
        "Пожалуйста, воспользуйтесь кнопками меню или /help.",
        reply_markup=get_main_keyboard(role)
    )

# ---------- HTTP Keep-Alive ----------
async def handle_health(request):
    return web.Response(text="✅ Bot is running!")

async def start_http_server():
    port = int(os.environ.get('PORT', 10000))
    app = web.Application()
    app.router.add_get('/health', handle_health)
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logging.info(f"✅ HTTP сервер запущен на порту {port}")

# ---------- ЗАПУСК ----------
async def main():
    asyncio.create_task(start_http_server())
    
    webhook_url = os.getenv("WEBHOOK_URL")
    if webhook_url:
        await bot.set_webhook(webhook_url)
        logging.info(f"✅ Webhook установлен: {webhook_url}")
    else:
        await bot.delete_webhook(drop_pending_updates=True)
        logging.info("✅ Запуск в режиме polling")
        await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
