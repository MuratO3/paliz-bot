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

# ---------- /start (исправлен: принудительный выбор роли) ----------
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user = get_user_by_telegram_id(message.from_user.id)
    
    # Если пользователь НЕ найден ИЛИ роль не задана корректно
    if not user or user.get('role') not in ['buyer', 'gardener', 'farmer', 'delivery', 'manager', 'admin']:
        await state.set_state(RegistrationStates.waiting_for_role)
        await message.answer(
            "🌾 *Paliz Marketga xush kelibsiz!*\n\n"
            "Kim bo'lib ro'yxatdan o'tmoqchisiz?\n\n"
            "🛒 Покупатель\n"
            "🏠 Садовод (частник)\n"
            "🌾 Фермер (с проверкой)\n"
            "🚚 Доставщик (с проверкой)",
            reply_markup=get_role_keyboard(),
            parse_mode="Markdown"
        )
    else:
        await show_main_menu(message, user)

# ---------- /reset (исправлен) ----------
@dp.message(Command("reset"))
async def cmd_reset(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    # Удаляем пользователя из всех таблиц
    try:
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
    except Exception as e:
        logging.error(f"reset error: {e}")
        await message.answer("❌ Ошибка при сбросе аккаунта. Попробуйте позже.")

# ---------- /setrole (принудительная смена роли) ----------
@dp.message(Command("setrole"))
async def cmd_setrole(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "❌ *Используйте:* `/setrole [роль]`\n\n"
            "Доступные роли:\n"
            "• `buyer` — Покупатель\n"
            "• `gardener` — Садовод\n"
            "• `farmer` — Фермер\n"
            "• `delivery` — Доставщик\n"
            "• `manager` — Менеджер\n"
            "• `admin` — Администратор",
            parse_mode="Markdown"
        )
        return
    
    new_role = args[1].lower()
    allowed_roles = ['buyer', 'gardener', 'farmer', 'delivery', 'manager', 'admin']
    
    if new_role not in allowed_roles:
        await message.answer(
            f"❌ Роль *{new_role}* не существует.\n\n"
            f"Доступные роли: {', '.join(allowed_roles)}",
            parse_mode="Markdown"
        )
        return
    
    update_user_role(message.from_user.id, new_role)
    
    role_names = {
        'buyer': '🛒 Покупатель',
        'gardener': '🏠 Садовод',
        'farmer': '🌾 Фермер',
        'delivery': '🚚 Доставщик',
        'manager': '👤 Менеджер',
        'admin': '⚙️ Администратор'
    }
    
    await message.answer(
        f"✅ *Роль изменена на {role_names.get(new_role, new_role)}!*\n\n"
        f"Напишите /start для обновления меню.",
        reply_markup=get_main_keyboard(new_role),
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

# ---------- Остальные обработчики (садовод, фермер, доставщик) ----------
# ... (они остаются без изменений, вставьте их сюда)

# ---------- HTTP Keep-Alive ----------
async def handle_health(request):
    return web.Response(text="✅ Bot is running!")

async def start_http_server():
    port = int(os.environ.get('PORT', 10000))  # Render ожидает 10000
    app = web.Application()
    app.router.add_get('/health', handle_health)
    app.router.add_get('/', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
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
