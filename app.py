import asyncio
import json
import logging
import os
import random
import io
import base64
import urllib.parse
from aiogram.types import WebAppInfo
from datetime import date, datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
import asyncpg
import qrcode
from PIL import Image, ImageDraw, ImageFont
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ContentType, FSInputFile, InputMediaPhoto, Location, Document
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

load_dotenv()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
DATABASE_URL = os.getenv("DATABASE_URL")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "YOUR_GOOGLE_MAPS_API_KEY")
WEBAPP_URL = os.getenv("WEBAPP_URL", "https://coreenergy1.github.io/bot/webapp_map.html")

# Directories
UPLOAD_DIR = "uploads"
os.makedirs(f"{UPLOAD_DIR}/generators", exist_ok=True)
os.makedirs(f"{UPLOAD_DIR}/installations", exist_ok=True)
os.makedirs(f"{UPLOAD_DIR}/documents", exist_ok=True)
os.makedirs(f"{UPLOAD_DIR}/service", exist_ok=True)
os.makedirs(f"{UPLOAD_DIR}/qrcodes", exist_ok=True)

# Initialize
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Roles
ROLES = {
    'admin': 'Administrator',
    'ombor': 'Ombor xodimi',
    'sotuvchi': 'Sotuvchi',
    'buxgalter': 'Buxgalter',
    'logist': 'Logist',
    'montajchi': 'Montajchi',
    'mijoz': 'Mijoz'
}

# Statuses
GEN_STATUSES = {
    'SKLADDA': '📦 Skladda',
    'SOTILDI': '💰 Sotildi',
    'DELIVERY': '🚚 Yetkazilmoqda',
    'INSTALLING': '🔧 O\'rnatilmoqda',
    'INSTALLED': "✅ O'rnatildi",
    'SERVICING': '🔧 Servisda',
    'REPAIR': '🔨 Tamirda'
}

DEAL_STATUSES = {
    'PENDING_PAYMENT': '⏳ To\'lov kutilmoqda',
    'PAID_SELLER_CONFIRM': '💰 Sotuvchi tasdiqladi',
    'PAID_ACCOUNTANT_CONFIRM': '✅ Buxgalter tasdiqladi',
    'IN_LOGISTICS': '🚚 Logistikada',
    'INSTALLING': '🔧 O\'rnatilmoqda',
    'COMPLETED': '🎉 Tugallandi',
    'CANCELLED': '❌ Bekor qilindi'
}

# Database pool
db_pool = None


async def init_db():
    """PostgreSQL bazasini ishga tushirish - TO'LIQ VILOYAT/TUMAN TIZIMI BILAN"""
    global db_pool
    
    try:
        db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60,
            ssl='require'
        )
        
        async with db_pool.acquire() as conn:
            
            # 1. EMPLOYEES JADVALI - VILOYAT/TUMAN BILAN
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    full_name VARCHAR(255) NOT NULL,
                    role VARCHAR(50) NOT NULL,
                    phone VARCHAR(20),
                    region VARCHAR(100),         -- YANGI: Viloyat
                    city VARCHAR(100),          -- YANGI: Tuman/Shahar
                    added_by BIGINT,
                    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE
                );
            """)
            
            # 2. CLIENTS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    full_name VARCHAR(255) NOT NULL,
                    phone VARCHAR(20) UNIQUE NOT NULL,
                    email VARCHAR(100),
                    address TEXT,
                    region VARCHAR(100),         -- Viloyat
                    city VARCHAR(100),          -- Tuman/Shahar
                    geo_lat DECIMAL(10,8),      -- Geolokatsiya (to'lovdan keyin)
                    geo_lon DECIMAL(11,8),
                    company VARCHAR(255),
                    is_approved BOOLEAN DEFAULT FALSE,
                    approved_by BIGINT,
                    approved_at TIMESTAMP,
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 3. CLIENT_REGISTRATION_REQUESTS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS client_registration_requests (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    full_name VARCHAR(255) NOT NULL,
                    phone VARCHAR(20) NOT NULL,
                    address TEXT,
                    region VARCHAR(100),         -- YANGI: Viloyat
                    city VARCHAR(100),           -- YANGI: Tuman
                    geo_lat DECIMAL(10,8),       -- NULL (keyinroq so'raladi)
                    geo_lon DECIMAL(11,8),
                    status VARCHAR(50) DEFAULT 'PENDING',
                    processed_by BIGINT,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 4. GENERATORS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS generators (
                    uid VARCHAR(50) PRIMARY KEY,
                    model VARCHAR(100) NOT NULL,
                    power_kw INTEGER NOT NULL,
                    serial_number VARCHAR(100) UNIQUE,
                    manufacturer VARCHAR(100),
                    manufacture_year INTEGER,
                    purchase_price DECIMAL(12,2),
                    sale_price DECIMAL(12,2),
                    status VARCHAR(50) DEFAULT 'SKLADDA',
                    current_client_id INTEGER,
                    current_deal_id INTEGER,
                    warranty_months INTEGER DEFAULT 12,
                    warranty_start_date DATE,
                    qr_code_path VARCHAR(255),
                    photos TEXT[],
                    documents JSONB,
                    added_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 5. DEALS JADVALI - MONTAJCHI BIRIKTIRISH BILAN
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS deals (
                    id SERIAL PRIMARY KEY,
                    generator_uid VARCHAR(50),
                    seller_id BIGINT NOT NULL,
                    client_id INTEGER,
                    installer_id BIGINT,              -- YANGI: Biriktirilgan montajchi
                    installer_assigned_at TIMESTAMP,   -- YANGI: Biriktirish vaqti
                    installer_assigned_by BIGINT,      -- YANGI: Kim biriktirgan
                    sale_price DECIMAL(12,2) NOT NULL,
                    delivery_cost DECIMAL(12,2) DEFAULT 0,
                    installation_cost DECIMAL(12,2) DEFAULT 0,
                    other_costs DECIMAL(12,2) DEFAULT 0,
                    total_cost DECIMAL(12,2) DEFAULT 0,
                    profit DECIMAL(12,2),
                    profit_margin DECIMAL(5,2),
                    status VARCHAR(50) DEFAULT 'PENDING_PAYMENT',
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                );
            """)
            
            # 6. PAYMENTS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    deal_id INTEGER,
                    amount DECIMAL(12,2) NOT NULL,
                    payment_method VARCHAR(50),
                    seller_confirmed BOOLEAN DEFAULT FALSE,
                    seller_confirmed_at TIMESTAMP,
                    seller_id BIGINT,
                    accountant_confirmed BOOLEAN DEFAULT FALSE,
                    accountant_confirmed_at TIMESTAMP,
                    accountant_id BIGINT,
                    payment_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 7. LOGISTICS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS logistics (
                    id SERIAL PRIMARY KEY,
                    deal_id INTEGER,
                    logist_id BIGINT NOT NULL,
                    vehicle_info VARCHAR(255),
                    driver_name VARCHAR(100),
                    driver_phone VARCHAR(20),
                    delivery_cost DECIMAL(12,2),
                    who_pays VARCHAR(20),
                    planned_date DATE,
                    actual_date DATE,
                    status VARCHAR(50) DEFAULT 'PLANNED',
                    tracking_notes TEXT,
                    photos TEXT[],
                    videos TEXT[],
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 8. INSTALLATIONS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS installations (
                    id SERIAL PRIMARY KEY,
                    deal_id INTEGER,
                    installer_id BIGINT NOT NULL,
                    installation_date TIMESTAMP,
                    motor_hours_start INTEGER DEFAULT 0,
                    motor_hours_current INTEGER DEFAULT 0,
                    photos TEXT[],
                    videos TEXT[],
                    act_signed BOOLEAN DEFAULT FALSE,
                    act_file_path VARCHAR(255),
                    geo_lat DECIMAL(10,8),
                    geo_lon DECIMAL(11,8),
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 9. SERVICE_HISTORY JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS service_history (
                    id SERIAL PRIMARY KEY,
                    generator_uid VARCHAR(50),
                    service_type VARCHAR(50),
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT,
                    motor_hours INTEGER,
                    next_service_date DATE,
                    next_service_hours INTEGER,
                    cost DECIMAL(12,2),
                    photos TEXT[],
                    videos TEXT[],
                    documents JSONB,
                    performed_by BIGINT,
                    client_notified BOOLEAN DEFAULT FALSE
                );
            """)
            
            # 10. FILES JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50),
                    entity_id VARCHAR(50),
                    file_type VARCHAR(50),
                    file_name VARCHAR(255),
                    file_path VARCHAR(500),
                    file_size INTEGER,
                    mime_type VARCHAR(100),
                    uploaded_by BIGINT,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_public BOOLEAN DEFAULT FALSE,
                    permissions JSONB
                );
            """)
            
            # 11. CORRECTION_REQUESTS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS correction_requests (
                    id SERIAL PRIMARY KEY,
                    requested_by BIGINT NOT NULL,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id VARCHAR(50) NOT NULL,
                    field_name VARCHAR(100) NOT NULL,
                    current_value TEXT,
                    proposed_value TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status VARCHAR(50) DEFAULT 'PENDING',
                    admin_id BIGINT,
                    admin_comment TEXT,
                    resolved_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 12. AUDIT_LOGS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    user_role VARCHAR(50),
                    action VARCHAR(50) NOT NULL,
                    table_name VARCHAR(50) NOT NULL,
                    record_id VARCHAR(50) NOT NULL,
                    old_data JSONB,
                    new_data JSONB,
                    ip_address VARCHAR(50),
                    user_agent TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 13. NOTIFICATIONS JADVALI
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    role VARCHAR(50),
                    title VARCHAR(255),
                    message TEXT,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # ============ MAVJUD JADVALLARNI YANGILASH ============
            
            # Employees jadvali - region va city qo'shish
            await conn.execute("""
                ALTER TABLE employees 
                ADD COLUMN IF NOT EXISTS region VARCHAR(100),
                ADD COLUMN IF NOT EXISTS city VARCHAR(100);
            """)
            
            # Clients jadvali - region va city tekshirish (mavjud bo'lsa)
            await conn.execute("""
                ALTER TABLE clients 
                ADD COLUMN IF NOT EXISTS region VARCHAR(100),
                ADD COLUMN IF NOT EXISTS city VARCHAR(100);
            """)
            
            # Client_registration_requests - region va city qo'shish
            await conn.execute("""
                ALTER TABLE client_registration_requests 
                ADD COLUMN IF NOT EXISTS region VARCHAR(100),
                ADD COLUMN IF NOT EXISTS city VARCHAR(100);
            """)
            
            # Generators jadvali
            await conn.execute("""
                ALTER TABLE generators 
                ADD COLUMN IF NOT EXISTS current_client_id INTEGER,
                ADD COLUMN IF NOT EXISTS manufacturer VARCHAR(100),
                ADD COLUMN IF NOT EXISTS manufacture_year INTEGER,
                ADD COLUMN IF NOT EXISTS sale_price DECIMAL(12,2),
                ADD COLUMN IF NOT EXISTS warranty_start_date DATE,
                ADD COLUMN IF NOT EXISTS documents JSONB,
                ADD COLUMN IF NOT EXISTS qr_code_path VARCHAR(255);
            """)
            
            # Deals jadvali - MONTAJCHI USTUNLARI
            await conn.execute("""
                ALTER TABLE deals
                ADD COLUMN IF NOT EXISTS client_id INTEGER,
                ADD COLUMN IF NOT EXISTS installer_id BIGINT,
                ADD COLUMN IF NOT EXISTS installer_assigned_at TIMESTAMP,
                ADD COLUMN IF NOT EXISTS installer_assigned_by BIGINT,
                ADD COLUMN IF NOT EXISTS other_costs DECIMAL(12,2) DEFAULT 0,
                ADD COLUMN IF NOT EXISTS total_cost DECIMAL(12,2) DEFAULT 0,
                ADD COLUMN IF NOT EXISTS profit DECIMAL(12,2),
                ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;
            """)
            
            # Payments jadvali
            await conn.execute("""
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS deal_id INTEGER,
                ADD COLUMN IF NOT EXISTS seller_id BIGINT,
                ADD COLUMN IF NOT EXISTS accountant_id BIGINT,
                ADD COLUMN IF NOT EXISTS payment_method VARCHAR(50),
                ADD COLUMN IF NOT EXISTS payment_date DATE;
            """)
            
            # Logistics jadvali
            await conn.execute("""
                ALTER TABLE logistics
                ADD COLUMN IF NOT EXISTS deal_id INTEGER,
                ADD COLUMN IF NOT EXISTS driver_name VARCHAR(100),
                ADD COLUMN IF NOT EXISTS videos TEXT[];
            """)
            
            # Installations jadvali
            await conn.execute("""
                ALTER TABLE installations
                ADD COLUMN IF NOT EXISTS deal_id INTEGER,
                ADD COLUMN IF NOT EXISTS videos TEXT[],
                ADD COLUMN IF NOT EXISTS act_file_path VARCHAR(255);
            """)
            
            # ============ FOREIGN KEY CONSTRAINTLAR ============
            
            await conn.execute("""
                DO $$
                BEGIN
                    -- generators -> clients foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_generators_client'
                    ) THEN
                        ALTER TABLE generators 
                        ADD CONSTRAINT fk_generators_client 
                        FOREIGN KEY (current_client_id) REFERENCES clients(id) 
                        ON DELETE SET NULL;
                    END IF;
                    
                    -- deals -> generators foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_deals_generator'
                    ) THEN
                        ALTER TABLE deals 
                        ADD CONSTRAINT fk_deals_generator 
                        FOREIGN KEY (generator_uid) REFERENCES generators(uid) 
                        ON DELETE SET NULL;
                    END IF;
                    
                    -- deals -> clients foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_deals_client'
                    ) THEN
                        ALTER TABLE deals 
                        ADD CONSTRAINT fk_deals_client 
                        FOREIGN KEY (client_id) REFERENCES clients(id) 
                        ON DELETE SET NULL;
                    END IF;
                    
                    -- YANGI: deals -> employees (installer) foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_deals_installer'
                    ) THEN
                        ALTER TABLE deals 
                        ADD CONSTRAINT fk_deals_installer 
                        FOREIGN KEY (installer_id) REFERENCES employees(telegram_id) 
                        ON DELETE SET NULL;
                    END IF;
                    
                    -- payments -> deals foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_payments_deal'
                    ) THEN
                        ALTER TABLE payments 
                        ADD CONSTRAINT fk_payments_deal 
                        FOREIGN KEY (deal_id) REFERENCES deals(id) 
                        ON DELETE CASCADE;
                    END IF;
                    
                    -- logistics -> deals foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_logistics_deal'
                    ) THEN
                        ALTER TABLE logistics 
                        ADD CONSTRAINT fk_logistics_deal 
                        FOREIGN KEY (deal_id) REFERENCES deals(id) 
                        ON DELETE CASCADE;
                    END IF;
                    
                    -- installations -> deals foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_installations_deal'
                    ) THEN
                        ALTER TABLE installations 
                        ADD CONSTRAINT fk_installations_deal 
                        FOREIGN KEY (deal_id) REFERENCES deals(id) 
                        ON DELETE CASCADE;
                    END IF;
                    
                    -- service_history -> generators foreign key
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_service_generator'
                    ) THEN
                        ALTER TABLE service_history 
                        ADD CONSTRAINT fk_service_generator 
                        FOREIGN KEY (generator_uid) REFERENCES generators(uid) 
                        ON DELETE CASCADE;
                    END IF;
                END $$;
            """)
            
            # ============ INDEXLAR ============
            
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_employees_role ON employees(role);
                CREATE INDEX IF NOT EXISTS idx_employees_telegram_id ON employees(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_employees_region ON employees(region);
                CREATE INDEX IF NOT EXISTS idx_employees_city ON employees(city);
                CREATE INDEX IF NOT EXISTS idx_generators_status ON generators(status);
                CREATE INDEX IF NOT EXISTS idx_generators_client ON generators(current_client_id);
                CREATE INDEX IF NOT EXISTS idx_deals_status ON deals(status);
                CREATE INDEX IF NOT EXISTS idx_deals_seller ON deals(seller_id);
                CREATE INDEX IF NOT EXISTS idx_deals_client ON deals(client_id);
                CREATE INDEX IF NOT EXISTS idx_deals_installer ON deals(installer_id);
                CREATE INDEX IF NOT EXISTS idx_clients_phone ON clients(phone);
                CREATE INDEX IF NOT EXISTS idx_clients_telegram ON clients(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_clients_region ON clients(region);
                CREATE INDEX IF NOT EXISTS idx_clients_city ON clients(city);
                CREATE INDEX IF NOT EXISTS idx_clients_approved ON clients(is_approved) WHERE is_approved = TRUE;
                CREATE INDEX IF NOT EXISTS idx_audit_logs_user ON audit_logs(user_id);
                CREATE INDEX IF NOT EXISTS idx_audit_logs_table ON audit_logs(table_name, record_id);
                CREATE INDEX IF NOT EXISTS idx_corrections_status ON correction_requests(status);
                CREATE INDEX IF NOT EXISTS idx_payments_deal ON payments(deal_id);
                CREATE INDEX IF NOT EXISTS idx_service_history_gen ON service_history(generator_uid);
                CREATE INDEX IF NOT EXISTS idx_files_entity ON files(entity_type, entity_id);
                CREATE INDEX IF NOT EXISTS idx_client_reg_status ON client_registration_requests(status);
                CREATE INDEX IF NOT EXISTS idx_client_reg_telegram ON client_registration_requests(telegram_id);
                CREATE INDEX IF NOT EXISTS idx_client_reg_phone ON client_registration_requests(phone);
                CREATE INDEX IF NOT EXISTS idx_client_reg_region ON client_registration_requests(region);
                CREATE INDEX IF NOT EXISTS idx_client_reg_city ON client_registration_requests(city);
            """)
            
            logger.info("✅ Database initialized successfully!")
            
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise


# ============ O'ZBEKISTON GEO MA'LUMOTLARI ============

UZBEKISTAN_REGIONS = {
    "Toshkent": {
        "districts": ["Bektemir", "Mirobod", "Mirzo-Ulug'bek", "Sergeli", "Olmazor", "Uchtepa", "Shayxontohur", "Yashnobod", "Chilonzor", "Yunusobod", "Yakkasaroy"]
    },
    "Toshkent viloyati": {
        "districts": ["Bekobod", "Bo'ka", "Bo'stonliq", "Chinoz", "Qibray", "Ohangaron", "Oqqo'rg'on", "Parkent", "Piskent", "Quyichirchiq", "Zangiota", "O'rta Chirchiq", "Yangiyo'l", "Yuqori Chirchiq"]
    },
    "Andijon": {
        "districts": ["Andijon shahri", "Asaka", "Baliqchi", "Bo'z", "Buloqboshi", "Izboskan", "Jalaquduq", "Xo'jaobod", "Qo'shko'pir", "Marhamat", "Oltinko'l", "Paxtaobod", "Shahrixon", "Ulug'nor"]
    },
    "Buxoro": {
        "districts": ["Buxoro shahri", "G'ijduvon", "Jondor", "Kogon", "Olot", "Peshku", "Qorako'l", "Qorovulbozor", "Romitan", "Shofirkon", "Vobkent"]
    },
    "Farg'ona": {
        "districts": ["Farg'ona shahri", "Bag'dod", "Beshariq", "Buvayda", "Dang'ara", "Farg'ona", "Furqat", "Qo'shtepa", "Quva", "Rishton", "So'x", "Toshloq", "Uchko'prik", "Yozyovon", "Oltiariq"]
    },
    "Jizzax": {
        "districts": ["Jizzax shahri", "Arnasoy", "Baxmal", "Do'stlik", "Forish", "G'allaorol", "Mirzacho'l", "Paxtakor", "Yangiobod", "Zomin", "Zarbdor", "Zafarobod"]
    },
    "Xorazm": {
        "districts": ["Urganch shahri", "Bog'ot", "Gurlan", "Xonqa", "Hazorasp", "Xiva", "Qo'shko'pir", "Shovot", "Urganch", "Yangiariq", "Yangibozor"]
    },
    "Namangan": {
        "districts": ["Namangan shahri", "Chortoq", "Chust", "Kosonsoy", "Mingbuloq", "Namangan", "Norin", "Pop", "To'raqo'rg'on", "Uchqo'rg'on", "Uychi", "Yangiqo'rg'on"]
    },
    "Navoiy": {
        "districts": ["Navoiy shahri", "Konimex", "Karmana", "Qiziltepa", "Xatirchi", "Nurota", "Tomdi", "Uchquduq", "Zarafshon"]
    },
    "Qashqadaryo": {
        "districts": ["Qarshi shahri", "Chiroqchi", "Dehqonobod", "G'uzor", "Kasbi", "Kitob", "Koson", "Mirishkor", "Muborak", "Nishon", "Qamashi", "Qarshi", "Shahrisabz", "Yakkabog'"]
    },
    "Qoraqalpog'iston": {
        "districts": ["Nukus shahri", "Amudaryo", "Beruniy", "Bo'zatov", "Chimboy", "Ellikqal'a", "Kegeyli", "Mo'ynoq", "Nukus", "Qonliko'l", "Qorao'zak", "Shumanay", "Taxtako'pir", "To'rtko'l", "Xo'jayli"]
    },
    "Samarqand": {
        "districts": ["Samarqand shahri", "Bulung'ur", "Ishtixon", "Jomboy", "Kattaqo'rg'on", "Narpay", "Nurobod", "Oqdaryo", "Pastdarg'om", "Paxtachi", "Payariq", "Samarqand", "Toyloq", "Urgut"]
    },
    "Sirdaryo": {
        "districts": ["Guliston shahri", "Boyovut", "Guliston", "Mirzaobod", "Oqoltin", "Sayxunobod", "Sardoba", "Shirin", "Xovos", "Yangiyer"]
    },
    "Surxondaryo": {
        "districts": ["Termiz shahri", "Angor", "Bandixon", "Boysun", "Denov", "Jarqo'rg'on", "Muzrabot", "Oltinsoy", "Qiziriq", "Qumqo'rg'on", "Sariosiyo", "Sherobod", "Sho'rchi", "Termiz", "Uzun"]
    }
}

def get_regions_keyboard():
    """Viloyatlar ro'yxati uchun keyboard"""
    buttons = []
    row = []
    for i, region in enumerate(UZBEKISTAN_REGIONS.keys(), 1):
        row.append(InlineKeyboardButton(text=region, callback_data=f"region_{region}"))
        if i % 2 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_districts_keyboard(region: str):
    """Tumanlar ro'yxati uchun keyboard"""
    districts = UZBEKISTAN_REGIONS.get(region, {}).get("districts", [])
    buttons = []
    row = []
    for i, district in enumerate(districts, 1):
        row.append(InlineKeyboardButton(text=district, callback_data=f"district_{district}"))
        if i % 2 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="back_to_regions")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)



def generate_uid():
    """Unique ID yaratish"""
    year = datetime.now().year
    number = random.randint(1000, 9999)
    return f"GEN-{year}-{number}"

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

async def get_user_role(user_id: int):
    """Foydalanuvchi rolini olish"""
    if is_admin(user_id):
        return 'admin'
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT role FROM employees WHERE telegram_id = $1 AND is_active = TRUE', 
            user_id
        )
        return row['role'] if row else None

async def get_employee_by_telegram_id(telegram_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM employees WHERE telegram_id = $1', telegram_id
        )

async def log_action(user_id: int, action: str, table: str, record_id: str, 
                    old_data=None, new_data=None, role: str = None):
    """Harakatlarni audit logga yozish"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO audit_logs 
                (user_id, user_role, action, table_name, record_id, old_data, new_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', user_id, role or await get_user_role(user_id), action, table, 
                str(record_id), 
                json.dumps(old_data) if old_data else None,
                json.dumps(new_data) if new_data else None)
    except Exception as e:
        logger.error(f"Audit log error: {e}")

async def calculate_deal_profit(deal_id: int):
    """Bitim foydasini hisoblash - YANGILANGAN"""
    async with db_pool.acquire() as conn:
        deal = await conn.fetchrow('''
            SELECT d.sale_price, d.delivery_cost, d.installation_cost, 
                   d.other_costs, g.purchase_price, d.id
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.id = $1
        ''', deal_id)
        
        if not deal:
            return None
        
        # Barcha xarajatlarni hisoblash
        purchase = float(deal['purchase_price'] or 0)
        delivery = float(deal['delivery_cost'] or 0)
        install = float(deal['installation_cost'] or 0)
        other = float(deal['other_costs'] or 0)
        sale = float(deal['sale_price'] or 0)
        
        total_cost = purchase + delivery + install + other
        profit = sale - total_cost
        margin = (profit / sale * 100) if sale > 0 else 0
        
        # Eski qiymatlarni olish (audit uchun)
        old_data = await conn.fetchrow('''
            SELECT profit, profit_margin, total_cost FROM deals WHERE id = $1
        ''', deal_id)
        
        # Yangilash
        await conn.execute('''
            UPDATE deals 
            SET profit = $1, 
                profit_margin = $2,
                total_cost = $3,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $4
        ''', profit, round(margin, 2), total_cost, deal_id)
        
        # Agar o'zgarish bo'lsa, audit logga yozish
        if old_data and (old_data['profit'] != profit or old_data['profit_margin'] != round(margin, 2)):
            await log_action(
                0,  # System
                'CALCULATE',
                'deals',
                deal_id,
                old_data={'profit': old_data['profit'], 'margin': old_data['profit_margin']},
                new_data={'profit': profit, 'margin': round(margin, 2), 'total_cost': total_cost},
                role='system'
            )
        
        return {
            'profit': profit,
            'margin': round(margin, 2),
            'total_cost': total_cost,
            'purchase': purchase,
            'delivery': delivery,
            'install': install,
            'other': other
        }

async def download_file(file_id: str, folder: str, filename: str, file_type: str = "photo"):
    """Faylni yuklab olish - PHOTO, DOCUMENT va VIDEO uchun"""
    try:
        if file_type == "photo":
            file = await bot.get_file(file_id)
            ext = file.file_path.split('.')[-1]
            filepath = f"{UPLOAD_DIR}/{folder}/{filename}.{ext}"
            await bot.download_file(file.file_path, filepath)
            
        elif file_type == "document":
            file = await bot.get_file(file_id)
            ext = file.file_path.split('.')[-1]
            filepath = f"{UPLOAD_DIR}/{folder}/{filename}.{ext}"
            await bot.download_file(file.file_path, filepath)
            
        elif file_type == "video":
            file = await bot.get_file(file_id)
            ext = file.file_path.split('.')[-1]
            # Agar kengaytma bo'lmasa, default mp4
            if not ext or ext == file.file_path:
                ext = "mp4"
            filepath = f"{UPLOAD_DIR}/{folder}/{filename}.{ext}"
            await bot.download_file(file.file_path, filepath)
            
        else:
            return None
            
        return filepath
        
    except Exception as e:
        logger.error(f"File download error ({file_type}): {e}")
        return None

async def generate_qr_code(uid: str):
    """QR kod yaratish"""
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(f"https://t.me/Coreenergy1_bot?start={uid}")
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Add text
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("arial.ttf", 20)
        except:
            font = ImageFont.load_default()
        
        text = f"UID: {uid}"
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        img_width = img.size[0]
        draw.text(((img_width - text_width) / 2, 10), text, font=font, fill="black")
        
        filepath = f"{UPLOAD_DIR}/qrcodes/{uid}.png"
        img.save(filepath)
        return filepath
    except Exception as e:
        logger.error(f"QR generation error: {e}")
        return None

# ============ NOTIFICATION FUNCTIONS ============

async def notify_user(user_id: int, message: str, parse_mode: str = "HTML"):
    """Bitta foydalanuvchiga xabar yuborish"""
    try:
        await bot.send_message(user_id, message, parse_mode=parse_mode)
    except Exception as e:
        logger.error(f"Notification error to {user_id}: {e}")

async def notify_by_role(role: str, message: str, parse_mode: str = "HTML"):
    """Rol bo'yicha xabar yuborish"""
    async with db_pool.acquire() as conn:
        employees = await conn.fetch(
            "SELECT telegram_id FROM employees WHERE role = $1 AND is_active = TRUE", 
            role
        )
        for emp in employees:
            await notify_user(emp['telegram_id'], message, parse_mode)

async def notify_admins(message: str, parse_mode: str = "HTML"):
    """Adminlarga xabar yuborish"""
    await notify_user(ADMIN_ID, message, parse_mode)

# ============ KEYBOARDS ============

def admin_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Xodim qo'shish", callback_data="add_user")],
        [InlineKeyboardButton(text="📋 Xodimlar ro'yxati", callback_data="list_employees")],
        [InlineKeyboardButton(text="👥 Mijozlar ro'yxati", callback_data="admin_clients_list")],  # YANGI
        [InlineKeyboardButton(text="👥 Mijoz so'rovlari", callback_data="admin_client_requests")],
        [InlineKeyboardButton(text="⚠️ So'rovlar", callback_data="admin_corrections")],
        [InlineKeyboardButton(text="📊 Hisobotlar", callback_data="admin_reports")],
        [InlineKeyboardButton(text="🗺 Xarita", callback_data="admin_map")],
        [InlineKeyboardButton(text="📁 Barcha fayllar", callback_data="admin_files")],
        [InlineKeyboardButton(text="📋 Audit Log", callback_data="admin_audit")]
    ])


@dp.callback_query(F.data == "admin_clients_list")
async def admin_clients_list(callback: CallbackQuery):
    """Admin uchun barcha mijozlar ro'yxati"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        # Barcha tasdiqlangan mijozlarni olish
        clients = await conn.fetch('''
            SELECT c.id, c.full_name, c.phone, c.company, c.region, c.city,
                   COUNT(g.uid) as gen_count
            FROM clients c
            LEFT JOIN generators g ON c.id = g.current_client_id
            WHERE c.is_approved = TRUE
            GROUP BY c.id, c.full_name, c.phone, c.company, c.region, c.city
            ORDER BY c.full_name
        ''')
    
    if not clients:
        await callback.message.edit_text(
            "👥 <b>Mijozlar ro'yxati bo'sh</b>\n\n"
            "Hozircha tasdiqlangan mijozlar yo'q.",
            reply_markup=admin_main_keyboard(),
            parse_mode="HTML"
        )
        return
    
    # Mijozlar soni statistikasi
    total_clients = len(clients)
    clients_with_gen = sum(1 for c in clients if c['gen_count'] > 0)
    
    text = f"👥 <b>Mijozlar ro'yxati</b>\n\n"
    text += f"📊 Jami mijozlar: {total_clients} ta\n"
    text += f"🔧 Generatorli mijozlar: {clients_with_gen} ta\n\n"
    text += f"<i>Mijoz ismini bosing:</i>"
    
    # Mijozlar tugmalari (3 ta ustunda)
    buttons = []
    row = []
    for i, client in enumerate(clients, 1):
        company = f" ({client['company']})" if client['company'] else ""
        gen_icon = "🔧" if client['gen_count'] > 0 else "👤"
        
        row.append(InlineKeyboardButton(
            text=f"{gen_icon} {client['full_name']}{company}",
            callback_data=f"admin_client_view_{client['id']}"
        ))
        
        # Har 2 ta tugmadan keyin yangi qator
        if i % 2 == 0:
            buttons.append(row)
            row = []
    
    # Qolgan tugmalarni qo'shish
    if row:
        buttons.append(row)
    
    # Orqaga tugmasi
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")])
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("admin_client_view_"))
async def admin_view_client_details(callback: CallbackQuery):
    """Admin uchun mijozning barcha ma'lumotlarini ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    client_id = int(callback.data.split("_")[3])
    
    async with db_pool.acquire() as conn:
        # Mijoz ma'lumotlari
        client = await conn.fetchrow('''
            SELECT c.*, 
                   (SELECT COUNT(*) FROM generators WHERE current_client_id = c.id) as gen_count,
                   (SELECT COUNT(*) FROM deals WHERE client_id = c.id) as deal_count,
                   (SELECT SUM(sale_price) FROM deals WHERE client_id = c.id AND status = 'COMPLETED') as total_paid
            FROM clients c
            WHERE c.id = $1
        ''', client_id)
        
        if not client:
            await callback.answer("Mijoz topilmadi!", show_alert=True)
            return
        
        # Mijozning generatorlari
        generators = await conn.fetch('''
            SELECT g.*, 
                   d.id as deal_id, d.sale_price, d.status as deal_status, d.created_at as deal_date,
                   d.delivery_cost, d.installation_cost, d.other_costs, d.profit,
                   i.installation_date, i.motor_hours_start, i.motor_hours_current,
                   i.installer_id, emp.full_name as installer_name,
                   l.vehicle_info, l.driver_name, l.delivery_cost as log_delivery_cost,
                   l.planned_date as delivery_date, l.status as log_status
            FROM generators g
            LEFT JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            LEFT JOIN logistics l ON d.id = l.deal_id
            WHERE g.current_client_id = $1
            ORDER BY g.created_at DESC
        ''', client_id)
        
        # To'lovlar tarixi
        payments = await conn.fetch('''
            SELECT p.*, d.generator_uid
            FROM payments p
            JOIN deals d ON p.deal_id = d.id
            WHERE d.client_id = $1
            ORDER BY p.created_at DESC
        ''', client_id)
        
        # Servis tarixi
        services = await conn.fetch('''
            SELECT sh.*, g.model as gen_model
            FROM service_history sh
            JOIN generators g ON sh.generator_uid = g.uid
            WHERE g.current_client_id = $1
            ORDER BY sh.date DESC
            LIMIT 10
        ''', client_id)
    
    # O'zgaruvchilarga saqlash (backslash muammosini oldini olish uchun)
    location_text = ""
    if client['geo_lat'] and client['geo_lon']:
        lat = client['geo_lat']
        lon = client['geo_lon']
        maps_link = f"https://maps.google.com/?q={lat},{lon}"
        location_text = f"\n📍 <a href='{maps_link}'>Xaritada ko'rish</a>"
    
    company_text = ""
    if client['company']:
        company_text = f"\n🏢 Kompaniya: {client['company']}"
    
    region_text = ""
    if client['region']:
        region_text = f"\n🌍 Viloyat: {client['region']}"
    
    city_text = ""
    if client['city']:
        city_text = f"\n🏙 Shahar: {client['city']}"
    
    address_str = client['address'] or 'Kiritilmagan'
    created_at_str = client['created_at'].strftime('%d.%m.%Y')
    total_paid_val = client['total_paid'] or 0
    
    # Asosiy mijoz ma'lumotlari
    main_text = (f"👤 <b>{client['full_name']}</b>\n\n"
                f"🆔 Mijoz ID: <code>{client_id}</code>\n"
                f"📱 Telefon: {client['phone']}"
                f"{company_text}"
                f"\n📍 Manzil: {address_str}"
                f"{region_text}{city_text}"
                f"{location_text}\n\n"
                f"📊 Statistika:\n"
                f"🔧 Generatorlar: {client['gen_count']} ta\n"
                f"📋 Bitimlar: {client['deal_count']} ta\n"
                f"💰 Jami to'langan: {total_paid_val:,.0f} so'm\n\n"
                f"📅 Ro'yxatdan o'tgan: {created_at_str}")
    
    # Birinchi xabar - mijoz ma'lumotlari
    await callback.message.edit_text(
        main_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Mijozni o'chirish", callback_data=f"admin_delete_client_{client_id}")],
            [InlineKeyboardButton(text="✏️ Ma'lumotni tahrirlash", callback_data=f"admin_edit_client_{client_id}")],
            [InlineKeyboardButton(text="◀️ Mijozlar ro'yxatiga", callback_data="admin_clients_list")]
        ]),
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    
    # Generatorlar bo'yicha alohida xabarlar
    if generators:
        for gen in generators:
            # QR kod yo'li
            qr_path = gen['qr_code_path'] or f"{UPLOAD_DIR}/qrcodes/{gen['uid']}.png"
            
            # Statuslar
            gen_status = GEN_STATUSES.get(gen['status'], gen['status'])
            deal_status = DEAL_STATUSES.get(gen['deal_status'], gen['deal_status'] or "Noma'lum")
            
            # Garantiya
            warranty_text = "Noma'lum"
            warranty_end_date_str = "Noma'lum"
            if gen['warranty_start_date'] and gen['warranty_months']:
                warranty_end = gen['warranty_start_date'] + timedelta(days=30*gen['warranty_months'])
                remaining = (warranty_end - datetime.now().date()).days
                warranty_end_date_str = warranty_end.strftime('%d.%m.%Y')
                warranty_text = f"{warranty_end_date_str} ({remaining} kun qoldi)" if remaining > 0 else f"{warranty_end_date_str} (⛔ TUGAGAN)"
            
            # Moliyaviy ma'lumotlar
            purchase = gen['purchase_price'] or 0
            sale = gen['sale_price'] or 0
            delivery = gen['delivery_cost'] or 0
            install = gen['installation_cost'] or 0
            other = gen['other_costs'] or 0
            profit = gen['profit'] or (sale - purchase - delivery - install - other)
            
            manufacturer_str = gen['manufacturer'] or "Noma'lum"
            year_str = str(gen['manufacture_year']) if gen['manufacture_year'] else "Noma'lum"
            serial_str = gen['serial_number'] or "Noma'lum"
            
            gen_text = (f"🔧 <b>Generator: {gen['model']}</b>\n\n"
                       f"🆔 UID: <code>{gen['uid']}</code>\n"
                       f"🔢 Seriya: {serial_str}\n"
                       f"⚡ Quvvat: {gen['power_kw']} kVA\n"
                       f"🏭 Ishlab chiqaruvchi: {manufacturer_str}\n"
                       f"📅 Ishlab chiqarilgan yil: {year_str}\n\n"
                       f"📦 Status: {gen_status}\n"
                       f"📋 Bitim statusi: {deal_status}\n\n"
                       f"💰 Moliyaviy ma'lumotlar:\n"
                       f"  Sotib olish: {purchase:,.0f} so'm\n"
                       f"  Sotuv: {sale:,.0f} so'm\n"
                       f"  Yetkazish: {delivery:,.0f} so'm\n"
                       f"  O'rnatish: {install:,.0f} so'm\n"
                       f"  Boshqa: {other:,.0f} so'm\n"
                       f"  <b>Foyda: {profit:,.0f} so'm</b>\n\n"
                       f"🛡 Garantiya: {gen['warranty_months']} oy\n"
                       f"📅 Garantiya tugaydi: {warranty_text}")
            
            # O'rnatish ma'lumotlari
            if gen['installation_date']:
                installer_name_str = gen['installer_name'] or "Noma'lum"
                install_date_str = gen['installation_date'].strftime('%d.%m.%Y')
                motor_start = gen['motor_hours_start'] or 0
                motor_current = gen['motor_hours_current'] or 0
                
                gen_text += (f"\n\n🔧 O'rnatish ma'lumotlari:\n"
                           f"  📅 Sana: {install_date_str}\n"
                           f"  👤 Montajchi: {installer_name_str}\n"
                           f"  ⏱ Boshlang'ich moto-soat: {motor_start}\n"
                           f"  ⏱ Joriy moto-soat: {motor_current}")
            
            # Yetkazish ma'lumotlari
            if gen['vehicle_info']:
                driver_name_str = gen['driver_name'] or "Noma'lum"
                delivery_date_str = gen['delivery_date'].strftime('%d.%m.%Y') if gen['delivery_date'] else "Noma'lum"
                log_delivery_cost = gen['log_delivery_cost'] or 0
                
                gen_text += (f"\n\n🚚 Yetkazish ma'lumotlari:\n"
                           f"  🚛 Mashina: {gen['vehicle_info']}\n"
                           f"  👤 Haydovchi: {driver_name_str}\n"
                           f"  📅 Reja: {delivery_date_str}\n"
                           f"  💵 Narxi: {log_delivery_cost:,.0f} so'm")
            
            # Tugmalar
            buttons = [
                [
                    InlineKeyboardButton(text="📋 Bitimni ko'rish", callback_data=f"admin_view_deal_{gen['deal_id']}"),
                    InlineKeyboardButton(text="📁 Fayllar", callback_data=f"admin_gen_files_{gen['uid']}")
                ],
                [
                    InlineKeyboardButton(text="🔧 Servis tarixi", callback_data=f"admin_gen_service_{gen['uid']}"),
                    InlineKeyboardButton(text="📊 Hisobot", callback_data=f"admin_gen_report_{gen['uid']}")
                ]
            ]
            
            # QR kodni yuborish (agar mavjud bo'lsa)
            if os.path.exists(qr_path):
                await callback.message.answer_photo(
                    FSInputFile(qr_path),
                    caption=gen_text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                    parse_mode="HTML"
                )
            else:
                await callback.message.answer(
                    gen_text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                    parse_mode="HTML"
                )
    else:
        await callback.message.answer(
            "❌ <b>Bu mijozda generatorlar yo'q</b>",
            parse_mode="HTML"
        )
    
    # To'lovlar tarixi (agar mavjud bo'lsa)
    if payments:
        pay_text = "💰 <b>To'lovlar tarixi:</b>\n\n"
        for pay in payments:
            seller_conf = "✅" if pay['seller_confirmed'] else "⏳"
            acc_conf = "✅" if pay['accountant_confirmed'] else "⏳"
            pay_date_str = pay['created_at'].strftime('%d.%m.%Y')
            payment_method_str = pay['payment_method'] or "Noma'lum"
            
            pay_text += (f"🆔 #{pay['id']} - {pay['amount']:,.0f} so'm\n"
                        f"   📅 {pay_date_str}\n"
                        f"   💳 Usul: {payment_method_str}\n"
                        f"   👤 Sotuvchi: {seller_conf} | 👤 Buxgalter: {acc_conf}\n\n")
        
        await callback.message.answer(
            pay_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💰 Yangi to'lov", callback_data=f"admin_add_payment_{client_id}")]
            ]),
            parse_mode="HTML"
        )
    
    # Servis tarixi (agar mavjud bo'lsa)
    if services:
        serv_text = "🔧 <b>Servis tarixi:</b>\n\n"
        for serv in services[:5]:  # Faqat 5 ta oxirgi
            serv_date_str = serv['date'].strftime('%d.%m.%Y') if serv['date'] else "Noma'lum"
            desc = serv['description'] or ""
            desc_short = desc[:100] + ('...' if len(desc) > 100 else '')
            motor_hours_str = str(serv['motor_hours']) if serv['motor_hours'] else "Noma'lum"
            cost_val = serv['cost'] or 0
            
            serv_text += (f"📅 {serv_date_str}\n"
                         f"   🔧 {serv['service_type']}\n"
                         f"   📝 {desc_short}\n"
                         f"   ⏱ Moto-soat: {motor_hours_str}\n"
                         f"   💵 Narxi: {cost_val:,.0f} so'm\n"
                         f"{'─' * 30}\n")
        
        await callback.message.answer(
            serv_text,
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("admin_view_deal_"))
async def admin_view_deal_details(callback: CallbackQuery):
    """Bitim ma'lumotlarini ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    deal_id = int(callback.data.split("_")[3])
    
    async with db_pool.acquire() as conn:
        deal = await conn.fetchrow('''
            SELECT d.*, 
                   c.full_name as client_name, c.phone as client_phone,
                   g.uid, g.model, g.power_kw, g.serial_number,
                   e.full_name as seller_name
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE d.id = $1
        ''', deal_id)
    
    if not deal:
        await callback.answer("Bitim topilmadi!", show_alert=True)
        return
    
    # O'zgaruvchilarga saqlash
    serial_str = deal['serial_number'] or "Noma'lum"
    seller_name_str = deal['seller_name'] or "Noma'lum"
    delivery_cost_val = deal['delivery_cost'] or 0
    install_cost_val = deal['installation_cost'] or 0
    other_costs_val = deal['other_costs'] or 0
    profit_val = deal['profit'] or 0
    margin_val = deal['profit_margin'] or 0
    created_at_str = deal['created_at'].strftime('%d.%m.%Y')
    deal_status_str = DEAL_STATUSES.get(deal['status'], deal['status'])
    
    text = (f"📋 <b>Bitim #{deal_id}</b>\n\n"
            f"🆔 Generator: <code>{deal['uid']}</code>\n"
            f"🔧 {deal['model']} ({deal['power_kw']}kVA)\n"
            f"🔢 Seriya: {serial_str}\n\n"
            f"👤 Mijoz: {deal['client_name']}\n"
            f"📱 {deal['client_phone']}\n"
            f"🧑‍💼 Sotuvchi: {seller_name_str}\n\n"
            f"💰 Sotuv narxi: {deal['sale_price']:,.0f} so'm\n"
            f"🚚 Yetkazish: {delivery_cost_val:,.0f} so'm\n"
            f"🔧 O'rnatish: {install_cost_val:,.0f} so'm\n"
            f"📦 Boshqa: {other_costs_val:,.0f} so'm\n"
            f"<b>💵 Jami foyda: {profit_val:,.0f} so'm</b>\n"
            f"<b>📊 Marja: {margin_val:.1f}%</b>\n\n"
            f"📅 Yaratilgan: {created_at_str}\n"
            f"📊 Status: {deal_status_str}")
    
    await callback.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"admin_client_view_{deal['client_id']}")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("admin_gen_files_"))
async def admin_gen_files(callback: CallbackQuery):
    """Generator fayllarini ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.split("_")[3]
    
    async with db_pool.acquire() as conn:
        files = await conn.fetch('''
            SELECT * FROM files 
            WHERE entity_type = 'gen' AND entity_id = $1
            ORDER BY uploaded_at DESC
        ''', uid)
    
    if not files:
        await callback.answer("Fayllar topilmadi!", show_alert=True)
        return
    
    await callback.message.answer(
        f"📁 <b>Generator {uid} fayllari ({len(files)} ta):</b>",
        parse_mode="HTML"
    )
    
    for file in files:
        size_mb = file['file_size'] / (1024 * 1024) if file['file_size'] else 0
        
        text = (f"📄 <b>{file['file_name']}</b>\n"
                f"📝 Tur: {file['file_type']}\n"
                f"📊 Hajmi: {size_mb:.2f} MB\n"
                f"👤 Yuklagan: {file['uploaded_by']}\n"
                f"📅 {file['uploaded_at'].strftime('%d.%m.%Y %H:%M')}")
        
        # Faylni yuborish
        try:
            if file['file_path'] and os.path.exists(file['file_path']):
                if file['mime_type'] and file['mime_type'].startswith('image'):
                    await callback.message.answer_photo(
                        FSInputFile(file['file_path']),
                        caption=text,
                        parse_mode="HTML"
                    )
                else:
                    await callback.message.answer_document(
                        FSInputFile(file['file_path']),
                        caption=text,
                        parse_mode="HTML"
                    )
            else:
                await callback.message.answer(
                    text + "\n\n❌ Fayl serverda topilmadi",
                    parse_mode="HTML"
                )
        except Exception as e:
            logger.error(f"Fayl yuborishda xatolik: {e}")
            await callback.message.answer(
                text + f"\n\n❌ Xatolik: {str(e)}",
                parse_mode="HTML"
            )

@dp.callback_query(F.data.startswith("admin_gen_service_"))
async def admin_gen_service_history(callback: CallbackQuery):
    """Generator servis tarixini ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.split("_")[3]
    
    async with db_pool.acquire() as conn:
        services = await conn.fetch('''
            SELECT sh.*, e.full_name as performer_name
            FROM service_history sh
            LEFT JOIN employees e ON sh.performed_by = e.telegram_id
            WHERE sh.generator_uid = $1
            ORDER BY sh.date DESC
        ''', uid)
    
    if not services:
        await callback.answer("Servis tarixi bo'sh!", show_alert=True)
        return
    
    text = f"🔧 <b>Generator {uid} servis tarixi</b>\n\n"
    
    for serv in services:
        performer_name_str = serv['performer_name'] or "Noma'lum"
        serv_date_str = serv['date'].strftime('%d.%m.%Y %H:%M') if serv['date'] else "Noma'lum"
        desc_str = serv['description'] or "Izoh yo'q"
        motor_hours_str = str(serv['motor_hours']) if serv['motor_hours'] else "Noma'lum"
        cost_val = serv['cost'] or 0
        
        text += (f"📅 {serv_date_str}\n"
                 f"🔧 {serv['service_type']}\n"
                 f"📝 {desc_str}\n"
                 f"⏱ Moto-soat: {motor_hours_str}\n"
                 f"👤 Bajaruvchi: {performer_name_str}\n"
                 f"💵 Narxi: {cost_val:,.0f} so'm\n"
                 f"{'─' * 30}\n")
    
    await callback.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Yangi servis yozuvi", callback_data=f"admin_add_service_{uid}")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_clients_list")]
        ]),
        parse_mode="HTML"
    ) 



@dp.callback_query(F.data == "admin_client_requests")
async def admin_client_requests(callback: CallbackQuery):
    """Admin uchun mijoz registratsiya so'rovlari"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        pending = await conn.fetch('''
            SELECT * FROM client_registration_requests 
            WHERE status = 'PENDING'
            ORDER BY created_at DESC
        ''')
    
    if not pending:
        await callback.message.edit_text(
            "✅ Kutilayotgan mijoz so'rovlari yo'q",
            reply_markup=admin_main_keyboard()
        )
        return
    
    await callback.message.edit_text(
        f"👥 <b>{len(pending)} ta mijoz so'rovi kutilmoqda</b>",
        reply_markup=admin_main_keyboard(),
        parse_mode="HTML"
    )
    
    for req in pending:
        text = (f"🆕 <b>So'rov #{req['id']}</b>\n\n"
                f"👤 {req['full_name']}\n"
                f"📱 {req['phone']}\n"
                f"📍 {req['address']}\n"
                f"🆔 Telegram ID: {req['telegram_id']}\n"
                f"📅 {req['created_at'].strftime('%d.%m.%Y %H:%M')}")
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Tasdiqlash", 
                        callback_data=f"approve_client_{req['id']}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Rad etish", 
                        callback_data=f"reject_client_{req['id']}"
                    )
                ]
            ]),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("approve_client_"))
async def approve_client_callback(callback: CallbackQuery):
    """Callback orqali mijozni tasdiqlash"""
    if not is_admin(callback.from_user.id):
        return
    
    request_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            'SELECT * FROM client_registration_requests WHERE id = $1', 
            request_id
        )
        
        if not req or req['status'] != 'PENDING':
            await callback.answer("So'rov topilmadi yoki allaqachon ko'rib chiqilgan!", show_alert=True)
            return
        
        # Mijozni yaratish
        client_id = await conn.fetchval('''
            INSERT INTO clients 
            (telegram_id, full_name, phone, address, geo_lat, geo_lon, 
             is_approved, approved_by, approved_at, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, CURRENT_TIMESTAMP, $7)
            RETURNING id
        ''', req['telegram_id'], req['full_name'], req['phone'],
             req['address'], req['geo_lat'], req['geo_lon'],
             callback.from_user.id)
        
        # So'rovni yangilash
        await conn.execute('''
            UPDATE client_registration_requests 
            SET status = 'APPROVED', processed_by = $1, processed_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', callback.from_user.id, request_id)
    
    # Mijozga xabar
    try:
        await bot.send_message(
            req['telegram_id'],
            f"🎉 <b>Tabriklaymiz, {req['full_name']}!</b>\n\n"
            f"Sizning so'rovingiz admin tomonidan tasdiqlandi.\n"
            f"Endi siz mijoz sifatida botdan foydalanishingiz mumkin.\n\n"
            f"/start - Mijoz panelini ochish",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Mijozga xabar yuborishda xatolik: {e}")
    
    await callback.answer("✅ Tasdiqlandi!")
    await callback.message.edit_text(
        f"✅ <b>Mijoz tasdiqlandi!</b>\n\n"
        f"👤 {req['full_name']}\n"
        f"📱 {req['phone']}"
    )

@dp.callback_query(F.data.startswith("reject_client_"))
async def reject_client_callback(callback: CallbackQuery):
    """Callback orqali mijozni rad etish"""
    if not is_admin(callback.from_user.id):
        return
    
    request_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            'SELECT * FROM client_registration_requests WHERE id = $1', 
            request_id
        )
        
        if not req:
            await callback.answer("So'rov topilmadi!", show_alert=True)
            return
        
        await conn.execute('''
            UPDATE client_registration_requests 
            SET status = 'REJECTED', processed_by = $1, processed_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', callback.from_user.id, request_id)
    
    # Mijozga xabar
    try:
        await bot.send_message(
            req['telegram_id'],
            "❌ <b>So'rovingiz rad etildi.</b>\n\n"
            "Qo'shimcha ma'lumot uchun admin bilan bog'laning.",
            parse_mode="HTML"
        )
    except:
        pass
    
    await callback.answer("❌ Rad etildi")
    await callback.message.edit_text("❌ So'rov rad etildi.")


def roles_keyboard():
    buttons = []
    for key, value in ROLES.items():
        # Faqat xodimlar rollari (mijoz yo'q)
        if key not in ['admin', 'mijoz']:
            buttons.append([InlineKeyboardButton(text=value, callback_data=f"role_{key}")])
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="back_admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def warehouse_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Generator qo'shish", callback_data="wh_add_gen")],
        [InlineKeyboardButton(text="📦 Sklad qoldig'i", callback_data="wh_inventory")],
        [InlineKeyboardButton(text="🔍 Qidirish", callback_data="wh_search")],
        [InlineKeyboardButton(text="📊 Hisobot", callback_data="wh_report")],
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])


def seller_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Yangi bitim", callback_data="sl_new_deal")],
        [InlineKeyboardButton(text="📋 Mening bitimlarim", callback_data="sl_my_deals")],
        [InlineKeyboardButton(text="💰 To'lovni tasdiqlash", callback_data="sl_confirm_payment")],
        [InlineKeyboardButton(text="✏️ Ma'lumotni to'g'rilash", callback_data="request_correction")],  # YANGI
        [InlineKeyboardButton(text="📊 Mening hisobotim", callback_data="sl_report")],
        [InlineKeyboardButton(text="🗺 Mijozlar xaritasi", callback_data="sl_map")],
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])

def accountant_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 To'lov tasdiqlash", callback_data="acc_pending")],
        [InlineKeyboardButton(text="📊 Moliyaviy hisobot", callback_data="acc_finance")],
        [InlineKeyboardButton(text="📈 Marja hisoboti", callback_data="acc_margin")],
        [InlineKeyboardButton(text="📋 Barcha bitimlar", callback_data="acc_all_deals")],
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])

def logistic_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚚 Yetkazish rejasi", callback_data="log_pending")],
        [InlineKeyboardButton(text="📍 Mening marshrutlarim", callback_data="log_my_routes")],
        [InlineKeyboardButton(text="✏️ Ma'lumotni to'g'rilash", callback_data="request_correction")],  # YANGI
        [InlineKeyboardButton(text="📊 Yetkazish hisoboti", callback_data="log_report")],
        [InlineKeyboardButton(text="🗺 Xarita", callback_data="log_map")],
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])

def installer_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 O'rnatish vazifalari", callback_data="inst_pending")],
        [InlineKeyboardButton(text="📸 Akt yuklash", callback_data="inst_upload")],
        [InlineKeyboardButton(text="📋 Mening ishlarim", callback_data="inst_my_works")],
        [InlineKeyboardButton(text="✏️ Ma'lumotni to'g'rilash", callback_data="request_correction")],  # YANGI
        [InlineKeyboardButton(text="📊 Hisobot", callback_data="inst_report")],
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])

def client_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Mening generatorim", callback_data="cl_my_gen")],
        [InlineKeyboardButton(text="📜 Garantiya", callback_data="cl_warranty")],
        [InlineKeyboardButton(text="🔧 Servis chaqirish", callback_data="cl_service")],
        [InlineKeyboardButton(text="📋 Xizmat tarixi", callback_data="cl_history")],
        [InlineKeyboardButton(text="📞 Aloqa", callback_data="cl_contact")]
    ])

def confirm_keyboard(confirm_data: str, cancel_data: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=confirm_data),
            InlineKeyboardButton(text="❌ Bekor qilish", callback_data=cancel_data)
        ]
    ])

# ============ STATES ============

class AddEmployee(StatesGroup):
    selecting_role = State()
    entering_name = State()
    entering_phone = State()
    entering_telegram_id = State()
    selecting_region = State()      # YANGI
    selecting_city = State()        # YANGI
    confirming = State()

class AddGenerator(StatesGroup):
    entering_model = State()
    entering_power = State()
    entering_serial = State()
    entering_manufacturer = State()
    entering_year = State()
    entering_price = State()
    entering_warranty = State()
    uploading_photos = State()
    uploading_documents = State()
    confirming = State()

class CreateDeal(StatesGroup):
    selecting_generator = State()
    selecting_client = State()
    entering_client_name = State()
    entering_client_phone = State()
    entering_client_company = State()
    entering_address = State()
    entering_location = State()
    selecting_installer_region = State()    # Viloyat tanlash
    selecting_installer_city = State()      # Tuman tanlash  
    selecting_installer = State()           # Montajchini tanlash
    entering_price = State()
    confirming = State()


class SellerPayment(StatesGroup):
    selecting_deal = State()
    entering_amount = State()
    entering_method = State()
    confirming = State()

class LogisticsAssign(StatesGroup):
    selecting_deal = State()
    entering_vehicle = State()
    entering_driver = State()
    entering_driver_phone = State()
    entering_cost = State()
    selecting_who_pays = State()
    entering_date = State()
    confirming = State()

class InstallationWork(StatesGroup):
    selecting_deal = State()
    entering_motor_hours = State()
    uploading_photos = State()
    uploading_videos = State()
    entering_notes = State()
    confirming = State()

class ServiceRequest(StatesGroup):
    selecting_generator = State()
    entering_problem = State()
    uploading_photos = State()
    confirming = State()

class UploadFile(StatesGroup):
    selecting_entity = State()
    entering_entity_id = State()
    selecting_file_type = State()
    uploading_file = State()
    setting_permissions = State()


class ClientRegistration(StatesGroup):
    entering_name = State()
    requesting_phone = State()
    selecting_region = State()      # YANGI - viloyat tanlash
    selecting_city = State()        # YANGI - tuman tanlash
    entering_address = State()      # Manzil (ko'cha, uy)
    entering_location = State()     # GEOLOKATSIYA - QO'SHING
    confirming = State()



class CorrectionRequest(StatesGroup):
    selecting_entity = State()
    entering_entity_id = State()
    selecting_field = State()
    entering_new_value = State()
    entering_reason = State()
    confirming = State()




class ClientGeoAfterPayment(StatesGroup):
    requesting_location = State()
    confirming_location = State()


class WarehouseSearch(StatesGroup):
    entering_query = State()



# ============ START & MAIN MENU ============

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: Command = None):
    user_id = message.from_user.id  # Bu har doim foydalanuvchi ID si bo'ladi (start komandasi)
    
    # Agar mijoz o'z generatorini ko'rishni xohlsa (UID orqali)
    if command and command.args:
        uid = command.args
        # user_id ni aniq uzatamiz
        await show_generator_by_uid(message, uid, user_id=user_id)
        return
    
    if is_admin(user_id):
        await message.answer(
            "👋 Assalomu alaykum, Admin!\n\nCore Energy Management System",
            reply_markup=admin_main_keyboard()
        )
        return
    
    # Xodim tekshiruvi
    employee = await get_employee_by_telegram_id(user_id)
    if employee:
        await show_role_menu(message, employee['role'], employee)
        return
    
    # Mijoz tekshiruvi - faqat admin tasdiqlagan mijozlar
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow(
            'SELECT * FROM clients WHERE telegram_id = $1 AND is_approved = TRUE', 
            user_id
        )
        if client:
            await message.answer(
                f"👋 Assalomu alaykum, {client['full_name']}!\n\n"
                f"Core Energy mijozlar paneli",
                reply_markup=client_menu()
            )
            return
        
        # Tekshirish - bu foydalanuvchi allaqachon so'rov yuborganmi
        pending = await conn.fetchrow(
            'SELECT * FROM client_registration_requests WHERE telegram_id = $1 AND status = $2',
            user_id, 'PENDING'
        )
        if pending:
            await message.answer(
                "⏳ <b>So'rovingiz ko'rib chiqilmoqda...</b>\n\n"
                f"Sizning so'rovingiz admin tasdig'ini kutilmoqda.\n"
                f"Iltimos, kuting. Tasdiqlangandan so'ng sizga xabar yuboriladi.",
                parse_mode="HTML"
            )
            return
    
    # Yangi mijoz - registratsiya boshlash
    await message.answer(
        "👋 <b>Assalomu alaykum!</b>\n\n"
        "Core Energy botiga xush kelibsiz!\n"
        "Mijoz sifatida ro'yxatdan o'tish uchun ma'lumotlaringizni yuboring.\n\n"
        "✏️ <b>Ism va familiyangizni kiriting:</b>",
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.entering_name)

# ============ CLIENT MODULE ============




async def show_role_menu(message: Message, role: str, employee: dict):
    """Rol bo'yicha menyu ko'rsatish - YANGILANGAN (viloyat/shahar bilan)"""
    role_name = ROLES.get(role, role)
    
    # Viloyat/shahar ma'lumoti
    location_text = ""
    if employee.get('region') and employee.get('city'):
        location_text = f"\n📍 Hudud: {employee['region']}, {employee['city']}"
    
    text = (f"👋 Assalomu alaykum, <b>{employee['full_name']}</b>!\n\n"
            f"🎓 Rol: {role_name}"
            f"{location_text}\n"
            f"📅 Qo'shilgan: {employee['added_date'].strftime('%d.%m.%Y')}")
    
    if role == 'ombor':
        kb = warehouse_menu()
    elif role == 'sotuvchi':
        kb = seller_menu()
    elif role == 'buxgalter':
        kb = accountant_menu()
    elif role == 'logist':
        kb = logistic_menu()
    elif role == 'montajchi':
        kb = installer_menu()
    else:
        kb = None
    
    # Login audit log
    await log_action(
        message.from_user.id,
        'LOGIN',
        'employees',
        str(message.from_user.id),
        new_data={'role': role, 'action': 'menu_opened'},
        role=role
    )
    
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(ClientRegistration.entering_name)
async def process_client_reg_name(message: Message, state: FSMContext):
    await state.update_data(full_name=message.text.strip())
    
    # CONTACT orqali telefon so'rash - MUHIM!
    await message.answer(
        "📱 <b>Telefon raqamingizni yuboring:</b>\n\n"
        "Pastdagi tugmani bosing:",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📱 Telefon raqamni yuborish", request_contact=True)]
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    await state.set_state(ClientRegistration.requesting_phone)


@dp.message(ClientRegistration.requesting_phone, F.contact)
async def process_client_reg_phone_contact(message: Message, state: FSMContext):
    contact = message.contact
    phone = contact.phone_number
    user_id = message.from_user.id
    
    # TEKSHIRISH: O'zining contactimi yoki boshqa odamniki?
    if contact.user_id != user_id:
        await message.answer(
            "❌ <b>Xatolik!</b> O'zingizning telefon raqamingizni yuboring!",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📱 Telefon raqamni yuborish", request_contact=True)]
                ],
                resize_keyboard=True,
                one_time_keyboard=True
            )
        )
        return
    
    # Formatni to'g'rilash
    if not phone.startswith("+"):
        phone = "+" + phone
    
    # Bazada tekshirish
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            'SELECT telegram_id FROM clients WHERE phone = $1 AND is_approved = TRUE',
            phone
        )
        if existing and existing['telegram_id'] != user_id:
            await message.answer(
                "❌ Bu telefon boshqa mijozga tegishli!",
                reply_markup=ReplyKeyboardRemove()
            )
            await state.clear()
            return
    
    await state.update_data(phone=phone)
    
    # YANGI: Viloyat tanlash
    await message.answer(
        "📍 <b>Viloyatingizni tanlang:</b>",
        parse_mode="HTML",
        reply_markup=get_regions_keyboard()
    )
    await state.set_state(ClientRegistration.selecting_region)

@dp.callback_query(F.data.startswith("region_"), ClientRegistration.selecting_region)
async def process_client_region(callback: CallbackQuery, state: FSMContext):
    """Mijoz uchun viloyat tanlash"""
    region = callback.data.split("_", 1)[1]
    await state.update_data(region=region)
    
    await callback.message.edit_text(
        f"📍 <b>Viloyat:</b> {region}\n\n"
        f"📍 <b>Tuman/Shaharingizni tanlang:</b>",
        reply_markup=get_districts_keyboard(region),
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.selecting_city)

@dp.callback_query(F.data.startswith("district_"), ClientRegistration.selecting_city)
async def process_client_city(callback: CallbackQuery, state: FSMContext):
    """Mijoz uchun tuman tanlash"""
    city = callback.data.split("_", 1)[1]
    await state.update_data(city=city)
    data = await state.get_data()
    
    await callback.message.edit_text(
        f"📍 <b>Manzil:</b> {data['region']}, {city}\n\n"
        f"🏠 <b>Aniq manzilingizni kiriting:</b>\n"
        f"(Ko'cha nomi, uy raqami, mo'jal):",
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.entering_address)

@dp.message(ClientRegistration.entering_address)
async def process_client_address_new(message: Message, state: FSMContext):
    """Aniq manzilni olish (geolokatsiya EMAS)"""
    await state.update_data(address=message.text.strip())
    data = await state.get_data()
    
    # Tasdiqlash oynasi
    location_text = f"{data['region']}, {data['city']}"
    
    await message.answer(
        f"📋 <b>Ma'lumotlaringiz:</b>\n\n"
        f"👤 Ism: {data['full_name']}\n"
        f"📱 Telefon: {data['phone']}\n"
        f"📍 Viloyat/Tuman: {location_text}\n"
        f"🏠 Manzil: {data['address']}\n\n"
        f"✅ Adminga yuborilsinmi?",
        reply_markup=confirm_keyboard("client_reg_submit", "client_reg_cancel"),
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.confirming)



@dp.message(ClientRegistration.requesting_phone)
async def process_client_reg_phone_invalid(message: Message, state: FSMContext):
    # Agar contact emas, matn yuborsa
    await message.answer(
        "❌ Iltimos, <b>tugma orqali</b> telefon raqamingizni yuboring!",
        parse_mode="HTML"
    )


    """
    Agar contact emas, matn yuborsa
    """
@dp.message(ClientRegistration.requesting_phone)
async def process_client_reg_phone(message: Message, state: FSMContext):
    # Agar contact emas, matn yuborsa
    await message.answer(
        "❌ Iltimos, <b>tugma orqali</b> telefon raqamingizni yuboring!",
        parse_mode="HTML"
    )
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Noto'g'ri format! (+998XXXXXXXXX):")
        return
    
    # Telefon mavjudligini tekshirish
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            'SELECT 1 FROM clients WHERE phone = $1 AND is_approved = TRUE', 
            phone
        )
        if existing:
            await message.answer(
                "❌ Bu telefon raqam allaqachon ro'yxatdan o'tgan!\n"
                "Agar bu sizning raqamingiz bo'lsa, admin bilan bog'laning."
            )
            await state.clear()
            return
    
    await state.update_data(phone=phone)
    await message.answer(
        "📍 <b>Manzilingizni kiriting:</b>\n"
        "(Viloyat, tuman, ko'cha, uy)",
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.entering_address)

@dp.message(ClientRegistration.entering_address)
async def process_client_reg_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text.strip())
    await message.answer(
        "🗺 <b>Geolokatsiyangizni yuboring:</b>\n\n"
        "Telegram attachment dan joylashuvni yuboring yoki 'keyinroq' deb yozing:",
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.entering_location)

@dp.message(ClientRegistration.entering_location, F.location)
async def process_client_reg_location(message: Message, state: FSMContext):
    await state.update_data(
        lat=message.location.latitude, 
        lon=message.location.longitude
    )
    await show_client_reg_confirmation(message, state)

@dp.message(ClientRegistration.entering_location, F.text.lower() == "keyinroq")
async def skip_client_reg_location(message: Message, state: FSMContext):
    await state.update_data(lat=None, lon=None)
    await show_client_reg_confirmation(message, state)

async def show_client_reg_confirmation(message: Message, state: FSMContext):
    data = await state.get_data()
    
    location_status = "✅ Yuborilgan" if data.get('lat') else "❌ Keyinroq"
    
    await message.answer(
        f"📋 <b>Ma'lumotlaringiz:</b>\n\n"
        f"👤 Ism: {data['full_name']}\n"
        f"📱 Telefon: {data['phone']}\n"
        f"📍 Manzil: {data['address']}\n"
        f"🗺 Lokatsiya: {location_status}\n\n"
        f"✅ Adminga yuborilsinmi?",
        reply_markup=confirm_keyboard("client_reg_submit", "client_reg_cancel"),
        parse_mode="HTML"
    )
    await state.set_state(ClientRegistration.confirming)

@dp.callback_query(F.data == "client_reg_submit")
async def submit_client_registration(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            # So'rovni saqlash (geo_lat, geo_lon = NULL)
            request_id = await conn.fetchval('''
                INSERT INTO client_registration_requests 
                (telegram_id, full_name, phone, address, geo_lat, geo_lon, 
                 region, city, status, created_at)
                VALUES ($1, $2, $3, $4, NULL, NULL, $5, $6, 'PENDING', CURRENT_TIMESTAMP)
                RETURNING id
            ''', user_id, data['full_name'], data['phone'], 
                 data['address'], data['region'], data['city'])
        
        # Adminga xabar - TUGMALAR BILAN
        location_text = f"{data['region']}, {data['city']}"
        
        # Admin uchun keyboard
        admin_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"approve_client_{request_id}"),
                InlineKeyboardButton(text="❌ Rad etish", callback_data=f"reject_client_{request_id}")
            ]
        ])
        
        # Adminga yuborish
        await bot.send_message(
            ADMIN_ID,
            f"🆕 <b>Yangi mijoz so'rovi!</b>\n\n"
            f"ID: #{request_id}\n"
            f"👤 {data['full_name']}\n"
            f"📱 {data['phone']}\n"
            f"📍 {location_text}\n"
            f"🏠 {data['address']}",
            reply_markup=admin_keyboard,
            parse_mode="HTML"
        )
        
        await callback.message.edit_text(
            "✅ <b>So'rovingiz yuborildi!</b>\n\n"
            "Admin tasdiqlagandan so'ng sizga xabar yuboriladi.\n"
            "Odatda bu 1-2 soat ichida amalga oshiriladi.\n\n"
            "⚠️ <b>Geolokatsiyangiz keyinroq so'raladi</b> (to'lov tasdiqlangandan so'ng).",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Xatolik: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "client_reg_cancel")
async def cancel_client_registration(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Bekor qilindi.")
    await state.clear()



@dp.message(Command("approve_client"))
async def approve_client_command(message: Message, command: Command):
    if not is_admin(message.from_user.id):
        return
    
    try:
        request_id = int(command.args.split("_")[2])
    except:
        await message.answer("❌ Format: /approve_client_<id>")
        return
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            'SELECT * FROM client_registration_requests WHERE id = $1', 
            request_id
        )
        
        if not req:
            await message.answer("❌ So'rov topilmadi!")
            return
        
        if req['status'] != 'PENDING':
            await message.answer(f"❌ So'rov allaqachon {req['status']}!")
            return
        
        # Mijozni yaratish
        client_id = await conn.fetchval('''
            INSERT INTO clients 
            (telegram_id, full_name, phone, address, geo_lat, geo_lon, 
             is_approved, approved_by, approved_at, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, CURRENT_TIMESTAMP, $7)
            RETURNING id
        ''', req['telegram_id'], req['full_name'], req['phone'],
             req['address'], req['geo_lat'], req['geo_lon'],
             message.from_user.id)
        
        # So'rovni yangilash
        await conn.execute('''
            UPDATE client_registration_requests 
            SET status = 'APPROVED', processed_by = $1, processed_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', message.from_user.id, request_id)
        
        await log_action(message.from_user.id, 'APPROVE', 'clients', client_id,
                        new_data={'name': req['full_name'], 'phone': req['phone']},
                        role='admin')
    
    # Mijozga xabar
    try:
        await bot.send_message(
            req['telegram_id'],
            f"🎉 <b>Tabriklaymiz, {req['full_name']}!</b>\n\n"
            f"Sizning so'rovingiz admin tomonidan tasdiqlandi.\n"
            f"Endi siz mijoz sifatida botdan foydalanishingiz mumkin.\n\n"
            f"/start - Mijoz panelini ochish",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Mijozga xabar yuborishda xatolik: {e}")
    
    await message.answer(
        f"✅ Mijoz tasdiqlandi!\n\n"
        f"👤 {req['full_name']}\n"
        f"📱 {req['phone']}\n"
        f"ID: {client_id}"
    )

@dp.message(Command("reject_client"))
async def reject_client_command(message: Message, command: Command):
    if not is_admin(message.from_user.id):
        return
    
    try:
        request_id = int(command.args.split("_")[2])
    except:
        await message.answer("❌ Format: /reject_client_<id>")
        return
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            'SELECT * FROM client_registration_requests WHERE id = $1', 
            request_id
        )
        
        if not req:
            await message.answer("❌ So'rov topilmadi!")
            return
        
        await conn.execute('''
            UPDATE client_registration_requests 
            SET status = 'REJECTED', processed_by = $1, processed_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', message.from_user.id, request_id)
    
    # Mijozga xabar
    try:
        await bot.send_message(
            req['telegram_id'],
            "❌ <b>So'rovingiz rad etildi.</b>\n\n"
            "Qo'shimcha ma'lumot uchun admin bilan bog'laning.",
            parse_mode="HTML"
        )
    except:
        pass
    
    await message.answer(f"❌ So'rov #{request_id} rad etildi.")




@dp.callback_query(F.data == "main_menu")
async def return_main(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if is_admin(user_id):
        await callback.message.edit_text(
            "👨‍💼 <b>Admin paneli</b>",
            reply_markup=admin_main_keyboard(),
            parse_mode="HTML"
        )
        return
    
    role = await get_user_role(user_id)
    if role:
        employee = await get_employee_by_telegram_id(user_id)
        await callback.message.delete()
        await show_role_menu(callback.message, role, employee)
    else:
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)

# ============ ADMIN PANEL ============

@dp.callback_query(F.data == "add_user")
async def process_add_user(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "Qaysi roldagi foydalanuvchini qo'shmoqchisiz?",
        reply_markup=roles_keyboard()
    )
    await state.set_state(AddEmployee.selecting_role)

@dp.callback_query(F.data.startswith("role_"))
async def process_role_selection(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    
    role = callback.data.split("_")[1]
    await state.update_data(selected_role=role, role_name=ROLES.get(role, role))
    
    # Agar montajchi bo'lsa, viloyat tanlash
    if role == 'montajchi':
        await callback.message.edit_text(
            f"✏️ <b>{ROLES.get(role, role)}</b> uchun yangi xodim\n\n"
            f"📍 <b>Viloyatni tanlang:</b>",
            reply_markup=get_regions_keyboard(),
            parse_mode="HTML"
        )
        await state.set_state(AddEmployee.selecting_region)
    else:
        await callback.message.edit_text(
            f"✏️ <b>{ROLES.get(role, role)}</b> uchun yangi xodim\n\n"
            f"To'liq ism va familiyani kiriting:",
            parse_mode="HTML"
        )
        await state.set_state(AddEmployee.entering_name)

@dp.callback_query(F.data.startswith("region_"), AddEmployee.selecting_region)
async def process_employee_region(callback: CallbackQuery, state: FSMContext):
    """Montajchi uchun viloyat tanlash"""
    if not is_admin(callback.from_user.id):
        return
    
    region = callback.data.split("_", 1)[1]
    await state.update_data(region=region)
    
    await callback.message.edit_text(
        f"✏️ <b>Viloyat:</b> {region}\n\n"
        f"📍 <b>Tuman/Shaharni tanlang:</b>",
        reply_markup=get_districts_keyboard(region),
        parse_mode="HTML"
    )
    await state.set_state(AddEmployee.selecting_city)

@dp.callback_query(F.data.startswith("district_"), AddEmployee.selecting_city)
async def process_employee_city(callback: CallbackQuery, state: FSMContext):
    """Montajchi uchun tuman tanlash"""
    if not is_admin(callback.from_user.id):
        return
    
    city = callback.data.split("_", 1)[1]
    await state.update_data(city=city)
    data = await state.get_data()
    
    await callback.message.edit_text(
        f"✏️ <b>Ma'lumotlar:</b>\n\n"
        f"🎓 Rol: {data['role_name']}\n"
        f"📍 Viloyat: {data['region']}\n"
        f"📍 Tuman: {city}\n\n"
        f"To'liq ism va familiyani kiriting:",
        parse_mode="HTML"
    )
    await state.set_state(AddEmployee.entering_name)

@dp.callback_query(F.data == "back_to_regions", AddEmployee.selecting_city)
async def back_to_regions_employee(callback: CallbackQuery, state: FSMContext):
    """Orqaga - viloyatlarga qaytish"""
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        f"📍 <b>Viloyatni tanlang:</b>",
        reply_markup=get_regions_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(AddEmployee.selecting_region)


@dp.message(AddEmployee.entering_name)
async def process_emp_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    await state.update_data(full_name=message.text.strip())
    await message.answer("📱 Telefon raqamini kiriting (+998XX XXX XX XX):")
    await state.set_state(AddEmployee.entering_phone)

@dp.message(AddEmployee.entering_phone)
async def process_emp_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Noto'g'ri format! (+998XXXXXXXXX):")
        return
    
    await state.update_data(phone=phone)
    await message.answer(
        "🆔 Telegram ID ni kiriting:\n"
        "(Faqat raqamlar, @userinfobot dan olish mumkin)"
    )
    await state.set_state(AddEmployee.entering_telegram_id)

@dp.message(AddEmployee.entering_telegram_id)
async def process_emp_telegram_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    
    try:
        telegram_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Faqat raqamlar kiriting!")
        return
    
    if telegram_id <= 0:
        await message.answer("❌ Noto'g'ri ID!")
        return
    
    # Tekshirish
    existing = await get_employee_by_telegram_id(telegram_id)
    if existing:
        await message.answer(
            f"❌ Bu ID allaqachon mavjud: {existing['full_name']}\n"
            f"Boshqa ID kiriting:"
        )
        return
    
    await state.update_data(telegram_id=telegram_id)
    data = await state.get_data()
    
    await message.answer(
        f"📋 <b>Tasdiqlang:</b>\n\n"
        f"👤 Ism: {data['full_name']}\n"
        f"📱 Tel: {data['phone']}\n"
        f"🎓 Rol: {data['role_name']}\n"
        f"🆔 ID: {telegram_id}\n\n"
        f"Saqlansinmi?",
        reply_markup=confirm_keyboard("confirm_save_emp", "cancel_add"),
        parse_mode="HTML"
    )
    await state.set_state(AddEmployee.confirming)

@dp.callback_query(F.data == "confirm_save_emp")
async def save_employee(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    
    data = await state.get_data()
    user_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            # Montajchi uchun region va city
            region = data.get('region')
            city = data.get('city')
            
            await conn.execute('''
                INSERT INTO employees 
                (telegram_id, full_name, phone, role, region, city, added_by, added_date, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, TRUE)
            ''', data['telegram_id'], data['full_name'], data['phone'],
                 data['selected_role'], region, city, user_id)
        
        # Xabarnoma
        location_text = ""
        if region and city:
            location_text = f"\n📍 {region}, {city}"
        
        try:
            await bot.send_message(
                data['telegram_id'],
                f"🎉 Tabriklaymiz, {data['full_name']}!\n\n"
                f"Siz <b>{data['role_name']}</b> sifatida tizimga qo'shildingiz."
                f"{location_text}\n\n"
                f"Botga qayta /start bosing.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Xabar yuborishda xatolik: {e}")
        
        await callback.message.edit_text(
            f"✅ <b>Xodim qo'shildi!</b>\n\n"
            f"👤 {data['full_name']}\n"
            f"🎓 {data['role_name']}"
            f"{location_text}",
            reply_markup=admin_main_keyboard(),
            parse_mode="HTML"
        )
        
        await log_action(callback.from_user.id, 'CREATE', 'employees', 
                        data['telegram_id'], new_data=data)
        
    except Exception as e:
        logger.error(f"Xodim saqlashda xatolik: {e}")
        await callback.message.edit_text(f"❌ Xatolik: {str(e)}")
    
    await state.clear()

# ============ ADMIN PANEL - XODIMLAR RO'YXATI (VILOYAT/SHAHAR BILAN) ============

@dp.callback_query(F.data == "list_employees")
async def list_employees(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT * FROM employees 
            WHERE is_active = TRUE 
            ORDER BY role, region, city, full_name
        ''')
    
    if not rows:
        await callback.answer("Xodimlar yo'q", show_alert=True)
        return
    
    text = "📋 <b>Xodimlar ro'yxati:</b>\n\n"
    current_role = None
    
    for row in rows:
        role_name = ROLES.get(row['role'], row['role'])
        if current_role != role_name:
            text += f"\n<b>{role_name}:</b>\n"
            current_role = role_name
        
        status = "🟢" if row['is_active'] else "🔴"
        
        # Viloyat/shahar
        location = ""
        if row.get('region') and row.get('city'):
            location = f"\n   📍 {row['region']}, {row['city']}"
        
        text += (f"{status} {row['full_name']} ({row['phone']})"
                f"{location}\n"
                f"   ID: <code>{row['telegram_id']}</code>\n")
    
    await callback.message.edit_text(
        text, 
        reply_markup=admin_main_keyboard(),
        parse_mode="HTML"
    )


def admin_files_menu():
    """Fayllar menyusi keyboard"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬆️ Fayl yuklash", callback_data="file_upload")],
        [InlineKeyboardButton(text="🔍 Generator fayllari", callback_data="file_search_gen")],
        [InlineKeyboardButton(text="📋 Bitim fayllari", callback_data="file_search_deal")],
        [InlineKeyboardButton(text="🔧 Servis fayllari", callback_data="file_search_service")],
        [InlineKeyboardButton(text="📊 Barcha fayllar", callback_data="file_list_all")],
        [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
    ])

# Handler - alohida nom!
@dp.callback_query(F.data == "admin_files")
async def admin_files_handler(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "📁 <b>Fayllar boshqaruvi</b>",
        reply_markup=admin_files_menu(),  # ✅ Parametrsiz!
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "file_upload")
async def start_file_upload(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📁 <b>Fayl yuklash</b>\n\nQaysi obyektga tegishli?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Generator (UID)", callback_data="filetype_gen")],
            [InlineKeyboardButton(text="📋 Bitim (ID)", callback_data="filetype_deal")],
            [InlineKeyboardButton(text="🔧 Servis", callback_data="filetype_service")],
            [InlineKeyboardButton(text="👤 Mijoz", callback_data="filetype_client")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_files")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(UploadFile.selecting_entity)

@dp.callback_query(F.data.startswith("filetype_"))
async def select_file_entity(callback: CallbackQuery, state: FSMContext):
    entity_type = callback.data.split("_")[1]
    await state.update_data(entity_type=entity_type)
    
    type_names = {
        'gen': 'Generator UID',
        'deal': 'Bitim ID',
        'service': 'Servis ID',
        'client': 'Mijoz ID'
    }
    
    await callback.message.edit_text(
        f"📝 <b>{type_names.get(entity_type)} ni kiriting:</b>",
        parse_mode="HTML"
    )
    await state.set_state(UploadFile.entering_entity_id)

@dp.message(UploadFile.entering_entity_id)
async def process_entity_id(message: Message, state: FSMContext):
    await state.update_data(entity_id=message.text.strip())
    
    await message.answer(
        "📂 <b>Fayl turini tanlang:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Shartnoma", callback_data="ftype_contract")],
            [InlineKeyboardButton(text="📋 Akt", callback_data="ftype_act")],
            [InlineKeyboardButton(text="🖼 Rasm", callback_data="ftype_photo")],
            [InlineKeyboardButton(text="🎥 Video", callback_data="ftype_video")],
            [InlineKeyboardButton(text="📑 Hujjat", callback_data="ftype_doc")],
            [InlineKeyboardButton(text="📊 Hisobot", callback_data="ftype_report")],
            [InlineKeyboardButton(text="🔧 Servis hujjati", callback_data="ftype_service")],
        ]),
        parse_mode="HTML"
    )
    await state.set_state(UploadFile.selecting_file_type)

@dp.callback_query(F.data.startswith("ftype_"))
async def select_file_type(callback: CallbackQuery, state: FSMContext):
    file_type = callback.data.split("_")[1]
    await state.update_data(file_type=file_type)
    
    await callback.message.edit_text(
        "📤 <b>Faylni yuboring:</b>\n\n"
        "PDF, Word, Excel, rasmlar yoki videolar qabul qilinadi.",
        parse_mode="HTML"
    )
    await state.set_state(UploadFile.uploading_file)

@dp.message(UploadFile.uploading_file, F.document | F.photo | F.video)
async def process_file_upload(message: Message, state: FSMContext):
    data = await state.get_data()
    
    # Faylni saqlash
    if message.document:
        file_obj = message.document
        file_type = "document"
    elif message.photo:
        file_obj = message.photo[-1]
        file_type = "photo"
    elif message.video:
        file_obj = message.video
        file_type = "video"  # BU YERDA VIDEO TURI ANIQLANADI
    else:
        await message.answer("❌ Noto'g'ri fayl turi!")
        return
    
    # Unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{data['entity_type']}_{data['entity_id']}_{timestamp}"
    
    file_path = await download_file(
        file_obj.file_id,
        "documents",
        filename,
        file_type  # BU YERDA "video" uzatiladi
    )
    
    if not file_path:
        await message.answer("❌ Faylni yuklashda xatolik!")
        return
    
    # Ruxsatlarni so'rash
    await state.update_data(
        file_path=file_path,
        file_name=getattr(file_obj, 'file_name', f'{filename}.jpg'),
        file_size=getattr(file_obj, 'file_size', 0),
        mime_type=getattr(file_obj, 'mime_type', 'image/jpeg')
    )
    
    await message.answer(
        "🔐 <b>Ruxsatlarni sozlash:</b>\n\nKim ko'rishi mumkin?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👥 Barcha xodimlar", callback_data="perm_employees")],
            [InlineKeyboardButton(text="🔒 Faqat admin", callback_data="perm_admin")],
            [InlineKeyboardButton(text="👤 Mijoz + xodimlar", callback_data="perm_client")],
            [InlineKeyboardButton(text="🌐 Ommaviy", callback_data="perm_public")],
        ]),
        parse_mode="HTML"
    )
    await state.set_state(UploadFile.setting_permissions)

@dp.callback_query(F.data.startswith("perm_"))
async def set_permissions(callback: CallbackQuery, state: FSMContext):
    perm_level = callback.data.split("_")[1]
    data = await state.get_data()
    user_id = callback.from_user.id
    
    # Ruxsatlar mapping
    permissions = {
        'employees': ['admin', 'ombor', 'sotuvchi', 'buxgalter', 'logist', 'montajchi'],
        'admin': ['admin'],
        'client': ['admin', 'sotuvchi', 'mijoz'],
        'public': ['all']
    }
    
    async with db_pool.acquire() as conn:
        file_id = await conn.fetchval('''
            INSERT INTO files 
            (entity_type, entity_id, file_type, file_name, file_path, 
             file_size, mime_type, uploaded_by, is_public, permissions)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
        ''', 
            data['entity_type'], 
            data['entity_id'], 
            data['file_type'],
            data['file_name'],
            data['file_path'],
            data['file_size'],
            data['mime_type'],
            user_id,
            perm_level == 'public',
            json.dumps(permissions.get(perm_level, ['admin']))
        )
        
        await log_action(user_id, 'UPLOAD', 'files', file_id,
                        new_data={'entity': f"{data['entity_type']}_{data['entity_id']}"},
                        role='admin')
    
    await callback.message.edit_text(
        f"✅ <b>Fayl yuklandi!</b>\n\n"
        f"ID: {file_id}\n"
        f"Obyekt: {data['entity_type']} - {data['entity_id']}\n"
        f"Ruxsat: {perm_level}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_files")]
        ]),
        parse_mode="HTML"
    )
    await state.clear()

async def get_entity_files(entity_type: str, entity_id: str, user_role: str, user_id: int = None):
    """Obyektga tegishli fayllarni olish (ruxsatlar bo'yicha)"""
    async with db_pool.acquire() as conn:
        files = await conn.fetch('''
            SELECT * FROM files 
            WHERE entity_type = $1 AND entity_id = $2
            AND (
                is_public = TRUE 
                OR $3 = 'admin'
                OR $3 IN (SELECT jsonb_array_elements_text(permissions))
                OR (uploaded_by = $4 AND $4 IS NOT NULL)
            )
            ORDER BY uploaded_at DESC
        ''', entity_type, entity_id, user_role, user_id)
    return files

@dp.callback_query(F.data.startswith("file_search_"))
async def search_files(callback: CallbackQuery, state: FSMContext):
    """Fayllarni qidirish - ID lar ro'yxatini inline knopkalar bilan ko'rsatish"""
    entity_type = callback.data.split("_")[2]
    user_id = callback.from_user.id
    role = await get_user_role(user_id)
    
    type_names = {
        'gen': 'Generatorlar', 
        'deal': 'Bitimlar', 
        'service': 'Servis yozuvlari'
    }
    
    # Bazadan ID lar ro'yxatini olish
    async with db_pool.acquire() as conn:
        if entity_type == 'gen':
            # Generator UID lari - ORDER BY ustuni SELECT ga qo'shildi
            rows = await conn.fetch('''
                SELECT DISTINCT g.uid, g.model, g.power_kw, g.status, g.created_at
                FROM generators g
                JOIN files f ON f.entity_type = 'gen' AND f.entity_id = g.uid
                ORDER BY g.created_at DESC
                LIMIT 50
            ''')
            buttons = []
            for row in rows:
                status_icon = {
                    'SKLADDA': '📦',
                    'SOTILDI': '💰',
                    'INSTALLED': '✅',
                    'SERVICING': '🔧'
                }.get(row['status'], '📋')
                
                btn_text = f"{status_icon} {row['uid'][:15]}... ({row['model'][:20]})"
                buttons.append([InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"filesel_gen_{row['uid']}"
                )])
                
        elif entity_type == 'deal':
            # Bitim ID lari - ORDER BY ustunlari SELECT ga qo'shildi
            rows = await conn.fetch('''
                SELECT DISTINCT d.id, c.full_name as client_name, 
                       g.model, d.status, d.created_at
                FROM deals d
                JOIN files f ON f.entity_type = 'deal' AND f.entity_id = d.id::text
                JOIN clients c ON d.client_id = c.id
                JOIN generators g ON d.generator_uid = g.uid
                ORDER BY d.created_at DESC
                LIMIT 50
            ''')
            buttons = []
            for row in rows:
                status_icon = {
                    'PENDING_PAYMENT': '⏳',
                    'COMPLETED': '✅',
                    'INSTALLING': '🔧'
                }.get(row['status'], '📋')
                
                date_str = row['created_at'].strftime('%d.%m') if row['created_at'] else ''
                btn_text = f"{status_icon} #{row['id']} - {row['client_name'][:15]} ({date_str})"
                buttons.append([InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"filesel_deal_{row['id']}"
                )])
                
        elif entity_type == 'service':
            # Servis ID lari - ORDER BY ustuni SELECT ga qo'shildi
            rows = await conn.fetch('''
                SELECT DISTINCT sh.id, sh.generator_uid, sh.service_type, 
                       sh.date, g.model
                FROM service_history sh
                JOIN files f ON f.entity_type = 'service' AND f.entity_id = sh.id::text
                JOIN generators g ON sh.generator_uid = g.uid
                ORDER BY sh.date DESC
                LIMIT 50
            ''')
            buttons = []
            for row in rows:
                date_str = row['date'].strftime('%d.%m') if row['date'] else ''
                btn_text = f"🔧 #{row['id']} - {row['generator_uid'][:12]}... ({date_str})"
                buttons.append([InlineKeyboardButton(
                    text=btn_text,
                    callback_data=f"filesel_service_{row['id']}"
                )])
        else:
            await callback.answer("Noto'g'ri tur!", show_alert=True)
            return
    
    if not buttons:
        await callback.message.edit_text(
            f"📂 <b>{type_names.get(entity_type)}</b>\n\n"
            f"Fayllari mavjud bo'lgan obyektlar topilmadi.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_files")]
            ]),
            parse_mode="HTML"
        )
        return
    
    # Orqaga tugmasi qo'shish
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_files")])
    
    await callback.message.edit_text(
        f"📂 <b>{type_names.get(entity_type)}</b>\n\n"
        f"Fayllarini ko'rish uchun obyektni tanlang:\n"
        f"<i>(Jami: {len(buttons)-1} ta)</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("filesel_"))
async def select_entity_files(callback: CallbackQuery, state: FSMContext):
    """Tanlangan obyektning fayllarini ko'rsatish"""
    parts = callback.data.split("_")
    entity_type = parts[1]
    entity_id = parts[2]
    
    user_id = callback.from_user.id
    role = await get_user_role(user_id)
    
    # Fayllarni olish
    files = await get_entity_files(entity_type, entity_id, role, user_id)
    
    if not files:
        await callback.answer("📂 Fayllar topilmadi!", show_alert=True)
        return
    
    # Obyekt haqida ma'lumot olish
    async with db_pool.acquire() as conn:
        if entity_type == 'gen':
            info = await conn.fetchrow('SELECT model, power_kw, status FROM generators WHERE uid = $1', entity_id)
            header_text = f"🔧 <b>Generator: {entity_id}</b>\nModel: {info['model'] if info else 'N/A'}\n"
        elif entity_type == 'deal':
            info = await conn.fetchrow('''
                SELECT d.id, c.full_name, g.model 
                FROM deals d 
                JOIN clients c ON d.client_id = c.id
                JOIN generators g ON d.generator_uid = g.uid
                WHERE d.id = $1
            ''', int(entity_id))
            header_text = f"📋 <b>Bitim #{entity_id}</b>\nMijoz: {info['full_name'] if info else 'N/A'}\n"
        elif entity_type == 'service':
            info = await conn.fetchrow('SELECT service_type, generator_uid FROM service_history WHERE id = $1', int(entity_id))
            header_text = f"🔧 <b>Servis #{entity_id}</b>\nGenerator: {info['generator_uid'] if info else 'N/A'}\n"
        else:
            header_text = f"📁 <b>Fayllar</b>\n"
    
    await callback.message.edit_text(
        f"{header_text}\n📁 <b>Jami fayllar: {len(files)} ta</b>",
        parse_mode="HTML"
    )
    
    # Har bir faylni alohida ko'rsatish
    for file in files:
        size_mb = file['file_size'] / (1024 * 1024) if file['file_size'] else 0
        
        text = (f"📄 <b>{file['file_name']}</b>\n"
                f"📝 Tur: {file['file_type']}\n"
                f"📊 Hajmi: {size_mb:.2f} MB\n"
                f"👤 Yuklagan: {file['uploaded_by']}\n"
                f"📅 {file['uploaded_at'].strftime('%d.%m.%Y %H:%M')}")
        
        buttons = [[InlineKeyboardButton(
            text="⬇️ Yuklab olish",
            callback_data=f"fileget_{file['id']}"
        )]]
        
        if role == 'admin':
            buttons[0].append(InlineKeyboardButton(
                text="🗑 O'chirish",
                callback_data=f"filedel_{file['id']}"
            ))
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
    
    # Orqaga tugmasi
    await callback.message.answer(
        "📂",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"file_search_{entity_type}")],
            [InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="main_menu")]
        ])
    )

@dp.message(State("file_search_id"))
async def process_file_search(message: Message, state: FSMContext):
    data = await state.get_data()
    entity_type = data['search_entity_type']
    entity_id = message.text.strip()
    user_id = message.from_user.id
    role = await get_user_role(user_id)
    
    files = await get_entity_files(entity_type, entity_id, role, user_id)
    
    if not files:
        await message.answer("📂 Fayllar topilmadi yoki ruxsat yo'q!")
        await state.clear()
        return
    
    await message.answer(f"📁 <b>Topilgan fayllar ({len(files)} ta):</b>", parse_mode="HTML")
    
    for file in files:
        size_mb = file['file_size'] / (1024 * 1024) if file['file_size'] else 0
        
        text = (f"📄 <b>{file['file_name']}</b>\n"
                f"📝 Tur: {file['file_type']}\n"
                f"📊 Hajmi: {size_mb:.2f} MB\n"
                f"👤 Yuklagan: {file['uploaded_by']}\n"
                f"📅 {file['uploaded_at'].strftime('%d.%m.%Y %H:%M')}")
        
        buttons = [[InlineKeyboardButton(
            text="⬇️ Yuklab olish",
            callback_data=f"fileget_{file['id']}"
        )]]
        
        if role == 'admin':
            buttons[0].append(InlineKeyboardButton(
                text="🗑 O'chirish",
                callback_data=f"filedel_{file['id']}"
            ))
        
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
    
    await state.clear()




# ============ AUDIT LOG MODULE - TO'LIQ YANGILANGAN (BARCHA LOG BIR XABARDA) ============

@dp.callback_query(F.data == "admin_audit")
async def audit_log_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "📋 <b>Audit Log - Harakatlar tarixi</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Qidirish (so'z bilan)", callback_data="audit_search")],
            [InlineKeyboardButton(text="👤 Foydalanuvchi bo'yicha", callback_data="audit_by_user")],
            [InlineKeyboardButton(text="📊 Jadval bo'yicha", callback_data="audit_by_table")],
            [InlineKeyboardButton(text="⚠️ So'nggi o'zgarishlar", callback_data="audit_changes")],
            [InlineKeyboardButton(text="📅 Bugun", callback_data="audit_today")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "audit_search")
async def audit_search(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "🔍 <b>Audit log qidirish</b>\n\n"
        "Qidirish so'zini kiriting (ID, ism, harakat):",
        parse_mode="HTML"
    )
    await state.set_state("audit_search_query")


@dp.message(State("audit_search_query"))
async def process_audit_search(message: Message, state: FSMContext):
    query = message.text.strip()
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE 
                a.record_id ILIKE $1 OR
                a.action ILIKE $1 OR
                a.table_name ILIKE $1 OR
                e.full_name ILIKE $1
            ORDER BY a.created_at DESC
            LIMIT 30
        ''', f'%{query}%')
    
    if not rows:
        await message.answer(
            "❌ Natijalar topilmadi!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Qayta qidirish", callback_data="audit_search")],
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ])
        )
        await state.clear()
        return
    
    # BARCHA LOGLAR BIR XABARDA
    full_text = await format_all_logs(rows, f"🔍 Qidiruv: '{query}'")
    
    # Agar xabar juda uzun bo'lsa, 2 qismga bo'lamiz
    if len(full_text) > 4000:
        parts = split_long_message(full_text, 4000)
        for i, part in enumerate(parts[:-1]):
            await message.answer(part, parse_mode="HTML")
        
        # Oxirgi qismga tugmalar qo'shamiz
        await message.answer(
            parts[-1],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )
    
    await state.clear()


async def format_all_logs(rows: list, header: str) -> str:
    """Barcha loglarni bitta matn sifatida formatlash"""
    
    action_icons = {
        'CREATE': '➕',
        'UPDATE': '✏️',
        'DELETE': '🗑',
        'CORRECTION': '🔧',
        'UPLOAD': '📤',
        'CONFIRM': '✅',
        'LOGIN': '🔑',
        'APPROVE': '✅',
        'CALCULATE': '🧮',
        'REJECT': '❌'
    }
    
    text = f"<b>{header}</b>\n"
    text += f"📊 Jami: {len(rows)} ta log\n\n"
    text += "═" * 30 + "\n\n"
    
    for i, row in enumerate(rows, 1):
        icon = action_icons.get(row['action'], '📝')
        user_name = row.get('user_name') or f"ID:{row['user_id']}"
        
        # Har bir log alohida blokda
        text += f"{i}. {icon} <b>{row['action']}</b> | {row['table_name']}\n"
        text += f"   🆔 ID: <code>{row['record_id']}</code>\n"
        text += f"   👤 {user_name} ({row['user_role']})\n"
        text += f"   📅 {row['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
        
        # O'zgarishlarni ko'rsatish
        if row['old_data'] and row['new_data']:
            try:
                old = json.loads(row['old_data']) if isinstance(row['old_data'], str) else row['old_data']
                new = json.loads(row['new_data']) if isinstance(row['new_data'], str) else row['new_data']
                
                changes = []
                for key in new:
                    old_val = str(old.get(key, 'N/A'))[:20] if isinstance(old, dict) else 'N/A'
                    new_val = str(new[key])[:20]
                    if old_val != new_val:
                        changes.append(f"{key}: {old_val}→{new_val}")
                
                if changes:
                    text += f"   🔄 {', '.join(changes[:2])}\n"  # Faqat 2 ta o'zgarish
            except:
                pass
        
        text += "\n"  # Loglar orasida bo'sh qator
    
    return text


def split_long_message(text: str, max_length: int = 4000) -> list:
    """Uzun xabarni bo'lish"""
    parts = []
    current_part = ""
    
    lines = text.split('\n')
    
    for line in lines:
        if len(current_part) + len(line) + 1 > max_length:
            parts.append(current_part)
            current_part = line + '\n'
        else:
            current_part += line + '\n'
    
    if current_part:
        parts.append(current_part)
    
    return parts


# ============ FOYDALANUVCHI BO'YICHA - INLINE KNOPKALAR ============

@dp.callback_query(F.data == "audit_by_user")
async def audit_by_user_menu(callback: CallbackQuery):
    """Foydalanuvchini tanlash menyusi"""
    async with db_pool.acquire() as conn:
        # Barcha aktiv foydalanuvchilarni olish
        users = await conn.fetch('''
            SELECT DISTINCT a.user_id, a.user_role, e.full_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.created_at > CURRENT_DATE - INTERVAL '30 days'
            ORDER BY e.full_name NULLS LAST, a.user_id
            LIMIT 50
        ''')
    
    if not users:
        await callback.answer("Foydalanuvchilar topilmadi!", show_alert=True)
        return
    
    # Foydalanuvchilarni knopkalar sifatida ko'rsatish
    buttons = []
    row = []
    
    for i, user in enumerate(users, 1):
        user_name = user['full_name'] or f"ID:{user['user_id']}"
        role_emoji = {
            'admin': '👑',
            'sotuvchi': '💼',
            'buxgalter': '📊',
            'logist': '🚚',
            'montajchi': '🔧',
            'ombor': '📦',
            'mijoz': '👤'
        }.get(user['user_role'], '👤')
        
        btn_text = f"{role_emoji} {user_name[:18]}"
        row.append(InlineKeyboardButton(
            text=btn_text,
            callback_data=f"audituser_{user['user_id']}"
        ))
        
        # Har 2 ta tugmadan keyin yangi qator
        if i % 2 == 0:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_audit")])
    
    await callback.message.edit_text(
        f"👤 <b>Foydalanuvchini tanlang</b>\n\n"
        f"Jami: {len(users)} ta (oxirgi 30 kun)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("audituser_"))
async def show_user_audit_logs(callback: CallbackQuery):
    """Tanlangan foydalanuvchining loglari - BIR XABARDA"""
    user_id = int(callback.data.split("_")[1])
    
    async with db_pool.acquire() as conn:
        # Foydalanuvchi haqida ma'lumot
        user_info = await conn.fetchrow('''
            SELECT a.user_role, e.full_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.user_id = $1
            LIMIT 1
        ''', user_id)
        
        # Loglar - oxirgi 30 ta
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.user_id = $1
            ORDER BY a.created_at DESC
            LIMIT 30
        ''', user_id)
    
    user_name = user_info['full_name'] if user_info else f"ID:{user_id}"
    user_role = user_info['user_role'] if user_info else "Noma'lum"
    
    # BARCHA LOGLAR BIR XABARDA
    full_text = await format_all_logs(rows, f"👤 {user_name} ({user_role})")
    
    # Tugmalar
    nav_buttons = [
        [InlineKeyboardButton(text="◀️ Foydalanuvchilar ro'yxati", callback_data="audit_by_user")],
        [InlineKeyboardButton(text="🏠 Audit menyu", callback_data="admin_audit")]
    ]
    
    # Xabar uzunligini tekshirish va yuborish
    if len(full_text) > 4000:
        parts = split_long_message(full_text, 4000)
        for i, part in enumerate(parts[:-1]):
            await callback.message.answer(part, parse_mode="HTML")
        
        await callback.message.answer(
            parts[-1],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
            parse_mode="HTML"
        )
    else:
        await callback.message.answer(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
            parse_mode="HTML"
        )


# ============ JADVAL BO'YICHA ============

@dp.callback_query(F.data == "audit_by_table")
async def audit_by_table_menu(callback: CallbackQuery):
    """Jadvalni tanlash menyusi"""
    tables = [
        ('employees', '👥 Xodimlar'),
        ('clients', '👤 Mijozlar'),
        ('generators', '🔧 Generatorlar'),
        ('deals', '💼 Bitimlar'),
        ('payments', '💰 To\'lovlar'),
        ('logistics', '🚚 Logistika'),
        ('installations', '🔧 O\'rnatishlar'),
        ('service_history', '🛠 Servis'),
        ('files', '📁 Fayllar'),
        ('correction_requests', '📝 So\'rovlar')
    ]
    
    buttons = []
    row = []
    
    for i, (table, label) in enumerate(tables, 1):
        row.append(InlineKeyboardButton(
            text=label,
            callback_data=f"audittable_{table}"
        ))
        
        if i % 2 == 0:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_audit")])
    
    await callback.message.edit_text(
        "📊 <b>Jadvalni tanlang:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("audittable_"))
async def show_table_audit_logs(callback: CallbackQuery):
    """Tanlangan jadval bo'yicha loglar - BIR XABARDA"""
    table = callback.data.split("_")[1]
    
    table_names = {
        'employees': '👥 Xodimlar',
        'clients': '👤 Mijozlar',
        'generators': '🔧 Generatorlar',
        'deals': '💼 Bitimlar',
        'payments': '💰 To\'lovlar',
        'logistics': '🚚 Logistika',
        'installations': '🔧 O\'rnatishlar',
        'service_history': '🛠 Servis',
        'files': '📁 Fayllar',
        'correction_requests': '📝 So\'rovlar'
    }
    
    table_label = table_names.get(table, table)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.table_name = $1
            ORDER BY a.created_at DESC
            LIMIT 30
        ''', table)
    
    if not rows:
        await callback.answer(f"{table_label} bo'yicha ma'lumotlar yo'q!", show_alert=True)
        return
    
    # BARCHA LOGLAR BIR XABARDA
    full_text = await format_all_logs(rows, f"📊 {table_label}")
    
    nav_buttons = [
        [InlineKeyboardButton(text="◀️ Jadvallar ro'yxati", callback_data="audit_by_table")],
        [InlineKeyboardButton(text="🏠 Audit menyu", callback_data="admin_audit")]
    ]
    
    if len(full_text) > 4000:
        parts = split_long_message(full_text, 4000)
        for i, part in enumerate(parts[:-1]):
            await callback.message.answer(part, parse_mode="HTML")
        
        await callback.message.answer(
            parts[-1],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
            parse_mode="HTML"
        )
    else:
        await callback.message.answer(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
            parse_mode="HTML"
        )


@dp.callback_query(F.data == "audit_changes")
async def audit_recent_changes(callback: CallbackQuery):
    """So'nggi o'zgarishlar - BIR XABARDA"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.old_data IS NOT NULL AND a.new_data IS NOT NULL
            ORDER BY a.created_at DESC
            LIMIT 30
        ''')
    
    if not rows:
        await callback.answer("O'zgarishlar topilmadi!", show_alert=True)
        return
    
    # BARCHA LOGLAR BIR XABARDA
    full_text = await format_all_logs(rows, "⚠️ So'nggi o'zgarishlar")
    
    if len(full_text) > 4000:
        parts = split_long_message(full_text, 4000)
        for i, part in enumerate(parts[:-1]):
            await callback.message.answer(part, parse_mode="HTML")
        
        await callback.message.answer(
            parts[-1],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )
    else:
        await callback.message.answer(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )


@dp.callback_query(F.data == "audit_today")
async def audit_today_logs(callback: CallbackQuery):
    """Bugungi audit loglari - BIR XABARDA"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.created_at > CURRENT_DATE
            ORDER BY a.created_at DESC
            LIMIT 30
        ''')
    
    if not rows:
        await callback.answer("Bugun hech narsa o'zgartirilmagan!", show_alert=True)
        return
    
    # BARCHA LOGLAR BIR XABARDA
    full_text = await format_all_logs(
        rows, 
        f"📅 Bugungi harakatlar ({datetime.now().strftime('%d.%m.%Y')})"
    )
    
    if len(full_text) > 4000:
        parts = split_long_message(full_text, 4000)
        for i, part in enumerate(parts[:-1]):
            await callback.message.answer(part, parse_mode="HTML")
        
        await callback.message.answer(
            parts[-1],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )
    else:
        await callback.message.answer(
            full_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
            ]),
            parse_mode="HTML"
        )


# Eski show_audit_logs funksiyasi (agar kerak bo'lsa)
async def show_audit_logs(message_or_callback, rows: list):
    """Audit loglarni ko'rsatish - eski versiya (faqat kerak bo'lsa)"""
    await show_audit_logs_as_messages(message_or_callback, rows)


async def log_action(user_id: int, action: str, table: str, record_id: str, 
                    old_data=None, new_data=None, role: str = None, 
                    ip_address: str = None, user_agent: str = None):
    """Harakatlarni audit logga yozish"""
    try:
        async with db_pool.acquire() as conn:
            if not role:
                role = await get_user_role(user_id)
            
            old_json = json.dumps(old_data, ensure_ascii=False, default=str) if old_data else None
            new_json = json.dumps(new_data, ensure_ascii=False, default=str) if new_data else None
            
            await conn.execute('''
                INSERT INTO audit_logs 
                (user_id, user_role, action, table_name, record_id, 
                 old_data, new_data, ip_address, user_agent, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
            ''', user_id, role, action, table, str(record_id), 
                old_json, new_json, ip_address, user_agent)
            
            if action in ['CORRECTION', 'DELETE'] or table in ['payments', 'deals']:
                await notify_admins(
                    f"🚨 <b>Muhim harakat:</b>\n\n"
                    f"Action: {action}\n"
                    f"Table: {table}\n"
                    f"Record: {record_id}\n"
                    f"User: {user_id}\n"
                    f"Time: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
                )
                
    except Exception as e:
        logger.error(f"Audit log error: {e}")




async def show_audit_logs_as_messages(message_or_callback, rows: list):
    """Audit loglarni alohida xabarlar sifatida yuborish"""
    
    action_icons = {
        'CREATE': '➕',
        'UPDATE': '✏️',
        'DELETE': '🗑',
        'CORRECTION': '🔧',
        'UPLOAD': '📤',
        'CONFIRM': '✅',
        'LOGIN': '🔑',
        'APPROVE': '✅',
        'CALCULATE': '🧮',
        'REJECT': '❌'
    }
    
    for row in rows[:10]:  # Faqat 10 ta
        icon = action_icons.get(row['action'], '📝')
        user_name = row.get('user_name') or f"ID:{row['user_id']}"
        
        # O'zgarishlarni formatlash
        changes = ""
        if row['old_data'] and row['new_data']:
            try:
                old = json.loads(row['old_data']) if isinstance(row['old_data'], str) else row['old_data']
                new = json.loads(row['new_data']) if isinstance(row['new_data'], str) else row['new_data']
                
                changes = "\n<b>O'zgarishlar:</b>\n"
                for key in new:
                    old_val = old.get(key, 'N/A') if isinstance(old, dict) else 'N/A'
                    new_val = new[key]
                    if old_val != new_val:
                        changes += f"  {key}: {old_val} → {new_val}\n"
            except:
                changes = "\n<i>O'zgarishlarni ko'rsatib bo'lmadi</i>\n"
        
        text = (f"{icon} <b>{row['action']}</b> | {row['table_name']}\n"
                f"🆔 ID: {row['record_id']}\n"
                f"👤 {user_name} ({row['user_role']})\n"
                f"📅 {row['created_at'].strftime('%d.%m.%Y %H:%M:%S')}"
                f"{changes}")
        
        # Har bir log alohida xabar
        await message_or_callback.answer(text, parse_mode="HTML")
    
    # Oxirida navigatsiya tugmalari
    await message_or_callback.answer(
        f"📋 <b>Jami: {len(rows)} ta log</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Audit menyu", callback_data="admin_audit")]
        ]),
        parse_mode="HTML"
    )











# ============ JADVAL BO'YICHA ============











# ============ WAREHOUSE MODULE ============

@dp.callback_query(F.data == "wh_add_gen")
async def start_add_generator(callback: CallbackQuery, state: FSMContext):
    role = await get_user_role(callback.from_user.id)
    if role not in ['ombor', 'admin']:
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📝 <b>Yangi generator qo'shish</b>\n\nModelni kiriting (masalan: Perkins 100kVA):",
        parse_mode="HTML"
    )
    await state.set_state(AddGenerator.entering_model)

@dp.message(AddGenerator.entering_model)
async def process_gen_model(message: Message, state: FSMContext):
    await state.update_data(model=message.text.strip())
    await message.answer("⚡ Quvvatini kiriting (kVA, faqat son):")
    await state.set_state(AddGenerator.entering_power)

@dp.message(AddGenerator.entering_power)
async def process_gen_power(message: Message, state: FSMContext):
    try:
        power = int(message.text)
        await state.update_data(power_kw=power)
        await message.answer("🏭 Ishlab chiqaruvchini kiriting:")
        await state.set_state(AddGenerator.entering_manufacturer)
    except:
        await message.answer("❌ Faqat son kiriting!")

@dp.message(AddGenerator.entering_manufacturer)
async def process_gen_manufacturer(message: Message, state: FSMContext):
    await state.update_data(manufacturer=message.text.strip())
    await message.answer("📅 Ishlab chiqarilgan yilni kiriting:")
    await state.set_state(AddGenerator.entering_year)

@dp.message(AddGenerator.entering_year)
async def process_gen_year(message: Message, state: FSMContext):
    try:
        year = int(message.text)
        if year < 1990 or year > datetime.now().year + 1:
            raise ValueError
        await state.update_data(manufacture_year=year)
        await message.answer("🔢 Seriya raqamini kiriting:")
        await state.set_state(AddGenerator.entering_serial)
    except:
        await message.answer("❌ Noto'g'ri yil!")

@dp.message(AddGenerator.entering_serial)
async def process_gen_serial(message: Message, state: FSMContext):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            'SELECT 1 FROM generators WHERE serial_number = $1', 
            message.text
        )
        if exists:
            await message.answer("❌ Bu seriya raqam mavjud! Boshqa kiriting:")
            return
    
    await state.update_data(serial=message.text.strip())
    await message.answer("💵 Sotib olish narxini kiriting (so'm):")
    await state.set_state(AddGenerator.entering_price)

@dp.message(AddGenerator.entering_price)
async def process_gen_price(message: Message, state: FSMContext):
    try:
        price = float(message.text)
        await state.update_data(purchase_price=price)
        await message.answer("🛡 Kafolat muddati (oy, masalan: 12):")
        await state.set_state(AddGenerator.entering_warranty)
    except:
        await message.answer("❌ Faqat son kiriting!")

@dp.message(AddGenerator.entering_warranty)
async def process_gen_warranty(message: Message, state: FSMContext):
    try:
        warranty = int(message.text)
        uid = generate_uid()
        await state.update_data(
            warranty=warranty, 
            uid=uid, 
            photos=[], 
            documents=[]
        )
        
        await message.answer(
            "📸 Generator rasmlarini yuboring (bir nechta bo'lishi mumkin).\n"
            "Tayyor bo'lsa 'tayyor' deb yozing:"
        )
        await state.set_state(AddGenerator.uploading_photos)
    except:
        await message.answer("❌ Faqat son kiriting!")

@dp.message(AddGenerator.uploading_photos, F.photo)
async def process_gen_photo(message: Message, state: FSMContext):
    photo = message.photo[-1]
    data = await state.get_data()
    photos = data.get('photos', [])
    
    file_path = await download_file(
        photo.file_id, 
        "generators", 
        f"{data['uid']}_{len(photos)}"
    )
    
    if file_path:
        photos.append(file_path)
        await state.update_data(photos=photos)
        await message.answer(f"✅ Rasm qo'shildi ({len(photos)} ta). Yana yuboring yoki 'tayyor':")
    else:
        await message.answer("❌ Xatolik! Qayta yuboring.")

@dp.message(AddGenerator.uploading_photos, F.text.lower() == "tayyor")
async def finish_gen_photos(message: Message, state: FSMContext):
    await message.answer(
        "📄 Hujjatlarni yuklang (PDF, Word, Excel).\n"
        "Tayyor bo'lsa 'tayyor' deb yozing:"
    )
    await state.set_state(AddGenerator.uploading_documents)

@dp.message(AddGenerator.uploading_documents, F.document)
async def process_gen_document(message: Message, state: FSMContext):
    document = message.document
    data = await state.get_data()
    documents = data.get('documents', [])
    
    file_path = await download_file(
        document.file_id,
        "documents",
        f"{data['uid']}_doc_{len(documents)}",
        "document"
    )
    
    if file_path:
        documents.append({
            'path': file_path,
            'name': document.file_name,
            'size': document.file_size
        })
        await state.update_data(documents=documents)
        await message.answer(f"✅ Hujjat qo'shildi ({len(documents)} ta).")
    else:
        await message.answer("❌ Xatolik!")

@dp.message(AddGenerator.uploading_documents, F.text.lower() == "tayyor")
async def finish_gen_documents(message: Message, state: FSMContext):
    data = await state.get_data()
    
    # QR kod yaratish
    qr_path = await generate_qr_code(data['uid'])
    
    text = (f"📋 <b>Ma'lumotlarni tasdiqlang:</b>\n\n"
            f"🆔 UID: <code>{data['uid']}</code>\n"
            f"🔧 Model: {data['model']}\n"
            f"⚡ Quvvat: {data['power_kw']} kVA\n"
            f"🏭 Ishlab chiqaruvchi: {data['manufacturer']}\n"
            f"📅 Yil: {data['manufacture_year']}\n"
            f"🔢 Seriya: {data['serial']}\n"
            f"💵 Narx: {data['purchase_price']:,.0f} so'm\n"
            f"🛡 Kafolat: {data['warranty']} oy\n"
            f"📷 Rasmlar: {len(data.get('photos', []))} ta\n"
            f"📄 Hujjatlar: {len(data.get('documents', []))} ta\n\n"
            f"Saqlansinmi?")
    
    await message.answer(
        text,
        reply_markup=confirm_keyboard("gen_save", "wh_cancel"),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "gen_save")
async def save_generator(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            # QR kod yo'lini olish
            qr_path = f"{UPLOAD_DIR}/qrcodes/{data['uid']}.png"
            
            await conn.execute('''
                INSERT INTO generators 
                (uid, model, power_kw, serial_number, manufacturer, manufacture_year,
                 purchase_price, warranty_months, status, added_by, photos, documents, qr_code_path)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'SKLADDA', $9, $10, $11, $12)
            ''', data['uid'], data['model'], data['power_kw'], data['serial'],
                 data['manufacturer'], data['manufacture_year'],
                 data['purchase_price'], data['warranty'], user_id,
                 data.get('photos', []), 
                 json.dumps(data.get('documents', [])),
                 qr_path)
            
            await log_action(user_id, 'CREATE', 'generators', data['uid'], 
                           new_data=data, role='ombor')
        
        # QR kodni yuborish
        try:
            await callback.message.answer_photo(
                FSInputFile(qr_path),
                caption=f"🆔 <code>{data['uid']}</code> uchun QR kod",
                parse_mode="HTML"
            )
        except:
            pass
        
        await callback.message.edit_text(
            f"✅ <b>Generator qo'shildi!</b>\n\n"
            f"🆔 UID: <code>{data['uid']}</code>\n"
            f"📦 Status: Skladda\n\n"
            f"QR kod yuqorida.",
            reply_markup=warehouse_menu(),
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Generator saqlashda xatolik: {e}")
        await callback.message.edit_text(f"❌ Xatolik: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "wh_inventory")
async def show_inventory(callback: CallbackQuery):
    role = await get_user_role(callback.from_user.id)
    if role not in ['ombor', 'admin']:
        return
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT * FROM generators 
            WHERE status = 'SKLADDA'
            ORDER BY created_at DESC
        ''')
    
    if not rows:
        await callback.answer("Sklad bo'sh", show_alert=True)
        return
    
    text = f"📦 <b>Skladdagi generatorlar ({len(rows)} ta):</b>\n\n"
    
    for i, row in enumerate(rows[:20], 1):  # Faqat 20 tasini ko'rsatish
        text += (f"{i}. <code>{row['uid']}</code>\n"
                f"   {row['model']} ({row['power_kw']}kVA)\n"
                f"   Seriya: {row['serial_number']}\n"
                f"   Narx: {row['purchase_price']:,.0f} so'm\n\n")
    
    if len(rows) > 20:
        text += f"... va yana {len(rows) - 20} ta"
    
    await callback.message.edit_text(
        text, 
        reply_markup=warehouse_menu(),
        parse_mode="HTML"
    )

# ============ SELLER MODULE ============

@dp.callback_query(F.data == "sl_new_deal")
async def start_new_deal(callback: CallbackQuery, state: FSMContext):
    role = await get_user_role(callback.from_user.id)
    if role not in ['sotuvchi', 'admin']:
        return
    
    async with db_pool.acquire() as conn:
        gens = await conn.fetch('''
            SELECT uid, model, power_kw, purchase_price 
            FROM generators 
            WHERE status = 'SKLADDA'
            ORDER BY model
        ''')
    
    if not gens:
        await callback.answer("❌ Skladda bo'sh generator yo'q!", show_alert=True)
        return
    
    buttons = []
    for gen in gens:
        buttons.append([InlineKeyboardButton(
            text=f"{gen['model']} ({gen['power_kw']}kVA) - {gen['uid']}",
            callback_data=f"deal_gen_{gen['uid']}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="menu_seller")])
    
    await callback.message.edit_text(
        "🔧 <b>Generator tanlang:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_generator)

@dp.callback_query(F.data.startswith("deal_gen_"))
async def select_gen_for_deal(callback: CallbackQuery, state: FSMContext):
    uid = callback.data.split("_")[2]
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('SELECT * FROM generators WHERE uid = $1', uid)
        
        # FAQAT ADMIN TASDIQLAGAN MIJOZLAR
        clients = await conn.fetch('''
            SELECT id, full_name, phone, company 
            FROM clients 
            WHERE is_approved = TRUE
            ORDER BY full_name 
            LIMIT 50
        ''')
    
    await state.update_data(
        generator_uid=uid,
        gen_model=gen['model'],
        gen_power=gen['power_kw'],
        purchase_price=float(gen['purchase_price']) if gen['purchase_price'] else 0
    )
    
    # Mijozlar ro'yxatini ko'rsatish
    buttons = []
    for client in clients:
        company = f" ({client['company']})" if client['company'] else ""
        buttons.append([InlineKeyboardButton(
            text=f"{client['full_name']}{company} - {client['phone']}",
            callback_data=f"client_{client['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(
        text="◀️ Orqaga", 
        callback_data="sl_new_deal"
    )])
    
    await callback.message.edit_text(
        "👤 <b>Mijozni tanlang:</b>\n\n"
        "⚠️ Faqat admin tasdiqlagan mijozlar ko'rsatiladi.\n"
        "Yangi mijoz qo'shish uchun admin bilan bog'laning.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_client)


@dp.callback_query(F.data.startswith("client_"), CreateDeal.selecting_client)
async def select_existing_client_for_deal(callback: CallbackQuery, state: FSMContext):
    """Mijoz tanlangandan keyin - AVVAL VILOYAT TANLASH"""
    client_id = callback.data.split("_")[1]
    
    # Mavjud mijozni olish
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow(
            'SELECT * FROM clients WHERE id = $1', int(client_id)
        )
    
    await state.update_data(
        existing_client_id=int(client_id),
        client_name=client['full_name'],
        client_phone=client['phone'],
        client_company=client['company'],
        client_address=client['address'],
        client_region=client['region'],
        client_city=client['city'],
        lat=client['geo_lat'],
        lon=client['geo_lon']
    )
    
    data = await state.get_data()
    
    # Mijoz joylashuvi
    location_text = ""
    if client['region'] and client['city']:
        location_text = f"{client['region']}, {client['city']}"
    else:
        location_text = "Noma'lum"
    
    # AVVAL VILOYAT TANLASHNI TAKLIF QILAMIZ
    await callback.message.edit_text(
        f"👤 <b>Mijoz:</b> {client['full_name']}\n"
        f"📍 <b>Mijoz joylashuvi:</b> {location_text}\n\n"
        f"🔧 <b>Montajchi uchun viloyat tanlang:</b>\n\n"
        f"💡 <i>Tavsiya: Mijoz bilan bir xil viloyatni tanlang</i>",
        reply_markup=get_regions_keyboard_for_installer(),  # Maxsus keyboard
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_installer_region)

def get_regions_keyboard_for_installer():
    """Montajchi mavjudligini ko'rsatuvchi viloyatlar keyboardi"""
    buttons = []
    row = []
    
    for i, region in enumerate(UZBEKISTAN_REGIONS.keys(), 1):
        # Har bir viloyatda nechta montajchi borligini tekshirish
        # (Bu yerda async emas, shuningcha statik ko'rsatamiz)
        row.append(InlineKeyboardButton(
            text=f"📍 {region}", 
            callback_data=f"instregion_{region}"
        ))
        if i % 2 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    # Orqaga tugmasi
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="sl_new_deal")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("instregion_"), CreateDeal.selecting_installer_region)
async def select_installer_region(callback: CallbackQuery, state: FSMContext):
    """Montajchi uchun viloyat tanlash"""
    region = callback.data.split("_", 1)[1]
    await state.update_data(selected_installer_region=region)
    
    # Shu viloyatdagi barcha montajchilarni olish
    async with db_pool.acquire() as conn:
        installers = await conn.fetch('''
            SELECT city, COUNT(*) as count
            FROM employees
            WHERE role = 'montajchi' 
            AND is_active = TRUE
            AND region = $1
            GROUP BY city
            ORDER BY city
        ''', region)
    
    # Tumanlar ro'yxati
    districts = UZBEKISTAN_REGIONS.get(region, {}).get("districts", [])
    
    buttons = []
    row = []
    
    for i, district in enumerate(districts, 1):
        # Shu tumanda nechta montajchi borligini tekshirish
        installer_count = next(
            (inst['count'] for inst in installers if inst['city'] == district), 
            0
        )
        
        # Agar montajchi bo'lsa ✅ belgisi
        text = f"✅ {district}" if installer_count > 0 else f"📍 {district}"
        
        row.append(InlineKeyboardButton(
            text=text,
            callback_data=f"instcity_{district}"
        ))
        if i % 2 == 0:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    
    # Orqaga tugmasi
    buttons.append([
        InlineKeyboardButton(text="◀️ Viloyatlarga", callback_data="back_to_inst_regions"),
        InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="main_menu")
    ])
    
    await callback.message.edit_text(
        f"📍 <b>Viloyat:</b> {region}\n\n"
        f"🔧 <b>Tuman/Shaharni tanlang:</b>\n"
        f"<i>✅ belgisi - bu tumanda montajchi mavjud</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_installer_city)

@dp.callback_query(F.data.startswith("instcity_"), CreateDeal.selecting_installer_city)
async def select_installer_city(callback: CallbackQuery, state: FSMContext):
    """Tuman tanlash va montajchilarni ko'rsatish"""
    city = callback.data.split("_", 1)[1]
    await state.update_data(selected_installer_city=city)
    
    data = await state.get_data()
    region = data['selected_installer_region']
    
    async with db_pool.acquire() as conn:
        # 1. AVVAL SHU TUMANDAGI MONTAJCHILARNI QIDIRAMIZ
        same_city_installers = await conn.fetch('''
            SELECT telegram_id, full_name, phone, city, region
            FROM employees
            WHERE role = 'montajchi' 
            AND is_active = TRUE
            AND region = $1
            AND city = $2
            ORDER BY full_name
        ''', region, city)
        
        # 2. AGAR SHU TUMANDA BO'LMASA, SHU VILOYATDAGI BOSHQA TUMANLARNI QIDIRAMIZ
        if not same_city_installers:
            same_region_installers = await conn.fetch('''
                SELECT telegram_id, full_name, phone, city, region
                FROM employees
                WHERE role = 'montajchi' 
                AND is_active = TRUE
                AND region = $1
                AND city != $2
                ORDER BY city, full_name
            ''', region, city)
        else:
            same_region_installers = []
    
    buttons = []
    
    # AGAR SHU TUMANDA MONTAJCHI BO'LSA
    if same_city_installers:
        text = (f"📍 <b>Viloyat:</b> {region}\n"
                f"📍 <b>Tuman:</b> {city}\n\n"
                f"✅ <b>Shu tumaning montajchilari:</b>\n"
                f"<i>Birini tanlang:</i>")
        
        for inst in same_city_installers:
            buttons.append([InlineKeyboardButton(
                text=f"👤 {inst['full_name']} | 📱{inst['phone']}",
                callback_data=f"instselect_{inst['telegram_id']}"
            )])
    
    # AGAR SHU TUMANDA BO'LMASA, VILOYATDAGI BOSHQA TUMANLAR
    elif same_region_installers:
        text = (f"📍 <b>Viloyat:</b> {region}\n"
                f"📍 <b>Tuman:</b> {city}\n\n"
                f"⚠️ <b>Bu tumanida montajchi yo'q!</b>\n\n"
                f"📍 <b>Shu viloyatdagi boshqa tumanlar:</b>\n"
                f"<i>Birini tanlang:</i>")
        
        # Guruhlash (tuman bo'yicha)
        current_city = None
        for inst in same_region_installers:
            if inst['city'] != current_city:
                current_city = inst['city']
                buttons.append([InlineKeyboardButton(
                    text=f"📍 {current_city}:",
                    callback_data="header_city"  # Bu tugma faqat sarlavha
                )])
            
            buttons.append([InlineKeyboardButton(
                text=f"  👤 {inst['full_name']} | 📱{inst['phone']}",
                callback_data=f"instselect_{inst['telegram_id']}"
            )])
    
    # AGAR VILOYATDA HAM BO'LMASA
    else:
        text = (f"📍 <b>Viloyat:</b> {region}\n"
                f"📍 <b>Tuman:</b> {city}\n\n"
                f"❌ <b>Bu viloyatda hech qanday montajchi yo'q!</b>\n\n"
                f"🔄 Boshqa viloyatni tanlash yoki admin bilan bog'laning.")
        
        buttons.append([InlineKeyboardButton(
            text="🔄 Boshqa viloyat",
            callback_data="back_to_inst_regions"
        )])
    
    # Orqaga tugmasi
    buttons.append([
        InlineKeyboardButton(text="◀️ Tumanga qaytish", callback_data=f"instregion_{region}"),
        InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="main_menu")
    ])
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_installer)

@dp.callback_query(F.data.startswith("instselect_"), CreateDeal.selecting_installer)
async def select_installer_final(callback: CallbackQuery, state: FSMContext):
    """Montajchini yakuniy tanlash"""
    installer_id = int(callback.data.split("_")[1])
    
    async with db_pool.acquire() as conn:
        installer = await conn.fetchrow('''
            SELECT full_name, phone, region, city 
            FROM employees 
            WHERE telegram_id = $1
        ''', installer_id)
    
    await state.update_data(
        installer_id=installer_id,
        installer_name=installer['full_name'],
        installer_phone=installer['phone'],
        installer_region=installer['region'],
        installer_city=installer['city']
    )
    
    data = await state.get_data()
    
    # Joylashuv mosligini tekshirish
    client_location = f"{data.get('client_region', '')}, {data.get('client_city', '')}"
    installer_location = f"{installer['region']}, {installer['city']}"
    
    same_city = data.get('client_city') == installer['city']
    same_region = data.get('client_region') == installer['region']
    
    location_status = ""
    if same_city:
        location_status = "✅ Shu tuman! (Ideal)"
    elif same_region:
        location_status = "📍 Shu viloyat, boshqa tuman"
    else:
        location_status = "🌐 Boshqa viloyat"
    
    # Narx kiritish
    await callback.message.edit_text(
        f"✅ <b>Montajchi tanlandi:</b>\n\n"
        f"👤 {installer['full_name']}\n"
        f"📱 {installer['phone']}\n"
        f"📍 {installer_location}\n"
        f"Status: {location_status}\n\n"
        f"👤 <b>Mijoz:</b> {data['client_name']}\n"
        f"📍 {client_location}\n\n"
        f"💵 <b>Sotuv narxini kiriting (so'm):</b>\n"
        f"💡 Sotib olish narxi: {data['purchase_price']:,.0f} so'm",
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.entering_price)

@dp.callback_query(F.data == "back_to_inst_regions")
async def back_to_installer_regions(callback: CallbackQuery, state: FSMContext):
    """Montajchi viloyatlariga qaytish"""
    await callback.message.edit_text(
        f"🔧 <b>Montajchi uchun viloyat tanlang:</b>",
        reply_markup=get_regions_keyboard_for_installer(),
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.selecting_installer_region)

@dp.callback_query(F.data.startswith("instregion_"))
async def back_to_installer_cities(callback: CallbackQuery, state: FSMContext):
    """Tumanlarga qaytish (viloyat tanlangandan keyin)"""
    # Bu yerda avvalgi viloyat tanlash funksiyasini chaqiramiz
    await select_installer_region(callback, state)


@dp.callback_query(F.data.startswith("inst_"), CreateDeal.selecting_installer)
async def select_installer_for_deal(callback: CallbackQuery, state: FSMContext):
    """Montajchini tanlash"""
    installer_id = callback.data.split("_")[1]
    
    if installer_id == "skip":
        await state.update_data(installer_id=None, installer_name=None)
        await callback.message.edit_text(
            "💵 <b>Sotuv narxini kiriting (so'm):</b>",
            parse_mode="HTML"
        )
        await state.set_state(CreateDeal.entering_price)
        return
    
    # Header tugmalarni e'tiborsiz qoldirish
    if installer_id.startswith("header"):
        await callback.answer("Iltimos, montajchini tanlang")
        return
    
    installer_id = int(installer_id)
    
    async with db_pool.acquire() as conn:
        installer = await conn.fetchrow(
            'SELECT full_name, region, city, phone FROM employees WHERE telegram_id = $1',
            installer_id
        )
    
    if not installer:
        await callback.answer("Montajchi topilmadi!", show_alert=True)
        return
    
    await state.update_data(
        installer_id=installer_id,
        installer_name=installer['full_name'],
        installer_phone=installer['phone'],
        installer_region=installer['region'],
        installer_city=installer['city']
    )
    
    data = await state.get_data()
    
    # Tasdiqlash xabari
    location_match = ""
    if data.get('client_city') == installer['city']:
        location_match = "✅ Shu tuman!"
    elif data.get('client_region') == installer['region']:
        location_match = "📍 Shu viloyat"
    else:
        location_match = f"🌐 {installer['region']}, {installer['city']}"
    
    await callback.message.edit_text(
        f"✅ <b>Montajchi tanlandi:</b>\n\n"
        f"👤 {installer['full_name']}\n"
        f"📱 {installer['phone']}\n"
        f"📍 {location_match}\n\n"
        f"💵 <b>Sotuv narxini kiriting (so'm):</b>\n"
        f"💡 Sotib olish narxi: {data['purchase_price']:,.0f} so'm",
        parse_mode="HTML"
    )
    await state.set_state(CreateDeal.entering_price)


@dp.message(CreateDeal.entering_client_name)
async def process_client_name(message: Message, state: FSMContext):
    await state.update_data(client_name=message.text.strip())
    await message.answer("📱 Telefon raqamini kiriting (+998XX XXX XX XX):")
    await state.set_state(CreateDeal.entering_client_phone)

@dp.message(CreateDeal.entering_client_phone)
async def process_client_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Noto'g'ri format! (+998XXXXXXXXX):")
        return
    
    # Telefon mavjudligini tekshirish
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow('SELECT id FROM clients WHERE phone = $1', phone)
        if existing:
            await state.update_data(existing_client_id=existing['id'])
    
    await state.update_data(client_phone=phone)
    await message.answer("🏢 Kompaniya nomini kiriting (yo'q bo'lsa 'yo'q' deb yozing):")
    await state.set_state(CreateDeal.entering_client_company)

@dp.message(CreateDeal.entering_client_company)
async def process_client_company(message: Message, state: FSMContext):
    company = None if message.text.lower() == 'yoq' else message.text.strip()
    await state.update_data(client_company=company)
    await message.answer("📍 Manzilni kiriting (viloyat, tuman, ko'cha):")
    await state.set_state(CreateDeal.entering_address)

@dp.message(CreateDeal.entering_address)
async def process_client_address(message: Message, state: FSMContext):
    # Tekshirish - text mavjudmi?
    if not message.text:
        await message.answer("❌ Iltimos, matn kiriting!")
        return
    
    await state.update_data(client_address=message.text.strip())
    await message.answer(
        "🗺 Geolokatsiya yuboring (Telegram attachment dan):\n\n"
        "Yoki 'keyinroq' deb yozing:"
    )
    await state.set_state(CreateDeal.entering_location)

@dp.message(CreateDeal.entering_location, F.location)
async def process_location(message: Message, state: FSMContext):
    await state.update_data(lat=message.location.latitude, lon=message.location.longitude)
    data = await state.get_data()
    
    await message.answer(
        f"💵 Sotuv narxini kiriting (so'm):\n\n"
        f"💡 Sotib olish narxi: {data['purchase_price']:,.0f} so'm"
    )
    await state.set_state(CreateDeal.entering_price)

@dp.message(CreateDeal.entering_location, F.text.lower() == "keyinroq")
async def skip_location(message: Message, state: FSMContext):
    await state.update_data(lat=None, lon=None)
    data = await state.get_data()
    
    await message.answer(
        f"💵 Sotuv narxini kiriting (so'm):\n\n"
        f"💡 Sotib olish narxi: {data['purchase_price']:,.0f} so'm"
    )
    await state.set_state(CreateDeal.entering_price)

@dp.message(CreateDeal.entering_price)
async def process_sale_price(message: Message, state: FSMContext):
    try:
        price = float(message.text)
        data = await state.get_data()
        
        # Minimal foyda tekshiruvi
        min_price = data['purchase_price'] * 1.1  # 10% minimal foyda
        if price < min_price:
            await message.answer(
                f"⚠️ Diqqat! Sotuv narxi juda past!\n"
                f"Minimal tavsiya etilgan: {min_price:,.0f} so'm\n\n"
                f"Yangi narx kiriting:"
            )
            return
        
        await state.update_data(sale_price=price)
        
        # Hisobot
        profit = price - data['purchase_price']
        margin = (profit / price * 100) if price else 0
        
        # Backslash muammosini oldini olish uchun
        company = data.get('client_company') or "Yo'q"
        location_status = '✅' if data.get('lat') else '❌'
        
        text = (f"📋 <b>Bitimni tasdiqlang:</b>\n\n"
                f"🆔 Generator: <code>{data['generator_uid']}</code>\n"
                f"🔧 {data['gen_model']} ({data['gen_power']}kVA)\n\n"
                f"👤 Mijoz: {data['client_name']}\n"
                f"📱 Tel: {data['client_phone']}\n"
                f"🏢 Kompaniya: {company}\n"
                f"📍 Manzil: {data['client_address']}\n"
                f"🗺 Lokatsiya: {location_status}\n\n"
                f"💵 Sotuv narxi: {price:,.0f} so'm\n"
                f"📊 Foyda: {profit:,.0f} so'm ({margin:.1f}%)\n\n"
                f"Saqlansinmi?")
        
        await message.answer(
            text,
            reply_markup=confirm_keyboard("deal_confirm", "menu_seller"),
            parse_mode="HTML"
        )
    except ValueError:
        await message.answer("❌ Faqat son kiriting!")

@dp.callback_query(F.data == "deal_confirm")
async def save_deal(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    seller_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            client_id = data['existing_client_id']
            installer_id = data.get('installer_id')
            
            # Bitim yaratish (montajchi bilan)
            if installer_id:
                deal_id = await conn.fetchval('''
                    INSERT INTO deals 
                    (generator_uid, seller_id, client_id, sale_price, status, 
                     installer_id, installer_assigned_at, installer_assigned_by)
                    VALUES ($1, $2, $3, $4, 'PENDING_PAYMENT', $5, 
                           CURRENT_TIMESTAMP, $6)
                    RETURNING id
                ''', data['generator_uid'], seller_id, client_id, 
                     data['sale_price'], installer_id, seller_id)
            else:
                deal_id = await conn.fetchval('''
                    INSERT INTO deals 
                    (generator_uid, seller_id, client_id, sale_price, status, 
                     installer_id, installer_assigned_at, installer_assigned_by)
                    VALUES ($1, $2, $3, $4, 'PENDING_PAYMENT', NULL, NULL, NULL)
                    RETURNING id
                ''', data['generator_uid'], seller_id, client_id, 
                     data['sale_price'])
            
            # Generator statusini o'zgartirish
            await conn.execute('''
                UPDATE generators 
                SET status = 'SOTILDI', current_deal_id = $1, current_client_id = $2,
                    sale_price = $3
                WHERE uid = $4
            ''', deal_id, client_id, data['sale_price'], data['generator_uid'])
            
            await log_action(seller_id, 'CREATE', 'deals', deal_id, 
                           new_data={'generator': data['generator_uid'], 
                                    'price': data['sale_price'],
                                    'client': data['client_name'],
                                    'installer': data.get('installer_name')},
                           role='sotuvchi')
        
        # Montajchiga xabar (agar tanlangan bo'lsa)
        if installer_id:
            # O'zgaruvchiga saqlash (backslash muammosini oldini olish uchun)
            client_address = data.get('client_address') or "Kiritilmagan"
            
            try:
                await bot.send_message(
                    installer_id,
                    f"🔧 <b>Sizga yangi o'rnatish vazifasi biriktirildi!</b>\n\n"
                    f"Bitim: #{deal_id}\n"
                    f"Generator: {data['gen_model']} ({data['gen_power']}kVA)\n"
                    f"Mijoz: {data['client_name']}\n"
                    f"Telefon: {data['client_phone']}\n"
                    f"Manzil: {client_address}\n\n"
                    f"To'lov tasdiqlangach sizga xabar yuboriladi.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Montajchiga xabar yuborishda xatolik: {e}")
        
        # Xabar matni
        installer_text = ""
        if data.get('installer_name'):
            installer_text = f"\n🔧 Montajchi: {data['installer_name']}"
            if data.get('installer_city'):
                installer_text += f" ({data['installer_city']})"
        
        await callback.message.edit_text(
            f"✅ <b>Bitim #{deal_id} yaratildi!</b>\n\n"
            f"🆔 Generator: <code>{data['generator_uid']}</code>\n"
            f"👤 Mijoz: {data['client_name']}\n"
            f"💵 Summa: {data['sale_price']:,.0f} so'm"
            f"{installer_text}\n\n"
            f"Status: ⏳ To'lov kutilmoqda\n\n"
            f"To'lov kelganda '💰 To'lovni tasdiqlash' bo'limidan tasdiqlang.",
            reply_markup=seller_menu(),
            parse_mode="HTML"
        )
        
        # Buxgalterga xabar
        await notify_by_role('buxgalter', 
                           f"📢 <b>Yangi bitim!</b>\n\n"
                           f"#{deal_id} - {data['gen_model']}\n"
                           f"Mijoz: {data['client_name']}\n"
                           f"Summa: {data['sale_price']:,.0f} so'm")
        
    except Exception as e:
        logger.error(f"Bitim yaratishda xatolik: {e}")
        await callback.message.edit_text(f"❌ Xatolik: {str(e)}")
    
    await state.clear()


async def get_available_installers(region: str, city: str):
    """Viloyat va tuman bo'yicha montajchilarni olish"""
    async with db_pool.acquire() as conn:
        # 1. Avval shu tumandagi montajchilarni qidirish
        same_city = await conn.fetch('''
            SELECT telegram_id, full_name, phone, region, city
            FROM employees
            WHERE role = 'montajchi' 
            AND is_active = TRUE
            AND region = $1
            AND city = $2
            ORDER BY full_name
        ''', region, city)
        
        # 2. Shu viloyatdagi boshqa tumanlardan
        same_region = await conn.fetch('''
            SELECT telegram_id, full_name, phone, region, city
            FROM employees
            WHERE role = 'montajchi' 
            AND is_active = TRUE
            AND region = $1
            AND city != $2
            ORDER BY city, full_name
        ''', region, city)
        
        # 3. Boshqa viloyatlardan (faqat agar yuqoridagilar bo'lmasa)
        other_regions = []
        if not same_city and not same_region:
            other_regions = await conn.fetch('''
                SELECT telegram_id, full_name, phone, region, city
                FROM employees
                WHERE role = 'montajchi' 
                AND is_active = TRUE
                AND region != $1
                ORDER BY region, city, full_name
                LIMIT 10
            ''', region)
    
    return {
        'same_city': same_city,
        'same_region': same_region,
        'other_regions': other_regions
    }





@dp.callback_query(F.data == "sl_my_deals")
async def seller_deals(callback: CallbackQuery):
    seller_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            # Avval clients jadvalida id borligini tekshiramiz
            check = await conn.fetchval('''
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'clients' AND column_name = 'id'
            ''')
            
            if not check:
                logger.error("❌ clients jadvalida 'id' ustuni yo'q!")
                await callback.answer(
                    "❌ Bazada xatolik! Admin bilan bog'laning.", 
                    show_alert=True
                )
                return
            
            rows = await conn.fetch('''
                SELECT d.*, c.full_name as client_name, c.phone as client_phone,
                       g.model, g.power_kw
                FROM deals d
                LEFT JOIN clients c ON d.client_id = c.id
                LEFT JOIN generators g ON d.generator_uid = g.uid
                WHERE d.seller_id = $1
                ORDER BY d.created_at DESC
                LIMIT 10
            ''', seller_id)
        
        if not rows:
            await callback.answer("Sizda bitimlar yo'q", show_alert=True)
            return
        
        text = "📋 <b>Sizning bitimlaringiz:</b>\n\n"
        
        for row in rows:
            status_icon = {
                'PENDING_PAYMENT': '⏳',
                'PAID_SELLER_CONFIRM': '💰',
                'PAID_ACCOUNTANT_CONFIRM': '✅',
                'IN_LOGISTICS': '🚚',
                'INSTALLING': '🔧',
                'COMPLETED': '🎉'
            }.get(row['status'], '❓')
            
            client_name = row['client_name'] or "Noma'lum"
            
            text += (f"{status_icon} <b>#{row['id']}</b> - {row['model'] or 'Nomalum'}\n"
                    f"👤 {client_name}\n"
                    f"💵 {row['sale_price']:,.0f} so'm\n"
                    f"📊 Marja: {row['profit_margin'] or 0:.1f}%\n\n")
        
        await callback.message.edit_text(text, reply_markup=seller_menu(), parse_mode="HTML")
        
    except Exception as e:
        logger.error(f"seller_deals xatolik: {e}")
        await callback.answer(f"❌ Xatolik: {str(e)}", show_alert=True)

@dp.callback_query(F.data == "sl_confirm_payment")
async def seller_confirm_payment_menu(callback: CallbackQuery):
    seller_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT d.id, d.sale_price, d.generator_uid, c.full_name
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            WHERE d.seller_id = $1 AND d.status = 'PENDING_PAYMENT'
            ORDER BY d.created_at DESC
        ''', seller_id)
    
    if not rows:
        await callback.answer("Tasdiqlash kutilayotgan to'lovlar yo'q", show_alert=True)
        return
    
    buttons = []
    for row in rows:
        buttons.append([InlineKeyboardButton(
            text=f"💰 #{row['id']} - {row['full_name']} ({row['sale_price']:,.0f})",
            callback_data=f"sell_conf_{row['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="menu_seller")])
    
    await callback.message.edit_text(
        "💰 <b>To'lovni tasdiqlash:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("sell_conf_"))
async def confirm_seller_payment(callback: CallbackQuery):
    deal_id = int(callback.data.split("_")[2])
    seller_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Tekshirish
        deal = await conn.fetchrow('SELECT seller_id FROM deals WHERE id = $1', deal_id)
        if not deal or deal['seller_id'] != seller_id:
            await callback.answer("❌ Bu sizning bitimingiz emas!", show_alert=True)
            return
        
        # Avval payments yozuvi mavjudmi tekshirish
        existing = await conn.fetchval('SELECT 1 FROM payments WHERE deal_id = $1', deal_id)
        
        if existing:
            # Yangilash
            await conn.execute('''
                UPDATE payments 
                SET seller_confirmed = TRUE,
                    seller_confirmed_at = CURRENT_TIMESTAMP,
                    seller_id = $1
                WHERE deal_id = $2
            ''', seller_id, deal_id)
        else:
            # Yaratish
            await conn.execute('''
                INSERT INTO payments 
                (deal_id, seller_id, seller_confirmed, seller_confirmed_at, amount)
                VALUES ($1, $2, TRUE, CURRENT_TIMESTAMP, 
                       (SELECT sale_price FROM deals WHERE id = $1))
            ''', deal_id, seller_id)
        
        await conn.execute('''
            UPDATE deals SET status = 'PAID_SELLER_CONFIRM' WHERE id = $1
        ''', deal_id)
        
        await log_action(seller_id, 'UPDATE', 'payments', deal_id, 
                        new_data={'seller_confirmed': True}, role='sotuvchi')
    
    await callback.answer("✅ To'lov tasdiqlandi!")
    await callback.message.edit_text(
        f"✅ <b>Bitim #{deal_id}</b> uchun to'lov tasdiqlandi!\n\n"
        f"Endi buxgalter tasdig'ini kutilmoqda.",
        reply_markup=seller_menu()
    )
    
    await notify_by_role('buxgalter', 
                        f"💰 <b>Yangi to'lov tasdiqi!</b>\n\n"
                        f"Bitim #{deal_id} - sotuvchi to'lov qabul qilganini tasdiqladi.")

@dp.callback_query(F.data == "sl_report")
async def seller_report(callback: CallbackQuery):
    seller_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Statistika
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_deals,
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed,
                SUM(sale_price) as total_sales,
                SUM(profit) as total_profit,
                AVG(profit_margin) as avg_margin
            FROM deals
            WHERE seller_id = $1 AND created_at > CURRENT_DATE - INTERVAL '30 days'
        ''', seller_id)
        
        # Oxirgi bitimlar
        recent = await conn.fetch('''
            SELECT id, sale_price, profit_margin, status, created_at
            FROM deals
            WHERE seller_id = $1
            ORDER BY created_at DESC
            LIMIT 5
        ''', seller_id)
    
    text = (f"📊 <b>Sizning hisobotingiz (oxirgi 30 kun):</b>\n\n"
            f"💼 Jami bitimlar: {stats['total_deals'] or 0}\n"
            f"✅ Tugallangan: {stats['completed'] or 0}\n"
            f"💰 Sotuv hajmi: {stats['total_sales'] or 0:,.0f} so'm\n"
            f"📈 Jami foyda: {stats['total_profit'] or 0:,.0f} so'm\n"
            f"📊 O'rtacha marja: {stats['avg_margin'] or 0:.1f}%\n\n"
            f"<b>Oxirgi bitimlar:</b>\n")
    
    for row in recent:
        status_icon = {
            'PENDING_PAYMENT': '⏳',
            'COMPLETED': '✅'
        }.get(row['status'], '🔄')
        text += (f"{status_icon} #{row['id']}: "
                f"{row['sale_price']:,.0f} so'm "
                f"({row['profit_margin'] or 0:.1f}%)\n")
    
    await callback.message.edit_text(text, reply_markup=seller_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "sl_map")
async def seller_map(callback: CallbackQuery):
    seller_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT c.full_name, c.geo_lat, c.geo_lon, g.model, d.status
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.seller_id = $1 AND c.geo_lat IS NOT NULL
        ''', seller_id)
    
    if not rows:
        await callback.answer("Mijozlar lokatsiyasi yo'q", show_alert=True)
        return
    
    text = f"🗺 <b>Sizning mijozlaringiz xaritada ({len(rows)} ta):</b>\n\n"
    
    for row in rows:
        status_icon = "🟢" if row['status'] == 'COMPLETED' else "🟡"
        maps_link = f"https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}"
        text += (f"{status_icon} {row['full_name']} - {row['model']}\n"
                f"📍 <a href='{maps_link}'>Xaritada ko'rish</a>\n\n")
    
    await callback.message.edit_text(
        text, 
        reply_markup=seller_menu(),
        parse_mode="HTML",
        disable_web_page_preview=True
    )

# ============ ACCOUNTANT MODULE ============

@dp.callback_query(F.data == "acc_pending")
async def pending_payments(callback: CallbackQuery):
    """Buxgalter uchun to'lov kutilayotgan bitimlar"""
    async with db_pool.acquire() as conn:
        # Faqat sotuvchi tasdiqlagan bitimlarni ko'rsatish
        rows = await conn.fetch('''
            SELECT d.*, c.full_name, c.phone, g.model, g.power_kw,
                   p.seller_confirmed, p.seller_confirmed_at, p.amount
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            JOIN payments p ON d.id = p.deal_id
            WHERE d.status = 'PAID_SELLER_CONFIRM' 
              AND p.seller_confirmed = TRUE
              AND (p.accountant_confirmed = FALSE OR p.accountant_confirmed IS NULL)
            ORDER BY p.seller_confirmed_at DESC
        ''')
    
    if not rows:
        await callback.answer("⏳ Hozircha tasdiqlash uchun bitimlar yo'q", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"💰 <b>{len(rows)} ta bitim tasdiqlash kutilmoqda:</b>",
        parse_mode="HTML"
    )
    
    for row in rows:
        text = (f"💰 <b>Bitim #{row['id']}</b>\n\n"
                f"🔧 {row['model']} ({row['power_kw']}kVA)\n"
                f"👤 {row['full_name']}\n"
                f"📱 {row['phone']}\n"
                f"💵 Summa: {row['sale_price']:,.0f} so'm\n"
                f"💰 To'langan: {row['amount']:,.0f} so'm\n"
                f"✅ Sotuvchi tasdiqladi: {row['seller_confirmed_at'].strftime('%d.%m.%Y %H:%M')}\n\n"
                f"📊 Status: Buxgalter tasdiqlashi kutilmoqda")
        
        buttons = [[InlineKeyboardButton(
            text="✅ To'lovni tasdiqlash",
            callback_data=f"acc_confirm_{row['id']}"
        )]]
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "acc_margin")
async def accountant_margin_report(callback: CallbackQuery):
    """Marja hisoboti - buxgalter uchun"""
    async with db_pool.acquire() as conn:
        # Umumiy marja statistikasi
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_deals,
                AVG(profit_margin) as avg_margin,
                MIN(profit_margin) as min_margin,
                MAX(profit_margin) as max_margin,
                SUM(profit) as total_profit
            FROM deals
            WHERE status = 'COMPLETED'
            AND completed_at > CURRENT_DATE - INTERVAL '30 days'
        ''')
        
        # Marja oralig'lari bo'yicha
        margin_ranges = await conn.fetch('''
            SELECT 
                CASE 
                    WHEN profit_margin < 10 THEN '0-10%'
                    WHEN profit_margin < 20 THEN '10-20%'
                    WHEN profit_margin < 30 THEN '20-30%'
                    ELSE '30%+'
                END as range,
                COUNT(*) as count,
                SUM(profit) as total_profit
            FROM deals
            WHERE status = 'COMPLETED'
            AND completed_at > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 1
            ORDER BY 1
        ''')
        
        # Sotuvchilar bo'yicha marja
        seller_margins = await conn.fetch('''
            SELECT 
                e.full_name,
                COUNT(*) as deals,
                AVG(d.profit_margin) as avg_margin,
                SUM(d.profit) as total_profit
            FROM deals d
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE d.status = 'COMPLETED'
            AND d.completed_at > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY e.full_name
            ORDER BY avg_margin DESC
            LIMIT 10
        ''')
    
    # Javobni tayyorlash
    text = (f"📈 <b>Marja hisoboti (oxirgi 30 kun)</b>\n\n"
            f"<b>📊 Umumiy ko'rsatkichlar:</b>\n"
            f"Bitimlar: {stats['total_deals'] or 0} ta\n"
            f"O'rtacha marja: {stats['avg_margin'] or 0:.1f}%\n"
            f"Min/Max: {stats['min_margin'] or 0:.1f}% / {stats['max_margin'] or 0:.1f}%\n"
            f"Jami foyda: {stats['total_profit'] or 0:,.0f} so'm\n\n")
    
    if margin_ranges:
        text += "<b>📈 Marja taqsimoti:</b>\n"
        for r in margin_ranges:
            text += f"• {r['range']}: {r['count']} ta ({r['total_profit']:,.0f} so'm)\n"
        text += "\n"
    
    if seller_margins:
        text += "<b>👤 Sotuvchilar bo'yicha (TOP 10):</b>\n"
        for i, s in enumerate(seller_margins, 1):
            text += (f"{i}. {s['full_name']}: {s['avg_margin']:.1f}% "
                    f"({s['deals']} ta, {s['total_profit']:,.0f} so'm)\n")
    
    await callback.message.edit_text(
        text,
        reply_markup=accountant_menu(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("acc_confirm_"))
async def confirm_accountant_payment(callback: CallbackQuery):
    """Buxgalter to'lovni tasdiqlaydi - mijozdan geolokatsiya so'rash"""
    deal_id = int(callback.data.split("_")[2])
    accountant_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Tekshirish - sotuvchi tasdiqlaganmi?
        payment = await conn.fetchrow('''
            SELECT seller_confirmed, amount 
            FROM payments 
            WHERE deal_id = $1
        ''', deal_id)
        
        if not payment:
            await callback.answer("❌ To'lov ma'lumotlari topilmadi!", show_alert=True)
            return
        
        if not payment['seller_confirmed']:
            await callback.answer("❌ Sotuvchi hali tasdiqlamagan!", show_alert=True)
            return
        
        # Buxgalter tasdiqlash
        await conn.execute('''
            UPDATE payments 
            SET accountant_confirmed = TRUE, 
                accountant_confirmed_at = CURRENT_TIMESTAMP,
                accountant_id = $1
            WHERE deal_id = $2
        ''', accountant_id, deal_id)
        
        await conn.execute('''
            UPDATE deals SET status = 'PAID_ACCOUNTANT_CONFIRM' WHERE id = $1
        ''', deal_id)
        
        # Foydani hisoblash
        profit_data = await calculate_deal_profit(deal_id)
        
        # Mijoz ma'lumotlarini olish
        deal = await conn.fetchrow('''
            SELECT d.*, c.full_name, c.phone, c.address, c.telegram_id,
                   c.geo_lat, c.geo_lon, g.uid, g.model
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.id = $1
        ''', deal_id)
        
        await log_action(accountant_id, 'UPDATE', 'payments', deal_id, 
                        new_data={'accountant_confirmed': True}, role='buxgalter')
    
    await callback.answer("✅ To'lov tasdiqlandi!")
    
    # Geolokatsiya tekshirish
    if deal['geo_lat'] is None:
        # Geolokatsiya yo'q - mijozdan so'rash
        await request_client_geo_after_payment(deal['client_id'], deal['telegram_id'], deal_id)
        geo_status = "\n\n📍 <b>Mijozdan geolokatsiya so'ralmoqda...</b>"
    else:
        geo_status = f"\n\n📍 <a href='https://maps.google.com/?q={deal['geo_lat']},{deal['geo_lon']}'>Mijoz lokatsiyasi</a>"
    
    text = (f"✅ <b>Bitim #{deal_id}</b> to'lovi tasdiqlandi!\n\n"
            f"📊 Foyda: {profit_data['profit']:,.0f} so'm\n"
            f"📈 Marja: {profit_data['margin']:.1f}%\n\n"
            f"🚚 Logistlarga xabar yuborildi.{geo_status}")
    
    await callback.message.edit_text(text, reply_markup=accountant_menu())
    
    # Logistlarga xabar
    msg = (f"🚚 <b>Yangi yetkazish vazifasi!</b>\n\n"
           f"Bitim: #{deal_id}\n"
           f"Generator: {deal['uid']} ({deal['model']})\n"
           f"Mijoz: {deal['full_name']}\n"
           f"Telefon: {deal['phone']}\n"
           f"Manzil: {deal['address']}")
    
    if deal['geo_lat']:
        msg += f"\n📍 <a href='https://maps.google.com/?q={deal['geo_lat']},{deal['geo_lon']}'>Xaritada ko'rish</a>"
    else:
        msg += "\n\n⚠️ <b>Geolokatsiya kutilmoqda...</b>"
    
    await notify_by_role('logist', msg)

async def request_client_geo_after_payment(client_id: int, client_telegram_id: int, deal_id: int):
    """Buxgalter to'lovni tasdiqlagandan keyin mijozdan geolokatsiya so'rash"""
    try:
        await bot.send_message(
            client_telegram_id,
            f"🎉 <b>Tabriklaymiz!</b>\n\n"
            f"Sizning buyurtmangiz (#{deal_id}) to'lovi tasdiqlandi.\n\n"
            f"📍 <b>Endi joylashuvingizni (geolokatsiyangizni) yuboring:</b>\n"
            f"Bu yetkazib berish va o'rnatish uchun zarur.\n\n"
            f"Telegram'da 📎 (clip) tugmasini bosing va 'Location' ni tanlang:",
            parse_mode="HTML"
        )
        
        # State ni majburlash uchun mijozni kuzatish
        # Bu yerda FSMContext ishlatilmaydi, chunki bu async funksiya
        # Shuning uchun mijozning keyingi lokatsiya xabarini ushlash uchun 
        # alohida handler yaratamiz
        
    except Exception as e:
        logger.error(f"Geolokatsiya so'rashda xatolik: {e}")


@dp.message(F.location)
async def process_client_geo_location(message: Message, state: FSMContext):
    """Mijozdan geolokatsiya qabul qilish (to'lovdan keyin)"""
    user_id = message.from_user.id
    
    # Tekshirish - bu mijozning to'lov tasdiqlangan bitimi bormi?
    async with db_pool.acquire() as conn:
        # Mijozni topish
        client = await conn.fetchrow(
            'SELECT id FROM clients WHERE telegram_id = $1 AND is_approved = TRUE',
            user_id
        )
        
        if not client:
            return  # Bu mijoz emas, ignore
        
        # To'lov tasdiqlangan, lekin geolokatsiya yo'q bitimlarni topish
        pending_geo_deals = await conn.fetch('''
            SELECT d.id, d.status
            FROM deals d
            WHERE d.client_id = $1 
            AND d.status IN ('PAID_ACCOUNTANT_CONFIRM', 'IN_LOGISTICS', 'INSTALLING')
            AND NOT EXISTS (
                SELECT 1 FROM clients c 
                WHERE c.id = $1 AND c.geo_lat IS NOT NULL
            )
            ORDER BY d.created_at DESC
            LIMIT 1
        ''', client['id'])
        
        if not pending_geo_deals:
            # Agar barcha bitimlarda geolokatsiya bor bo'lsa, yoki to'lov tasdiqlanmagan bo'lsa
            return
    
    # Geolokatsiyani saqlash
    lat = message.location.latitude
    lon = message.location.longitude
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE clients 
            SET geo_lat = $1, geo_lon = $2
            WHERE telegram_id = $3
        ''', lat, lon, user_id)
        
        # Log
        await log_action(user_id, 'UPDATE', 'clients', client['id'],
                        new_data={'geo_lat': lat, 'geo_lon': lon},
                        role='mijoz')
    
    # Xabar
    maps_link = f"https://maps.google.com/?q={lat},{lon}"
    
    await message.answer(
        f"✅ <b>Joylashuvingiz qabul qilindi!</b>\n\n"
        f"📍 Kenglik: {lat}\n"
        f"📍 Uzunlik: {lon}\n\n"
        f"<a href='{maps_link}'>Xaritada ko'rish</a>\n\n"
        f"Tez orada logistika bo'limi siz bilan bog'lanadi.",
        parse_mode="HTML",
        disable_web_page_preview=True
    )
    
    # Logistlarga xabar
    await notify_by_role('logist', 
                        f"📍 <b>Yangi geolokatsiya!</b>\n\n"
                        f"Mijoz: {user_id}\n"
                        f"<a href='{maps_link}'>Xaritada ko'rish</a>")



@dp.callback_query(F.data == "acc_finance")
async def accountant_finance_report(callback: CallbackQuery):
    async with db_pool.acquire() as conn:
        # Umumiy statistika
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_deals,
                SUM(d.sale_price) as total_sales,
                SUM(g.purchase_price) as total_cost,
                SUM(d.profit) as total_profit,
                AVG(d.profit_margin) as avg_margin
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.status = 'COMPLETED'
            AND d.completed_at > CURRENT_DATE - INTERVAL '30 days'
        ''')
        
        # Sotuvchilar bo'yicha
        sellers = await conn.fetch('''
            SELECT 
                e.full_name,
                COUNT(*) as deals_count,
                SUM(d.sale_price) as sales,
                SUM(d.profit) as profit
            FROM deals d
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE d.status = 'COMPLETED'
            AND d.completed_at > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY e.full_name
            ORDER BY sales DESC
        ''')
    
    text = (f"📊 <b>Moliyaviy hisobot (oxirgi 30 kun)</b>\n\n"
            f"<b>📅 Umumiy:</b>\n"
            f"Bitimlar: {stats['total_deals'] or 0} ta\n"
            f"Sotuv: {stats['total_sales'] or 0:,.0f} so'm\n"
            f"Xarajat: {stats['total_cost'] or 0:,.0f} so'm\n"
            f"<b>💵 Sof foyda: {stats['total_profit'] or 0:,.0f} so'm</b>\n"
            f"<b>📊 O'rtacha marja: {stats['avg_margin'] or 0:.1f}%</b>\n\n"
            f"<b>👤 Sotuvchilar bo'yicha:</b>\n")
    
    for seller in sellers:
        text += (f"• {seller['full_name']}: "
                f"{seller['deals_count']} ta, "
                f"{seller['sales'] or 0:,.0f} so'm\n")
    
    await callback.message.edit_text(text, reply_markup=accountant_menu(), parse_mode="HTML")

# ============ LOGISTICS MODULE ============

@dp.callback_query(F.data == "log_pending")
async def pending_deliveries(callback: CallbackQuery):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT d.id, d.generator_uid, c.full_name, c.phone, 
                   c.address, c.geo_lat, c.geo_lon, g.model, g.power_kw
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.status = 'PAID_ACCOUNTANT_CONFIRM'
            ORDER BY d.created_at
        ''')
    
    if not rows:
        await callback.answer("Yetkazish kutilayotgan buyurtmalar yo'q", show_alert=True)
        return
    
    for row in rows:
        text = (f"📦 <b>Yetkazish #{row['id']}</b>\n\n"
                f"🔧 {row['model']} ({row['power_kw']}kVA)\n"
                f"🆔 {row['generator_uid']}\n"
                f"👤 {row['full_name']}\n"
                f"📱 {row['phone']}\n"
                f"📍 {row['address']}")
        
        buttons = [[InlineKeyboardButton(
            text="🚚 Yetkazishni olish",
            callback_data=f"log_take_{row['id']}"
        )]]
        
        if row['geo_lat']:
            buttons.append([InlineKeyboardButton(
                text="📍 Xarita",
                url=f"https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}"
            )])
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("log_take_"))
async def take_delivery(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split("_")[2])
    await state.update_data(deal_id=deal_id)
    
    await callback.message.edit_text(
        "🚚 <b>Yetkazishni rejalashtirish</b>\n\nMashina ma'lumotlari (rusumi, nomer):",
        parse_mode="HTML"
    )
    await state.set_state(LogisticsAssign.entering_vehicle)

@dp.message(LogisticsAssign.entering_vehicle)
async def process_vehicle(message: Message, state: FSMContext):
    await state.update_data(vehicle=message.text.strip())
    await message.answer("👤 Haydovchi ismi:")
    await state.set_state(LogisticsAssign.entering_driver)

@dp.message(LogisticsAssign.entering_driver)
async def process_driver(message: Message, state: FSMContext):
    await state.update_data(driver_name=message.text.strip())
    await message.answer("📱 Haydovchi telefoni:")
    await state.set_state(LogisticsAssign.entering_driver_phone)

@dp.message(LogisticsAssign.entering_driver_phone)
async def process_driver_phone(message: Message, state: FSMContext):
    await state.update_data(driver_phone=message.text.strip())
    await message.answer("💵 Yetkazish narxi (so'm):")
    await state.set_state(LogisticsAssign.entering_cost)

@dp.message(LogisticsAssign.entering_cost)
async def process_delivery_cost(message: Message, state: FSMContext):
    try:
        cost = float(message.text)
        await state.update_data(delivery_cost=cost)
        await message.answer(
            "🤔 Kim to'laydi?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Mijoz", callback_data="pay_client")],
                [InlineKeyboardButton(text="🏢 Kompaniya", callback_data="pay_company")]
            ])
        )
        await state.set_state(LogisticsAssign.selecting_who_pays)
    except:
        await message.answer("❌ Faqat son kiriting!")

@dp.callback_query(LogisticsAssign.selecting_who_pays, F.data.startswith("pay_"))
async def process_who_pays(callback: CallbackQuery, state: FSMContext):
    who = callback.data.split("_")[1]
    await state.update_data(who_pays=who)
    
    await callback.message.edit_text("📅 Yetkazish sanasi (KK.OO.YYYY):")
    await state.set_state(LogisticsAssign.entering_date)

@dp.message(LogisticsAssign.entering_date)
async def process_delivery_date(message: Message, state: FSMContext):
    try:
        date = datetime.strptime(message.text.strip(), "%d.%m.%Y").date()
        await state.update_data(planned_date=date)
        data = await state.get_data()
        
        text = (f"📋 <b>Tasdiqlang:</b>\n\n"
                f"🚚 Mashina: {data['vehicle']}\n"
                f"👤 Haydovchi: {data['driver_name']}\n"
                f"📱 Tel: {data['driver_phone']}\n"
                f"💵 Narx: {data['delivery_cost']:,.0f} so'm\n"
                f"🤔 Kim to'laydi: {data['who_pays']}\n"
                f"📅 Sana: {date.strftime('%d.%m.%Y')}\n\n"
                f"Saqlansinmi?")
        
        await message.answer(
            text,
            reply_markup=confirm_keyboard("log_save", "menu_logistic"),
            parse_mode="HTML"
        )
    except:
        await message.answer("❌ Noto'g'ri format! (masalan: 15.02.2025)")

@dp.callback_query(F.data == "log_save")
async def save_logistics(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    logist_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO logistics 
                (deal_id, logist_id, vehicle_info, driver_name, driver_phone,
                 delivery_cost, who_pays, planned_date, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PLANNED')
            ''', data['deal_id'], logist_id, data['vehicle'], data['driver_name'],
                 data['driver_phone'], data['delivery_cost'], data['who_pays'],
                 data['planned_date'])
            
            await conn.execute('''
                UPDATE deals SET status = 'IN_LOGISTICS' WHERE id = $1
            ''', data['deal_id'])
            
            await log_action(logist_id, 'CREATE', 'logistics', data['deal_id'], 
                           new_data=data, role='logist')
        
        await callback.message.edit_text(
            "✅ <b>Yetkazish rejalashtirildi!</b>",
            reply_markup=logistic_menu(),
            parse_mode="HTML"
        )
        
        await notify_by_role('montajchi', 
                           f"🔧 <b>Yangi o'rnatish!</b>\n\n"
                           f"Bitim #{data['deal_id']}\n"
                           f"Yetkazish rejalashtirildi.")
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Xatolik: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "log_my_routes")
async def my_routes(callback: CallbackQuery):
    logist_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT l.*, d.generator_uid, c.full_name, c.phone, c.address,
                   c.geo_lat, c.geo_lon, g.model
            FROM logistics l
            JOIN deals d ON l.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE l.logist_id = $1 AND l.status IN ('PLANNED', 'IN_TRANSIT')
            ORDER BY l.planned_date
        ''', logist_id)
    
    if not rows:
        await callback.answer("Sizda aktiv marshrutlar yo'q", show_alert=True)
        return
    
    for row in rows:
        status = "🟡 Rejalashtirilgan" if row['status'] == 'PLANNED' else "🟢 Yo'lda"
        
        text = (f"🚚 <b>Marshrut #{row['deal_id']}</b>\n\n"
                f"Status: {status}\n"
                f"📅 {row['planned_date'].strftime('%d.%m.%Y')}\n"
                f"🆔 {row['generator_uid']}\n"
                f"🔧 {row['model']}\n"
                f"👤 {row['full_name']} ({row['phone']})\n"
                f"📍 {row['address']}\n"
                f"🚛 {row['vehicle_info']}")
        
        buttons = []
        if row['status'] == 'PLANNED':
            buttons.append([InlineKeyboardButton(
                text="🚦 Yo'lda", callback_data=f"log_transit_{row['deal_id']}"
            )])
        elif row['status'] == 'IN_TRANSIT':
            buttons.append([InlineKeyboardButton(
                text="✅ Yetkazildi", callback_data=f"log_delivered_{row['deal_id']}"
            )])
        
        if row['geo_lat']:
            buttons.append([InlineKeyboardButton(
                text="📍 Xarita",
                url=f"https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}"
            )])
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None,
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("log_transit_"))
async def set_transit(callback: CallbackQuery):
    deal_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE logistics SET status = 'IN_TRANSIT' WHERE deal_id = $1
        ''', deal_id)
    
    await callback.answer("🚦 Status: Yo'lda")
    await my_routes(callback)

@dp.callback_query(F.data.startswith("log_delivered_"))
async def set_delivered(callback: CallbackQuery):
    deal_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE logistics 
            SET status = 'DELIVERED', actual_date = CURRENT_DATE 
            WHERE deal_id = $1
        ''', deal_id)
        
        await conn.execute('''
            UPDATE deals SET status = 'INSTALLING' WHERE id = $1
        ''', deal_id)
    
    await callback.answer("✅ Yetkazildi!")
    await callback.message.edit_text(
        "✅ Yetkazildi! Montajchilar xabardor qilindi.",
        reply_markup=logistic_menu()
    )
    
    await notify_by_role('montajchi', 
                        f"📦 <b>Generator yetkazildi!</b>\n\n"
                        f"Bitim #{deal_id}\nO'rnatishni boshlashingiz mumkin.")

# ============ INSTALLER MODULE ============

@dp.callback_query(F.data == "inst_pending")
async def pending_installations(callback: CallbackQuery):
    """Montajchi uchun biriktirilgan vazifalar (viloyat/shahar bo'yicha)"""
    installer_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Montajchining viloyat/shaharini olish
        installer = await conn.fetchrow(
            'SELECT region, city FROM employees WHERE telegram_id = $1',
            installer_id
        )
        
        # Barcha biriktirilgan vazifalar
        rows = await conn.fetch('''
            SELECT d.id, d.generator_uid, d.installer_assigned_at,
                   c.full_name, c.address, c.region as client_region, 
                   c.city as client_city, c.geo_lat, c.geo_lon,
                   g.model, g.power_kw, g.serial_number
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.installer_id = $1 
            AND d.status IN ('INSTALLING', 'PAID_ACCOUNTANT_CONFIRM', 'IN_LOGISTICS')
            ORDER BY 
                CASE WHEN c.city = $2 THEN 0 ELSE 1 END,
                CASE WHEN c.region = $3 THEN 0 ELSE 1 END,
                d.installer_assigned_at DESC
        ''', installer_id, installer['city'] if installer else '', 
             installer['region'] if installer else '')
    
    if not rows:
        await callback.answer("Sizga biriktirilgan vazifalar yo'q", show_alert=True)
        return
    
    # Guruplash
    same_city = [r for r in rows if r['client_city'] == installer.get('city')]
    same_region = [r for r in rows if r['client_region'] == installer.get('region') 
                   and r['client_city'] != installer.get('city')]
    other = [r for r in rows if r['client_region'] != installer.get('region')]
    
    text = f"🔧 <b>Sizga biriktirilgan vazifalar ({len(rows)} ta)</b>\n"
    
    if same_city:
        text += f"\n📍 <b>Sizning tumaningiz ({len(same_city)} ta):</b>\n"
    if same_region:
        text += f"\n📍 <b>Sizning viloyatingiz ({len(same_region)} ta):</b>\n"
    if other:
        text += f"\n🌐 <b>Boshqa viloyatlar ({len(other)} ta):</b>\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    
    # Har bir vazifani alohida ko'rsatish
    for row in rows:
        location_match = ""
        if row['client_city'] == installer.get('city'):
            location_match = "✅ <b>Shu tuman!</b>\n"
        elif row['client_region'] == installer.get('region'):
            location_match = "📍 Shu viloyat\n"
        else:
            location_match = f"🌐 {row['client_region']}, {row['client_city']}\n"
        
        text = (f"🔧 <b>O'rnatish #{row['id']}</b>\n\n"
                f"{location_match}"
                f"🆔 {row['generator_uid']}\n"
                f"🔩 {row['model']} ({row['power_kw']}kVA)\n"
                f"🔢 Seriya: {row['serial_number']}\n"
                f"👤 {row['full_name']}\n"
                f"📍 {row['address']}\n")
        
        if row['geo_lat']:
            text += f"🗺 <a href='https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}'>Xaritada ko'rish</a>\n"
        
        buttons = [[InlineKeyboardButton(
            text="✅ Boshlash",
            callback_data=f"inst_start_{row['id']}"
        )]]
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML",
            disable_web_page_preview=True
        )

@dp.callback_query(F.data.startswith("inst_start_"))
async def start_installation(callback: CallbackQuery, state: FSMContext):
    deal_id = int(callback.data.split("_")[2])
    await state.update_data(deal_id=deal_id, photos=[], videos=[])
    
    await callback.message.edit_text(
        "🔧 <b>O'rnatishni boshlash</b>\n\nBoshlang'ich moto-soatni kiriting:",
        parse_mode="HTML"
    )
    await state.set_state(InstallationWork.entering_motor_hours)

@dp.message(InstallationWork.entering_motor_hours)
async def process_motor_hours(message: Message, state: FSMContext):
    try:
        hours = int(message.text)
        await state.update_data(motor_hours=hours)
        await message.answer(
            "📸 O'rnatish rasmlarini yuboring.\n"
            "Tayyor bo'lsa 'tayyor' deb yozing:"
        )
        await state.set_state(InstallationWork.uploading_photos)
    except:
        await message.answer("❌ Faqat son kiriting!")

@dp.message(InstallationWork.uploading_photos, F.photo)
async def process_inst_photo(message: Message, state: FSMContext):
    photo = message.photo[-1]
    data = await state.get_data()
    photos = data.get('photos', [])
    
    file_path = await download_file(
        photo.file_id,
        "installations",
        f"inst_{data['deal_id']}_{len(photos)}"
    )
    
    if file_path:
        photos.append(file_path)
        await state.update_data(photos=photos)
        await message.answer(f"✅ Rasm qo'shildi ({len(photos)} ta).")
    else:
        await message.answer("❌ Xatolik!")

@dp.message(InstallationWork.uploading_photos, F.text.lower() == "tayyor")
async def finish_inst_photos(message: Message, state: FSMContext):
    await message.answer(
        "🎥 Video yuboring (ixtiyoriy).\n"
        "Tayyor bo'lsa 'tayyor' deb yozing:"
    )
    await state.set_state(InstallationWork.uploading_videos)

@dp.message(InstallationWork.uploading_videos, F.video)
async def process_inst_video(message: Message, state: FSMContext):
    """Video qayta ishlash - TO'G'RILANGAN"""
    data = await state.get_data()
    videos = data.get('videos', [])
    
    # Video faylini olish - eng yuqori sifatli videoni tanlash
    video = message.video
    
    # Kengaytmani aniqlash
    mime_type = video.mime_type or "video/mp4"
    ext = "mp4"
    if mime_type == "video/quicktime":
        ext = "mov"
    elif mime_type == "video/x-msvideo":
        ext = "avi"
    elif mime_type == "video/webm":
        ext = "webm"
    
    # Faylni yuklash
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"inst_{data['deal_id']}_{len(videos)}_{timestamp}"
    
    file_path = await download_file(
        video.file_id,
        "installations",
        filename,
        "video"  # MUHIM: file_type="video" berilishi kerak!
    )
    
    if file_path:
        videos.append(file_path)
        await state.update_data(videos=videos)
        await message.answer(f"✅ Video qo'shildi ({len(videos)} ta). Yana yuboring yoki 'tayyor' deb yozing:")
    else:
        await message.answer("❌ Video yuklashda xatolik! Qayta urinib ko'ring.")

@dp.message(InstallationWork.uploading_videos, F.text.lower() == "tayyor")
async def finish_inst_videos(message: Message, state: FSMContext):
    await message.answer("📝 Izohlar kiriting (yo'q bo'lsa 'yo'q' deb yozing):")
    await state.set_state(InstallationWork.entering_notes)

@dp.message(InstallationWork.entering_notes)
async def process_inst_notes(message: Message, state: FSMContext):
    notes = "" if message.text.lower() == 'yoq' else message.text
    await state.update_data(notes=notes)
    data = await state.get_data()
    
    # Backslash muammosini oldini olish uchun
    notes_display = notes or "Yo'q"
    
    await message.answer(
        f"💾 <b>Tasdiqlang:</b>\n\n"
        f"Moto-soat: {data['motor_hours']}\n"
        f"Rasmlar: {len(data.get('photos', []))} ta\n"
        f"Videos: {len(data.get('videos', []))} ta\n"
        f"Izoh: {notes_display}\n\n"
        f"O'rnatish tugallandimi?",
        reply_markup=confirm_keyboard("inst_complete", "menu_installer"),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "inst_complete")
async def complete_installation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    installer_id = callback.from_user.id
    
    try:
        async with db_pool.acquire() as conn:
            deal = await conn.fetchrow('''
                SELECT generator_uid, client_id, seller_id 
                FROM deals WHERE id = $1
            ''', data['deal_id'])
            
            # O'rnatishni saqlash
            await conn.execute('''
                INSERT INTO installations 
                (deal_id, installer_id, installation_date, motor_hours_start,
                 motor_hours_current, photos, videos, notes, act_signed)
                VALUES ($1, $2, CURRENT_TIMESTAMP, $3, $3, $4, $5, $6, TRUE)
            ''', data['deal_id'], installer_id, data['motor_hours'],
                 data.get('photos', []), data.get('videos', []), data.get('notes', ''))
            
            # Bitimni yangilash
            await conn.execute('''
                UPDATE deals 
                SET status = 'COMPLETED', 
                    completed_at = CURRENT_TIMESTAMP,
                    installation_cost = COALESCE(installation_cost, 0)
                WHERE id = $1
            ''', data['deal_id'])
            
            # Generatorni yangilash
            await conn.execute('''
                UPDATE generators 
                SET status = 'INSTALLED',
                    warranty_start_date = CURRENT_DATE,
                    current_client_id = $2
                WHERE uid = $1
            ''', deal['generator_uid'], deal['client_id'])
            
            # Foydani hisoblash
            await calculate_deal_profit(data['deal_id'])
            
            await log_action(installer_id, 'CREATE', 'installations', data['deal_id'],
                           new_data={'motor_hours': data['motor_hours']}, role='montajchi')
            
            # Mijozni olish
            client = await conn.fetchrow(
                'SELECT full_name, phone FROM clients WHERE id = $1',
                deal['client_id']
            )
        
        # Xabarnomalar
        notification = (f"🎉 <b>O'rnatish tugallandi!</b>\n\n"
                       f"🔧 Bitim #{data['deal_id']}\n"
                       f"🆔 {deal['generator_uid']}\n"
                       f"⏱ Moto-soat: {data['motor_hours']}\n"
                       f"👤 Mijoz: {client['full_name']}\n"
                       f"📱 {client['phone']}")
        
        await asyncio.gather(
            notify_admins(notification),
            notify_user(deal['seller_id'], 
                       f"✅ Sizning bitimingiz (#{data['deal_id']}) o'rnatildi!"),
            notify_by_role('ombor', 
                          f"📦 {deal['generator_uid']} o'rnatildi.")
        )
        
        await callback.message.edit_text(
            f"✅ <b>O'rnatish yakunlandi!</b>\n\n"
            f"Bitim #{data['deal_id']}\n"
            f"Ma'lumotlar saqlandi.",
            reply_markup=installer_menu(),
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"O'rnatishda xatolik: {e}")
        await callback.answer("❌ Xatolik!", show_alert=True)
    
    await state.clear()

# ============ CLIENT MODULE ============





@dp.callback_query(F.data == "cl_my_gen")
async def client_my_generator(callback: CallbackQuery):
    user_id = callback.from_user.id  # Callback dan to'g'ri user_id olamiz
    
    async with db_pool.acquire() as conn:
        # FAQAT ADMIN TASDIQLAGAN VA SHU MIJOZGA TEGISHLI GENERATORLAR
        gens = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            WHERE c.telegram_id = $1 
            AND c.is_approved = TRUE
            AND g.status IN ('INSTALLED', 'INSTALLING', 'SERVICING')
        ''', user_id)
    
    if not gens:
        await callback.answer("Sizda generatorlar yo'q yoki tasdiqlanmagan", show_alert=True)
        return
    
    if len(gens) == 1:
        # MUHIM: user_id ni aniq uzatamiz
        await show_generator_by_uid(callback.message, gens[0]['uid'], user_id=user_id)
    else:
        buttons = []
        for gen in gens:
            buttons.append([InlineKeyboardButton(
                text=f"{gen['model']} ({gen['power_kw']}kVA)",
                callback_data=f"cl_gen_{gen['uid']}"
            )])
        await callback.message.edit_text(
            "🔧 <b>Sizning generatorlaringiz:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("cl_gen_"))
async def show_client_generator(callback: CallbackQuery):
    uid = callback.data.split("_")[2]
    user_id = callback.from_user.id  # Callback dan to'g'ri user_id olamiz
    
    # MUHIM: user_id ni aniq uzatamiz
    await show_generator_by_uid(callback.message, uid, user_id=user_id)


@dp.message(F.text.startswith("/start GEN-"))
async def start_with_uid(message: Message):
    """Start bilan UID kirganda"""
    parts = message.text.split()
    if len(parts) > 1:
        uid = parts[1]
        user_id = message.from_user.id
        await show_generator_by_uid(message, uid, user_id=user_id)
    else:
        await cmd_start(message, FSMContext())


@dp.callback_query(F.data == "cl_service")
async def client_service_request(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        gens = await conn.fetch('''
            SELECT uid, model FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            WHERE c.telegram_id = $1
        ''', user_id)
    
    if not gens:
        await callback.answer("Generator topilmadi!", show_alert=True)
        return
    
    if len(gens) == 1:
        await state.update_data(generator_uid=gens[0]['uid'])
        await callback.message.edit_text(
            "🔧 <b>Servis so'rovi</b>\n\nMuammoni tavsiflab bering:",
            parse_mode="HTML"
        )
        await state.set_state(ServiceRequest.entering_problem)
    else:
        buttons = []
        for gen in gens:
            buttons.append([InlineKeyboardButton(
                text=gen['model'],
                callback_data=f"serv_gen_{gen['uid']}"
            )])
        await callback.message.edit_text(
            "Qaysi generator uchun?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )

@dp.message(ServiceRequest.entering_problem)
async def process_service_problem(message: Message, state: FSMContext):
    await state.update_data(problem=message.text)
    await message.answer("📸 Muammoni ko'rsatuvchi rasm yuboring (ixtiyoriy):")
    await state.set_state(ServiceRequest.uploading_photos)

@dp.message(ServiceRequest.uploading_photos, F.photo)
async def process_service_photo(message: Message, state: FSMContext):
    # Rasmni qayta ishlash
    await message.answer("✅ Rasm qabul qilindi. Yana yuboring yoki 'tayyor' deb yozing:")

@dp.message(ServiceRequest.uploading_photos, F.text.lower() == "tayyor")
async def finish_service_request(message: Message, state: FSMContext):
    data = await state.get_data()
    
    # Servis so'rovini saqlash
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO service_history 
            (generator_uid, service_type, description, date)
            VALUES ($1, 'repair_request', $2, CURRENT_TIMESTAMP)
        ''', data['generator_uid'], data['problem'])
    
    # Admin va montajchilarga xabar
    await notify_admins(f"🚨 <b>Yangi servis so'rovi!</b>\n\n"
                       f"🆔 {data['generator_uid']}\n"
                       f"📝 {data['problem']}")
    
    await notify_by_role('montajchi', 
                        f"🔧 <b>Servis so'rovi!</b>\n\n"
                        f"Generator: {data['generator_uid']}\n"
                        f"Muammo: {data['problem']}")
    
    await message.answer(
        "✅ So'rovingiz qabul qilindi!\n\n"
        "Tez orada siz bilan bog'lanamiz.",
        reply_markup=client_menu()
    )
    await state.clear()



async def show_generator_by_uid(message: Message, uid: str, user_id: int = None):
    """Mijozga uning generatorini ko'rsatish - TO'G'RILANGAN"""
    
    # MUHIM: user_id ni to'g'ri aniqlash
    if user_id is None:
        # Agar message.from_user mavjud bo'lsa (foydalanuvchi xabari)
        if message.from_user:
            user_id = message.from_user.id
            logger.info(f"user_id message.from_user.id dan olindi: {user_id}")
        else:
            logger.error("XATOLIK: message.from_user mavjud emas!")
            await message.answer("❌ Tizim xatoligi!")
            return
    
    # DEBUG: Logga yozish
    logger.info(f"=== DEBUG show_generator_by_uid ===")
    logger.info(f"UID: {uid}")
    logger.info(f"User ID (parametr): {user_id}")
    logger.info(f"User ID type: {type(user_id)}")
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.*, c.full_name as client_name, c.phone as client_phone,
                   c.telegram_id as client_tg, c.is_approved,
                   d.sale_price, g.warranty_months, d.completed_at,
                   i.motor_hours_current, i.installation_date
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            LEFT JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE g.uid = $1
        ''', uid)
    
    if not gen:
        logger.warning(f"Generator topilmadi: {uid}")
        await message.answer("❌ Generator topilmadi!")
        return
    
    # DEBUG: Barcha qiymatlarni logga yozish
    logger.info(f"=== Generator ma'lumotlari ===")
    logger.info(f"UID: {gen['uid']}")
    logger.info(f"Model: {gen['model']}")
    logger.info(f"Client ID: {gen['current_client_id']}")
    logger.info(f"Client Telegram ID (from DB): {gen['client_tg']}")
    logger.info(f"Client Telegram ID type: {type(gen['client_tg'])}")
    
    # Asosiy muammo: client_tg None yoki boshqa tipda bo'lishi mumkin
    if gen['client_tg'] is None:
        logger.error("client_tg NULL! Mijozga telegram_id biriktirilmagan!")
        await message.answer("❌ Bu generatorga mijoz biriktirilmagan!")
        return
    
    # Tipni tekshirish va bir xil qilish - IKKI TOMON HAM INT BO'LSIN
    client_tg_from_db = gen['client_tg']
    
    # Agar string bo'lsa, int ga o'tkazish
    if isinstance(client_tg_from_db, str):
        try:
            client_tg_from_db = int(client_tg_from_db)
            logger.info(f"client_tg str->int konvertatsiya qilindi: {client_tg_from_db}")
        except ValueError:
            logger.error(f"client_tg ni int ga o'tkazib bo'lmadi: {client_tg_from_db}")
            await message.answer("❌ Ma'lumotlar bazasida xatolik!")
            return
    
    # user_id ham int bo'lishi kerak
    if not isinstance(user_id, int):
        try:
            user_id = int(user_id)
            logger.info(f"user_id int ga konvertatsiya qilindi: {user_id}")
        except:
            logger.error(f"user_id ni int ga o'tkazib bo'lmadi: {user_id}")
            await message.answer("❌ Foydalanuvchi ID xatolik!")
            return
    
    # Qayta taqqoslash
    logger.info(f"=== Taqqoslash ===")
    logger.info(f"user_id: {user_id} (type: {type(user_id)})")
    logger.info(f"client_tg_from_db: {client_tg_from_db} (type: {type(client_tg_from_db)})")
    logger.info(f"Equal: {user_id == client_tg_from_db}")
    
    # TEKSHIRUV KETMA-KETLIGI
    is_owner = (user_id == client_tg_from_db)
    is_admin_user = is_admin(user_id)
    is_approved_client = gen['is_approved']
    
    logger.info(f"=== Tekshiruv natijalari ===")
    logger.info(f"is_owner: {is_owner}")
    logger.info(f"is_admin_user: {is_admin_user}")
    logger.info(f"is_approved_client: {is_approved_client}")
    
    # Admin tekshiruvi
    if is_admin_user:
        logger.info("Admin detected - access granted")
        pass  # Admin uchun cheklov yo'q
    
    # Mijoz tekshiruvi
    elif is_owner:
        logger.info("Owner detected")
        if not is_approved_client:
            logger.warning("Owner not approved!")
            await message.answer("❌ Sizning profilingiz hali tasdiqlanmagan!")
            return
        logger.info("Owner approved - access granted")
    
    else:
        logger.warning(f"Access denied! user_id={user_id} != client_tg={client_tg_from_db}")
        await message.answer("❌ Bu sizning generatoringiz emas!")
        return
    
    # O'zgaruvchilarga saqlash (backslash muammosini oldini olish uchun)
    warranty_end = None
    if gen['warranty_start_date'] and gen['warranty_months']:
        warranty_end = gen['warranty_start_date'] + timedelta(days=30*gen['warranty_months'])
        remaining = (warranty_end - datetime.now().date()).days
        warranty_text = f"{remaining} kun qoldi" if remaining > 0 else "⛔ TUGAGAN"
    else:
        warranty_text = "Noma'lum"
    
    install_date_str = "Noma'lum"
    if gen['installation_date']:
        install_date_str = gen['installation_date'].strftime('%d.%m.%Y')
    
    warranty_end_date_str = "Noma'lum"
    if warranty_end:
        warranty_end_date_str = warranty_end.strftime('%d.%m.%Y')
    
    motor_hours_current = gen['motor_hours_current'] or 0
    sale_price = gen['sale_price'] or 0
    
    # Endi f-stringlarda backslashsiz ishlatamiz
    text = (f"🔧 <b>Sizning generatoringiz</b>\n\n"
            f"🆔 UID: <code>{uid}</code>\n"
            f"🔩 Model: {gen['model']} ({gen['power_kw']}kVA)\n"
            f"🔢 Seriya: {gen['serial_number']}\n\n"
            f"📅 O'rnatilgan: {install_date_str}\n"
            f"⏱ Moto-soat: {motor_hours_current}\n\n"
            f"🛡 Garantiya: {warranty_text}\n"
            f"📅 Garantiya tugaydi: {warranty_end_date_str}\n\n"
            f"💰 Sotuv narxi: {sale_price:,.0f} so'm")
    
    await message.answer(text, reply_markup=client_menu(), parse_mode="HTML")




# ============ CORRECTION REQUESTS ============

@dp.callback_query(F.data == "admin_corrections")
async def admin_corrections_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT cr.*, e.full_name as requester_name
            FROM correction_requests cr
            JOIN employees e ON cr.requested_by = e.telegram_id
            WHERE cr.status = 'PENDING'
            ORDER BY cr.created_at DESC
        ''')
    
    if not rows:
        await callback.message.edit_text(
            "✅ Kutilayotgan so'rovlar yo'q",
            reply_markup=admin_main_keyboard()
        )
        return
    
    await callback.message.edit_text(
        f"⚠️ <b>{len(rows)} ta so'rov kutilmoqda</b>\n\n"
        f"Tafsilotlar alohida xabarlarda yuborildi...",
        reply_markup=admin_main_keyboard(),
        parse_mode="HTML"
    )
    
    for row in rows:
        # Backslash muammosini oldini olish uchun
        current_val = row['current_value'] or "Noma'lum"
        
        text = (f"⚠️ <b>So'rov #{row['id']}</b>\n\n"
                f"👤 So'ragan: {row['requester_name']}\n"
                f"📋 Jadval: {row['entity_type']}\n"
                f"🆔 ID: {row['entity_id']}\n"
                f"📝 Maydon: {row['field_name']}\n"
                f"↩️ Eski: {current_val}\n"
                f"↪️ Yangi: {row['proposed_value']}\n"
                f"💬 Sabab: {row['reason']}")
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Tasdiqlash", 
                                       callback_data=f"corr_app_{row['id']}"),
                    InlineKeyboardButton(text="❌ Rad etish", 
                                       callback_data=f"corr_rej_{row['id']}")
                ]
            ]),
            parse_mode="HTML"
        )



# ============ MAP & REPORTS ============

@dp.callback_query(F.data.startswith("corr_app_"))
async def approve_correction(callback: CallbackQuery):
    """Admin so'rovni tasdiqlaydi"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    req_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow('SELECT * FROM correction_requests WHERE id = $1', req_id)
        
        if not req or req['status'] != 'PENDING':
            await callback.answer("So'rov allaqachon ko'rib chiqilgan!", show_alert=True)
            return
        
        table_map = {
            'deal': ('deals', 'id'),
            'client': ('clients', 'id'),
            'logistics': ('logistics', 'id'),
            'install': ('installations', 'deal_id')
        }
        
        entity_type = req['entity_type']
        table_info = table_map.get(entity_type)
        
        if not table_info:
            await callback.answer(f"❌ Noto'g'ri obyekt turi: {entity_type}", show_alert=True)
            return
        
        table, id_col = table_info
        
        try:
            # ENTITY_ID ni aniq INTEGER ga aylantirish
            entity_id_raw = req['entity_id']
            if isinstance(entity_id_raw, str):
                entity_id = int(entity_id_raw)
            elif isinstance(entity_id_raw, int):
                entity_id = entity_id_raw
            else:
                entity_id = int(str(entity_id_raw))
            
            # ✅ PROPOSED_VALUE ni to'g'ri tipga aylantirish
            proposed_value = req['proposed_value']
            field_name = req['field_name']
            
            # Agar maydon integer bo'lishi kerak bo'lsa (motor_hours_start, motor_hours_current)
            integer_fields = ['motor_hours_start', 'motor_hours_current', 'sale_price', 
                            'delivery_cost', 'installation_cost', 'other_costs']
            
            if field_name in integer_fields:
                try:
                    proposed_value = int(proposed_value)
                except (ValueError, TypeError):
                    pass  # Agar aylantirib bo'lmasa, string qoldirish
            
            # Agar decimal/number maydon bo'lsa
            decimal_fields = ['sale_price', 'delivery_cost', 'installation_cost', 
                            'other_costs', 'profit', 'profit_margin']
            
            if field_name in decimal_fields:
                try:
                    proposed_value = float(proposed_value)
                except (ValueError, TypeError):
                    pass
            
            logger.info(f"approve_correction: entity_id={entity_id}, field={field_name}, proposed_value={proposed_value} (type={type(proposed_value)})")
            
            # ESKI QIYMATNI OLISH
            old_val = await conn.fetchval(f'''
                SELECT {field_name} FROM {table} WHERE {id_col} = $1
            ''', entity_id)
            
            # YANGILASH - proposed_value endi to'g'ri tipda
            await conn.execute(f'''
                UPDATE {table} SET {field_name} = $1 WHERE {id_col} = $2
            ''', proposed_value, entity_id)
            
            # So'rovni yangilash
            await conn.execute('''
                UPDATE correction_requests 
                SET status = 'APPROVED', admin_id = $1, resolved_at = CURRENT_TIMESTAMP
                WHERE id = $2
            ''', callback.from_user.id, req_id)
            
            # Audit log
            await log_action(callback.from_user.id, 'CORRECTION', entity_type,
                           str(entity_id),
                           old_data={field_name: old_val},
                           new_data={field_name: proposed_value},
                           role='admin')
            
            # Xabar yuborish
            old_display = old_val if old_val is not None else "Noma'lum"
            await notify_user(req['requested_by'], 
                            "✅ So'rovingiz tasdiqlandi!\n\n"
                            f"#{req_id}: {field_name} o'zgartirildi.\n"
                            f"Eski: {old_display}\n"
                            f"Yangi: {proposed_value}")
            
            # Agar bitim bo'lsa, foydani qayta hisoblash
            if entity_type == 'deal' and field_name in ['sale_price', 'delivery_cost', 'installation_cost', 'other_costs']:
                await calculate_deal_profit(entity_id)
            
            await callback.answer("✅ Tasdiqlandi!")
            
            # Javob xabari
            await callback.message.edit_text(
                f"✅ <b>So'rov #{req_id} tasdiqlandi</b>\n\n"
                f"📋 Jadval: {table}\n"
                f"🆔 ID: {entity_id}\n"
                f"📝 {field_name}: {old_display} → {proposed_value}"
            )
            
        except ValueError as ve:
            logger.error(f"ValueError: {ve}, entity_id_raw={entity_id_raw}, proposed_value={req.get('proposed_value')}")
            await callback.answer(f"❌ Qiymat raqamga aylantirishda xatolik", show_alert=True)
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error(f"entity_type={entity_type}, field={req.get('field_name')}, entity_id={req.get('entity_id')}, proposed={req.get('proposed_value')}")
            await callback.answer(f"❌ Xatolik: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("corr_rej_"))
async def reject_correction(callback: CallbackQuery):
    """Admin so'rovni rad etadi"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    req_id = int(callback.data.split("_")[2])
    
    async with db_pool.acquire() as conn:
        req = await conn.fetchrow(
            'SELECT requested_by FROM correction_requests WHERE id = $1', 
            req_id
        )
        
        if not req:
            await callback.answer("❌ So'rov topilmadi!", show_alert=True)
            return
        
        await conn.execute('''
            UPDATE correction_requests 
            SET status = 'REJECTED', admin_id = $1, resolved_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', callback.from_user.id, req_id)
        
        # Xabar yuborish
        try:
            await notify_user(req['requested_by'], 
                            f"❌ So'rovingiz (#{req_id}) rad etildi.\n"
                            f"Qo'shimcha ma'lumot uchun admin bilan bog'laning.")
        except Exception as e:
            logger.error(f"Xabar yuborishda xatolik: {e}")
    
    await callback.answer("❌ Rad etildi")
    await callback.message.edit_text(
        f"❌ <b>So'rov #{req_id} rad etildi</b>",
        reply_markup=admin_main_keyboard()
    )
# admin map menu 

# ============ MAP MODULE ============

@dp.callback_query(F.data == "admin_map")
async def admin_map_menu(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "🗺 <b>Karta rejimlari</b>\n\n"
        "Kerakli xarita turini tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏢 Ichki karta (to'liq)", callback_data="map_internal")],
            [InlineKeyboardButton(text="📱 Web App Xarita", callback_data="map_webapp")],  # YANGI
            [InlineKeyboardButton(text="📊 Statistika", callback_data="map_stats")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "map_webapp")
async def open_webapp_map(callback: CallbackQuery):
    """Web App xaritani ochish - mijozlar joylashuvi bilan"""
    if not is_admin(callback.from_user.id):
        return
    
    # Mijozlarning geolokatsiya ma'lumotlarini olish
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT 
                g.uid, g.model, g.power_kw, g.status as gen_status,
                c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                c.region, c.city,
                d.status as deal_status, d.sale_price,
                e.full_name as seller_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            WHERE c.geo_lat IS NOT NULL 
              AND c.geo_lon IS NOT NULL
              AND c.is_approved = TRUE
            ORDER BY c.region, c.city
        ''')
    
    if not rows:
        await callback.answer("❌ Geolokatsiya ma'lumotlari mavjud bo'lgan mijozlar topilmadi!", show_alert=True)
        return
    
    # Ma'lumotlarni JSON formatga o'tkazish
    locations = []
    for row in rows:
        # Statusni aniqlash
        status = row['deal_status'] or row['gen_status'] or 'SOTILDI'
        
        # Manzilni yaratish
        address_parts = []
        if row['region']:
            address_parts.append(row['region'])
        if row['city'] and row['city'] != row['region']:
            address_parts.append(row['city'])
        if row['address']:
            address_parts.append(row['address'])
        
        full_address = ', '.join(address_parts) if address_parts else 'Noma\'lum'
        
        locations.append({
            'lat': float(row['geo_lat']),
            'lon': float(row['geo_lon']),
            'model': row['model'] or 'Noma\'lum',
            'power': row['power_kw'] or 0,
            'client_name': row['full_name'] or 'Noma\'lum',
            'phone': row['phone'] or 'Noma\'lum',
            'address': full_address,
            'status': status,
            'uid': row['uid'],
            'seller': row['seller_name'] or 'Noma\'lum'
        })
    
    # JSON ma'lumotlarni URL parameter sifatida yuborish
    import json
    data_json = json.dumps({'locations': locations})
    encoded_data = urllib.parse.quote(data_json)
    
    # Web App URL - bu sizning server manzilingiz
    # PRODUCTION uchun o'z domeningizni qo'ying
    
    # Yoki lokal test uchun (ngrok orqali):
    # WEBAPP_URL = "https://your-ngrok-url.ngrok.io/static/webapp_map.html"
    
    # URL ga ma'lumotlarni qo'shish
    full_url = f"{WEBAPP_URL}?data={encoded_data}"
    
    # Web App tugmasi bilan xabar yuborish
    await callback.message.answer(
        f"🗺 <b>Web App Xarita</b>\n\n"
        f"📍 Jami obyektlar: <b>{len(locations)} ta</b>\n\n"
        f"Quyidagi tugma orqali xaritani oching. "
        f"Barcha mijozlar joylashuvi real vaqt rejimida ko'rsatilgan:\n\n"
        f"✅ O'rnatilgan - Yashil\n"
        f"🔧 O'rnatilmoqda - Sariq\n"  
        f"🚚 Yetkazilmoqda - Ko'k\n"
        f"💰 Sotilgan - Siyohrang",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🗺 XARITANI OCHISH", 
                web_app=WebAppInfo(url=full_url)
            )],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_map")]
        ]),
        parse_mode="HTML"
    )
    
    await callback.answer()

@dp.message(F.web_app_data)
async def handle_webapp_data(message: Message):
    """Web App dan kelgan ma'lumotlarni qabul qilish"""
    try:
        data = json.loads(message.web_app_data.data)
        
        if data.get('action') == 'get_map_data':
            # Bu yerda qo'shimcha ma'lumotlar yuborish mumkin
            await message.answer("✅ Xarita ma'lumotlari yangilandi!")
        else:
            await message.answer(f"📊 Web App ma'lumotlari:\n<pre>{json.dumps(data, indent=2, ensure_ascii=False)}</pre>")
            
    except Exception as e:
        logger.error(f"WebApp data xatolik: {e}")
        await message.answer("❌ Ma'lumotlarni qayta ishlashda xatolik")

@dp.callback_query(F.data == "map_internal")
async def internal_map(callback: CallbackQuery):
    """Ichki karta - to'liq ma'lumotlar"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   d.status as deal_status, d.seller_id,
                   e.full_name as seller_name,
                   i.installer_id, emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
            ORDER BY g.status, c.region, c.city
        ''')
    
    if not rows:
        await callback.answer("Lokatsiyalar topilmadi!", show_alert=True)
        return
    
    # Filtrlar
    await callback.message.edit_text(
        f"🏢 <b>Ichki karta</b> ({len(rows)} ta obyekt)\n\n"
        f"Filtrlash:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ O'rnatilganlar", callback_data="map_filter_installed")],
            [InlineKeyboardButton(text="🔧 O'rnatilmoqda", callback_data="map_filter_installing")],
            [InlineKeyboardButton(text="🚚 Yetkazilmoqda", callback_data="map_filter_delivery")],
            [InlineKeyboardButton(text="⚡ Quvvat bo'yicha", callback_data="map_filter_power")],
            [InlineKeyboardButton(text="👤 Sotuvchi bo'yicha", callback_data="map_filter_seller")],
            [InlineKeyboardButton(text="📍 Barchasini ko'rish", callback_data="map_show_all_internal")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_map")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("map_filter_"))
async def map_filters(callback: CallbackQuery):
    filter_type = callback.data.split("_")[2]
    
    async with db_pool.acquire() as conn:
        base_query = '''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   c.region, c.city,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
        '''
        
        # Default title
        title = "📍 Barcha obyektlar"
        
        if filter_type == 'installed':
            base_query += " AND g.status = 'INSTALLED'"
            title = "✅ O'rnatilganlar"
        elif filter_type == 'installing':
            base_query += " AND d.status = 'INSTALLING'"
            title = "🔧 O'rnatilmoqda"
        elif filter_type == 'delivery':
            base_query += " AND d.status = 'IN_LOGISTICS'"
            title = "🚚 Yetkazilmoqda"
        elif filter_type == 'power':
            # Quvvat bo'yicha guruhlash
            rows = await conn.fetch(base_query + " ORDER BY g.power_kw")
            power_groups = {}
            for row in rows:
                power = row['power_kw']
                if power <= 50:
                    group = "0-50 kVA"
                elif power <= 100:
                    group = "51-100 kVA"
                elif power <= 200:
                    group = "101-200 kVA"
                else:
                    group = "200+ kVA"
                power_groups.setdefault(group, []).append(row)
            
            buttons = []
            for group, items in sorted(power_groups.items()):
                buttons.append([InlineKeyboardButton(
                    text=f"⚡ {group} ({len(items)} ta)",
                    callback_data=f"mappower_{group.replace(' ', '_')}"
                )])
            buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="map_internal")])
            
            await callback.message.edit_text(
                "⚡ <b>Quvvat bo'yicha filtr:</b>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                parse_mode="HTML"
            )
            return
        elif filter_type == 'seller':
            # Sotuvchi bo'yicha
            rows = await conn.fetch(base_query + " ORDER BY e.full_name")
            seller_groups = {}
            for row in rows:
                seller = row['seller_name'] or "Nomalum"
                seller_groups.setdefault(seller, []).append(row)
            
            buttons = []
            for seller, items in sorted(seller_groups.items()):
                buttons.append([InlineKeyboardButton(
                    text=f"👤 {seller} ({len(items)} ta)",
                    callback_data=f"mapseller_{seller.replace(' ', '_')}"
                )])
            buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="map_internal")])
            
            await callback.message.edit_text(
                "👤 <b>Sotuvchi bo'yicha filtr:</b>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
                parse_mode="HTML"
            )
            return
        
        rows = await conn.fetch(base_query + " ORDER BY c.region, c.city")
    
    if not rows:
        await callback.answer("Ma'lumotlar yo'q!", show_alert=True)
        return
    
    # Ko'rsatish
    await show_map_points(callback, rows, title, detailed=True)

async def show_map_points(callback: CallbackQuery, rows: list, title: str, detailed: bool = True):
    """Xarita nuqtalarini ko'rsatish"""
    
    text = f"🗺 <b>{title}</b> ({len(rows)} ta)\n\n"
    
    # Statistika
    regions = {}
    for row in rows:
        region = row.get('region') or "Noma'lum"
        regions[region] = regions.get(region, 0) + 1
    
    text += "<b>📍 Viloyatlar bo'yicha:</b>\n"
    for region, count in sorted(regions.items()):
        text += f"• {region}: {count} ta\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    
    # Har bir nuqta alohida xabar
    for row in rows[:20]:  # Faqat 20 ta
        status_icon = {
            'INSTALLED': '✅',
            'INSTALLING': '🔧',
            'DELIVERY': '🚚',
            'SKLADDA': '📦'
        }.get(row['status'], '📍')
        
        # Backslash muammosini oldini olish - o'zgaruvchilarga saqlash
        lat = row.get('geo_lat')
        lon = row.get('geo_lon')
        maps_link = f"https://maps.google.com/?q={lat},{lon}"
        
        # Default qiymatlar
        unknown = "Noma'lum"
        
        if detailed:
            # Ichki karta - to'liq ma'lumot
            address = row.get('address') or row.get('city') or unknown
            seller_name = row.get('seller_name') or unknown
            
            point_text = (
                f"{status_icon} <b>{row['model']}</b> ({row['power_kw']}kVA)\n"
                f"🆔 {row['uid']}\n"
                f"👤 {row['full_name']}\n"
                f"📱 {row['phone']}\n"
                f"📍 {address}\n"
                f"🧑‍💼 Sotuvchi: {seller_name}\n"
            )
            
            installer = row.get('installer_name')
            if installer:
                point_text += f"🔧 Montajchi: {installer}\n"
        else:
            # Portfoliya - qisqa
            region_val = row.get('region', '')
            city_val = row.get('city', '')
            location = f"{region_val}, {city_val}".strip(', ')
            
            point_text = (
                f"{status_icon} <b>{row['model']}</b> ({row['power_kw']}kVA)\n"
                f"📍 {location or 'Uzbekistan'}\n"
            )
        
        buttons = [[InlineKeyboardButton(
            text="📍 Xaritada ko'rish",
            url=maps_link
        )]]
        
        if detailed:
            uid = row.get('uid')
            if uid:
                buttons[0].append(InlineKeyboardButton(
                    text="📋 Batafsil",
                    callback_data=f"mapdetail_{uid}"
                ))
        
        await callback.message.answer(
            point_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("mappower_"))
async def map_power_filter(callback: CallbackQuery):
    """Quvvat bo'yicha filtrlangan natijalarni ko'rsatish"""
    if not is_admin(callback.from_user.id):
        return
    
    # Guruh nomini olish (masalan: "0-50_kVA" -> "0-50 kVA")
    group = callback.data.replace("mappower_", "").replace("_", " ")
    
    await callback.answer(f"⚡ {group} yuklanmoqda...")
    
    # Quvvat oralig'ini aniqlash
    if "0-50" in group:
        min_power, max_power = 0, 50
    elif "51-100" in group:
        min_power, max_power = 51, 100
    elif "101-200" in group:
        min_power, max_power = 101, 200
    else:  # 200+
        min_power, max_power = 201, 999999
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   c.region, c.city,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
            AND g.power_kw >= $1 AND g.power_kw <= $2
            ORDER BY g.power_kw, c.region, c.city
        ''', min_power, max_power)
    
    if not rows:
        await callback.answer("Bu quvvat oralig'ida generatorlar yo'q!", show_alert=True)
        return
    
    title = f"⚡ {group} ({len(rows)} ta)"
    await show_map_points(callback, rows, title, detailed=True)

@dp.callback_query(F.data.startswith("mappower_"))
async def map_power_group(callback: CallbackQuery):
    """Quvvat guruhini tanlaganda natijalarni ko'rsatish"""
    if not is_admin(callback.from_user.id):
        return
    
    group_data = callback.data.replace("mappower_", "")
    
    # Quvvat oralig'ini aniqlash
    power_ranges = {
        "0-50_kVA": (0, 50),
        "51-100_kVA": (51, 100),
        "101-200_kVA": (101, 200),
        "200+_kVA": (201, 999999)
    }
    
    min_power, max_power = power_ranges.get(group_data, (0, 999999))
    group_name = group_data.replace("_", " ")
    
    await callback.answer(f"⚡ {group_name} yuklanmoqda...")
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   c.region, c.city,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
            AND g.power_kw >= $1 AND g.power_kw <= $2
            ORDER BY g.power_kw DESC, c.region, c.city
        ''', min_power, max_power)
    
    if not rows:
        await callback.message.edit_text(
            f"⚡ <b>{group_name}</b>\n\n"
            f"Bu quvvat oralig'ida generatorlar topilmadi.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data="map_filter_power")]
            ]),
            parse_mode="HTML"
        )
        return
    
    title = f"⚡ {group_name} ({len(rows)} ta)"
    await show_map_points(callback, rows, title, detailed=True)

@dp.callback_query(F.data.startswith("mapseller_"))
async def map_seller_filter(callback: CallbackQuery):
    """Sotuvchi bo'yicha filtrlangan natijalarni ko'rsatish"""
    if not is_admin(callback.from_user.id):
        return
    
    seller = callback.data.replace("mapseller_", "").replace("_", " ")
    
    await callback.answer(f"👤 {seller} yuklanmoqda...")
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   c.region, c.city,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
            AND (e.full_name = $1 OR ($1 = 'Nomalum' AND e.full_name IS NULL))
            ORDER BY c.region, c.city
        ''', seller if seller != "Nomalum" else None)
    
    if not rows:
        await callback.answer("Bu sotuvchida generatorlar yo'q!", show_alert=True)
        return
    
    title = f"👤 {seller} ({len(rows)} ta)"
    await show_map_points(callback, rows, title, detailed=True)







@dp.callback_query(F.data.startswith("download_portfolio_"))
async def download_portfolio_file(callback: CallbackQuery):
    """Portfolio faylini qayta yuborish"""
    filename = callback.data.replace("download_portfolio_", "")
    file_path = f"{UPLOAD_DIR}/{filename}"
    
    if os.path.exists(file_path):
        await callback.message.answer_document(
            FSInputFile(file_path),
            caption="🗺 Portfoliya xaritasi"
        )
    else:
        await callback.answer("Fayl topilmadi!", show_alert=True)


@dp.callback_query(F.data == "admin_map")
async def admin_map(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        # Barcha o'rnatilgan generatorlar
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, c.full_name, c.geo_lat, c.geo_lon,
                   d.status, e.full_name as seller_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE c.geo_lat IS NOT NULL AND g.status IN ('INSTALLED', 'INSTALLING')
        ''')
    
    if not rows:
        await callback.answer("Lokatsiya ma'lumotlari yo'q", show_alert=True)
        return
    
    text = f"🗺 <b>Generatorlar xaritada ({len(rows)} ta):</b>\n\n"
    
    # Statistika
    installed = len([r for r in rows if r['status'] == 'INSTALLED'])
    installing = len([r for r in rows if r['status'] == 'INSTALLING'])
    
    text += f"✅ O'rnatilgan: {installed} ta\n"
    text += f"🔧 O'rnatilmoqda: {installing} ta\n\n"
    
    # Filtrlar
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📍 Barcha lokatsiyalar", callback_data="map_all")],
            [InlineKeyboardButton(text="🔧 Faqat o'rnatilayotganlar", callback_data="map_installing")],
            [InlineKeyboardButton(text="✅ Faqat o'rnatilganlar", callback_data="map_installed")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("map_"))
async def show_map_details(callback: CallbackQuery):
    filter_type = callback.data.split("_")[1]
    
    async with db_pool.acquire() as conn:
        query = '''
            SELECT g.uid, g.model, g.power_kw, c.full_name, c.phone,
                   c.geo_lat, c.geo_lon, d.status
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            WHERE c.geo_lat IS NOT NULL
        '''
        
        if filter_type == 'installing':
            query += " AND d.status = 'INSTALLING'"
        elif filter_type == 'installed':
            query += " AND d.status = 'INSTALLED'"
        
        rows = await conn.fetch(query)
    
    if not rows:
        await callback.answer("Ma'lumotlar yo'q", show_alert=True)
        return
    
    # Xarita linklarini yuborish
    for row in rows[:10]:  # Faqat 10 tasini ko'rsatish
        status_icon = "🔧" if row['status'] == 'INSTALLING' else "✅"
        maps_link = f"https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}"
        
        text = (f"{status_icon} <b>{row['model']}</b> ({row['power_kw']}kVA)\n"
                f"👤 {row['full_name']}\n"
                f"📱 {row['phone']}\n"
                f"🆔 {row['uid']}")
        
        await callback.message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📍 Xaritada ko'rish", url=maps_link)]
            ]),
            parse_mode="HTML"
        )

# ============ REPORTS MODULE ============

@dp.callback_query(F.data == "admin_reports")
async def admin_reports(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    await callback.message.edit_text(
        "📊 <b>Hisobotlar</b>\n\nKerakli hisobotni tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📈 Umumiy statistika", callback_data="rep_general")],
            [InlineKeyboardButton(text="💰 Moliyaviy hisobot (Foyda/Marja)", callback_data="rep_profit_margin")],
            [InlineKeyboardButton(text="📊 UID bo'yicha hisobot", callback_data="rep_by_uid")],
            [InlineKeyboardButton(text="👤 Sotuvchilar hisoboti", callback_data="rep_sellers")],
            [InlineKeyboardButton(text="🔧 Montajchilar hisoboti", callback_data="rep_installers")],
            [InlineKeyboardButton(text="🚚 Logistika hisoboti", callback_data="rep_logistics")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "rep_profit_margin")
async def profit_margin_report(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        # Oxirgi 30 kun
        monthly = await conn.fetchrow('''
            SELECT 
                COUNT(*) as deals,
                SUM(d.sale_price) as sales,
                SUM(g.purchase_price) as costs,
                SUM(d.delivery_cost) as delivery,
                SUM(d.installation_cost) as install,
                SUM(d.other_costs) as other,
                SUM(d.profit) as profit,
                AVG(d.profit_margin) as avg_margin
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.status = 'COMPLETED'
            AND d.completed_at > CURRENT_DATE - INTERVAL '30 days'
        ''')
        
        # Barcha vaqt
        all_time = await conn.fetchrow('''
            SELECT 
                COUNT(*) as deals,
                SUM(d.profit) as profit,
                AVG(d.profit_margin) as avg_margin
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            WHERE d.status = 'COMPLETED'
        ''')
        
        # Eng foydali bitimlar
        top_deals = await conn.fetch('''
            SELECT d.id, g.uid, g.model, d.sale_price, d.profit, d.profit_margin,
                   c.full_name as client, e.full_name as seller
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE d.status = 'COMPLETED'
            ORDER BY d.profit DESC
            LIMIT 5
        ''')
    
    text = (f"💰 <b>Foyda va Marja hisoboti</b>\n\n"
            f"<b>📅 Oxirgi 30 kun:</b>\n"
            f"Bitimlar: {monthly['deals'] or 0} ta\n"
            f"Sotuv: {monthly['sales'] or 0:,.0f} so'm\n"
            f"Xarajatlar:\n"
            f"  - Sotib olish: {monthly['costs'] or 0:,.0f}\n"
            f"  - Yetkazish: {monthly['delivery'] or 0:,.0f}\n"
            f"  - O'rnatish: {monthly['install'] or 0:,.0f}\n"
            f"  - Boshqa: {monthly['other'] or 0:,.0f}\n"
            f"<b>💵 Sof foyda: {monthly['profit'] or 0:,.0f} so'm</b>\n"
            f"<b>📊 O'rtacha marja: {monthly['avg_margin'] or 0:.1f}%</b>\n\n"
            f"<b>🏆 Barcha vaqt:</b>\n"
            f"Jami bitimlar: {all_time['deals'] or 0}\n"
            f"Jami foyda: {all_time['profit'] or 0:,.0f} so'm\n"
            f"O'rtacha marja: {all_time['avg_margin'] or 0:.1f}%\n\n"
            f"<b>🥇 TOP 5 foydali bitim:</b>\n")
    
    for i, deal in enumerate(top_deals, 1):
        text += (f"{i}. #{deal['id']} - {deal['profit']:,.0f} so'm "
                f"({deal['profit_margin']:.1f}%) - {deal['seller']}\n")
    
    await callback.message.edit_text(
        text, 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Excel yuklash", callback_data="export_profit")],
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_reports")]
        ]),
        parse_mode="HTML"
    )

class ReportByUID(StatesGroup):
    entering_uid = State()

@dp.callback_query(F.data == "rep_by_uid")
async def report_by_uid_start(callback: CallbackQuery, state: FSMContext):
    """UID bo'yicha hisobot - boshlash"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    # Callbackni javob qilish (loading holatini o'chirish)
    await callback.answer()
    
    # Yangi xabar yuborish (eski xabar qoladi yoki o'chiriladi)
    await callback.message.answer(
        "🆔 <b>UID bo'yicha hisobot</b>\n\n"
        "Generator UID sini kiriting (masalan: GEN-2024-1234):",
        parse_mode="HTML"
    )
    await state.set_state(ReportByUID.entering_uid)

@dp.message(ReportByUID.entering_uid)
async def process_uid_report(message: Message, state: FSMContext):
    """UID qabul qilish va to'liq hisobot ko'rsatish"""
    uid = message.text.strip().upper()
    
    # UID formatini tekshirish
    if not uid.startswith("GEN-"):
        await message.answer(
            "❌ <b>Noto'g'ri UID format!</b>\n\n"
            "Format: GEN-YYYY-XXXX (masalan: GEN-2024-1234)\n"
            "Qayta kiriting:",
            parse_mode="HTML"
        )
        return
    
    await bot.send_chat_action(message.chat.id, "typing")
    
    async with db_pool.acquire() as conn:
        # Generator ma'lumotlari
        gen = await conn.fetchrow('''
            SELECT g.*, 
                   c.full_name as client_name, 
                   c.phone as client_phone,
                   c.address as client_address,
                   c.region as client_region,
                   c.city as client_city,
                   c.geo_lat, c.geo_lon,
                   d.id as deal_id, 
                   d.sale_price, 
                   d.delivery_cost, 
                   d.installation_cost, 
                   d.other_costs, 
                   d.profit, 
                   d.profit_margin,
                   d.created_at as deal_date, 
                   d.completed_at,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   l.vehicle_info, 
                   l.driver_name, 
                   l.driver_phone,
                   l.delivery_cost as log_cost,
                   l.planned_date as delivery_date,
                   l.actual_date as actual_delivery_date,
                   l.status as log_status,
                   i.id as installation_id,
                   i.installation_date, 
                   i.motor_hours_start, 
                   i.motor_hours_current,
                   i.photos as install_photos,
                   i.videos as install_videos,
                   i.act_signed,
                   i.act_file_path,
                   i.notes as install_notes,
                   emp.full_name as installer_name,
                   emp.phone as installer_phone
            FROM generators g
            LEFT JOIN clients c ON g.current_client_id = c.id
            LEFT JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN logistics l ON d.id = l.deal_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE g.uid = $1
        ''', uid)
        
        if not gen:
            await message.answer(
                "❌ <b>Generator topilmadi!</b>\n\n"
                f"UID: <code>{uid}</code>\n\n"
                "Iltimos, to'g'ri UID kiriting.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Qayta urinish", callback_data="rep_by_uid")],
                    [InlineKeyboardButton(text="◀️ Hisobotlarga", callback_data="admin_reports")]
                ]),
                parse_mode="HTML"
            )
            await state.clear()
            return
        
        # To'lovlar
        payments = await conn.fetch('''
            SELECT p.*, 
                   s.full_name as seller_name,
                   a.full_name as accountant_name
            FROM payments p
            LEFT JOIN employees s ON p.seller_id = s.telegram_id
            LEFT JOIN employees a ON p.accountant_id = a.telegram_id
            WHERE p.deal_id = $1
            ORDER BY p.created_at DESC
        ''', gen['deal_id'] or 0)
        
        # Servis tarixi
        services = await conn.fetch('''
            SELECT sh.*, e.full_name as performer_name
            FROM service_history sh
            LEFT JOIN employees e ON sh.performed_by = e.telegram_id
            WHERE sh.generator_uid = $1
            ORDER BY sh.date DESC
        ''', uid)
        
        # Fayllar jadvalidan
        other_files = await conn.fetch('''
            SELECT * FROM files 
            WHERE entity_id = $1 OR entity_id = $2
            ORDER BY uploaded_at DESC
        ''', uid, str(gen['deal_id'] or ''))
    
    # ===== MA'LUMOTLARNI TAYYORLASH =====
    
    # Generator rasmlari (yaratishda)
    gen_photos = gen['photos'] or []
    
    # Generator hujjatlari
    gen_docs = []
    if gen['documents']:
        try:
            gen_docs = json.loads(gen['documents']) if isinstance(gen['documents'], str) else gen['documents']
        except:
            gen_docs = []
    
    # O'rnatish rasmlari
    install_photos = gen['install_photos'] or []
    
    # O'rnatish videolari
    install_videos = gen['install_videos'] or []
    
    # Akt fayli
    act_file = gen['act_file_path']
    
    # QR kod
    qr_path = gen['qr_code_path'] or f"{UPLOAD_DIR}/qrcodes/{uid}.png"
    
    # Fayllar sonini hisoblash
    total_photos = len([p for p in gen_photos if os.path.exists(p)]) + len([p for p in install_photos if os.path.exists(p)])
    total_videos = len([v for v in install_videos if os.path.exists(v)])
    total_docs = len([d for d in gen_docs if os.path.exists(d.get('path', ''))]) + (1 if act_file and os.path.exists(act_file) else 0) + len([f for f in other_files if f['file_path'] and os.path.exists(f['file_path'])])
    
    # ===== ASOSIY MA'LUMOTLAR =====
    
    text = f"📊 <b>UID: {uid}</b>\n\n"
    
    # Texnik ma'lumotlar
    model = gen['model'] or "Nomalum"
    power = gen['power_kw'] or 0
    serial = gen['serial_number'] or "Nomalum"
    manufacturer = gen['manufacturer'] or "Nomalum"
    year = gen['manufacture_year'] or "Nomalum"
    status = GEN_STATUSES.get(gen['status'], gen['status'] or "Nomalum")
    
    text += (f"<b>🔧 Texnik ma'lumotlar:</b>\n"
             f"Model: {model} ({power}kVA)\n"
             f"Seriya: {serial}\n"
             f"Ishlab chiqaruvchi: {manufacturer}\n"
             f"Yil: {year}\n"
             f"Status: {status}\n\n")
    
    # Moliyaviy ma'lumotlar
    purchase = gen['purchase_price'] or 0
    sale = gen['sale_price'] or 0
    delivery = gen['delivery_cost'] or 0
    install_cost = gen['installation_cost'] or 0
    other = gen['other_costs'] or 0
    profit = gen['profit'] or 0
    margin = gen['profit_margin'] or 0
    
    text += (f"<b>💰 Moliyaviy ma'lumotlar:</b>\n"
             f"Sotib olish: {purchase:,.0f} so'm\n")
    
    if sale > 0:
        total_cost = purchase + delivery + install_cost + other
        text += (f"Sotuv: {sale:,.0f} so'm\n"
                f"Yetkazish: {delivery:,.0f} so'm\n"
                f"O'rnatish: {install_cost:,.0f} so'm\n"
                f"Boshqa: {other:,.0f} so'm\n"
                f"<b>Jami xarajat: {total_cost:,.0f} so'm</b>\n"
                f"<b>Foyda: {profit:,.0f} so'm ({margin:.1f}%)</b>\n\n")
    else:
        text += "<i>Sotuv ma'lumotlari yo'q</i>\n\n"
    
    # Mijoz ma'lumotlari
    if gen['client_name']:
        client = gen['client_name']
        phone = gen['client_phone'] or "Nomalum"
        address = gen['client_address'] or "Nomalum"
        region = gen['client_region'] or ""
        city = gen['client_city'] or ""
        location = f"{region}, {city}".strip(", ")
        seller = gen['seller_name'] or "Nomalum"
        
        maps_link = ""
        if gen['geo_lat'] and gen['geo_lon']:
            maps_link = f"\n📍 <a href='https://maps.google.com/?q={gen['geo_lat']},{gen['geo_lon']}'>Xaritada ko'rish</a>"
        
        text += (f"<b>👤 Mijoz ma'lumotlari:</b>\n"
                f"Ism: {client}\n"
                f"Telefon: {phone}\n"
                f"Manzil: {address}\n"
                f"Hudud: {location or 'Nomalum'}{maps_link}\n"
                f"Sotuvchi: {seller}\n\n")
    
    # Yetkazish ma'lumotlari
    if gen['vehicle_info']:
        vehicle = gen['vehicle_info']
        driver = gen['driver_name'] or "Nomalum"
        driver_phone = gen['driver_phone'] or "Nomalum"
        planned = gen['delivery_date'].strftime('%d.%m.%Y') if gen['delivery_date'] else 'Nomalum'
        actual = gen['actual_delivery_date'].strftime('%d.%m.%Y') if gen['actual_delivery_date'] else 'Nomalum'
        log_cost = gen['log_cost'] or 0
        
        text += (f"<b>🚚 Yetkazish ma'lumotlari:</b>\n"
                f"Mashina: {vehicle}\n"
                f"Haydovchi: {driver} ({driver_phone})\n"
                f"Reja: {planned} | Haqiqiy: {actual}\n"
                f"Narxi: {log_cost:,.0f} so'm\n\n")
    
    # O'rnatish ma'lumotlari
    if gen['installer_name']:
        installer = gen['installer_name']
        installer_phone = gen['installer_phone'] or "Nomalum"
        install_date = gen['installation_date'].strftime('%d.%m.%Y %H:%M') if gen['installation_date'] else 'Nomalum'
        motor_start = gen['motor_hours_start'] or 0
        motor_current = gen['motor_hours_current'] or motor_start
        worked = motor_current - motor_start
        act_status = "✅ Imzolangan" if gen['act_signed'] else "❌ Imzolanmagan"
        notes = gen['install_notes'] or "Yoq"
        
        text += (f"<b>🔧 O'rnatish ma'lumotlari:</b>\n"
                f"Montajchi: {installer} ({installer_phone})\n"
                f"Sana: {install_date}\n"
                f"Moto-soat: {motor_start} → {motor_current} ({worked} soat)\n"
                f"Akt: {act_status}\n"
                f"Izoh: {notes}\n\n")
    
    # Garantiya
    if gen['warranty_start_date'] and gen['warranty_months']:
        end_date = gen['warranty_start_date'] + timedelta(days=30 * gen['warranty_months'])
        remaining = (end_date - datetime.now().date()).days
        warranty_status = f"{remaining} kun qoldi" if remaining > 0 else "⛔ TUGAGAN"
        
        text += (f"<b>🛡 Garantiya:</b>\n"
                f"Muddat: {gen['warranty_months']} oy\n"
                f"Boshlangan: {gen['warranty_start_date'].strftime('%d.%m.%Y')}\n"
                f"Tugaydi: {end_date.strftime('%d.%m.%Y')} ({warranty_status})\n\n")
    
    # To'lovlar
    if payments:
        text += f"<b>💰 To'lovlar ({len(payments)} ta):</b>\n"
        for p in payments[:5]:
            date = p['created_at'].strftime('%d.%m.%Y') if p['created_at'] else 'Nomalum'
            amount = p['amount'] or 0
            method = p['payment_method'] or "Nomalum"
            seller_conf = "✅" if p['seller_confirmed'] else "⏳"
            acc_conf = "✅" if p['accountant_confirmed'] else "⏳"
            text += f"• {date}: {amount:,.0f} so'm ({method}) S:{seller_conf} B:{acc_conf}\n"
        if len(payments) > 5:
            text += f"... va {len(payments) - 5} ta boshqa\n"
        text += "\n"
    
    # Servis tarixi
    if services:
        text += f"<b>🔧 Servis tarixi ({len(services)} ta):</b>\n"
        for s in services[:5]:
            date = s['date'].strftime('%d.%m.%Y') if s['date'] else 'Nomalum'
            serv_type = s['service_type'] or "Nomalum"
            desc = s['description'][:30] if s['description'] else "Yoq"
            cost = s['cost'] or 0
            text += f"• {date}: {serv_type} - {desc}... ({cost:,.0f} so'm)\n"
        if len(services) > 5:
            text += f"... va {len(services) - 5} ta boshqa\n"
        text += "\n"
    
    # Fayllar statistikasi
    text += f"<b>📁 Fayllar:</b>\n"
    text += f"🖼 Rasmlar: {total_photos} ta\n"
    text += f"🎥 Videolar: {total_videos} ta\n"
    text += f"📄 Hujjatlar: {total_docs} ta\n"
    text += f"📱 QR Kod: {'✅' if os.path.exists(qr_path) else '❌'}\n"
    
    # ===== TUGMALAR =====
    
    buttons = []
    
    # Rasmlar tugmasi
    if total_photos > 0:
        buttons.append([InlineKeyboardButton(
            text=f"🖼 Rasmlarni ko'rish ({total_photos} ta)",
            callback_data=f"uid_photos_{uid}"
        )])
    
    # Videolar tugmasi
    if total_videos > 0:
        buttons.append([InlineKeyboardButton(
            text=f"🎥 Videolarni ko'rish ({total_videos} ta)",
            callback_data=f"uid_videos_{uid}"
        )])
    
    # Hujjatlar tugmasi
    if total_docs > 0:
        buttons.append([InlineKeyboardButton(
            text=f"📄 Hujjatlarni ko'rish ({total_docs} ta)",
            callback_data=f"uid_docs_{uid}"
        )])
    
    # QR kod tugmasi
    if os.path.exists(qr_path):
        buttons.append([InlineKeyboardButton(
            text="📱 QR Kodni ko'rish",
            callback_data=f"uid_qr_{uid}"
        )])
    
    # Akt tugmasi (agar alohida ko'rsatmoqchi bo'lsangiz)
    if act_file and os.path.exists(act_file):
        buttons.append([InlineKeyboardButton(
            text="📋 Aktni ko'rish",
            callback_data=f"uid_act_{uid}"
        )])
    
    # Servis tarixi tugmasi
    if services:
        buttons.append([InlineKeyboardButton(
            text=f"🔧 Servis tarixi ({len(services)} ta)",
            callback_data=f"uid_services_{uid}"
        )])
    
    # To'lovlar tugmasi
    if payments:
        buttons.append([InlineKeyboardButton(
            text=f"💰 To'lovlar ({len(payments)} ta)",
            callback_data=f"uid_payments_{uid}"
        )])
    
    # Yangi fayl yuklash
    buttons.append([InlineKeyboardButton(
        text="⬆️ Yangi fayl yuklash",
        callback_data=f"upload_to_gen_{uid}"
    )])
    
    # Navigatsiya
    buttons.append([
        InlineKeyboardButton(text="🔄 Boshqa UID", callback_data="rep_by_uid"),
        InlineKeyboardButton(text="◀️ Hisobotlarga", callback_data="admin_reports")
    ])
    
    # ===== XABARNI YUBORISH =====
    
    # Xabar uzunligini tekshirish
    if len(text) > 4000:
        # Birinchi qism
        await message.answer(text[:4000], parse_mode="HTML")
        # Qolgan qism va tugmalar
        await message.answer(
            text[4000:],
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
    else:
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )
    
    await state.clear()


@dp.callback_query(F.data.startswith("uid_photos_"))
async def uid_view_photos(callback: CallbackQuery):
    """UID dan rasmlarni ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("uid_photos_", "")
    
    await callback.answer("🖼 Rasmlar yuklanmoqda...")
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.photos, i.photos as install_photos
            FROM generators g
            LEFT JOIN deals d ON g.uid = d.generator_uid
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE g.uid = $1
        ''', uid)
        
        gen_photos = gen['photos'] or []
        install_photos = gen['install_photos'] or []
    
    all_photos = []
    
    # Generator rasmlari
    for i, photo in enumerate(gen_photos):
        if isinstance(photo, str) and os.path.exists(photo):
            all_photos.append(("Generator yaratish", photo, i+1))
    
    # O'rnatish rasmlari
    for i, photo in enumerate(install_photos):
        if isinstance(photo, str) and os.path.exists(photo):
            all_photos.append(("O'rnatish", photo, i+1))
    
    if not all_photos:
        await callback.message.answer(
            "❌ Rasmlar topilmadi!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"rep_by_uid")]
            ])
        )
        return
    
    await callback.message.answer(
        f"🖼 <b>{uid} - Rasmlar ({len(all_photos)} ta)</b>",
        parse_mode="HTML"
    )
    
    for label, path, num in all_photos:
        try:
            await callback.message.answer_photo(
                FSInputFile(path),
                caption=f"{label} rasmi #{num}"
            )
        except Exception as e:
            await callback.message.answer(f"❌ Rasm yuklanmadi: {path[:50]}...")
    
    # Orqaga tugma
    await callback.message.answer(
        "📋",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Hisobotga qaytish", callback_data=f"uid_back_{uid}")]
        ])
    )

@dp.callback_query(F.data.startswith("uid_videos_"))
async def uid_view_videos(callback: CallbackQuery):
    """UID dan videolarni ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("uid_videos_", "")
    
    await callback.answer("🎥 Videolar yuklanmoqda...")
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT i.videos
            FROM generators g
            LEFT JOIN deals d ON g.uid = d.generator_uid
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE g.uid = $1
        ''', uid)
        
        videos = gen['videos'] or []
    
    valid_videos = [v for v in videos if isinstance(v, str) and os.path.exists(v)]
    
    if not valid_videos:
        await callback.message.answer(
            "❌ Videolar topilmadi!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"rep_by_uid")]
            ])
        )
        return
    
    await callback.message.answer(
        f"🎥 <b>{uid} - Videolar ({len(valid_videos)} ta)</b>",
        parse_mode="HTML"
    )
    
    for i, video in enumerate(valid_videos):
        try:
            await callback.message.answer_video(
                FSInputFile(video),
                caption=f"O'rnatish video #{i+1}"
            )
        except Exception as e:
            await callback.message.answer(f"❌ Video yuklanmadi: {video[:50]}...")
    
    await callback.message.answer(
        "📋",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Hisobotga qaytish", callback_data=f"uid_back_{uid}")]
        ])
    )

@dp.callback_query(F.data.startswith("uid_docs_"))
async def uid_view_docs(callback: CallbackQuery):
    """UID dan hujjatlarni ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("uid_docs_", "")
    
    await callback.answer("📄 Hujjatlar yuklanmoqda...")
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.documents, g.qr_code_path, i.act_file_path, d.id as deal_id
            FROM generators g
            LEFT JOIN deals d ON g.uid = d.generator_uid
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE g.uid = $1
        ''', uid)
        
        # Generator hujjatlari
        gen_docs = []
        if gen['documents']:
            try:
                docs = json.loads(gen['documents']) if isinstance(gen['documents'], str) else gen['documents']
                gen_docs = [d for d in docs if isinstance(d, dict) and os.path.exists(d.get('path', ''))]
            except:
                gen_docs = []
        
        # Akt
        act_file = gen['act_file_path']
        if act_file and os.path.exists(act_file):
            gen_docs.append({'name': 'Akt (imzolangan)', 'path': act_file})
        
        # QR kod
        qr = gen['qr_code_path'] or f"{UPLOAD_DIR}/qrcodes/{uid}.png"
        if os.path.exists(qr):
            gen_docs.append({'name': 'QR Kod', 'path': qr})
        
        # Boshqa fayllar
        other_files = await conn.fetch('''
            SELECT * FROM files WHERE entity_id = $1 ORDER BY uploaded_at DESC
        ''', uid)
        
        for f in other_files:
            if f['file_path'] and os.path.exists(f['file_path']):
                gen_docs.append({'name': f['file_name'] or f['file_type'] or 'Fayl', 'path': f['file_path']})
    
    if not gen_docs:
        await callback.message.answer(
            "❌ Hujjatlar topilmadi!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"rep_by_uid")]
            ])
        )
        return
    
    await callback.message.answer(
        f"📄 <b>{uid} - Hujjatlar ({len(gen_docs)} ta)</b>",
        parse_mode="HTML"
    )
    
    for doc in gen_docs:
        name = doc.get('name', 'Hujjat')
        path = doc.get('path', '')
        try:
            await callback.message.answer_document(
                FSInputFile(path),
                caption=name
            )
        except Exception as e:
            await callback.message.answer(f"❌ {name} yuklanmadi")
    
    await callback.message.answer(
        "📋",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Hisobotga qaytish", callback_data=f"uid_back_{uid}")]
        ])
    )

@dp.callback_query(F.data.startswith("uid_qr_"))
async def uid_view_qr(callback: CallbackQuery):
    """QR kodni ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("uid_qr_", "")
    
    qr_path = f"{UPLOAD_DIR}/qrcodes/{uid}.png"
    
    if not os.path.exists(qr_path):
        await callback.answer("QR kod topilmadi!", show_alert=True)
        return
    
    await callback.message.answer_photo(
        FSInputFile(qr_path),
        caption=f"📱 <b>{uid}</b> uchun QR kod",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"uid_back_{uid}")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("uid_act_"))
async def uid_view_act(callback: CallbackQuery):
    """Akt faylini ko'rish"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("uid_act_", "")
    
    async with db_pool.acquire() as conn:
        act_path = await conn.fetchval('''
            SELECT i.act_file_path
            FROM generators g
            LEFT JOIN deals d ON g.uid = d.generator_uid
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE g.uid = $1
        ''', uid)
    
    if not act_path or not os.path.exists(act_path):
        await callback.answer("Akt topilmadi!", show_alert=True)
        return
    
    await callback.message.answer_document(
        FSInputFile(act_path),
        caption=f"📋 <b>{uid}</b> - O'rnatish akti",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"uid_back_{uid}")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("uid_back_"))
async def uid_back_to_report(callback: CallbackQuery):
    """Hisobotga qaytish"""
    uid = callback.data.replace("uid_back_", "")
    
    # Yangi hisobot ko'rsatish
    await callback.message.answer(
        f"🔄 <b>{uid}</b> hisiboti yangilanmoqda...",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🖼 Rasmlar", callback_data=f"uid_photos_{uid}")],
            [InlineKeyboardButton(text="🎥 Videolar", callback_data=f"uid_videos_{uid}")],
            [InlineKeyboardButton(text="📄 Hujjatlar", callback_data=f"uid_docs_{uid}")],
            [InlineKeyboardButton(text="◀️ Boshqa UID", callback_data="rep_by_uid")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("admin_gen_report_"))
async def admin_gen_report_simple(callback: CallbackQuery):
    """Soddaroq batafsil hisobot"""
    if not is_admin(callback.from_user.id):
        return
    
    uid = callback.data.replace("admin_gen_report_", "")
    
    await callback.answer("📊 Hisobot tayyorlanmoqda...")
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.*, c.full_name as client_name, c.phone as client_phone,
                   d.sale_price, d.profit, d.profit_margin, d.status as deal_status,
                   e.full_name as seller_name
            FROM generators g
            LEFT JOIN clients c ON g.current_client_id = c.id
            LEFT JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            WHERE g.uid = $1
        ''', uid)
    
    if not gen:
        await callback.message.answer("❌ Generator topilmadi!")
        return
    
    profit = gen['profit'] or 0
    margin = gen['profit_margin'] or 0
    sale = gen['sale_price'] or 0
    purchase = gen['purchase_price'] or 0
    
    # O'zgaruvchilarga saqlash (backslash muammosini oldini olish)
    model = gen['model'] or "Nomalum"
    power = gen['power_kw'] or 0
    client = gen['client_name'] or "Nomalum"
    phone = gen['client_phone'] or "Nomalum"
    seller = gen['seller_name'] or "Nomalum"
    
    text = (f"📊 <b>{uid} - Batafsil</b>\n\n"
            f"🔧 {model} ({power}kVA)\n"
            f"👤 Mijoz: {client}\n"
            f"📱 {phone}\n"
            f"🧑‍💼 Sotuvchi: {seller}\n\n"
            f"💰 <b>Moliyaviy:</b>\n"
            f"Sotib olish: {purchase:,.0f} so'm\n"
            f"Sotuv: {sale:,.0f} so'm\n"
            f"<b>Foyda: {profit:,.0f} so'm ({margin:.1f}%)</b>")
    
    buttons = [
        [InlineKeyboardButton(text="📁 Fayllar", callback_data=f"admin_gen_files_{uid}")],
        [InlineKeyboardButton(text="◀️ Orqaga", callback_data="rep_by_uid")]
    ]
    
    await callback.message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "rep_general")
async def general_report(callback: CallbackQuery):
    async with db_pool.acquire() as conn:
        # Umumiy statistika
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_gens,
                COUNT(CASE WHEN status = 'SKLADDA' THEN 1 END) as in_stock,
                COUNT(CASE WHEN status = 'INSTALLED' THEN 1 END) as installed,
                COUNT(CASE WHEN status = 'SERVICING' THEN 1 END) as in_service
            FROM generators
        ''')
        
        deals_stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_deals,
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'PENDING_PAYMENT' THEN 1 END) as pending,
                SUM(sale_price) as total_sales,
                SUM(profit) as total_profit
            FROM deals
            WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
        ''')
    
    text = (f"📊 <b>Umumiy statistika</b>\n\n"
            f"<b>Generatorlar:</b>\n"
            f"📦 Jami: {stats['total_gens']} ta\n"
            f"📦 Skladda: {stats['in_stock']} ta\n"
            f"✅ O'rnatilgan: {stats['installed']} ta\n"
            f"🔧 Servisda: {stats['in_service']} ta\n\n"
            f"<b>Oxirgi 30 kun (Bitimlar):</b>\n"
            f"💼 Jami: {deals_stats['total_deals']} ta\n"
            f"✅ Tugallangan: {deals_stats['completed']} ta\n"
            f"⏳ Kutilmoqda: {deals_stats['pending']} ta\n"
            f"💰 Sotuv: {deals_stats['total_sales'] or 0:,.0f} so'm\n"
            f"📈 Foyda: {deals_stats['total_profit'] or 0:,.0f} so'm")
    
    await callback.message.edit_text(text, reply_markup=admin_main_keyboard(), parse_mode="HTML")



# ============ CORRECTION REQUESTS MODULE - TO'LIQ ============

class CorrectionRequest(StatesGroup):
    selecting_entity = State()
    entering_entity_id = State()
    selecting_field = State()
    entering_new_value = State()
    entering_reason = State()
    confirming = State()

@dp.callback_query(F.data == "request_correction")
async def start_correction_request(callback: CallbackQuery, state: FSMContext):
    """Xodim o'zgarish so'rovini yaratish - TUGMALAR bilan"""
    user_id = callback.from_user.id
    role = await get_user_role(user_id)
    
    if role == 'admin':
        await callback.answer("Adminlar so'rov yaratmaydi, to'g'ridan-to'g'ri o'zgartiradi!", show_alert=True)
        return
    
    # Rolga qarab boshlang'ich menyuni ko'rsatish
    if role == 'sotuvchi':
        # Sotuvchi uchun - faqat o'z bitimlari va mijozlari
        await show_seller_correction_menu(callback, user_id, state)
    elif role == 'logist':
        await show_logist_correction_menu(callback, user_id, state)
    elif role == 'montajchi':
        await show_installer_correction_menu(callback, user_id, state)
    else:
        # Boshqa rollar uchun umumiy
        await callback.message.edit_text(
            "📝 <b>So'rov yaratish</b>\n\n"
            "Qaysi jadval uchun?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📋 Bitim", callback_data="corrent_deal")],
                [InlineKeyboardButton(text="👤 Mijoz", callback_data="corrent_client")],
                [InlineKeyboardButton(text="🚚 Logistika", callback_data="corrent_logistics")],
                [InlineKeyboardButton(text="🔧 O'rnatish", callback_data="corrent_install")],
                [InlineKeyboardButton(text="◀️ Bekor qilish", callback_data="main_menu")]
            ]),
            parse_mode="HTML"
        )
        await state.set_state(CorrectionRequest.selecting_entity)


async def show_seller_correction_menu(callback: CallbackQuery, user_id: int, state: FSMContext):
    """Sotuvchi uchun - o'z bitimlari va mijozlari tugmalar bilan"""
    await callback.message.edit_text(
        "📝 <b>Ma'lumotni to'g'rilash</b>\n\n"
        "Nimani to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Mening bitimlarim", callback_data="corrent_list_deals")],
            [InlineKeyboardButton(text="👤 Mening mijozlarim", callback_data="corrent_list_clients")],
            [InlineKeyboardButton(text="◀️ Bekor qilish", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )


async def show_logist_correction_menu(callback: CallbackQuery, user_id: int, state: FSMContext):
    """Logist uchun - o'z vazifalari tugmalar bilan"""
    await callback.message.edit_text(
        "📝 <b>Ma'lumotni to'g'rilash</b>\n\n"
        "Nimani to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚚 Mening yetkazishlarim", callback_data="corrent_list_logistics")],
            [InlineKeyboardButton(text="◀️ Bekor qilish", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )


async def show_installer_correction_menu(callback: CallbackQuery, user_id: int, state: FSMContext):
    """Montajchi uchun - o'z o'rnatishlari tugmalar bilan"""
    await callback.message.edit_text(
        "📝 <b>Ma'lumotni to'g'rilash</b>\n\n"
        "Nimani to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔧 Mening o'rnatishlarim", callback_data="corrent_list_installs")],
            [InlineKeyboardButton(text="◀️ Bekor qilish", callback_data="main_menu")]
        ]),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("corfield_"), CorrectionRequest.selecting_field)
async def select_field_for_correction(callback: CallbackQuery, state: FSMContext):
    """Maydon tanlandi - yangi qiymatni so'rash"""
    
    # ✅ TO'G'RI: "corfield_" dan keyingi HAMMA qismini olish
    # Masalan: "corfield_sale_price" -> "sale_price"
    # Masalan: "corfield_motor_hours_start" -> "motor_hours_start"
    field_name = callback.data[9:]  # "corfield_" = 9 ta belgi
    
    # Yoki replace bilan:
    # field_name = callback.data.replace("corfield_", "", 1)  # faqat birinchi occurrence
    
    data = await state.get_data()
    available_fields = data.get('available_fields', [])
    
    # Maydon nomini topish
    field_labels = dict(available_fields)
    field_label = field_labels.get(field_name, field_name)
    
    # DEBUG - log qilish
    logger.info(f"=== DEBUG select_field_for_correction ===")
    logger.info(f"Callback data: {callback.data}")
    logger.info(f"Extracted field_name: {field_name}")
    logger.info(f"Available fields: {available_fields}")
    
    # Joriy qiymatni olish
    entity_type = data['entity_type']
    entity_id = data['entity_id']
    
    async with db_pool.acquire() as conn:
        # TO'G'RILANGAN JADVAL VA ID USTUNI
        table_map = {
            'deal': ('deals', 'id'),
            'client': ('clients', 'id'),
            'logistics': ('logistics', 'id'),
            'install': ('installations', 'deal_id')
        }
        
        table, id_col = table_map.get(entity_type, (entity_type + 's', 'id'))
        
        # Tekshirish - bu obyektga ruxsat bormi?
        if entity_type == 'deal':
            row = await conn.fetchrow(
                f'SELECT * FROM {table} WHERE {id_col} = $1',
                int(entity_id)
            )
        elif entity_type == 'client':
            row = await conn.fetchrow(f'''
                SELECT c.* FROM {table} c
                JOIN deals d ON c.id = d.client_id 
                WHERE c.{id_col} = $1
                LIMIT 1
            ''', int(entity_id))
        elif entity_type == 'logistics':
            row = await conn.fetchrow(
                f'SELECT * FROM {table} WHERE {id_col} = $1',
                int(entity_id)
            )
        elif entity_type == 'install':
            row = await conn.fetchrow(
                f'SELECT * FROM {table} WHERE {id_col} = $1',
                int(entity_id)
            )
        else:
            await callback.answer("Noto'g'ri obyekt turi!", show_alert=True)
            return
        
        if not row:
            await callback.answer("Bu obyekt topilmadi!", show_alert=True)
            return
        
        # ✅ JORIY QIYMATNI ANIQ OLISH
        current_value = row.get(field_name)
        
        logger.info(f"Current value from DB: {current_value}")
        
        # None yoki bo'sh qiymatlarni tekshirish
        if current_value is None:
            current_display = "Noma'lum"
        elif isinstance(current_value, (int, float)):
            current_display = str(current_value)
        elif isinstance(current_value, datetime):
            current_display = current_value.strftime('%d.%m.%Y %H:%M')
        elif isinstance(current_value, date):
            current_display = current_value.strftime('%d.%m.%Y')
        else:
            current_display = str(current_value) if str(current_value).strip() else "Noma'lum"
    
    # State'ga saqlash
    await state.update_data(
        field_name=field_name,
        current_value=current_display,
        field_label=field_label
    )
    
    # O'zgaruvchilarga saqlash
    entity_info = (data.get('deal_info') or 
                   data.get('client_info') or 
                   data.get('log_info') or 
                   data.get('install_info') or 
                   f"ID: {entity_id}")
    
    await callback.message.edit_text(
        f"✏️ <b>{field_label} ni o'zgartirish</b>\n\n"
        f"📋 {entity_info}\n"
        f"↩️ Joriy qiymat: {current_display}\n\n"
        f"📝 <b>Yangi qiymatni kiriting:</b>",
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.entering_new_value)


# ========== YANGI QIYMAT KIRITISH ==========

@dp.message(CorrectionRequest.entering_new_value)
async def process_new_value(message: Message, state: FSMContext):
    """Yangi qiymatni qabul qilish"""
    proposed_value = message.text.strip()
    await state.update_data(proposed_value=proposed_value)
    
    await message.answer(
        "💬 <b>Sababni tushuntiring:</b>\n\n"
        "Nima uchun o'zgartirish kerak?\n"
        "Masalan: 'Mijoz bilan kelishildi', 'Xatolik ketdi' va h.k.",
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.entering_reason)


# ========== SABAB KIRITISH ==========

@dp.message(CorrectionRequest.entering_reason)
async def process_correction_reason(message: Message, state: FSMContext):
    """Sababni qabul qilish va tasdiqlash"""
    await state.update_data(reason=message.text.strip())
    data = await state.get_data()
    
    # O'zgaruvchilarga saqlash
    current_value = data.get('current_value') or "Noma'lum"
    field_label = data.get('field_label') or data.get('field_name', 'Noma\'lum')
    proposed_value = data['proposed_value']
    reason = data['reason']
    entity_type = data['entity_type']
    entity_id = data['entity_id']
    
    # Entity info ni olish
    entity_info = (data.get('deal_info') or 
                   data.get('client_info') or 
                   data.get('log_info') or 
                   data.get('install_info') or 
                   f"ID: {entity_id}")
    
    await message.answer(
        f"📋 <b>So'rovni tasdiqlang:</b>\n\n"
        f"📊 Jadval: {entity_type}\n"
        f"🆔 {entity_info}\n"
        f"📝 Maydon: {field_label}\n"
        f"↩️ Eski: {current_value}\n"
        f"↪️ Yangi: {proposed_value}\n"
        f"💬 Sabab: {reason}\n\n"
        f"✅ Adminga yuborilsinmi?",
        reply_markup=confirm_keyboard("cor_send", "main_menu"),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.confirming)


# ========== SO'ROVNI YUBORISH ==========

@dp.callback_query(F.data == "cor_send", CorrectionRequest.confirming)
async def send_correction_request(callback: CallbackQuery, state: FSMContext):
    """So'rovni adminga yuborish"""
    data = await state.get_data()
    user_id = callback.from_user.id
    
    try:
        # ✅ ENTITY_ID ni har doim STRING saqlash
        entity_id = str(data['entity_id'])
        
        # ✅ JORIY QIYMATNI ANIQ OLIB KETISH (None bo'lsa "Noma'lum")
        current_value = data.get('current_value')
        if current_value is None or current_value == '':
            current_display = "Noma'lum"
        else:
            current_display = str(current_value)
        
        async with db_pool.acquire() as conn:
            req_id = await conn.fetchval('''
                INSERT INTO correction_requests 
                (requested_by, entity_type, entity_id, field_name, 
                 current_value, proposed_value, reason, status, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', CURRENT_TIMESTAMP)
                RETURNING id
            ''', 
                user_id, 
                data['entity_type'], 
                entity_id,
                data['field_name'], 
                current_display,  # ✅ ANIQ QIYMAT
                data['proposed_value'], 
                data['reason']
            )
            
            # Audit log
            await log_action(user_id, 'CREATE', 'correction_requests', req_id,
                           new_data={'field': data['field_name'], 
                                    'old_value': current_display,
                                    'new_value': data['proposed_value']},
                           role=await get_user_role(user_id))
        
        # Adminlarga xabar
        field_label = data.get('field_label') or data['field_name']
        entity_info = (data.get('deal_info') or 
                      data.get('client_info') or 
                      data.get('log_info') or 
                      data.get('install_info') or 
                      f"ID: {entity_id}")
        
        await notify_admins(
            f"⚠️ <b>Yangi so'rov!</b>\n\n"
            f"#{req_id}\n"
            f"👤 Xodim: {user_id}\n"
            f"📋 {data['entity_type']} - {entity_info}\n"
            f"📝 {field_label}: {current_display} → {data['proposed_value']}\n"  # ✅ TO'G'RI KO'RSATISH
            f"💬 Sabab: {data['reason']}"
        )
        
        await callback.message.edit_text(
            "✅ <b>So'rov yuborildi!</b>\n\n"
            f"Raqami: #{req_id}\n"
            "Admin tasdiqlagandan so'ng o'zgarish amalga oshiriladi.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
            ])
        )
        
    except Exception as e:
        logger.error(f"So'rov yuborishda xatolik: {e}")
        await callback.message.edit_text(
            f"❌ Xatolik: {str(e)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
            ])
        )
    
    await state.clear()

# ========== SOTUVCHI BITIMLAR RO'YXATI ==========

@dp.callback_query(F.data == "corrent_list_deals")
async def list_seller_deals_for_correction(callback: CallbackQuery, state: FSMContext):
    """Sotuvchining bitimlarini tugmalar bilan ko'rsatish"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        deals = await conn.fetch('''
            SELECT d.id, d.sale_price, d.status, 
                   g.model, g.power_kw, g.uid,
                   c.full_name as client_name, c.phone as client_phone
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            WHERE d.seller_id = $1
            ORDER BY d.created_at DESC
            LIMIT 20
        ''', user_id)
    
    if not deals:
        await callback.answer("Sizda bitimlar yo'q!", show_alert=True)
        return
    
    buttons = []
    for deal in deals:
        status_icon = {
            'PENDING_PAYMENT': '⏳',
            'PAID_SELLER_CONFIRM': '💰',
            'PAID_ACCOUNTANT_CONFIRM': '✅',
            'IN_LOGISTICS': '🚚',
            'INSTALLING': '🔧',
            'COMPLETED': '🎉'
        }.get(deal['status'], '❓')
        
        btn_text = f"{status_icon} #{deal['id']} - {deal['client_name'][:15]} ({deal['model'][:15]})"
        
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"cordeal_{deal['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="request_correction")])
    
    await callback.message.edit_text(
        f"📋 <b>Sizning bitimlaringiz ({len(deals)} ta)</b>\n\n"
        f"To'g'rilamoqchi bo'lgan bitimni tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("cordeal_"))
async def select_deal_for_correction(callback: CallbackQuery, state: FSMContext):
    """Bitim tanlandi - qaysi maydon to'g'rilanishini so'rash"""
    deal_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        deal = await conn.fetchrow('''
            SELECT d.*, g.model, c.full_name 
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            WHERE d.id = $1 AND d.seller_id = $2
        ''', deal_id, user_id)
        
        if not deal:
            await callback.answer("❌ Bu bitim sizga tegishli emas!", show_alert=True)
            return
    
    # ✅ INTEGER sifatida saqlash
    await state.update_data(
        entity_type='deal',
        entity_id=deal_id,
        deal_info=f"#{deal_id} - {deal['full_name']} ({deal['model']})"
    )
    
    # ✅ TO'G'RI MAYDON NOMLARI (prefiksiz!)
    fields = [
        ('sale_price', '💰 Sotuv narxi'),
        ('delivery_cost', '🚚 Yetkazish narxi'),
        ('installation_cost', '🔧 O\'rnatish narxi'),
        ('other_costs', '📦 Boshqa xarajatlar'),
        ('notes', '📝 Izohlar')
    ]
    
    buttons = []
    for key, name in fields:
        buttons.append([InlineKeyboardButton(
            text=name,
            callback_data=f"corfield_{key}"  # Masalan: "corfield_sale_price"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="corrent_list_deals")])
    
    await callback.message.edit_text(
        f"📋 <b>Bitim: #{deal_id}</b>\n"
        f"👤 {deal['full_name']}\n"
        f"🔧 {deal['model']}\n\n"
        f"Qaysi maydonni to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.selecting_field)


# ========== SOTUVCHI MIJOZLAR RO'YXATI ==========

@dp.callback_query(F.data == "corrent_list_clients")
async def list_seller_clients_for_correction(callback: CallbackQuery, state: FSMContext):
    """Sotuvchining mijozlarini tugmalar bilan ko'rsatish"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        clients = await conn.fetch('''
            SELECT DISTINCT c.id, c.full_name, c.phone, c.company, c.address
            FROM clients c
            JOIN deals d ON c.id = d.client_id
            WHERE d.seller_id = $1 AND c.is_approved = TRUE
            ORDER BY c.full_name
            LIMIT 20
        ''', user_id)
    
    if not clients:
        await callback.answer("Sizda mijozlar yo'q!", show_alert=True)
        return
    
    buttons = []
    for client in clients:
        company = f" ({client['company']})" if client['company'] else ""
        btn_text = f"👤 {client['full_name'][:20]}{company}"
        
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"corclient_{client['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="request_correction")])
    
    await callback.message.edit_text(
        f"👥 <b>Sizning mijozlaringiz ({len(clients)} ta)</b>\n\n"
        f"To'g'rilamoqchi bo'lgan mijozni tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("corclient_"))
async def select_client_for_correction(callback: CallbackQuery, state: FSMContext):
    """Mijoz tanlandi - qaysi maydon to'g'rilanishini so'rash"""
    client_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        client = await conn.fetchrow('''
            SELECT c.* FROM clients c
            JOIN deals d ON c.id = d.client_id
            WHERE c.id = $1 AND d.seller_id = $2
            LIMIT 1
        ''', client_id, user_id)
        
        if not client:
            await callback.answer("❌ Bu mijoz sizga tegishli emas!", show_alert=True)
            return
    
    # ✅ INTEGER sifatida saqlash
    await state.update_data(
        entity_type='client',
        entity_id=client_id,
        client_info=client['full_name']
    )
    
    # ✅ TO'G'RI MAYDON NOMLARI
    fields = [
        ('full_name', '👤 Ism'),
        ('phone', '📱 Telefon'),
        ('address', '📍 Manzil'),
        ('company', '🏢 Kompaniya')
    ]
    
    buttons = []
    for key, name in fields:
        buttons.append([InlineKeyboardButton(
            text=name,
            callback_data=f"corfield_{key}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="corrent_list_clients")])
    
    await callback.message.edit_text(
        f"👤 <b>Mijoz: {client['full_name']}</b>\n"
        f"📱 {client['phone']}\n\n"
        f"Qaysi maydonni to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.selecting_field)


# ========== LOGIST VAZIFALARI ==========

@dp.callback_query(F.data == "corrent_list_logistics")
async def list_logist_tasks_for_correction(callback: CallbackQuery, state: FSMContext):
    """Logistning vazifalarini tugmalar bilan ko'rsatish"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        tasks = await conn.fetch('''
            SELECT l.id, l.vehicle_info, l.driver_name, l.planned_date, l.status,
                   c.full_name as client_name, g.model
            FROM logistics l
            JOIN deals d ON l.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE l.logist_id = $1
            ORDER BY l.planned_date DESC
            LIMIT 20
        ''', user_id)
    
    if not tasks:
        await callback.answer("Sizda vazifalar yo'q!", show_alert=True)
        return
    
    buttons = []
    for task in tasks:
        status_icon = {
            'PLANNED': '🟡',
            'IN_TRANSIT': '🟢',
            'DELIVERED': '✅'
        }.get(task['status'], '❓')
        
        date_str = task['planned_date'].strftime('%d.%m') if task['planned_date'] else 'N/A'
        btn_text = f"{status_icon} #{task['id']} - {task['client_name'][:15]} ({date_str})"
        
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"corlog_{task['id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="request_correction")])
    
    await callback.message.edit_text(
        f"🚚 <b>Sizning vazifalaringiz ({len(tasks)} ta)</b>\n\n"
        f"To'g'rilamoqchi bo'lgan vazifani tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("corlog_"))
async def select_logistics_for_correction(callback: CallbackQuery, state: FSMContext):
    """Logistika vazifasi tanlandi"""
    log_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        task = await conn.fetchrow('''
            SELECT l.*, c.full_name, g.model 
            FROM logistics l
            JOIN deals d ON l.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE l.id = $1 AND l.logist_id = $2
        ''', log_id, user_id)
        
        if not task:
            await callback.answer("❌ Bu vazifa sizga tegishli emas!", show_alert=True)
            return
    
    # ✅ INTEGER sifatida saqlash
    await state.update_data(
        entity_type='logistics',
        entity_id=log_id,
        log_info=f"#{log_id} - {task['full_name']}"
    )
    
    # ✅ TO'G'RI MAYDON NOMLARI
    fields = [
        ('vehicle_info', '🚛 Mashina ma\'lumotlari'),
        ('driver_name', '👤 Haydovchi ismi'),
        ('driver_phone', '📱 Haydovchi telefoni'),
        ('delivery_cost', '💵 Yetkazish narxi'),
        ('planned_date', '📅 Rejalashtirilgan sana')
    ]
    
    buttons = []
    for key, name in fields:
        buttons.append([InlineKeyboardButton(
            text=name,
            callback_data=f"corfield_{key}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="corrent_list_logistics")])
    
    await callback.message.edit_text(
        f"🚚 <b>Vazifa: #{log_id}</b>\n"
        f"👤 {task['full_name']}\n"
        f"🔧 {task['model']}\n\n"
        f"Qaysi maydonni to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.selecting_field)


# ========== MONTAJCHI VAZIFALARI ==========

@dp.callback_query(F.data == "corrent_list_installs")
async def list_installer_tasks_for_correction(callback: CallbackQuery, state: FSMContext):
    """Montajchining vazifalarini tugmalar bilan ko'rsatish"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        tasks = await conn.fetch('''
            SELECT i.id, i.deal_id, i.installation_date, i.motor_hours_start, i.motor_hours_current,
                   c.full_name as client_name, g.model, g.power_kw
            FROM installations i
            JOIN deals d ON i.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE i.installer_id = $1
            ORDER BY i.installation_date DESC
            LIMIT 20
        ''', user_id)
    
    if not tasks:
        await callback.answer("Sizda vazifalar yo'q!", show_alert=True)
        return
    
    buttons = []
    for task in tasks:
        date_str = task['installation_date'].strftime('%d.%m') if task['installation_date'] else 'N/A'
        btn_text = f"🔧 #{task['deal_id']} - {task['client_name'][:15]} ({task['model'][:15]})"
        
        buttons.append([InlineKeyboardButton(
            text=btn_text,
            callback_data=f"corinst_{task['deal_id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="request_correction")])
    
    await callback.message.edit_text(
        f"🔧 <b>Sizning vazifalaringiz ({len(tasks)} ta)</b>\n\n"
        f"To'g'rilamoqchi bo'lgan vazifani tanlang:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("corinst_"))
async def select_install_for_correction(callback: CallbackQuery, state: FSMContext):
    """O'rnatish vazifasi tanlandi"""
    deal_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        task = await conn.fetchrow('''
            SELECT i.*, c.full_name, g.model 
            FROM installations i
            JOIN deals d ON i.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE i.deal_id = $1 AND i.installer_id = $2
        ''', deal_id, user_id)
        
        if not task:
            await callback.answer("❌ Bu vazifa sizga tegishli emas!", show_alert=True)
            return
    
    # ✅ INTEGER sifatida saqlash
    await state.update_data(
        entity_type='install',
        entity_id=deal_id,  # installations jadvalida deal_id bo'yicha qidiriladi
        install_info=f"#{deal_id} - {task['full_name']}"
    )
    
    # ✅ TO'G'RI MAYDON NOMLARI (installations jadvalidagi ustunlar!)
    fields = [
        ('motor_hours_start', '⏱ Boshlang\'ich moto-soat'),
        ('motor_hours_current', '⏱ Joriy moto-soat'),
        ('notes', '📝 Izohlar')
    ]
    
    buttons = []
    for key, name in fields:
        buttons.append([InlineKeyboardButton(
            text=name,
            callback_data=f"corfield_{key}"
        )])
    
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="corrent_list_installs")])
    
    await callback.message.edit_text(
        f"🔧 <b>O'rnatish: #{deal_id}</b>\n"
        f"👤 {task['full_name']}\n"
        f"🔧 {task['model']}\n\n"
        f"Qaysi maydonni to'g'rilamoqchisiz?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.selecting_field)

@dp.callback_query(F.data.startswith("corrent_"))
async def select_correction_entity(callback: CallbackQuery, state: FSMContext):
    entity_type = callback.data.split("_")[1]
    await state.update_data(entity_type=entity_type)
    
    type_names = {
        'deal': 'Bitim ID',
        'client': 'Mijoz ID', 
        'logistics': 'Logistika ID',
        'install': 'O\'rnatish ID'
    }
    
    await callback.message.edit_text(
        f"🆔 <b>{type_names.get(entity_type)} ni kiriting:</b>",
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.entering_entity_id)

@dp.message(CorrectionRequest.entering_entity_id)
async def process_correction_id(message: Message, state: FSMContext):
    entity_id = message.text.strip()
    data = await state.get_data()
    user_id = message.from_user.id
    
    # Tekshirish - bu obyektga ruxsat bormi?
    async with db_pool.acquire() as conn:
        if data['entity_type'] == 'deal':
            row = await conn.fetchrow(
                'SELECT * FROM deals WHERE id = $1 AND seller_id = $2',
                int(entity_id), user_id
            )
            if not row:
                await message.answer("❌ Bu bitim sizga tegishli emas!")
                await state.clear()
                return
            
            fields = [
                ('sale_price', 'Sotuv narxi'),
                ('delivery_cost', 'Yetkazish narxi'),
                ('installation_cost', 'O\'rnatish narxi'),
                ('notes', 'Izohlar')
            ]
        elif data['entity_type'] == 'client':
            # Sotuvchining mijozlarini tekshirish
            row = await conn.fetchrow('''
                SELECT c.* FROM clients c
                JOIN deals d ON c.id = d.client_id
                WHERE c.id = $1 AND d.seller_id = $2
            ''', int(entity_id), user_id)
            
            if not row:
                await message.answer("❌ Bu mijoz sizga tegishli emas!")
                await state.clear()
                return
            
            fields = [
                ('full_name', 'Ism'),
                ('phone', 'Telefon'),
                ('address', 'Manzil'),
                ('company', 'Kompaniya')
            ]
        elif data['entity_type'] == 'logistics':
            row = await conn.fetchrow(
                'SELECT * FROM logistics WHERE id = $1 AND logist_id = $2',
                int(entity_id), user_id
            )
            if not row:
                await message.answer("❌ Bu sizning vazifangiz emas!")
                await state.clear()
                return
            
            fields = [
                ('vehicle_info', 'Mashina ma\'lumotlari'),
                ('driver_name', 'Haydovchi ismi'),
                ('delivery_cost', 'Yetkazish narxi'),
                ('planned_date', 'Rejalashtirilgan sana')
            ]
        else:
            fields = []
    
    await state.update_data(entity_id=entity_id, available_fields=fields)
    
    # Maydonlarni ko'rsatish
    buttons = []
    for key, name in fields:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"corfield_{key}")])
    buttons.append([InlineKeyboardButton(text="◀️ Bekor qilish", callback_data="main_menu")])
    
    await message.answer(
        "📝 <b>Qaysi maydon o'zgartirilsin?</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.selecting_field)



@dp.message(CorrectionRequest.entering_new_value)
async def process_new_value(message: Message, state: FSMContext):
    await state.update_data(proposed_value=message.text.strip())
    
    await message.answer(
        "💬 <b>Sababni tushuntiring:</b>\n\n"
        "Nima uchun o'zgartirish kerak?",
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.entering_reason)

@dp.message(CorrectionRequest.entering_reason)
async def process_correction_reason(message: Message, state: FSMContext):
    await state.update_data(reason=message.text.strip())
    data = await state.get_data()
    
    field_labels = dict(data['available_fields'])
    
    # Backslash muammosini oldini olish
    current_value = data['current_value'] or "Noma'lum"
    field_label = field_labels.get(data['field_name']) or "Noma'lum"
    
    await message.answer(
        f"📋 <b>So'rovni tasdiqlang:</b>\n\n"
        f"📊 Jadval: {data['entity_type']}\n"
        f"🆔 ID: {data['entity_id']}\n"
        f"📝 Maydon: {field_label}\n"
        f"↩️ Eski: {current_value}\n"
        f"↪️ Yangi: {data['proposed_value']}\n"
        f"💬 Sabab: {data['reason']}\n\n"
        f"Yuborilsinmi?",
        reply_markup=confirm_keyboard("cor_send", "main_menu"),
        parse_mode="HTML"
    )
    await state.set_state(CorrectionRequest.confirming)


def main_menu_keyboard(user_id: int):
    """Foydalanuvchi roliga qarab to'g'ri menyuni qaytarish"""
    if is_admin(user_id):
        return admin_main_keyboard()
    
    # Boshqa rollar uchun...
    # (Bu funksiyani to'ldirish kerak)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="main_menu")]
    ])


# Admin tasdiqlash/rad etish (mavjud kodni yangilash)



@dp.callback_query(F.data == "menu_seller")
async def menu_seller(callback: CallbackQuery):
    """Sotuvchi menyusiga qaytish"""
    employee = await get_employee_by_telegram_id(callback.from_user.id)
    if employee:
        await show_role_menu(callback.message, 'sotuvchi', employee)
    else:
        await callback.answer("Xodim topilmadi!", show_alert=True)

@dp.callback_query(F.data == "menu_logistic")
async def menu_logistic(callback: CallbackQuery):
    """Logist menyusiga qaytish"""
    employee = await get_employee_by_telegram_id(callback.from_user.id)
    if employee:
        await show_role_menu(callback.message, 'logist', employee)
    else:
        await callback.answer("Xodim topilmadi!", show_alert=True)

@dp.callback_query(F.data == "menu_installer")
async def menu_installer(callback: CallbackQuery):
    """Montajchi menyusiga qaytish"""
    employee = await get_employee_by_telegram_id(callback.from_user.id)
    if employee:
        await show_role_menu(callback.message, 'montajchi', employee)
    else:
        await callback.answer("Xodim topilmadi!", show_alert=True)

@dp.callback_query(F.data == "menu_warehouse")
async def menu_warehouse(callback: CallbackQuery):
    """Ombor xodimi menyusiga qaytish"""
    employee = await get_employee_by_telegram_id(callback.from_user.id)
    if employee:
        await show_role_menu(callback.message, 'ombor', employee)
    else:
        await callback.answer("Xodim topilmadi!", show_alert=True)

@dp.callback_query(F.data == "wh_search")
async def warehouse_search_start(callback: CallbackQuery, state: FSMContext):
    """Sklad qidiruvi - boshlash"""
    role = await get_user_role(callback.from_user.id)
    if role not in ['ombor', 'admin']:
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🔍 <b>Qidirish</b>\n\n"
        "Quyidagilardan birini kiriting:\n"
        "• 🆔 UID (masalan: GEN-2024-1234)\n"
        "• 🔢 Seriya raqami\n"
        "• 🔧 Model nomi\n"
        "• ⚡ Quvvat (kVA)\n\n"
        "Qidirish so'zini kiriting:",
        parse_mode="HTML"
    )
    await state.set_state(WarehouseSearch.entering_query)

@dp.message(WarehouseSearch.entering_query)
async def process_warehouse_search(message: Message, state: FSMContext):
    """Sklad qidiruvi - natijalar"""
    query = message.text.strip()
    
    # Log
    logger.info(f"Ombor qidiruvi: user_id={message.from_user.id}, query='{query}'")
    
    try:
        async with db_pool.acquire() as conn:
            # TO'G'RILANGAN SQL - faqat 1 ta parametr
            rows = await conn.fetch('''
                SELECT * FROM generators 
                WHERE 
                    uid ILIKE $1 
                    OR model ILIKE $1 
                    OR serial_number ILIKE $1
                    OR CAST(power_kw AS TEXT) ILIKE $1
                    OR manufacturer ILIKE $1
                    OR status ILIKE $1
                ORDER BY 
                    CASE 
                        WHEN uid ILIKE $1 THEN 1
                        WHEN serial_number ILIKE $1 THEN 2
                        WHEN model ILIKE $1 THEN 3
                        ELSE 4
                    END,
                    created_at DESC
                LIMIT 20
            ''', f'%{query}%')
        
        if not rows:
            await message.answer(
                f"❌ <b>Hech narsa topilmadi!</b>\n\n"
                f"Qidiruv: '<code>{query}</code>'\n\n"
                f"Boshqa so'z bilan urinib ko'ring:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔍 Qayta qidirish", callback_data="wh_search")],
                    [InlineKeyboardButton(text="◀️ Ombor menyusi", callback_data="menu_warehouse")]
                ]),
                parse_mode="HTML"
            )
            await state.clear()
            return
        
        # Natijalar
        total_found = len(rows)
        
        text = f"🔍 <b>Natijalar: {total_found} ta topildi</b>\n\n"
        
        for i, row in enumerate(rows, 1):
            status = GEN_STATUSES.get(row['status'], row['status'])
            price = row['purchase_price'] or 0
            
            # O'zgaruvchilarga saqlash (backslash muammosini oldini olish)
            serial = row['serial_number'] or "Noma'lum"
            created_str = row['created_at'].strftime('%d.%m.%Y')
            
            text += (
                f"<b>{i}. 🆔 {row['uid']}</b>\n"
                f"   🔧 {row['model']} ({row['power_kw']}kVA)\n"
                f"   🔢 Seriya: {serial}\n"
                f"   📦 Status: {status}\n"
                f"   💵 Narx: {price:,.0f} so'm\n"
                f"   📅 Qo'shilgan: {created_str}\n"
                f"{'─' * 30}\n"
            )
        
        # Tugmalar
        buttons = [
            [InlineKeyboardButton(text="🔍 Yangi qidiruv", callback_data="wh_search")],
            [InlineKeyboardButton(text="📦 Sklad qoldig'i", callback_data="wh_inventory")],
            [InlineKeyboardButton(text="◀️ Asosiy menyu", callback_data="menu_warehouse")]
        ]
        
        await message.answer(
            text, 
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), 
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Ombor qidiruvida xatolik: {e}")
        await message.answer(
            f"❌ <b>Xatolik yuz berdi!</b>\n\n"
            f"Qayta urinib ko'ring:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Qayta qidirish", callback_data="wh_search")]
            ]),
            parse_mode="HTML"
        )
    
    await state.clear()

@dp.callback_query(F.data == "wh_report")
async def warehouse_report(callback: CallbackQuery):
    """Ombor hisoboti"""
    role = await get_user_role(callback.from_user.id)
    if role not in ['ombor', 'admin']:
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    async with db_pool.acquire() as conn:
        # Status bo'yicha
        by_status = await conn.fetch('''
            SELECT status, COUNT(*) as count, 
                   SUM(purchase_price) as total_value
            FROM generators
            GROUP BY status
        ''')
        
        # Model bo'yicha top 5
        top_models = await conn.fetch('''
            SELECT model, COUNT(*) as count
            FROM generators
            GROUP BY model
            ORDER BY count DESC
            LIMIT 5
        ''')
    
    text = "📊 <b>Ombor hisoboti</b>\n\n"
    
    text += "<b>📦 Status bo'yicha:</b>\n"
    for row in by_status:
        status_name = GEN_STATUSES.get(row['status'], row['status'])
        value = row['total_value'] or 0
        text += f"• {status_name}: {row['count']} ta ({value:,.0f} so'm)\n"
    
    text += f"\n<b>🔧 Top modellar:</b>\n"
    for i, row in enumerate(top_models, 1):
        text += f"{i}. {row['model']}: {row['count']} ta\n"
    
    await callback.message.edit_text(text, reply_markup=warehouse_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "wh_cancel")
async def warehouse_cancel(callback: CallbackQuery, state: FSMContext):
    """Ombor bekor qilish"""
    await state.clear()
    await callback.message.edit_text(
        "❌ Bekor qilindi.",
        reply_markup=warehouse_menu()
    )

@dp.callback_query(F.data == "back_admin")
async def back_to_admin(callback: CallbackQuery):
    """Admin menyusiga qaytish"""
    await callback.message.edit_text(
        "👨‍💼 <b>Admin paneli</b>",
        reply_markup=admin_main_keyboard(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "cl_warranty")
async def client_warranty(callback: CallbackQuery):
    """Mijozning garantiya ma'lumotlari"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.*, d.completed_at, i.installation_date
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            LEFT JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN installations i ON d.id = i.deal_id
            WHERE c.telegram_id = $1 AND g.status = 'INSTALLED'
            ORDER BY d.completed_at DESC
            LIMIT 1
        ''', user_id)
    
    if not gen:
        await callback.answer("Sizda o'rnatilgan generator yo'q!", show_alert=True)
        return
    
    # Garantiya hisoblash
    if gen['warranty_start_date'] and gen['warranty_months']:
        warranty_end = gen['warranty_start_date'] + timedelta(days=30*gen['warranty_months'])
        remaining = (warranty_end - datetime.now().date()).days
        
        if remaining > 0:
            warranty_status = f"✅ Faol ({remaining} kun qoldi)"
            warranty_emoji = "🟢"
        else:
            warranty_status = "⛔ TUGAGAN"
            warranty_emoji = "🔴"
        
        warranty_text = (f"{warranty_emoji} <b>Garantiya ma'lumotlari</b>\n\n"
                      f"🔧 Model: {gen['model']}\n"
                      f"🆔 UID: <code>{gen['uid']}</code>\n\n"
                      f"📅 Boshlangan: {gen['warranty_start_date'].strftime('%d.%m.%Y')}\n"
                      f"📅 Tugaydi: {warranty_end.strftime('%d.%m.%Y')}\n"
                      f"⏳ Muddat: {gen['warranty_months']} oy\n"
                      f"📊 Status: {warranty_status}\n\n")
        
        if remaining > 0:
            warranty_text += (f"🛠 <b>Garantiya doirasida:</b>\n"
                            f"• Bepul ta'mirlash\n"
                            f"• Bepul ehtiyot qismlar\n"
                            f"• Texnik xizmat ko'rsatish\n\n"
                            f"⚠️ Garantiya tugashidan 30 kun oldin sizga xabar yuboriladi.")
        else:
            warranty_text += (f"💰 <b>Garantiya tugagan</b>\n"
                            f"Servis xizmatlari pullik.\n"
                            f"📞 Batafsil ma'lumot uchun: +998XX XXX XX XX")
    else:
        warranty_text = "❌ Garantiya ma'lumotlari kiritilmagan!"
    
    await callback.message.edit_text(
        warranty_text,
        reply_markup=client_menu(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "cl_history")
async def client_service_history(callback: CallbackQuery):
    """Mijozning servis tarixi"""
    user_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Mijozning generatorlari
        gens = await conn.fetch('''
            SELECT uid, model FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            WHERE c.telegram_id = $1
        ''', user_id)
        
        if not gens:
            await callback.answer("Generator topilmadi!", show_alert=True)
            return
        
        # Servis tarixi
        services = await conn.fetch('''
            SELECT sh.*, g.model
            FROM service_history sh
            JOIN generators g ON sh.generator_uid = g.uid
            JOIN clients c ON g.current_client_id = c.id
            WHERE c.telegram_id = $1
            ORDER BY sh.date DESC
            LIMIT 10
        ''', user_id)
    
    if not services:
        await callback.message.edit_text(
            "📋 <b>Servis tarixi bo'sh</b>\n\n"
            "Hozircha servis xizmatlari ko'rsatilmagan.",
            reply_markup=client_menu(),
            parse_mode="HTML"
        )
        return
    
    text = "📋 <b>Sizning servis tarixingiz:</b>\n\n"
    
    for serv in services:
        date_str = serv['date'].strftime('%d.%m.%Y %H:%M') if serv['date'] else "Noma'lum"
        cost_str = f"{serv['cost']:,.0f} so'm" if serv['cost'] else "Bepul"
        
        text += (f"🔧 <b>{serv['model']}</b>\n"
                f"📅 {date_str}\n"
                f"📝 {serv['service_type']}\n"
                f"📄 {serv['description'] or 'Izoh yoq'}\n"
                f"💵 {cost_str}\n"
                f"{'─' * 30}\n")
    
    await callback.message.edit_text(text, reply_markup=client_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "cl_contact")
async def client_contact(callback: CallbackQuery):
    """Aloqa ma'lumotlari - Yangilangan"""
    await callback.message.edit_text(
        "📞 <b>Aloqa ma'lumotlari</b>\n\n"
        "🏢 <b>Core Energy</b>\n\n"
        "📱 <b>Telefon raqamlari:</b>\n"
        "• <code>+998 99 402 58 64</code>\n"
        "• <code>+998 77 362 65 55</code>\n\n"
        "💬 <b>Telegram:</b>\n"
        "• @Ilhom_gulamjanovich\n"
        "• @core_energy\n\n"
        "⏰ <b>Ish vaqti:</b>\n"
        "Du-Sha: 09:00 - 18:00\n\n"
        "🚨 <b>Tezkor yordam (24/7):</b>\n"
        "• <code>+998 99 402 58 64</code>",
        reply_markup=client_menu(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "acc_all_deals")
async def accountant_all_deals(callback: CallbackQuery):
    """Barcha bitimlar ro'yxati"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT d.*, c.full_name as client_name, g.model, g.power_kw,
                   e.full_name as seller_name
            FROM deals d
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            JOIN employees e ON d.seller_id = e.telegram_id
            ORDER BY d.created_at DESC
            LIMIT 20
        ''')
    
    if not rows:
        await callback.answer("Bitimlar yo'q!", show_alert=True)
        return
    
    text = "📋 <b>Barcha bitimlar (oxirgi 20 ta):</b>\n\n"
    
    for row in rows:
        status = DEAL_STATUSES.get(row['status'], row['status'])
        profit = row['profit'] or 0
        margin = row['profit_margin'] or 0
        
        text += (f"#{row['id']} - {status}\n"
                f"🔧 {row['model']} ({row['power_kw']}kVA)\n"
                f"👤 {row['client_name']}\n"
                f"💰 {row['sale_price']:,.0f} so'm\n"
                f"📈 Foyda: {profit:,.0f} ({margin:.1f}%)\n"
                f"🧑‍💼 {row['seller_name']}\n"
                f"{'─' * 30}\n")
    
    await callback.message.edit_text(
        text,
        reply_markup=accountant_menu(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "log_report")
async def logistic_report(callback: CallbackQuery):
    """Logist shaxsiy hisoboti"""
    logist_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as delivered,
                SUM(delivery_cost) as earnings
            FROM logistics
            WHERE logist_id = $1
            AND created_at > CURRENT_DATE - INTERVAL '30 days'
        ''', logist_id)
        
        recent = await conn.fetch('''
            SELECT l.*, c.full_name, g.model
            FROM logistics l
            JOIN deals d ON l.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE l.logist_id = $1
            ORDER BY l.created_at DESC
            LIMIT 5
        ''', logist_id)
    
    text = (f"📊 <b>Sizning hisobotingiz (oxirgi 30 kun)</b>\n\n"
            f"🚚 Jami yetkazish: {stats['total'] or 0} ta\n"
            f"✅ Yetkazilgan: {stats['delivered'] or 0} ta\n"
            f"💰 Daromad: {stats['earnings'] or 0:,.0f} so'm\n\n"
            f"<b>Oxirgi yetkazishlar:</b>\n")
    
    for r in recent:
        status = "✅" if r['status'] == 'DELIVERED' else "🚚"
        text += f"{status} {r['full_name']} - {r['model']}\n"
    
    await callback.message.edit_text(text, reply_markup=logistic_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "log_map")
async def logistic_map(callback: CallbackQuery):
    """Logist uchun xarita - faqat o'z vazifalari"""
    logist_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT l.deal_id, c.full_name, c.geo_lat, c.geo_lon,
                   c.address, g.model, l.status
            FROM logistics l
            JOIN deals d ON l.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            JOIN generators g ON d.generator_uid = g.uid
            WHERE l.logist_id = $1 
            AND l.status IN ('PLANNED', 'IN_TRANSIT')
            AND c.geo_lat IS NOT NULL
        ''', logist_id)
    
    if not rows:
        await callback.answer("Aktiv marshrutlar yo'q!", show_alert=True)
        return
    
    text = f"🗺 <b>Sizning marshrutlaringiz ({len(rows)} ta):</b>\n\n"
    
    for row in rows:
        status_icon = "🟡" if row['status'] == 'PLANNED' else "🟢"
        maps_link = f"https://maps.google.com/?q={row['geo_lat']},{row['geo_lon']}"
        
        text += (f"{status_icon} <b>#{row['deal_id']}</b>\n"
                f"👤 {row['full_name']}\n"
                f"🔧 {row['model']}\n"
                f"📍 {row['address']}\n"
                f"<a href='{maps_link}'>🗺 Xaritada ko'rish</a>\n\n")
    
    await callback.message.edit_text(
        text,
        reply_markup=logistic_menu(),
        parse_mode="HTML",
        disable_web_page_preview=True
    )

@dp.callback_query(F.data == "inst_upload")
async def installer_upload_act(callback: CallbackQuery, state: FSMContext):
    """Montajchi akt yuklash"""
    installer_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Tugallangan o'rnatishlarni topish
        installs = await conn.fetch('''
            SELECT i.id, i.deal_id, g.model, c.full_name
            FROM installations i
            JOIN deals d ON i.deal_id = d.id
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            WHERE i.installer_id = $1
            AND (i.act_file_path IS NULL OR i.act_file_path = '')
            ORDER BY i.installation_date DESC
        ''', installer_id)
    
    if not installs:
        await callback.answer("Yuklash uchun akt yo'q!", show_alert=True)
        return
    
    if len(installs) == 1:
        await state.update_data(installation_id=installs[0]['id'])
        await callback.message.edit_text(
            f"📸 <b>Akt yuklash</b>\n\n"
            f"Bitim: #{installs[0]['deal_id']}\n"
            f"Generator: {installs[0]['model']}\n\n"
            f"Akt faylini (PDF yoki rasm) yuboring:",
            parse_mode="HTML"
        )
        await state.set_state("installer_uploading_act")
    else:
        buttons = []
        for inst in installs:
            buttons.append([InlineKeyboardButton(
                text=f"#{inst['deal_id']} - {inst['model']}",
                callback_data=f"instupload_{inst['id']}"
            )])
        buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")])
        
        await callback.message.edit_text(
            "📋 <b>Qaysi bitim uchun akt yuklaysiz?</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("instupload_"))
async def select_install_for_upload(callback: CallbackQuery, state: FSMContext):
    installation_id = int(callback.data.split("_")[1])
    await state.update_data(installation_id=installation_id)
    
    await callback.message.edit_text(
        "📸 <b>Akt faylini yuboring</b> (PDF yoki rasm):",
        parse_mode="HTML"
    )
    await state.set_state("installer_uploading_act")

@dp.message(State("installer_uploading_act"), F.document | F.photo)
async def process_act_upload(message: Message, state: FSMContext):
    data = await state.get_data()
    
    # Faylni saqlash
    if message.document:
        file_obj = message.document
        file_type = "document"
        ext = file_obj.file_name.split('.')[-1]
    else:
        file_obj = message.photo[-1]
        file_type = "photo"
        ext = "jpg"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"act_{data['installation_id']}_{timestamp}.{ext}"
    
    file_path = await download_file(
        file_obj.file_id,
        "documents",
        filename,
        file_type
    )
    
    if not file_path:
        await message.answer("❌ Faylni yuklashda xatolik!")
        return
    
    # Bazaga saqlash
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE installations 
            SET act_file_path = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', file_path, data['installation_id'])
    
    await message.answer(
        "✅ <b>Akt muvaffaqiyatli yuklandi!</b>",
        reply_markup=installer_menu(),
        parse_mode="HTML"
    )
    await state.clear()

@dp.callback_query(F.data == "inst_my_works")
async def installer_my_works(callback: CallbackQuery):
    """Montajchi bajarilgan ishlar"""
    installer_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        works = await conn.fetch('''
            SELECT i.*, g.model, g.power_kw, c.full_name, c.city
            FROM installations i
            JOIN deals d ON i.deal_id = d.id
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            WHERE i.installer_id = $1
            ORDER BY i.installation_date DESC
            LIMIT 10
        ''', installer_id)
    
    if not works:
        await callback.answer("Hali ishlar yo'q!", show_alert=True)
        return
    
    text = "📋 <b>Sizning bajarilgan ishlaringiz:</b>\n\n"
    
    for w in works:
        date_str = w['installation_date'].strftime('%d.%m.%Y') if w['installation_date'] else "Noma'lum"
        hours = w['motor_hours_current'] or 0
        
        text += (f"🔧 <b>{w['model']}</b> ({w['power_kw']}kVA)\n"
                f"👤 {w['full_name']} ({w['city'] or 'N/A'})\n"
                f"📅 {date_str}\n"
                f"⏱ Moto-soat: {hours}\n"
                f"{'─' * 30}\n")
    
    await callback.message.edit_text(text, reply_markup=installer_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "inst_report")
async def installer_report(callback: CallbackQuery):
    """Montajchi hisoboti"""
    installer_id = callback.from_user.id
    
    async with db_pool.acquire() as conn:
        # Umumiy statistika
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_installs,
                COUNT(CASE WHEN installation_date > CURRENT_DATE - INTERVAL '30 days' 
                      THEN 1 END) as recent,
                AVG(motor_hours_current - motor_hours_start) as avg_hours,
                SUM(CASE WHEN act_signed THEN 1 ELSE 0 END) as signed_acts
            FROM installations
            WHERE installer_id = $1
        ''', installer_id)
        
        # Viloyat bo'yicha
        by_region = await conn.fetch('''
            SELECT c.region, COUNT(*) as count
            FROM installations i
            JOIN deals d ON i.deal_id = d.id
            JOIN clients c ON d.client_id = c.id
            WHERE i.installer_id = $1
            GROUP BY c.region
            ORDER BY count DESC
        ''', installer_id)
    
    total = stats['total_installs'] or 0
    recent = stats['recent'] or 0
    avg_hours = stats['avg_hours'] or 0
    signed = stats['signed_acts'] or 0
    
    text = (f"📊 <b>Sizning hisobotingiz</b>\n\n"
            f"🔧 Jami o'rnatish: {total} ta\n"
            f"📅 Oxirgi 30 kun: {recent} ta\n"
            f"⏱ O'rtacha moto-soat: {avg_hours:.0f}\n"
            f"📝 Akt imzolangan: {signed}/{total}\n\n")
    
    if by_region:
        text += "<b>📍 Viloyatlar bo'yicha:</b>\n"
        for r in by_region:
            region = r['region'] or "Noma'lum"
            text += f"• {region}: {r['count']} ta\n"
    
    await callback.message.edit_text(text, reply_markup=installer_menu(), parse_mode="HTML")

@dp.callback_query(F.data == "rep_sellers")
async def report_by_sellers(callback: CallbackQuery):
    """Sotuvchilar hisoboti"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        # Sotuvchilar statistikasi
        sellers = await conn.fetch('''
            SELECT 
                e.full_name,
                e.telegram_id,
                COUNT(d.id) as total_deals,
                COUNT(CASE WHEN d.status = 'COMPLETED' THEN 1 END) as completed,
                SUM(d.sale_price) as total_sales,
                SUM(d.profit) as total_profit,
                AVG(d.profit_margin) as avg_margin
            FROM employees e
            LEFT JOIN deals d ON e.telegram_id = d.seller_id
                AND d.created_at > CURRENT_DATE - INTERVAL '30 days'
            WHERE e.role = 'sotuvchi' AND e.is_active = TRUE
            GROUP BY e.full_name, e.telegram_id
            ORDER BY total_sales DESC NULLS LAST
        ''')
        
        # Oxirgi 7 kun dinamikasi
        daily = await conn.fetch('''
            SELECT 
                DATE(d.created_at) as date,
                COUNT(*) as deals,
                SUM(d.sale_price) as sales
            FROM deals d
            WHERE d.created_at > CURRENT_DATE - INTERVAL '7 days'
            GROUP BY DATE(d.created_at)
            ORDER BY date DESC
        ''')
    
    text = "👤 <b>Sotuvchilar hisoboti (oxirgi 30 kun)</b>\n\n"
    
    for i, s in enumerate(sellers, 1):
        name = s['full_name'] or f"ID: {s['telegram_id']}"
        sales = s['total_sales'] or 0
        profit = s['total_profit'] or 0
        margin = s['avg_margin'] or 0
        completed = s['completed'] or 0
        total = s['total_deals'] or 0
        
        text += (f"{i}. <b>{name}</b>\n"
                f"   💰 Sotuv: {sales:,.0f} so'm\n"
                f"   📈 Foyda: {profit:,.0f} ({margin:.1f}%)\n"
                f"   ✅ {completed}/{total} bitim\n\n")
    
    text += "<b>📅 Oxirgi 7 kun:</b>\n"
    for d in daily:
        date_str = d['date'].strftime('%d.%m')
        text += f"• {date_str}: {d['deals']} ta ({d['sales']:,.0f} so'm)\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_reports")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "rep_installers")
async def report_by_installers(callback: CallbackQuery):
    """Montajchilar hisoboti"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        installers = await conn.fetch('''
            SELECT 
                e.full_name,
                e.region,
                e.city,
                COUNT(i.id) as total_installs,
                COUNT(CASE WHEN i.installation_date > CURRENT_DATE - INTERVAL '30 days' 
                      THEN 1 END) as recent,
                AVG(i.motor_hours_current - i.motor_hours_start) as avg_hours
            FROM employees e
            LEFT JOIN installations i ON e.telegram_id = i.installer_id
            WHERE e.role = 'montajchi' AND e.is_active = TRUE
            GROUP BY e.full_name, e.region, e.city
            ORDER BY total_installs DESC
        ''')
    
    text = "🔧 <b>Montajchilar hisoboti</b>\n\n"
    
    for inst in installers:
        location = f"{inst['region'] or 'N/A'}, {inst['city'] or 'N/A'}"
        avg_hours = inst['avg_hours'] or 0
        
        text += (f"👤 <b>{inst['full_name']}</b>\n"
                f"📍 {location}\n"
                f"🔧 Jami o'rnatish: {inst['total_installs']} ta\n"
                f"📅 Oxirgi 30 kun: {inst['recent']} ta\n"
                f"⏱ O'rtacha moto-soat: {avg_hours:.0f}\n\n")
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_reports")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "rep_logistics")
async def report_logistics(callback: CallbackQuery):
    """Logistika hisoboti"""
    if not is_admin(callback.from_user.id):
        return
    
    async with db_pool.acquire() as conn:
        # Umumiy statistika
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) as delivered,
                COUNT(CASE WHEN status = 'PLANNED' THEN 1 END) as planned,
                COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) as in_transit,
                SUM(delivery_cost) as total_cost
            FROM logistics
            WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
        ''')
        
        # Logistlar reytingi - TO'G'RILANGAN
        logists = await conn.fetch('''
            SELECT 
                e.full_name,
                COUNT(l.id) as deliveries,
                SUM(l.delivery_cost) as costs,
                AVG(CASE 
                    WHEN l.status = 'DELIVERED' AND l.actual_date IS NOT NULL AND l.planned_date IS NOT NULL
                    THEN (l.actual_date - l.planned_date)::INTEGER
                    ELSE NULL 
                END) as avg_delay_days
            FROM employees e
            JOIN logistics l ON e.telegram_id = l.logist_id
            WHERE l.created_at > CURRENT_DATE - INTERVAL '30 days'
            GROUP BY e.full_name
            ORDER BY deliveries DESC
        ''')
    
    text = "🚚 <b>Logistika hisoboti (oxirgi 30 kun)</b>\n\n"
    
    text += (f"<b>📊 Umumiy:</b>\n"
            f"🚚 Jami: {stats['total'] or 0} ta\n"
            f"✅ Yetkazilgan: {stats['delivered'] or 0}\n"
            f"🟡 Rejalashtirilgan: {stats['planned'] or 0}\n"
            f"🟢 Yo'lda: {stats['in_transit'] or 0}\n"
            f"💰 Jami xarajat: {stats['total_cost'] or 0:,.0f} so'm\n\n")
    
    text += "<b>👤 Logistlar:</b>\n"
    for log in logists:
        delay = log['avg_delay_days'] or 0
        # delay kunlarda, musbat = kechikish, manfiy = erta
        if delay > 0:
            delay_text = f"({delay:.1f} kun kechikish)"
        elif delay < 0:
            delay_text = f"({abs(delay):.1f} kun erta)"
        else:
            delay_text = "(o'z vaqtida)"
        
        text += (f"• {log['full_name']}: {log['deliveries']} ta "
                f"{delay_text}\n")
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_reports")]
        ]),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "export_profit")
async def export_profit_report(callback: CallbackQuery):
    """Excel hisobot yuklash (CSV format)"""
    if not is_admin(callback.from_user.id):
        return
    
    await callback.answer("📊 Hisobot tayyorlanmoqda...", show_alert=True)
    
    import csv
    import io
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT d.id, d.sale_price, d.profit, d.profit_margin,
                   d.created_at, g.model, c.full_name as client,
                   e.full_name as seller
            FROM deals d
            JOIN generators g ON d.generator_uid = g.uid
            JOIN clients c ON d.client_id = c.id
            JOIN employees e ON d.seller_id = e.telegram_id
            WHERE d.status = 'COMPLETED'
            AND d.created_at > CURRENT_DATE - INTERVAL '30 days'
            ORDER BY d.created_at DESC
        ''')
    
    # CSV yaratish
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['ID', 'Sana', 'Model', 'Mijoz', 'Sotuvchi', 
                     'Sotuv narxi', 'Foyda', 'Marja (%)'])
    
    for row in rows:
        writer.writerow([
            row['id'],
            row['created_at'].strftime('%d.%m.%Y'),
            row['model'],
            row['client'],
            row['seller'],
            row['sale_price'],
            row['profit'],
            row['profit_margin']
        ])
    
    # Faylga saqlash
    file_path = f"{UPLOAD_DIR}/profit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
        f.write(output.getvalue())
    
    await callback.message.answer_document(
        FSInputFile(file_path),
        caption="📊 <b>Foyda hisoboti (CSV)</b>\n\n"
                f"Period: Oxirgi 30 kun\n"
                f"Yozuvlar: {len(rows)} ta",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "audit_by_user")
async def audit_by_user(callback: CallbackQuery, state: FSMContext):
    """Foydalanuvchi bo'yicha audit"""
    await callback.message.edit_text(
        "👤 <b>Telegram ID ni kiriting:</b>",
        parse_mode="HTML"
    )
    await state.set_state("audit_user_id")

@dp.message(State("audit_user_id"))
async def process_audit_user(message: Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
    except:
        await message.answer("❌ Faqat raqam kiriting!")
        return
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.user_id = $1
            ORDER BY a.created_at DESC
            LIMIT 20
        ''', user_id)
    
    if not rows:
        await message.answer("❌ Natijalar topilmadi!")
        await state.clear()
        return
    
    await show_audit_logs(message, rows)
    await state.clear()

@dp.callback_query(F.data == "audit_by_table")
async def audit_by_table(callback: CallbackQuery):
    """Jadval bo'yicha audit"""
    tables = ['employees', 'clients', 'generators', 'deals', 'payments', 
              'logistics', 'installations', 'service_history']
    
    buttons = []
    for table in tables:
        buttons.append([InlineKeyboardButton(
            text=table,
            callback_data=f"audittable_{table}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Orqaga", callback_data="admin_audit")])
    
    await callback.message.edit_text(
        "📊 <b>Jadvalni tanlang:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("audittable_"))
async def show_table_audit(callback: CallbackQuery):
    table = callback.data.split("_")[1]
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.table_name = $1
            ORDER BY a.created_at DESC
            LIMIT 15
        ''', table)
    
    if not rows:
        await callback.answer("Ma'lumotlar yo'q!", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"📊 <b>{table} - Oxirgi o'zgarishlar</b>",
        parse_mode="HTML"
    )
    await show_audit_logs(callback, rows)

@dp.callback_query(F.data == "audit_today")
async def audit_today(callback: CallbackQuery):
    """Bugungi audit"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT a.*, e.full_name as user_name
            FROM audit_logs a
            LEFT JOIN employees e ON a.user_id = e.telegram_id
            WHERE a.created_at > CURRENT_DATE
            ORDER BY a.created_at DESC
            LIMIT 20
        ''')
    
    if not rows:
        await callback.answer("Bugun hech narsa o'zgartirilmagan!", show_alert=True)
        return
    
    await callback.message.edit_text(
        f"📅 <b>Bugungi harakatlar ({len(rows)} ta)</b>",
        parse_mode="HTML"
    )
    await show_audit_logs(callback, rows)

@dp.callback_query(F.data == "map_show_all_internal")
async def map_show_all_internal(callback: CallbackQuery):
    """Barcha obyektlarni ko'rsatish"""
    await callback.answer("Yuklanmoqda...", show_alert=True)
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT g.uid, g.model, g.power_kw, g.status,
                   c.full_name, c.phone, c.address, c.geo_lat, c.geo_lon,
                   c.region, c.city,
                   d.status as deal_status,
                   e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE c.geo_lat IS NOT NULL
            ORDER BY c.region, c.city
            LIMIT 50
        ''')
    
    await show_map_points(callback, rows, "Barcha obyektlar", detailed=True)

@dp.callback_query(F.data.startswith("mapdetail_"))
async def map_detail(callback: CallbackQuery):
    """Xaritadagi obyekt batafsili"""
    uid = callback.data.split("_")[1]
    
    async with db_pool.acquire() as conn:
        gen = await conn.fetchrow('''
            SELECT g.*, c.*, d.*, e.full_name as seller_name,
                   emp.full_name as installer_name
            FROM generators g
            JOIN clients c ON g.current_client_id = c.id
            JOIN deals d ON g.current_deal_id = d.id
            LEFT JOIN employees e ON d.seller_id = e.telegram_id
            LEFT JOIN installations i ON d.id = i.deal_id
            LEFT JOIN employees emp ON i.installer_id = emp.telegram_id
            WHERE g.uid = $1
        ''', uid)
    
    if not gen:
        await callback.answer("Topilmadi!", show_alert=True)
        return
    
    maps_link = f"https://maps.google.com/?q={gen['geo_lat']},{gen['geo_lon']}"
    
    text = (f"🔧 <b>{gen['model']}</b> ({gen['power_kw']}kVA)\n\n"
            f"🆔 {uid}\n"
            f"👤 {gen['full_name']}\n"
            f"📱 {gen['phone']}\n"
            f"📍 {gen['address']}\n"
            f"🧑‍💼 Sotuvchi: {gen['seller_name'] or 'N/A'}\n"
            f"🔧 Montajchi: {gen['installer_name'] or 'N/A'}\n\n"
            f"<a href='{maps_link}'>📍 Xaritada ko'rish</a>")
    
    await callback.message.answer(text, parse_mode="HTML", disable_web_page_preview=True)

@dp.callback_query(F.data == "back_to_regions")
async def back_to_regions_handler(callback: CallbackQuery, state: FSMContext):
    """Viloyatlarga qaytish (montajchi tanlash jarayonida)"""
    data = await state.get_data()
    
    # Agar sotuvchi bitim yaratayotgan bo'lsa
    if 'client_name' in data:
        await callback.message.edit_text(
            f"🔧 <b>Montajchi uchun viloyat tanlang:</b>",
            reply_markup=get_regions_keyboard_for_installer(),
            parse_mode="HTML"
        )
        await state.set_state(CreateDeal.selecting_installer_region)
    else:
        # Admin xodim qo'shayotgan bo'lsa
        await callback.message.edit_text(
            f"📍 <b>Viloyatni tanlang:</b>",
            reply_markup=get_regions_keyboard(),
            parse_mode="HTML"
        )
        await state.set_state(AddEmployee.selecting_region)

@dp.callback_query(F.data.startswith("instregion_"))
async def back_to_installer_cities(callback: CallbackQuery, state: FSMContext):
    """Tumanlarga qaytish (viloyat tanlangandan keyin)"""
    # Bu yerda viloyat tanlash funksiyasini chaqiramiz
    await select_installer_region(callback, state)

@dp.callback_query(F.data.startswith("fileget_"))
async def get_file(callback: CallbackQuery):
    """Faylni yuklab olish"""
    file_id = int(callback.data.split("_")[1])
    
    async with db_pool.acquire() as conn:
        file = await conn.fetchrow('SELECT * FROM files WHERE id = $1', file_id)
    
    if not file:
        await callback.answer("Fayl topilmadi!", show_alert=True)
        return
    
    # Faylni yuborish
    try:
        if file['file_path'] and os.path.exists(file['file_path']):
            if file['mime_type'] and file['mime_type'].startswith('image'):
                await callback.message.answer_photo(
                    FSInputFile(file['file_path']),
                    caption=f"📄 {file['file_name']}"
                )
            else:
                await callback.message.answer_document(
                    FSInputFile(file['file_path']),
                    caption=f"📄 {file['file_name']}"
                )
        else:
            await callback.answer("Fayl serverda topilmadi!", show_alert=True)
    except Exception as e:
        await callback.answer(f"Xatolik: {str(e)}", show_alert=True)

@dp.callback_query(F.data.startswith("filedel_"))
async def delete_file(callback: CallbackQuery):
    """Faylni o'chirish"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    file_id = int(callback.data.split("_")[1])
    
    async with db_pool.acquire() as conn:
        file = await conn.fetchrow('SELECT * FROM files WHERE id = $1', file_id)
        
        if not file:
            await callback.answer("Fayl topilmadi!", show_alert=True)
            return
        
        # Faylni o'chirish
        await conn.execute('DELETE FROM files WHERE id = $1', file_id)
        
        # Fayl tizimidan ham o'chirish (ixtiyoriy)
        if file['file_path'] and os.path.exists(file['file_path']):
            try:
                os.remove(file['file_path'])
            except:
                pass
    
    await callback.answer("✅ Fayl o'chirildi!")
    await callback.message.edit_text("🗑 Fayl o'chirildi.")

@dp.callback_query(F.data == "file_list_all")
async def list_all_files(callback: CallbackQuery):
    """Barcha fayllar ro'yxati"""
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    
    async with db_pool.acquire() as conn:
        files = await conn.fetch('''
            SELECT f.*, 
                   e.full_name as uploader_name,
                   CASE 
                       WHEN f.entity_type = 'gen' THEN g.model
                       WHEN f.entity_type = 'deal' THEN d.id::text
                       WHEN f.entity_type = 'client' THEN c.full_name
                       ELSE f.entity_id
                   END as entity_name
            FROM files f
            LEFT JOIN employees e ON f.uploaded_by = e.telegram_id
            LEFT JOIN generators g ON f.entity_type = 'gen' AND f.entity_id = g.uid
            LEFT JOIN deals d ON f.entity_type = 'deal' AND f.entity_id = d.id::text
            LEFT JOIN clients c ON f.entity_type = 'client' AND f.entity_id = c.id::text
            ORDER BY f.uploaded_at DESC
            LIMIT 50
        ''')
    
    # Keyboard yaratish (callback kerak emas!)
    files_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬆️ Fayl yuklash", callback_data="file_upload")],
        [InlineKeyboardButton(text="🔍 Generator fayllari", callback_data="file_search_gen")],
        [InlineKeyboardButton(text="📋 Bitim fayllari", callback_data="file_search_deal")],
        [InlineKeyboardButton(text="🔧 Servis fayllari", callback_data="file_search_service")],
        [InlineKeyboardButton(text="📊 Barcha fayllar", callback_data="file_list_all")],
        [InlineKeyboardButton(text="◀️ Orqaga", callback_data="main_menu")]
    ])
    
    if not files:
        await callback.message.edit_text(
            "📁 <b>Fayllar ro'yxati bo'sh</b>",
            reply_markup=admin_files_menu(),  # ✅ Parametrsiz!
            parse_mode="HTML"
        )
        return
    
    # Statistika
    total_count = len(files)
    total_size = sum(f['file_size'] or 0 for f in files)
    total_size_mb = total_size / (1024 * 1024)
    
    # Turlar bo'yicha
    types = {}
    for f in files:
        t = f['file_type'] or 'Noma\'lum'
        types[t] = types.get(t, 0) + 1
    
    text = (f"📁 <b>Barcha fayllar</b>\n\n"
            f"📊 Jami: {total_count} ta\n"
            f"💾 Umumiy hajm: {total_size_mb:.2f} MB\n\n"
            f"<b>📂 Turlar bo'yicha:</b>\n")
    
    for t, count in sorted(types.items(), key=lambda x: x[1], reverse=True):
        text += f"• {t}: {count} ta\n"
    
    text += f"\n<i>Oxirgi 50 ta fayl:</i>"
    
    await callback.message.edit_text(
        text,
        reply_markup=files_keyboard,  # ✅ To'g'ridan-to'g'ri keyboard
        parse_mode="HTML"
    )
    
    # Fayllar ro'yxatini alohida xabarlar bilan yuborish
    for file in files[:20]:  # Faqat 20 tasini ko'rsatish
        size_mb = (file['file_size'] or 0) / (1024 * 1024)
        uploader = file['uploader_name'] or f"ID: {file['uploaded_by']}"
        entity_name = file['entity_name'] or file['entity_id']
        
        file_text = (f"📄 <b>{file['file_name']}</b>\n"
                    f"🆔 ID: {file['id']}\n"
                    f"📂 Tur: {file['file_type']}\n"
                    f"📦 Obyekt: {file['entity_type']} - {entity_name}\n"
                    f"📊 Hajm: {size_mb:.2f} MB\n"
                    f"👤 Yuklagan: {uploader}\n"
                    f"📅 {file['uploaded_at'].strftime('%d.%m.%Y %H:%M')}")
        
        buttons = [[
            InlineKeyboardButton(
                text="⬇️ Yuklab olish",
                callback_data=f"fileget_{file['id']}"
            ),
            InlineKeyboardButton(
                text="🗑 O'chirish",
                callback_data=f"filedel_{file['id']}"
            )
        ]]
        
        await callback.message.answer(
            file_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode="HTML"
        )

# ============ MAIN ============

# ============ MAIN ============

async def main():
    await init_db()
    
    # START COMMAND QO'SHISH
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start", description="Boshlash")
    ])
    # -----------------------
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())