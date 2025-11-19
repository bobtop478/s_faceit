import asyncio
import sqlite3 # [PG-REMOVED]
import asyncpg # [PG-ADDED]
import os # [PG-ADDED]
import html
import re
import logging
import json
import random
from async_lru import alru_cache
from datetime import datetime, timedelta
from aiohttp import ClientSession
from dotenv import load_dotenv # [PG-ADDED]

#Импорты Aiogram
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import BaseFilter
from aiogram.filters import CommandStart
from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.markdown import link, code, bold
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InputMediaPhoto
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from flask import Flask
from threading import Thread

# [PG-ADDED] Загружаем переменные окружения (токен, ДБ)
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# [PG-ADDED] Глобальный пул соединений
db_pool: asyncpg.Pool = None

MAIN_MENU_FILE_ID = "AgACAgIAAxkBAAMYaRezEEXoXv8vhxKEKERlm93V1mUAAgEPaxuThMFI7aG8He5IoigBAAMCAAN3AAM2BA"
PROFILE_FILE_ID = "AgACAgIAAxkBAAMRaReyRB-7l41VfFlI7mZ5r8MWvp0AAv4OaxuThMFIvkdELK3BNqEBAAMCAAN3AAM2BA"
PLAYER_RATING_FILE_ID = "AgACAgIAAxkBAAMUaRezB5rllWQExIMxXAU6-QwZ6p8AAv8OaxuThMFIbwdQTIAzLyIBAAMCAAN3AAM2BA"
PARTY_FILE_ID = "AgACAgIAAxkBAAMWaRezC4a9ZzW3GCyWoFn2wQsYsjUAAw9rG5OEwUhE45IB0TdcngEAAwIAA3cAAzYE"
LOBBY_FILE_ID = "AgACAgIAAxkBAAMaaRezFPyQ5X_YrAR1bo7nJTi3u8wAAgIPaxuThMFIuQAB1neBqOXKAQADAgADdwADNgQ"
SEASON_INFO_FILE_ID = "AgACAgQAAxkBAAIMy2kdFE7GqRJXmRo293WKurbolo5IAAK_DGsbG2_pUDVNAAFweS8augEAAwIAA3kAAzYE"

# [PG-REMOVED] DB_NAME = 'faceit_tracker.db'
DEFAULT_LEAGUE = "Default"
QUAL_LEAGUE = "Qualification"
FPL_LEAGUE = "FPL"
ROLE_OWNER = "Owner"
ROLE_ADMIN = "Administrator"
ROLE_GAME_REG = "Game Reg"
ROLE_PLAYER = "Player"

LEAGUE_LEVELS = {
    DEFAULT_LEAGUE: 0,
    QUAL_LEAGUE: 1,
    FPL_LEAGUE: 2
}

ROLE_LEVELS = {
    ROLE_PLAYER: 0,
    ROLE_GAME_REG: 1,
    ROLE_ADMIN: 2,
    ROLE_OWNER: 3
}

# [PG-REMOVED] Удален COLUMNS_TO_ADD, так как миграция
# будет встроена в `init_db` по-другому.

def is_valid_game_id(game_id: str) -> bool:
    """Проверяет, что game_id содержит только латинские буквы (a-z, A-Z) и цифры (0-9) и имеет длину 1-12."""
    return re.fullmatch(r"^[a-zA-Z0-9]{1,12}$", game_id) is not None

# ... (FSM States, без изменений) ...
class GameIDState(StatesGroup):
    waiting_for_game_selection = State()
    waiting_for_game_id = State()
    waiting_for_nickname = State()
    waiting_for_device = State()

class Party(StatesGroup):
    waiting_for_invite_id = State()

class ChangeGameIDState(StatesGroup):
    waiting_for_new_game_id = State()

class EditProfile(StatesGroup):
    waiting_for_new_nickname = State()
    waiting_for_new_gameid = State()

class Registration(StatesGroup):
    waiting_for_game_choice = State()
    waiting_for_game_id = State()
    waiting_for_nickname = State()
    waiting_for_device = State()

class Ticket(StatesGroup):
    waiting_for_ticket_game_choice = State()
    waiting_for_match_id = State()
    waiting_for_ticket_text = State()
    waiting_for_media = State()

class AdminResponse(StatesGroup):
    waiting_for_answer = State()

class LobbyState(StatesGroup):
    in_lobby = State()
    confirming_participation = State()

class MapBanState(StatesGroup):
    waiting_for_ban = State()

class MatchResultState(StatesGroup):
    waiting_for_screenshot = State()

class AdminMatchRegistration(StatesGroup):
    waiting_for_match_data = State()

class AdminActions(StatesGroup):
    waiting_for_delete_id = State()
    waiting_for_mute_data = State()
    waiting_for_ban_id = State()
    waiting_for_unban_id = State()
    waiting_for_unmute_id = State()
    waiting_for_qual_access_id = State()
    waiting_for_fpl_access_id = State()
    waiting_for_bot_league = State()
    waiting_for_bot_lobby_number = State()
    waiting_for_bot_count = State()
    waiting_for_remove_bot_lobby_id = State()
    waiting_for_role_target_id = State()
    waiting_for_change_nick_data = State()
    waiting_for_change_gameid_data = State()
    waiting_for_revoke_qual_id = State()
    waiting_for_revoke_fpl_id = State()
    waiting_for_broadcast_message = State()
    waiting_for_revoke_premium_id = State()
    
class AdminPromo(StatesGroup):
    waiting_for_role_type = State()
    waiting_for_duration = State()
    waiting_for_uses = State()
    
class ActivatePromo(StatesGroup):
    waiting_for_code = State()

# [PG-REWRITE] Загрузка конфигурации из .env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL") # "postgresql://user:password@host:port/dbname"
if not TELEGRAM_BOT_TOKEN or not DATABASE_URL:
    logger.critical("КРИТИЧЕСКАЯ ОШИБКА: TELEGRAM_BOT_TOKEN или DATABASE_URL не найдены в .env!")
    exit()

BOT_ID = int(TELEGRAM_BOT_TOKEN.split(":", 1)[0])
FACEIT_API_KEY = os.getenv("FACEIT_API_KEY", "YOUR_FACEIT_API_KEY")
CHANNEL_USERNAME = "@senpaifaceit1"
CHAT_LINK = "https://t.me/senpaifaceit1"
RISE_CHAT_USERNAME = "@chatsenpaifaceit"
HELP_LINK = "https://telegra.ph/PRAVILA-SENPAI-FACEIT-11-14"

TICKET_CHAT_ID = -1003260656194
TICKET_THREAD_ID = 238
TICKET_CANCEL_TEXT = "<blockquote><b>❌ Создание тикета отменено.</b></blockquote>\n\nВы возвращены в главное меню."

RESULTS_CHANNEL_ID = -1003260656194
MATCH_THREAD_ID = 236

SINGLE_GAME_NAME = "Project Evolution"

# ... (Константы ELO_LEVELS, MAPS - без изменений) ...
LEVEL_EMOJI_MAP = {
    1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣", 
    6: "6️⃣", 7: "7️⃣", 8: "8️⃣", 9: "9️⃣", 10: "🔟"
}

ELO_LEVELS = [
    (0, 300, 1),
    (300, 500, 2),
    (500, 700, 3),
    (700, 900, 4),
    (900, 1100, 5),
    (1100, 1350, 6),
    (1350, 1600, 7),
    (1600, 1750, 8),
    (1750, 2100, 9),
    (2100, float('inf'), 10),
]

MAPS = ["Sandstone", "Province", "Rust", "Breeze", "Zone 7", "Dune", "Hanami"]

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# [ASYNC-REWRITE] Фильтр должен быть асинхронным
class MinRoleFilter(BaseFilter):
    """
    Фильтр, который пропускает хэндлер, только если у пользователя
    уровень роли НЕ НИЖЕ указанного.
    """
    def __init__(self, min_level_name: str):
        self.min_level = ROLE_LEVELS.get(min_level_name, 0)

    async def __call__(self, event: types.Message | types.CallbackQuery) -> bool:
        # [ASYNC-REWRITE] get_user_role теперь асинхронный
        role = await get_user_role(event.from_user.id) 
        
        user_level = ROLE_LEVELS.get(role, 0)
        
        if user_level >= self.min_level:
            return True
        else:
            if isinstance(event, types.CallbackQuery):
                try:
                    await event.answer("⛔ У вас нет прав для этого действия!", show_alert=True)
                except TelegramBadRequest:
                    pass
            return False

# [PG-ADDED] Хелпер для миграций
async def check_and_add_column(conn: asyncpg.Connection, table_name: str, column_name: str, column_def: str):
    """Проверяет наличие колонки и добавляет ее, если ее нет."""
    exists = await conn.fetchval(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = $2
        )
    """, table_name, column_name)
    
    if not exists:
        try:
            await conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
            print(f"✅ Миграция: добавлен столбец {table_name}.{column_name}")
        except Exception as e:
            print(f"⚠️ Ошибка при добавлении столбца {table_name}.{column_name}: {e}")

# [PG-REWRITE] Полностью переписана функция инициализации БД под PostgreSQL
# [PG-REWRITE] Полностью переписана функция инициализации БД под новые требования (Промокоды, Тикеты, Премиум)
async def init_db(pool: asyncpg.Pool):

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Таблица users
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    nickname TEXT NULL,
                    game_id TEXT NULL,
                    device TEXT NULL,
                    is_registered BOOLEAN DEFAULT FALSE
                );
            """)

            # Таблица tickets (Обновлена)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    match_id TEXT,
                    game_name TEXT,
                    ticket_text TEXT NOT NULL,
                    admin_message_id BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                );
            """)

            # Таблица promo_codes (НОВАЯ)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS promo_codes (
                    code TEXT PRIMARY KEY,
                    reward_type TEXT NOT NULL, -- 'premium', 'qual', 'fpl'
                    duration_days INTEGER DEFAULT 0,
                    uses_left INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Таблица lobbies
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS lobbies (
                    lobby_id SERIAL PRIMARY KEY,
                    league TEXT NOT NULL,
                    status TEXT DEFAULT 'waiting', -- waiting, full, confirming, playing
                    current_players INTEGER DEFAULT 0,
                    map_pool TEXT
                );
            """)

            # Таблица lobby_members
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS lobby_members (
                    id SERIAL PRIMARY KEY,
                    lobby_id INTEGER,
                    user_id BIGINT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    confirmed BOOLEAN DEFAULT FALSE,
                    lobby_message_id BIGINT,
                    FOREIGN KEY(lobby_id) REFERENCES lobbies(lobby_id) ON DELETE CASCADE,
                    FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
                );
            """)

            # Таблица matches
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS matches (
                    match_id VARCHAR(255) PRIMARY KEY,
                    lobby_id INTEGER NOT NULL,
                    captain1_id BIGINT NOT NULL,
                    captain2_id BIGINT NOT NULL,
                    status TEXT DEFAULT 'picking',
                    FOREIGN KEY(lobby_id) REFERENCES lobbies(lobby_id)
                );
            """)

            # Таблица user_league_stats
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_league_stats (
                    stat_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    league_name TEXT NOT NULL,
                    elo INTEGER DEFAULT 0,
                    matches_played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    kills INTEGER DEFAULT 0,
                    deaths INTEGER DEFAULT 0,
                    total_score INTEGER DEFAULT 0,
                    UNIQUE(user_id, league_name) 
                );
            """)
            
            # --- Миграции users (Добавлены новые поля: premium, cooldowns) ---
            columns_to_add_users = {
                "elo": "INTEGER DEFAULT 0",
                "league": f"TEXT DEFAULT '{DEFAULT_LEAGUE}'",
                "is_admin": "BOOLEAN DEFAULT FALSE",
                "teammate_user_id": "BIGINT NULL",
                "teammate2_user_id": "BIGINT NULL", # [NEW] Для 3-го игрока в пати
                "premium_until": "TIMESTAMP NULL", # [NEW] Срок действия премиума
                "last_ticket_at": "TIMESTAMP NULL", # [NEW] Кулдаун тикетов
                "pending_invite_to": "BIGINT NULL",
                "matches_played": "INTEGER DEFAULT 0",
                "wins": "INTEGER DEFAULT 0",
                "losses": "INTEGER DEFAULT 0",
                "avg_score": "REAL DEFAULT 0.0",
                "kd_ratio": "REAL DEFAULT 0.0",
                "registration_date": "TEXT",
                "kills": "INTEGER DEFAULT 0",
                "deaths": "INTEGER DEFAULT 0",
                "total_score": "INTEGER DEFAULT 0",
                "banned": "BOOLEAN DEFAULT FALSE",
                "muted_until": "TIMESTAMP NULL",
                "warns": "INTEGER DEFAULT 0",
                "role": f"TEXT DEFAULT '{ROLE_PLAYER}'",
                "game_key": "TEXT DEFAULT 'project_evolution'" 
            }
            for col, defin in columns_to_add_users.items():
                await check_and_add_column(conn, 'users', col, defin)

            # --- Миграции matches ---
            columns_to_add_matches = {
                "captain_turn": "BIGINT",
                "map_name": "TEXT",
                "banned_maps": "TEXT",
                "team_ct": "TEXT",
                "team_t": "TEXT",
                "last_registration_data": "TEXT"
            }
            for col, defin in columns_to_add_matches.items():
                await check_and_add_column(conn, 'matches', col, defin)
            
            # --- Миграция lobby_members ---
            await check_and_add_column(conn, 'lobby_members', 'lobby_message_id', 'BIGINT')
            
            # --- Миграция ELO из users в user_league_stats ---
            stats_exist = await conn.fetchval("SELECT 1 FROM user_league_stats LIMIT 1")

            if not stats_exist:
                print("🔄 Начало миграции статистики в 'user_league_stats'...")
                all_users_data = await conn.fetch(
                    "SELECT user_id, elo, matches_played, wins, losses, kills, deaths, total_score FROM users WHERE is_registered = TRUE"
                )
                
                migrated_count = 0
                if all_users_data:
                    for user_row in all_users_data:
                        try:
                            await conn.execute(
                                """INSERT INTO user_league_stats 
                                   (user_id, league_name, elo, matches_played, wins, losses, kills, deaths, total_score)
                                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                                   ON CONFLICT (user_id, league_name) DO NOTHING
                                """,
                                user_row['user_id'], DEFAULT_LEAGUE, user_row['elo'], user_row['matches_played'],
                                user_row['wins'], user_row['losses'], user_row['kills'], user_row['deaths'], user_row['total_score']
                            )
                            migrated_count += 1
                        except Exception as e:
                            logger.error(f"Ошибка миграции ELO для {user_row['user_id']}: {e}")
                    print(f"✅ Миграция ELO завершена. Перенесено {migrated_count} записей в 'Default' лигу.")
                else:
                     print("✅ Миграция ELO: Таблица 'users' пуста, перенос не требуется.")
            else:
                print("✅ Миграция ELO/статистики не требуется (user_league_stats уже заполнена).")

            # --- Назначение ролей Owner ---
            if 'ADMIN_IDS' in globals():
                for owner_id in ADMIN_IDS:
                    await conn.execute(
                        "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT(user_id) DO NOTHING", 
                        owner_id
                    )
                    await conn.execute(
                        "UPDATE users SET role = $1, is_admin = TRUE WHERE user_id = $2", 
                        ROLE_OWNER, owner_id
                    )
                print(f"✅ Роли 'Owner' назначены для {len(ADMIN_IDS)} пользователей.")
            else:
                print("⚠️ Константа ADMIN_IDS не найдена! Роли Owner не назначены.")

            # --- Инициализация лобби ---
            DEFAULT_MAP_POOL = f"['{', '.join(MAPS)}']" 
            LEAGUES_TO_INIT = [DEFAULT_LEAGUE, QUAL_LEAGUE, FPL_LEAGUE]

            for league in LEAGUES_TO_INIT:
                lobby_count = await conn.fetchval("SELECT COUNT(*) FROM lobbies WHERE league = $1", league)

                if lobby_count < 5:
                    if lobby_count > 0:
                        await conn.execute("DELETE FROM lobbies WHERE league = $1", league)
                        print(f"🔄 Очищены старые лобби для лиги: {league}")
            
                    for i in range(5):
                        try:
                            await conn.execute("""
                                INSERT INTO lobbies (league, status, current_players, map_pool) 
                                VALUES ($1, 'waiting', 0, $2)
                            """, league, DEFAULT_MAP_POOL)
                        except Exception as e:
                            logger.error(f"Ошибка создания лобби для {league}: {e}")
                            
                    print(f"✅ Добавлено 5 начальных лобби для лиги: {league}.")
                else:
                    print(f"Базовые лобби для лиги {league} уже существуют.")
        
        print("База данных PostgreSQL успешно инициализирована/обновлена.")

# [PG-REWRITE] Новые асинхронные хелперы БД
async def db_execute(query: str, *args):
    """(PG) Выполняет SQL-запрос (INSERT, UPDATE, DELETE) с аргументами."""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(query, *args)
    except Exception as e:
        logger.error(f"Ошибка выполнения DB Execute: {query} \nArgs: {args} \nError: {e}", exc_info=True)
        raise e

async def db_fetchone(query: str, *args) -> asyncpg.Record | None:
    """(PG) Получает одну строку (Record) или None."""
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    except Exception as e:
        logger.error(f"Ошибка выполнения DB Fetchone: {query} \nArgs: {args} \nError: {e}", exc_info=True)
        return None

async def db_fetchall(query: str, *args) -> list[asyncpg.Record]:
    """(PG) Получает список (list) строк (Record) или []."""
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetch(query, *args)
    except Exception as e:
        logger.error(f"Ошибка выполнения DB Fetchall: {query} \nArgs: {args} \nError: {e}", exc_info=True)
        return []

@alru_cache(maxsize=1000, ttl=60)
async def get_cached_user_data(user_id: int) -> dict | None:
    """
    (НОВАЯ КЭШИРУЮЩАЯ ФУНКЦИЯ)
    Получает ВСЕ основные данные о пользователе (из 'users' и статистику для ЕГО ГЛАВНОЙ лиги) 
    ОДНИМ запросом и кэширует результат на 60 секунд.
    """
    if user_id <= 0: # Не кэшируем ботов
        user_data_record = await db_fetchone("SELECT * FROM users WHERE user_id = $1", user_id)
        return dict(user_data_record) if user_data_record else None

    # Этот запрос объединяет 'users' и статистику для ИХ ВЫСШЕЙ ЛИГИ (которая в u.league)
    query = """
    SELECT 
        u.*, 
        s.elo, 
        s.matches_played, 
        s.wins, 
        s.losses, 
        s.kills, 
        s.deaths, 
        s.total_score
    FROM users u
    LEFT JOIN user_league_stats s ON u.user_id = s.user_id AND u.league = s.league_name
    WHERE u.user_id = $1
    """
    user_data_record = await db_fetchone(query, user_id)
    
    if not user_data_record:
        return None
        
    # Возвращаем данные как dict, чтобы с ними было удобнее работать
    return dict(user_data_record)

async def clear_user_cache(user_id: int):
    """
    (НОВАЯ ФУНКЦИЯ)
    Принудительно очищает кэш для одного пользователя.
    """
    # Самый простой способ - очистить весь кэш. 
    # Это надежно и быстро, он сам заполнится при следующем клике.
    get_cached_user_data.cache_clear()
    logger.info(f"Кэш очищен (полностью) из-за обновления {user_id}")

# [ASYNC-REWRITE]
async def check_permission(user_id: int, required_role_level: int) -> bool:
    """
    Проверяет, имеет ли пользователь user_id роль, уровень которой
    равен или выше required_role_level.
    """
    user_data = await db_fetchone("SELECT role FROM users WHERE user_id = $1", user_id)
    if not user_data:
        return False
        
    user_role = user_data.get('role', ROLE_PLAYER)
    user_level = ROLE_LEVELS.get(user_role, 0)
    
    return user_level >= required_role_level

# [ASYNC-REWRITE]
async def get_user_league_stats(user_id: int, league_name: str) -> asyncpg.Record:
    """
    (PG) Получает ELO и статистику пользователя для КОНКРЕТНОЙ лиги.
    Если записи нет - создает ее с ELO 0.
    Возвращает Record (работает как dict) или dict-заглушку.
    """
    if not league_name:
        league_name = DEFAULT_LEAGUE
        
    stats = await db_fetchone(
        "SELECT * FROM user_league_stats WHERE user_id = $1 AND league_name = $2",
        user_id, league_name
    )
    
    default_stats = {
        'elo': 0, 'matches_played': 0, 'wins': 0, 'losses': 0,
        'kills': 0, 'deaths': 0, 'total_score': 0
    }
    
    if not stats:
        try:
            await db_execute(
                "INSERT INTO user_league_stats (user_id, league_name, elo) VALUES ($1, $2, $3) ON CONFLICT (user_id, league_name) DO NOTHING",
                user_id, league_name, 0
            )
        except Exception as e:
            logger.error(f"Не удалось создать статы для {user_id} в {league_name}: {e}")

        # Повторно пытаемся получить, вдруг гонка состояний
        stats = await db_fetchone(
            "SELECT * FROM user_league_stats WHERE user_id = $1 AND league_name = $2",
            user_id, league_name
        )
        
        if not stats:
             # Если все равно не получили (ошибка), возвращаем заглушку
            logger.warning(f"Возврат default_stats для {user_id} (не удалось создать/найти).")
            return default_stats # Возвращаем dict, т.к. Record не создан
    
    # Проверка на None в полях (для совместимости)
    # asyncpg.Record неизменяемый, поэтому создаем новый dict
    final_stats = dict(stats)
    for key in default_stats:
        if final_stats.get(key) is None:
            final_stats[key] = default_stats[key]
            
    return final_stats # Возвращаем dict

# [ASYNC-REWRITE]
async def get_user_highest_league_stats(user_id: int) -> dict:
    """
    Получает статистику для самой высокой лиги пользователя (FPL > QUAL > Default).
    """
    user = await db_fetchone("SELECT league FROM users WHERE user_id = $1", user_id)
    highest_league = user.get('league', DEFAULT_LEAGUE) if user else DEFAULT_LEAGUE
    
    return await get_user_league_stats(user_id, highest_league)


# ... (is_subscribed - без изменений, он уже был async) ...
async def is_subscribed(bot: Bot, user_id: int, channel_username: str) -> bool:
    """Проверяет, подписан ли пользователь на канал."""
    try:
        chat_member = await bot.get_chat_member(channel_username, user_id)
        return chat_member.status in ['member', 'creator', 'administrator']
    except Exception:
        return False

# ... (get_faceit_level_emoji, get_static_elo_change - без изменений) ...
def get_faceit_level_emoji(elo: int) -> str:
    """Определяет Faceit Level и возвращает соответствующий эмодзи."""
    for min_elo, max_elo, level in ELO_LEVELS:
        if min_elo <= elo < max_elo:
            return LEVEL_EMOJI_MAP.get(level, "❓")
    return "❓"

def get_static_elo_change(player_elo: int) -> tuple[int, int]:
    """
    (НОВАЯ ФУНКЦИЯ)
    Возвращает (win_points, loss_points) на основе ELO игрока.
    """
    if player_elo < 1100:
        return 30, -20
    elif player_elo < 2100:
        return 20, -30
    else:
        return 15, -30

# ... (get_subscription_keyboard, get_back_to_menu_keyboard,
# ... get_single_game_keyboard, get_registration_keyboard,
# ... get_game_choice_keyboard, get_device_choice_keyboard - без изменений) ...
def get_subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для проверки подписки."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{CHANNEL_USERNAME[1:]}")],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")]
    ])

def get_back_to_menu_keyboard(back_callback_data: str) -> InlineKeyboardMarkup:
    """Генерирует Inline-клавиатуру с одной кнопкой 'Назад в меню'."""
    keyboard = [
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data=back_callback_data)]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_single_game_keyboard(game_name: str) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру с одной кнопкой-игрой."""
    button = InlineKeyboardButton(text=game_name, callback_data="start_id_input") 
    return InlineKeyboardMarkup(inline_keyboard=[[button]])

def get_registration_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для начала регистрации."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Зарегистрироваться", callback_data="start_registration")]
    ])

def get_game_choice_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора игры (пока только Project Evolution)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=SINGLE_GAME_NAME, callback_data=f"game_select_{SINGLE_GAME_NAME}")]
    ])

def get_device_choice_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора устройства."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="PC", callback_data="device_PC")],
        [InlineKeyboardButton(text="Tab", callback_data="device_Tab")],
        [InlineKeyboardButton(text="Phone", callback_data="device_Phone")]
    ])

# [ASYNC-REWRITE] Клавиатура теперь асинхронная, т.к. делает запрос к БД
async def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Инлайн-клавиатура для главного меню. (Оптимизировано + Информация о сезоне)"""
    
    # [ОПТИМИЗАЦИЯ] 
    # Получаем is_admin из кэша, а лобби - отдельным запросом (т.к. оно динамическое)
    user_data = await get_cached_user_data(user_id)
    is_admin = user_data and user_data.get('is_admin', False)
    
    lobby_data = await db_fetchone("SELECT lobby_id FROM lobby_members WHERE user_id = $1", user_id)
    
    keyboard = []
    
    if lobby_data:
        keyboard.append([
            InlineKeyboardButton(
                text="🚪 Покинуть лобби", 
                callback_data=f"leave_lobby_{lobby_data['lobby_id']}"
            )
        ])
    else:
        keyboard.append([InlineKeyboardButton(text="🔍 Найти матч", callback_data="main_find_match")])

    keyboard.extend([
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="main_profile"),
            InlineKeyboardButton(text="🏆 Рейтинг игроков", callback_data="main_leaderboard"),
        ],
        [
            InlineKeyboardButton(text="🥇 Команды", callback_data="main_teams"),
            InlineKeyboardButton(text="🎁 Промокод", callback_data="main_promo"), # [NEW]
        ],
        [
            InlineKeyboardButton(text="❓ Помощь", url=HELP_LINK),
            InlineKeyboardButton(text="🎟️ Создать тикет", callback_data="main_ticket")
        ],
        [InlineKeyboardButton(text="ℹ️ Информация о сезоне", callback_data="main_season_info")]
    ])
    
    if is_admin:
        keyboard.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="main_admin")])
        
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ... (get_party_main_keyboard, get_invite_response_keyboard, 
# ... get_device_keyboard, get_leaderboard_keyboard, get_elo_info_keyboard,
# ... get_ticket_game_choice_keyboard, get_ticket_cancel_keyboard,
# ... get_ticket_sent_keyboard, get_ticket_cancelled_keyboard,
# ... get_admin_ticket_keyboard, get_profile_menu_keyboard - без изменений) ...

def get_party_main_keyboard(has_teammate: bool) -> InlineKeyboardMarkup:
    """Клавиатура для раздела 'Команды'."""
    keyboard = []
    if has_teammate:
        keyboard.append([InlineKeyboardButton(text="🚪 Покинуть команду", callback_data="party_leave")])
    else:
        keyboard.append([InlineKeyboardButton(text="✉️ Пригласить в команду", callback_data="party_invite_start")])
        
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_invite_response_keyboard(inviter_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для ответа на приглашение."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"invite_accept_{inviter_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"invite_decline_{inviter_id}")
        ]
    ])

def get_device_keyboard() -> InlineKeyboardMarkup:
    """Генерирует Inline-клавиатуру для выбора устройства (ПК/Мобильный/Планшет)."""
    
    keyboard = [
        [
            InlineKeyboardButton(text="PC", callback_data="device_PC"),
            InlineKeyboardButton(text="Mobile", callback_data="device_Mobile"),
            InlineKeyboardButton(text="Tab", callback_data="device_Tab"),
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_leaderboard_keyboard(current_league: str) -> InlineKeyboardMarkup:
    """Клавиатура для меню рейтинга с выбором лиги (горизонтально)."""
    leagues = [
        (DEFAULT_LEAGUE, "lb_Default"), 
        (QUAL_LEAGUE, "lb_QUAL"), 
        (FPL_LEAGUE, "lb_FPL")
    ]
    
    league_buttons = []
    for name, data in leagues:
        button_text = f"✅ {name}" if name == current_league else name
        league_buttons.append(InlineKeyboardButton(text=button_text, callback_data=data))
    
    keyboard = [
        league_buttons,
        [InlineKeyboardButton(text="ℹ️ Все о ELO", callback_data="show_elo_info")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_elo_info_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой 'Назад' для раздела 'Все о ELO'."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_leaderboard")] 
    ])

def get_ticket_game_choice_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора игры для тикета."""
    cancel_button = [InlineKeyboardButton(text="❌ Отменить", callback_data="ticket_cancel")]
    
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=SINGLE_GAME_NAME, callback_data=f"ticket_game_{SINGLE_GAME_NAME}")],
        cancel_button
    ])

def get_ticket_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой "Отменить" для этапа ввода Match ID и текста жалобы."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="ticket_cancel")]
    ])

def get_ticket_sent_keyboard(admin_message_id: int) -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой "Отменить тикет" после отправки тикета."""
    cancel_data = f"cancel_sent_{admin_message_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить тикет", callback_data=cancel_data)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")]
    ])

def get_ticket_cancelled_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой "Главная" после отмены тикета."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главная", callback_data="back_to_main_menu")]
    ])

def get_admin_ticket_keyboard(original_user_id: int, ticket_message_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для сообщения с тикетом в админ-чате."""
    callback_data = f"admin_answer_{original_user_id}_{ticket_message_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ответить на тикет", callback_data=callback_data)]
    ])

def get_profile_menu_keyboard(active_league: str) -> InlineKeyboardMarkup:
    """Инлайн-клавиатура для меню профиля с компактным расположением кнопок."""
    keyboard_inline = []
    
    # ИСПРАВЛЕНО: Важно сохранить структуру списка кортежей (текст, дата, имя_лиги)
    leagues = [
        ("🥇 Default Лига", f"profile_league_{DEFAULT_LEAGUE}", DEFAULT_LEAGUE),
        ("🌟 QUAL Лига", f"profile_league_{QUAL_LEAGUE}", QUAL_LEAGUE),
        ("🏆 FPL Лига", f"profile_league_{FPL_LEAGUE}", FPL_LEAGUE),
    ]
    
    row1_leagues = []
    for text, data, league_name in leagues:
        button_text = f"✅ {text}" if league_name == active_league else text
        row1_leagues.append(InlineKeyboardButton(text=button_text, callback_data=data))
        
    keyboard_inline.append(row1_leagues)
    
    keyboard_inline.append([InlineKeyboardButton(text="⭐️ Premium", callback_data="profile_premium")])
    
    row2_edit = [
        InlineKeyboardButton(text="✏️ Редактировать Профиль", callback_data="edit_profile"),
    ]
    keyboard_inline.append(row2_edit)

    row3_back = [
        InlineKeyboardButton(text="🏠 В Главное Меню", callback_data="back_to_main_menu"),
    ]
    keyboard_inline.append(row3_back)
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard_inline)

# [ASYNC-REWRITE]
async def get_league_choice_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура выбора лиги для поиска матча (с проверкой доступа)."""
    logger.info("Генерация клавиатуры выбора лиги")
    
    user_data = await db_fetchone("SELECT league FROM users WHERE user_id = $1", user_id)
    user_league = user_data.get('league', DEFAULT_LEAGUE) if user_data else DEFAULT_LEAGUE
    user_level = LEAGUE_LEVELS.get(user_league, 0)
    
    keyboard = []
    
    keyboard.append([InlineKeyboardButton(text="Default", callback_data="select_league_Default")])
    
    if user_level >= LEAGUE_LEVELS[QUAL_LEAGUE]:
        keyboard.append([InlineKeyboardButton(text="Qualifications", callback_data="select_league_Qualification")])
    
    if user_level >= LEAGUE_LEVELS[FPL_LEAGUE]:
        keyboard.append([InlineKeyboardButton(text="FPL", callback_data="select_league_FPL")])
        
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# [ASYNC-REWRITE]
async def get_lobby_list_keyboard(league: str) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру со списком 5 лобби для выбранной лиги."""
    lobbies = await db_fetchall(
        "SELECT lobby_id, current_players FROM lobbies WHERE league = $1 ORDER BY lobby_id LIMIT 5",
        league
    )
    
    keyboard = []
    if not lobbies:
        # На случай, если что-то пошло не так при init_db
        logger.error(f"Не найдено ни одного лобби для лиги {league}!")
        keyboard.append([InlineKeyboardButton(text="Ошибка: лобби не найдены", callback_data="ignore")])
        
    for i, lobby in enumerate(lobbies, 1):
        actual_count = await db_fetchone(
            "SELECT COUNT(*) as count FROM lobby_members WHERE lobby_id = $1",
            lobby['lobby_id']
        )
        real_count = actual_count['count'] if actual_count else 0
        
        if real_count != lobby['current_players']:
            await db_execute(
                "UPDATE lobbies SET current_players = $1 WHERE lobby_id = $2",
                real_count, lobby['lobby_id']
            )
        
        keyboard.append([
            InlineKeyboardButton(
                text=f"Лобби #{i} ({real_count}/10)",
                callback_data=f"join_lobby_{lobby['lobby_id']}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_find_match")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# [ASYNC-REWRITE]
async def get_user_role(user_id: int) -> str:
    """Получает роль пользователя из БД (асинхронно)."""
    # ADMIN_IDS должны быть определены глобально (позже в файле)
    if 'ADMIN_IDS' in globals() and user_id in ADMIN_IDS:
        return ROLE_OWNER
        
    user = await db_fetchone("SELECT role FROM users WHERE user_id = $1", user_id)
    return user['role'] if user and user['role'] else ROLE_PLAYER

# ... (get_lobby_keyboard, get_confirmation_keyboard, get_map_ban_keyboard, 
# ... get_match_result_keyboard, get_register_match_keyboard, 
# ... get_admin_post_registration_keyboard - без изменений) ...

def get_lobby_keyboard(lobby_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для нахождения в лобби."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚪 Покинуть лобби", callback_data=f"leave_lobby_{lobby_id}")]
    ])

def get_confirmation_keyboard(lobby_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения участия в матче."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить участие", callback_data=f"confirm_participation_{lobby_id}")]
    ])

def get_map_ban_keyboard(banned_maps: list, current_captain_id: int) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для бана карт."""
    keyboard = []
    for map_name in MAPS:
        if map_name not in banned_maps:
            keyboard.append([
                InlineKeyboardButton(
                    text=f"⚪ {map_name}",
                    callback_data=f"ban_map_{map_name}_{current_captain_id}"
                )
            ])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_match_result_keyboard(match_id: str) -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой отправки результатов."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Отправить результаты", callback_data=f"submit_result_{match_id}")]
    ])

def get_register_match_keyboard(match_id: str) -> InlineKeyboardMarkup:
    """Клавиатура для подтверждения регистрации матча администратором."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="✅ Зарегистрировать матч", 
                callback_data=f"register_match_{match_id}"
            )
        ]
    ]
    # [PG-REMOVED] 'thread_id' не является валидным параметром для InlineKeyboardMarkup
    # Он должен быть в bot.send_message
    )

def get_admin_post_registration_keyboard(match_id: str) -> InlineKeyboardMarkup:
    """
    Клавиатура, которая отправляется ПОСЛЕ успешной регистрации матча.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Перерегистрировать", callback_data=f"admin_reregister_{match_id}"),
            InlineKeyboardButton(text="❌ Отменить матч", callback_data=f"admin_cancel_{match_id}")
        ]
    ])

# ... (notify_players_of_change, start_broadcast - без изменений) ...
async def notify_players_of_change(bot: Bot, user_ids: list, message_text: str):
    """
    Отправляет уведомление списку игроков (пропуская ботов).
    """
    logger.info(f"Отправка уведомления {len(user_ids)} игрокам...")
    for user_id in user_ids:
        if user_id > 0:
            try:
                await bot.send_message(user_id, message_text, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"Не удалось уведомить {user_id} об изменении матча: {e}")

async def start_broadcast(admin_id: int, message_to_copy: types.Message, user_ids: list):
    """
    (НОВАЯ ФУНКЦИЯ)
    Асинхронно выполняет рассылку, копируя сообщение админа.
    Работает в фоновом режиме (через asyncio.create_task).
    """
    success_count = 0
    fail_count = 0
    
    total_users = len(user_ids)
    logger.info(f"Начало рассылки для {total_users} пользователей...")

    for i, user_id in enumerate(user_ids, 1):
        try:
            await message_to_copy.copy_to(chat_id=user_id)
            success_count += 1
        except (TelegramBadRequest, TelegramForbiddenError) as e:
            fail_count += 1
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка при рассылке пользователю {user_id}: {e}")
        
        await asyncio.sleep(0.05)
        
        if i % 100 == 0 or i == total_users:
             logger.info(f"Прогресс рассылки: {i}/{total_users} (Успешно: {success_count}, Ошибки: {fail_count})")
    
    try:
        await bot.send_message(
            admin_id,
            f"<b>📢 Рассылка завершена!</b>\n\n"
            f"✅ Успешно отправлено: <b>{success_count}</b>\n"
            f"❌ Ошибок (заблокировали бота): <b>{fail_count}</b>\n"
            f"👥 Всего пользователей: <b>{total_users}</b>",
            parse_mode="HTML"
        )
    except Exception:
        logger.error(f"Не удалось отправить отчет о рассылке админу {admin_id}")

# [ASYNC-REWRITE]
async def rollback_match_stats(match_id: str) -> tuple[bool, str, list]:
    """
    Откатывает статистику матча на основе данных из 'last_registration_data'.
    Возвращает (Success, Error Message, Affected User IDs)
    """
    match_db = await db_fetchone(
        """SELECT l.league, m.status, m.last_registration_data 
           FROM matches m 
           JOIN lobbies l ON m.lobby_id = l.lobby_id 
           WHERE m.match_id = $1""", 
        match_id
    )
    
    if not match_db:
        return False, f"Матч {match_id} не найден.", []
        
    if not match_db['last_registration_data']:
        return False, f"Нет данных для отката матча {match_id} (last_registration_data пусто).", []
        
    league_name = match_db['league']
    affected_user_ids = []

    try:
        old_data = json.loads(match_db['last_registration_data'])
        
        # [PG-REWRITE] Используем транзакцию для отката
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for player_stat in old_data:
                    user_id = player_stat['user_id']
                    affected_user_ids.append(user_id)
                    
                    elo_revert = -int(player_stat['elo_change'])
                    win_revert = -1 if player_stat['win'] == 1 else 0
                    loss_revert = -1 if player_stat['win'] == 0 else 0
                    kills_revert = player_stat['kills']
                    deaths_revert = player_stat['deaths']
                    score_revert = player_stat['score_change']
                    
                    await conn.execute("""
                        UPDATE user_league_stats SET 
                            elo = elo + $1, 
                            wins = CASE WHEN wins > 0 THEN wins + $2 ELSE 0 END, 
                            losses = CASE WHEN losses > 0 THEN losses + $3 ELSE 0 END, 
                            matches_played = CASE WHEN matches_played > 0 THEN matches_played - 1 ELSE 0 END,
                            kills = CASE WHEN kills >= $4 THEN kills - $4 ELSE 0 END, 
                            deaths = CASE WHEN deaths >= $5 THEN deaths - $5 ELSE 0 END, 
                            total_score = CASE WHEN total_score >= $6 THEN total_score - $6 ELSE 0 END
                        WHERE user_id = $7 AND league_name = $8
                    """, 
                        elo_revert, 
                        win_revert, 
                        loss_revert, 
                        kills_revert,
                        deaths_revert,
                        score_revert,
                        user_id, 
                        league_name
                    )
        
        logger.info(f"Успешный откат статистики для {match_id}.")
        return True, "", affected_user_ids

    except Exception as e:
        logger.error(f"Критическая ошибка при откате матча {match_id}: {e}", exc_info=True)
        return False, f"Ошибка JSON или БД: {e}", []

# ... (get_party_info_text, calculate_win_rate, calculate_kd, calculate_avg_score - без изменений) ...
def get_party_info_text(user_data: dict, teammate_data: dict) -> str:
    """Формирует текст для раздела 'Команды' (party_main.jpg / party.jpg)."""
    game_line = f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>"
    header_line = "<b>🎯 Ваша команда на Faceit:</b>"
    
    player1_id = user_data['user_id']
    player1_nickname = html.escape(user_data['nickname'])
    player1_line = (
        f"👤 Игрок 1: {player1_nickname} "
        f"(<b><code>{player1_id}</code></b>)"
    )
    
    if teammate_data:
        player2_id = teammate_data['user_id']
        player2_nickname = html.escape(teammate_data['nickname'])
        player2_line = (
            f"👥 Игрок 2: {player2_nickname} "
            f"(<b><code>{player2_id}</code></b>)"
        )
    else:
        player2_line = "👥 Игрок 2: Нет тиммейта"
        
    chat_link_line = f"<b>💬 Найти себе тиммейта можно в нашем чате: {RISE_CHAT_USERNAME}</b>"
    
    return "\n\n".join([
        game_line,
        header_line,
        player1_line,
        player2_line,
        chat_link_line
    ])

def calculate_win_rate(wins, played):
    """
    Рассчитывает процент побед.
    Возвращает строку, например "55.00%".
    """
    if played == 0:
        return "0.00%"
    
    win_rate = (wins / played) * 100
    return f"{win_rate:.2f}%"


def calculate_kd(kills, deaths):
    """Рассчитывает K/D."""
    if deaths == 0:
        return f"{float(kills):.2f}"
    
    kd_ratio = kills / deaths
    return f"{kd_ratio:.2f}"

def calculate_avg_kills(kills, played):
    """Рассчитывает среднее кол-во убийств (AVG Kills)."""
    if played == 0:
        return "0.00"
        
    # Убедимся, что kills не None, на всякий случай
    if kills is None:
        kills = 0
        
    avg = kills / played
    return f"{avg:.2f}"

# [ASYNC-REWRITE]
async def get_profile_text(user_id: int, current_date: str, league_to_display: str = None) -> tuple[str, str]:
    """
    Формирует красиво оформленный текст для профиля.
    Возвращает (profile_text, league_used)
    """

    if not user_id:
        return "❌ Ошибка: Профиль не найден. Пожалуйста, запустите команду /start.", DEFAULT_LEAGUE

    user_main_data = await db_fetchone("SELECT * FROM users WHERE user_id = $1", user_id)
    if not user_main_data:
        return "❌ Ошибка: Профиль не найден. Пожалуйста, запустите команду /start.", DEFAULT_LEAGUE

    display_league = league_to_display
    if display_league is None:
        display_league = user_main_data.get('league', DEFAULT_LEAGUE)

    # [ASYNC-REWRITE]
    user_stats_data = await get_user_league_stats(user_id, display_league)

    # Объединяем Record и dict
    user_data = {**user_main_data, **user_stats_data}

    # Добавляем звезду
    nickname = await format_nickname(user_id, user_data.get('nickname', 'N/A'))

    elo = user_data.get('elo', 0)
    
    league = display_league 
    level_emoji = get_faceit_level_emoji(elo) 
    
    played = user_data.get('matches_played', 0)
    wins = user_data.get('wins', 0)
    losses = user_data.get('losses', 0)
    kills = user_data.get('kills', 0)
    deaths = user_data.get('deaths', 0)
    total_score = user_data.get('total_score', 0)
    
    # Добавляем строку Premium, если есть звезда
    premium_line = ""
    if "⭐️" in nickname:
        premium_line = "⭐️ <b>Premium:</b> Активирован\n"

    header = (
        f"<blockquote><b>👤 Профиль Игрока</b></blockquote>"
        f"<blockquote><b>{nickname}</b></blockquote>\n"
    )

    rating_block = (
        f"🏆 <b>Лига:</b> {level_emoji} (<b>{league}</b>)\n"
        f"📈 <b>Рейтинг ELO:</b> <b>{elo}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖➖\n"
        f"{premium_line}" # Вставляем здесь
    )

    stats_block = (
        "📊 <b>Статистика Матчей:</b>\n"
        f"• Сыграно матчей: <b>{played}</b>\n"
        f"• Побед/Поражений: <b>{wins}</b> / <b>{losses}</b>\n"
        f"• W/R: <b>{calculate_win_rate(wins, played)}</b>\n"
        f"• K/D: <b>{calculate_kd(kills, deaths)}</b>\n"
        f"• AVG Убийств: <b>{calculate_avg_kills(kills, played)}</b>\n"
        f"➖➖➖➖➖➖➖➖➖➖➖\n"
    )

    info_block = (
        f"🆔 <b>Game ID:</b> <code>{user_data.get('game_id', 'N/A')}</code>\n"
        f"💻 <b>Платформа:</b> {user_data.get('device', 'N/A')}\n"
        f"🗓️ <b>Регистрация:</b> {user_data.get('registration_date', 'N/A')}\n"
        f"⏰ <b>Обновлено:</b> {current_date}"
    )
    
    final_text = "\n".join([header, rating_block, stats_block, info_block])
    
    return final_text, league

async def send_main_menu(chat_id: int, user_id: int, message_to_edit: types.Message = None):
    """Формирует и отправляет/редактирует главное меню с Inline-клавиатурой."""
    
    user_data = await get_cached_user_data(user_id)
    
    if not user_data:
        try:
            await bot.send_message(chat_id, "Ошибка: ваш профиль не найден. Пожалуйста, нажмите /start")
        except Exception:
            pass
        return

    nickname = user_data.get('nickname', 'Игрок')
    
    # Форматируем ник со звездой
    formatted_nick = await format_nickname(user_id, nickname)
    
    # Проверка премиума для текста ниже
    premium_text = ""
    if "⭐️" in formatted_nick:
        premium_text = "\n⭐️ <b>Premium: Активирован</b>"

    game_line = f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>"
    nickname_line = f"<blockquote><b>👋 Привет, {formatted_nick}</b></blockquote>"
    
    main_menu_text = (
        f"{game_line}\n"
        f"{nickname_line}"
        f"{premium_text}\n\n"
        f"<b>💬 Наш чат: {RISE_CHAT_USERNAME}</b>\n\n"
        f"<b>📌 Выберите действие в меню ниже 👇</b>" 
    )

    keyboard = await get_main_menu_keyboard(user_id)
    photo_id = MAIN_MENU_FILE_ID 

    if message_to_edit:
        try:
            await message_to_edit.edit_media(
                media=InputMediaPhoto(media=photo_id, caption=main_menu_text, parse_mode="HTML"),
                reply_markup=keyboard
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                # Если редактирование не удалось, отправляем новое сообщение
                await bot.send_photo(chat_id, photo=photo_id, caption=main_menu_text, reply_markup=keyboard, parse_mode="HTML")
                # И пытаемся удалить старое, чтобы избежать дублирования
                try:
                    await message_to_edit.delete()
                except Exception:
                    pass
    else:
        # Если message_to_edit не передан, просто отправляем новое сообщение
        await bot.send_photo(chat_id, photo=photo_id, caption=main_menu_text, reply_markup=keyboard, parse_mode="HTML")

# --- ГЕНЕРАЦИЯ ПРОМОКОДОВ (OWNER) ---
@dp.callback_query(F.data == "admin_create_promo", MinRoleFilter(ROLE_OWNER)) # Добавьте эту кнопку в админ панель!
async def admin_promo_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    text = "<b>🎁 Создание Промокода</b>\n\nВыберите, что дает код:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Premium", callback_data="promo_type_premium")],
        [InlineKeyboardButton(text="🌟 QUAL Access", callback_data="promo_type_qual")],
        [InlineKeyboardButton(text="🏆 FPL Access", callback_data="promo_type_fpl")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_admin")]
    ])
    await callback.message.edit_media(
        media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=text, parse_mode="HTML"), reply_markup=kb
    )
    await state.set_state(AdminPromo.waiting_for_role_type)

@dp.callback_query(AdminPromo.waiting_for_role_type, F.data.startswith("promo_type_"))
async def admin_promo_type(callback: types.CallbackQuery, state: FSMContext):
    r_type = callback.data.split("_")[-1]
    await state.update_data(promo_reward=r_type)
    
    await callback.message.edit_caption(
        caption="<b>⏱ Введите длительность действия (в днях):</b>\n(0 = навсегда)",
        reply_markup=None, parse_mode="HTML"
    )
    await state.set_state(AdminPromo.waiting_for_duration)
    await callback.answer()

@dp.message(AdminPromo.waiting_for_duration, F.text)
async def admin_promo_duration(message: types.Message, state: FSMContext):
    try:
        days = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число.")
        return
    await state.update_data(promo_days=days)
    await message.answer("<b>🔢 Введите количество активаций (например, 1, 10, 100):</b>")
    await state.set_state(AdminPromo.waiting_for_uses)

@dp.message(AdminPromo.waiting_for_uses, F.text)
async def admin_promo_finish(message: types.Message, state: FSMContext):
    try:
        uses = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число.")
        return
        
    data = await state.get_data()
    reward = data['promo_reward']
    days = data['promo_days']
    
    # Генерация кода
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    code = "".join(random.choice(chars) for _ in range(12))
    
    await db_execute(
        "INSERT INTO promo_codes (code, reward_type, duration_days, uses_left) VALUES ($1, $2, $3, $4)",
        code, reward, days, uses
    )
    
    await message.answer(
        f"✅ <b>Промокод создан!</b>\n\n"
        f"🔑 Код: <code>{code}</code>\n"
        f"🎁 Награда: {reward.upper()}\n"
        f"⏳ Дней: {days if days > 0 else 'Навсегда'}\n"
        f"👥 Активаций: {uses}",
        parse_mode="HTML"
    )
    await state.clear()

@dp.callback_query(F.data == "profile_premium")
async def profile_premium_info(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_data = await get_cached_user_data(user_id)
    
    is_premium = False
    expires_str = "Не активен"
    
    if user_data and user_data.get('premium_until'):
        p_date = user_data['premium_until']
        if isinstance(p_date, str): # На всякий случай
             p_date = datetime.fromisoformat(p_date)
        if p_date > datetime.now():
            is_premium = True
            expires_str = p_date.strftime("%d.%m.%Y")
            
    text = (
        "<b>⭐️ PREMIUM STATUS</b>\n\n"
        f"✅ <b>Статус:</b> {('Активен' if is_premium else 'Не активен')}\n"
        f"📅 <b>Истекает:</b> {expires_str}\n\n"
        "<b>Возможности Premium:</b>\n"
        "• 👑 Приоритет капитана (вы всегда становитесь капитаном)\n"
        "• 👥 Расширенное лобби (до 3 игроков в команде)\n"
        "• 📈 Бонус к ELO (+5 за победу / -5 за поражение)\n"
        "• ✨ Уникальная звезда возле никнейма"
    )
    
    # Если премиума нет - показываем кнопки Купить/Назад
    if not is_premium:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Купить", callback_data="buy_premium_click")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_profile")]
        ])
    else:
        # Если есть - только Назад
        keyboard = get_back_to_menu_keyboard("main_profile")
    
    await callback.message.edit_caption(
        caption=text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "buy_premium_click")
async def buy_premium_alert_handler(callback: types.CallbackQuery):
    """Показывает алерт при попытке купить."""
    await callback.answer("У вас нет этой роли, приобрести можно у @jackha1337", show_alert=True)

# --- АКТИВАЦИЯ ПРОМОКОДА (USER) ---
@dp.callback_query(F.data == "main_promo")
async def main_promo_handler(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_media(
        media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption="<b>🎁 Введите ваш промокод:</b>", parse_mode="HTML"),
        reply_markup=get_back_to_menu_keyboard("back_to_main_menu")
    )
    await state.set_state(ActivatePromo.waiting_for_code)

@dp.message(ActivatePromo.waiting_for_code, F.text)
async def process_promo_activation(message: types.Message, state: FSMContext):
    code_input = message.text.strip()
    user_id = message.from_user.id
    
    promo = await db_fetchone("SELECT * FROM promo_codes WHERE code = $1", code_input)
    
    if not promo or promo['uses_left'] <= 0:
        await message.answer("❌ Промокод не найден или закончился.")
        await state.clear()
        return
        
    # Активация
    reward = promo['reward_type']
    days = promo['duration_days']
    
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            if reward == 'premium':
                if days > 0:
                    until_date = datetime.now() + timedelta(days=days)
                    await conn.execute("UPDATE users SET premium_until = $1 WHERE user_id = $2", until_date, user_id)
                else:
                    # Навсегда (ставим далекий год)
                    await conn.execute("UPDATE users SET premium_until = '2099-01-01 00:00:00' WHERE user_id = $1", user_id)
                    
            elif reward == 'qual':
                await conn.execute("UPDATE users SET league = $1 WHERE user_id = $2", QUAL_LEAGUE, user_id)
                
            elif reward == 'fpl':
                await conn.execute("UPDATE users SET league = $1 WHERE user_id = $2", FPL_LEAGUE, user_id)
            
            # Уменьшаем использования
            await conn.execute("UPDATE promo_codes SET uses_left = uses_left - 1 WHERE code = $1", code_input)

    await clear_user_cache(user_id)
    await message.answer(f"✅ <b>Промокод активирован!</b> Вы получили: {reward.upper()}", parse_mode="HTML")
    await send_main_menu(message.chat.id, user_id, None)
    await state.clear()

async def format_nickname(user_id: int, raw_nickname: str) -> str:
    """Добавляет звезду, если у пользователя активен премиум."""
    user_data = await get_cached_user_data(user_id)
    if not user_data: return html.escape(raw_nickname)
    
    premium_until = user_data.get('premium_until')
    is_premium = False
    if premium_until:
        if isinstance(premium_until, str):
             premium_until = datetime.fromisoformat(premium_until)
        if premium_until > datetime.now():
            is_premium = True
            
    nick = html.escape(raw_nickname)
    return f"{nick}⭐️" if is_premium else nick

@dp.callback_query(F.data == "back_to_main_menu")
async def back_to_main_menu_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Возвращает пользователя в главное меню из любого раздела. (Оптимизировано)"""

    # [FIX] Оборачиваем answer() в try-except, чтобы игнорировать старые нажатия
    try:
        await callback.answer()
    except TelegramBadRequest:
        pass # Игнорируем ошибку, если кнопка "протухла"

    await state.clear() 
    user_id = callback.from_user.id
    
    # [ОПТИМИЗАЦИЯ]
    # Просто вызываем send_main_menu. 
    # Он сам возьмет все данные из кэша (или 1 запросом).
    await send_main_menu(
        callback.message.chat.id, 
        user_id, 
        message_to_edit=callback.message
    )

# [ASYNC-REWRITE]
async def broadcast_lobby_update(lobby_id: int, bot: Bot, text: str):
    """(PG) Обновляет сообщения лобби (с картинкой) у всех участников."""
    logger.info(f"Начало трансляции обновления для лобби {lobby_id}")
    members = await db_fetchall(
        "SELECT user_id, lobby_message_id FROM lobby_members WHERE lobby_id = $1", 
        lobby_id
    )
    
    keyboard = get_lobby_keyboard(lobby_id)
    
    for member in members:
        user_id = member['user_id']
        message_id = member['lobby_message_id']
        
        if user_id < 0: 
            logger.info(f"Пропуск отправки Telegram-сообщения для ID бота: {user_id}")
            continue 

        should_send_new = True 
        
        if message_id:
            try:
                await bot.edit_message_media(
                    chat_id=user_id,
                    message_id=message_id,
                    media=InputMediaPhoto(
                        media=LOBBY_FILE_ID,
                        caption=text,
                        parse_mode="HTML"
                    ),
                    reply_markup=keyboard
                )
                logger.info(f"Обновлено (edit media) для {user_id}")
                should_send_new = False
            
            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    logger.info(f"Сообщение для {user_id} (msg {message_id}) не изменилось, пропуск.")
                    should_send_new = False 
                else:
                    logger.warning(f"Ошибка редактирования (media) для {user_id} (msg {message_id}): {e}. Попытка отправки нового.")
            except Exception as e:
                logger.error(f"Непредвиденная ошибка при редактировании (media) для {user_id}: {e}. Попытка отправки нового.")

        if should_send_new:
            try:
                sent_msg = await bot.send_photo(
                    user_id,
                    photo=LOBBY_FILE_ID,
                    caption=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                # [ASYNC-REWRITE]
                await db_execute(
                    "UPDATE lobby_members SET lobby_message_id = $1 WHERE user_id = $2 AND lobby_id = $3",
                    sent_msg.message_id, user_id, lobby_id
                )
                logger.info(f"Обновлено (send photo) для {user_id}, сохранен msg_id {sent_msg.message_id}")
            except Exception as e:
                logger.error(f"Непредвиденная ошибка при отправке (photo) пользователю {user_id}: {e}")

# [ASYNC-REWRITE]
async def update_lobby_message(lobby_id: int, bot: Bot):
    """(PG) Обновляет сообщение лобби для всех участников."""
    members = await db_fetchall("SELECT user_id FROM lobby_members WHERE lobby_id = $1", lobby_id)
    
    lobby_text = await get_lobby_text(lobby_id)
    lobby_data = await db_fetchone("SELECT current_players FROM lobbies WHERE lobby_id = $1", lobby_id)
    
    for member in members:
        try:
            await bot.send_message(
                member['user_id'],
                lobby_text,
                reply_markup=get_lobby_keyboard(lobby_id),
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка обновления для пользователя {member['user_id']}: {e}")

# [ASYNC-REWRITE]
async def get_lobby_text(lobby_id: int) -> str:
    """Формирует текст для отображения лобби со звездами."""
    lobby = await db_fetchone("SELECT * FROM lobbies WHERE lobby_id = $1", lobby_id)
    if not lobby: return "Ошибка: лобби не найдено."
        
    league = lobby['league']
    current = lobby['current_players']
    
    text = (
        f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>\n"
        f"<blockquote><b>{league}</b></blockquote>\n"
        f"<b>🎮 ЛОББИ 5v5</b>\n\n"
        f"<b>✅ Вы в Лобби (5v5)</b>\n\n"
        f"<b>Игроков в лобби: {current}/10</b>\n\n"
        f"<b>Игроки в лобби:</b>\n"
    )

    query = """
    SELECT u.user_id, u.nickname, COALESCE(s.elo, 0) as elo
    FROM lobby_members lm
    JOIN users u ON lm.user_id = u.user_id
    LEFT JOIN user_league_stats s ON u.user_id = s.user_id AND s.league_name = $1
    WHERE lm.lobby_id = $2
    ORDER BY lm.joined_at
    """
    
    members_data = await db_fetchall(query, league, lobby_id)
    
    if not members_data:
        text += "<i>...Лобби пусто...</i>"
    else:
        for member in members_data:
            elo = member.get('elo', 0)
            level_emoji = get_faceit_level_emoji(elo)
            # Добавляем звезду
            formatted_nick = await format_nickname(member['user_id'], member['nickname'])
            text += f"{level_emoji} {formatted_nick} (ELO: {elo})\n"
    
    return text

# [ASYNC-REWRITE]
async def cleanup_expired_mutes():
    """
    (PG) Очищает истекшие муты И сбрасывает варны, если мут истек.
    """
    current_time = datetime.now() # PG работает с объектами datetime
    
    await db_execute(
        "UPDATE users SET muted_until = NULL, warns = 0 "
        "WHERE muted_until IS NOT NULL AND muted_until < $1", 
        current_time
    )
    logger.info("Истекшие муты и связанные с ними варны очищены.")

# [ASYNC-REWRITE]
async def get_dynamic_confirmation_keyboard(lobby_id: int) -> InlineKeyboardMarkup:
    """
    (PG) Генерирует клавиатуру 5x2 с ✅ и ⬜️, показывая, кто подтвердил.
    """
    
    members = await db_fetchall(
        "SELECT confirmed FROM lobby_members WHERE lobby_id = $1 ORDER BY id", 
        lobby_id
    )
    
    slots = []
    for member in members:
        slots.append("✅" if member['confirmed'] else "⬜️")
    
    if len(slots) < 10:
        slots.extend(["⬜️"] * (10 - len(slots)))
    
    row1_buttons = [InlineKeyboardButton(text=s, callback_data="ignore") for s in slots[:5]]
    row2_buttons = [InlineKeyboardButton(text=s, callback_data="ignore") for s in slots[5:]]
    
    confirm_button = [
        InlineKeyboardButton(
            text="✅ Подтвердить участие", 
            callback_data=f"confirm_participation_{lobby_id}"
        )
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=[row1_buttons, row2_buttons, confirm_button])

# [ASYNC-REWRITE]
async def broadcast_ready_check_update(lobby_id: int, bot: Bot):
    """
    (PG) Отправляет/обновляет сообщение о подтверждении (с 10 слотами)
    всем 10 участникам.
    """
    logger.info(f"[Lobby {lobby_id}] Обновление Ready Check...")
    
    keyboard = await get_dynamic_confirmation_keyboard(lobby_id)
    
    text = (
        "<b>🎮 Подтвердите участие в матче!</b>\n\n"
        "Когда все 10 игроков подтвердят участие — матч начнётся."
    )
    
    members = await db_fetchall(
        "SELECT user_id, lobby_message_id FROM lobby_members WHERE lobby_id = $1", 
        lobby_id
    )
    
    for member in members:
        user_id = member['user_id']
        message_id = member['lobby_message_id']
        
        if user_id < 0:
            continue

        should_send_new = True 
        
        if message_id:
            try:
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                should_send_new = False
                logger.info(f"Ready Check (edit) для {user_id} [OK]")

            except TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    logger.info(f"Ready Check (edit) для {user_id} [Not Modified]")
                    should_send_new = False 
                    
                elif "chat not found" in str(e) or "bot was blocked" in str(e) or "user is deactivated" in str(e):
                    logger.error(f"Ошибка Ready Check (chat not found/blocked) для {user_id}.")
                    should_send_new = False
                
                else:
                    logger.warning(f"Ошибка редактирования Ready Check для {user_id} (msg {message_id}): {e}. Попытка отправки нового.")

            except Exception as e:
                logger.error(f"Непредвиденная ошибка при редактировании Ready Check для {user_id}: {e}. Попытка отправки нового.")
        
        if should_send_new:
            try:
                sent_msg = await bot.send_message(
                    user_id,
                    text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
                # [ASYNC-REWRITE]
                await db_execute(
                    "UPDATE lobby_members SET lobby_message_id = $1 WHERE user_id = $2 AND lobby_id = $3",
                    sent_msg.message_id, user_id, lobby_id
                )
                logger.info(f"Ready Check (send new) для {user_id} [OK]")
            except Exception as e_send:
                logger.error(f"Не удалось отправить (новое) Ready Check {user_id}: {e_send}")

# [ASYNC-REWRITE]
async def broadcast_final_message(lobby_id: int, bot: Bot, text: str):
    """
    (PG) Редактирует сообщения у всех участников, убирая клавиатуру.
    Используется для "Все подтвердили" или "Подтверждение провалено".
    """
    members = await db_fetchall(
        "SELECT user_id, lobby_message_id FROM lobby_members WHERE lobby_id = $1", 
        lobby_id
    )
    
    for member in members:
        user_id = member['user_id']
        message_id = member['lobby_message_id']
        
        if user_id < 0: continue

        try:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=message_id,
                text=text,
                reply_markup=None,
                parse_mode="HTML"
            )
        except Exception:
            pass

# [ASYNC-REWRITE]
async def handle_warn(user_id: int, bot: Bot):
    """
    (PG) Выдает варн игроку и проверяет на мут.
    """
    user_data = await db_fetchone("SELECT warns FROM users WHERE user_id = $1", user_id)
    current_warns = user_data.get('warns', 0) if user_data else 0
    
    new_warns = current_warns + 1
    
    if new_warns >= 3:
        mute_until = datetime.now() + timedelta(hours=2) # PG
        await db_execute(
            "UPDATE users SET warns = 0, muted_until = $1 WHERE user_id = $2", 
            mute_until, user_id
        )
        
        try:
            await bot.send_message(
                user_id,
                "<b>❗️ Вы получили 3/3 варна за уклонение от матчей.</b>\n\n"
                f"🔇 Вы замучены на <b>2 часа</b>.\n"
                f"Счетчик варнов сброшен.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Не удалось уведомить {user_id} о муте: {e}")
            
    else:
        await db_execute("UPDATE users SET warns = $1 WHERE user_id = $2", new_warns, user_id)
        
        try:
            await bot.send_message(
                user_id,
                f"<b>⚠️ Вы не подтвердили участие в матче!</b>\n\n"
                f"Вам выдан варн (<b>{new_warns}/3</b>).\n"
                f"При получении 3 варнов вы будете замучены.",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"Не удалось уведомить {user_id} о варне: {e}")

# [ASYNC-REWRITE]
async def start_confirmation_timer(lobby_id: int, bot: Bot):
    """
    (PG) Таймер на 60 секунд. Кикает ТОЛЬКО неподтвердивших.
    """
    await asyncio.sleep(60)
    
    try:
        lobby = await db_fetchone("SELECT status FROM lobbies WHERE lobby_id = $1", lobby_id)
        if not lobby or lobby['status'] != 'confirming':
            logger.info(f"[Timer {lobby_id}] Таймер отменен (матч начался или отменен).")
            return

        logger.info(f"[Timer {lobby_id}] Время вышло! Проверка...")
        
        not_confirmed_users = await db_fetchall(
            "SELECT user_id FROM lobby_members WHERE lobby_id = $1 AND confirmed = FALSE",
            lobby_id
        )
        
        if not_confirmed_users:
            logger.info(f"[Timer {lobby_id}] Провал! {len(not_confirmed_users)} не подтвердили.")
            
            # 1. Выдаем варны и уведомляем кикнутых
            for user in not_confirmed_users:
                uid = user['user_id']
                if uid > 0: # Не уведомляем ботов
                    await handle_warn(uid, bot)
                    try:
                        await bot.send_message(
                            uid,
                            "<b>⚠️ Вы были исключены из лобби за отсутствие подтверждения.</b>",
                            parse_mode="HTML"
                        )
                        # Возвращаем кикнутого в главное меню
                        await send_main_menu(uid, uid, None)
                    except Exception:
                        pass

            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    # 2. Удаляем из БД только неподтвердивших
                    await conn.execute(
                        "DELETE FROM lobby_members WHERE lobby_id = $1 AND confirmed = FALSE", 
                        lobby_id
                    )
                    
                    # 3. Сбрасываем статус confirmed у оставшихся
                    await conn.execute(
                        "UPDATE lobby_members SET confirmed = FALSE WHERE lobby_id = $1", 
                        lobby_id
                    )
                    
                    # 4. Обновляем статус лобби на waiting и пересчитываем игроков
                    remaining_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1", 
                        lobby_id
                    )
                    
                    await conn.execute(
                        "UPDATE lobbies SET current_players = $1, status = 'waiting' WHERE lobby_id = $2", 
                        remaining_count, lobby_id
                    )

            # 5. Обновляем интерфейс оставшимся игрокам
            logger.info(f"[Timer {lobby_id}] Лобби возвращено в поиск. Осталось: {remaining_count}")
            
            lobby_text = await get_lobby_text(lobby_id)
            await broadcast_lobby_update(lobby_id, bot, lobby_text)
            
        else:
            logger.info(f"[Timer {lobby_id}] Все подтвердили (обнаружено таймером, но матч должен был начаться раньше).")

    except Exception as e:
        logger.error(f"Критическая ошибка в таймере {lobby_id}: {e}", exc_info=True)

# [ASYNC-REWRITE]
async def simulate_bot_confirmation(lobby_id: int, bot: Bot):
    """
    (PG) Симулирует нажатие "Подтвердить" для всех ботов в лобби
    """
    await asyncio.sleep(2.0) 
    
    bots_in_lobby = await db_fetchall(
        "SELECT user_id FROM lobby_members WHERE lobby_id = $1 AND user_id < 0 AND confirmed = FALSE", 
        lobby_id
    )
    
    if not bots_in_lobby:
        return

    logger.info(f"[Bot Sim {lobby_id}] Начинаю симуляцию для {len(bots_in_lobby)} ботов...")

    random.shuffle(bots_in_lobby)

    for bot_member in bots_in_lobby:
        bot_id = bot_member['user_id']
        
        lobby = await db_fetchone("SELECT status FROM lobbies WHERE lobby_id = $1", lobby_id)
        if not lobby or lobby['status'] != 'confirming':
            logger.info(f"[Bot Sim {lobby_id}] Симуляция остановлена: статус лобби {lobby.get('status', 'N/A')}.")
            return

        delay = random.uniform(0.5, 1.0)
        await asyncio.sleep(delay)
        
        await db_execute(
            "UPDATE lobby_members SET confirmed = TRUE WHERE user_id = $1 AND lobby_id = $2", 
            bot_id, lobby_id
        )
        logger.info(f"[Bot Sim {lobby_id}] Бот {bot_id} 'подтвердил' участие.")

        await broadcast_ready_check_update(lobby_id, bot)
        
        confirmed_count = await db_fetchone(
            "SELECT COUNT(*) as count FROM lobby_members WHERE lobby_id = $1 AND confirmed = TRUE",
            lobby_id
        )
        
        if confirmed_count and confirmed_count['count'] == 10:
            logger.info(f"[Bot Sim {lobby_id}] Бот {bot_id} был 10-м! Запускаем матч.")
            
            await broadcast_final_message(
                lobby_id, 
                bot, 
                "<b>✅ Все игроки подтвердили участие!</b>\n"
                "🚀 Запускаем выбор капитанов и бан карт..."
            )
            await start_captain_selection(lobby_id, bot)

            return

# [ASYNC-REWRITE]
async def start_confirmation_phase(lobby_id: int, bot: Bot):
    """
    (PG) Запускает фазу 'Ready Check' (подтверждение) для 10 игроков.
    """
    logger.info(f"[Lobby {lobby_id}] Запуск фазы подтверждения (Ready Check)...")

    members = await db_fetchall("SELECT user_id FROM lobby_members WHERE lobby_id = $1", lobby_id)
    for member in members:
        user_id = member['user_id']
        if user_id > 0:
            try:
                await bot.send_message(
                    user_id,
                    "<b>✅ Лобби заполнено (10/10)!</b>\n\n"
                    "Сейчас вам придет сообщение для подтверждения участия.",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить pre-confirm-notify {user_id}: {e}")
    
    await asyncio.sleep(1.5)
    
    await db_execute("UPDATE lobbies SET status = 'confirming' WHERE lobby_id = $1", lobby_id)
    await db_execute("UPDATE lobby_members SET confirmed = FALSE WHERE lobby_id = $1", lobby_id)
    
    await broadcast_ready_check_update(lobby_id, bot)
    
    asyncio.create_task(start_confirmation_timer(lobby_id, bot))
    asyncio.create_task(simulate_bot_confirmation(lobby_id, bot))

# --- Инициализация Aiogram ---

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("confirm_participation_"))
async def confirm_participation_handler(callback: types.CallbackQuery):
    """
    (PG) Обрабатывает нажатие кнопки '✅ Подтвердить участие'.
    """
    user_id = callback.from_user.id
    try:
        lobby_id = int(callback.data.split("_")[-1])
    except:
        await callback.answer("❌ Ошибка: ID лобби не найден.", show_alert=True)
        return

    member = await db_fetchone(
        """SELECT lm.confirmed, l.status 
           FROM lobby_members lm 
           JOIN lobbies l ON lm.lobby_id = l.lobby_id
           WHERE lm.user_id = $1 AND lm.lobby_id = $2""",
        user_id, lobby_id
    )
    
    if not member:
        await callback.answer("❌ Вы больше не в этом лобби.", show_alert=True)
        return
    
    if member['status'] != 'confirming':
        await callback.answer("⚠️ Подтверждение уже завершено.", show_alert=False)
        return

    if member['confirmed']:
        await callback.answer("✅ Вы уже подтвердили участие.", show_alert=False)
        return
    
    await db_execute(
        "UPDATE lobby_members SET confirmed = TRUE WHERE user_id = $1 AND lobby_id = $2",
        user_id, lobby_id
    )
    
    await callback.answer("✅ Вы подтвердили участие!", show_alert=False)

    await broadcast_ready_check_update(lobby_id, bot)
    
    confirmed_count = await db_fetchone(
        "SELECT COUNT(*) as count FROM lobby_members WHERE lobby_id = $1 AND confirmed = TRUE",
        lobby_id
    )
    
    if confirmed_count and confirmed_count['count'] == 10:
        logger.info(f"[Lobby {lobby_id}] Все 10/10 подтвердили!")
        
        await broadcast_final_message(
            lobby_id, 
            bot, 
            "<b>✅ Все игроки подтвердили участие!</b>\n"
            "🚀 Запускаем выбор капитанов и бан карт..."
        )
        
        await start_captain_selection(lobby_id, bot)

# [ASYNC-REWRITE]
async def start_captain_selection(lobby_id: int, bot: Bot):
    """(PG) Выбирает двух капитанов (с учетом пати) и начинает фазу банов карт."""
    
    # 1. Получаем всех участников
    members = await db_fetchall("SELECT user_id FROM lobby_members WHERE lobby_id = $1", lobby_id)
    all_member_ids = [m['user_id'] for m in members]
    
    premium_members = []
    
    # 2. Определяем премиумов
    for uid in all_member_ids:
        ud = await get_cached_user_data(uid)
        is_prem = False
        if ud and ud.get('premium_until') and ud['premium_until'] > datetime.now():
            is_prem = True
        
        if is_prem: premium_members.append(uid)
    
    # --- ЛОГИКА ВЫБОРА КАПИТАНОВ ---
    captain1_id = None
    captain2_id = None

    # Шаг A: Выбираем первого капитана (приоритет премиумам)
    pool_for_cap1 = premium_members if premium_members else all_member_ids
    if not pool_for_cap1: # Если вдруг список пуст (аварийно)
        pool_for_cap1 = all_member_ids
        
    captain1_id = random.choice(pool_for_cap1)

    # Шаг B: Определяем тиммейтов первого капитана, чтобы НЕ выбрать их вторым капитаном
    cap1_data = await get_cached_user_data(captain1_id)
    excluded_from_cap2 = {captain1_id} # Самого себя тоже исключаем
    
    if cap1_data:
        if cap1_data.get('teammate_user_id'): 
            excluded_from_cap2.add(cap1_data['teammate_user_id'])
        if cap1_data.get('teammate2_user_id'): 
            excluded_from_cap2.add(cap1_data['teammate2_user_id'])

    # Шаг C: Выбираем второго капитана
    # Сначала пытаемся найти премиума, который НЕ друг первого кэпа
    premium_candidates_cap2 = [uid for uid in premium_members if uid not in excluded_from_cap2]
    
    if premium_candidates_cap2:
        captain2_id = random.choice(premium_candidates_cap2)
    else:
        # Если премиумов больше нет (или они все в пати с первым), берем обычных игроков
        # Но тоже проверяем, чтобы они не были в пати с первым
        regular_candidates_cap2 = [uid for uid in all_member_ids if uid not in excluded_from_cap2]
        
        if regular_candidates_cap2:
            captain2_id = random.choice(regular_candidates_cap2)
        else:
            # КРАЙНИЙ СЛУЧАЙ: Все 10 человек в одном пати? (невозможно по лимитам, но для защиты)
            # Просто берем любого, кто не кэп 1
            fallback_pool = [uid for uid in all_member_ids if uid != captain1_id]
            captain2_id = random.choice(fallback_pool)

    # --- СОХРАНЕНИЕ ---
    match_id = f"match_{lobby_id}_{int(datetime.now().timestamp())}"
    await db_execute(
        "INSERT INTO matches (match_id, lobby_id, captain1_id, captain2_id, banned_maps, status) VALUES ($1, $2, $3, $4, $5, 'picking')",
        match_id, lobby_id, captain1_id, captain2_id, ""
    )

    logger.info(f"Матч {match_id} создан. Капитаны: {captain1_id} vs {captain2_id}")

    await start_map_banning(match_id, captain1_id, captain2_id, bot)

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
async def notify_all_players_of_ban_state(match_id: str, lobby_id: int, captain1_id: int, captain2_id: int, banned_maps: list, current_captain_id: int, bot: Bot):
    logger.info(f"[Match {match_id}] Обновление состояния банов для лобби {lobby_id}")
    
    members = await db_fetchall("SELECT user_id, lobby_message_id FROM lobby_members WHERE lobby_id = $1", lobby_id)
    
    captain1 = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", captain1_id)
    captain2 = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", captain2_id)
    current_captain_data = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", current_captain_id)
    
    # Добавляем звезды
    c1_nick = await format_nickname(captain1_id, captain1['nickname']) if captain1 else "Капитан 1"
    c2_nick = await format_nickname(captain2_id, captain2['nickname']) if captain2 else "Капитан 2"
    current_nick = await format_nickname(current_captain_id, current_captain_data['nickname']) if current_captain_data else "Капитан"

    ban_text = (
        f"<b>🚀 FACEIT BAN LOBBY — PROJECT EVOLUTION</b>\n\n"
        f"<b>🧩 Формат: Best of 1 (7 карт)</b>\n\n"
        f"👑 <b>Игрок 1:</b> {c1_nick}\n"
        f"⚔️ <b>Игрок 2:</b> {c2_nick}\n\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"📜 <b>Карты для бана:</b>\n\n"
    )
    
    for i, m in enumerate(MAPS, 1):
        if m in banned_maps:
            ban_text += f"{i}️⃣ ❌ {m}\n"
        else:
            ban_text += f"{i}️⃣ {m} ⚪\n"
    
    ban_text += f"\n🕹️ <b>Сейчас банит:</b> {current_nick}\n"
    
    is_human_captain = current_captain_id > 0
    if is_human_captain: ban_text += "(нажми на карту, чтобы забанить)"

    for member in members:
        user_id = member['user_id']
        message_id = member['lobby_message_id']
        if user_id < 0: continue 
            
        keyboard = None
        if is_human_captain and user_id == current_captain_id:
            keyboard = get_map_ban_keyboard(banned_maps, current_captain_id)
        
        if message_id:
            try:
                await bot.edit_message_text(chat_id=user_id, message_id=message_id, text=ban_text, reply_markup=keyboard, parse_mode="HTML")
                continue
            except Exception: pass
        
        try:
            sent_msg = await bot.send_message(user_id, ban_text, reply_markup=keyboard, parse_mode="HTML")
            await db_execute("UPDATE lobby_members SET lobby_message_id = $1 WHERE user_id = $2 AND lobby_id = $3", sent_msg.message_id, user_id, lobby_id)
        except Exception: pass

# [ASYNC-REWRITE]
async def handle_next_ban_turn(match_id: str, bot: Bot):
    """
    (PG) Проверяет, чья очередь банить. 
    """
    logger.info(f"[Match {match_id}] Обработка следующего бана...")
    
    match = await db_fetchone("SELECT * FROM matches WHERE match_id = $1", match_id)
    if not match:
        logger.error(f"[Match {match_id}] Матч не найден в handle_next_ban_turn.")
        return

    banned_maps_str = match['banned_maps']
    banned_maps = banned_maps_str.split(",") if banned_maps_str else []
    
    lobby_id = match['lobby_id']
    captain1_id = match['captain1_id']
    captain2_id = match['captain2_id']
    
    if len(banned_maps) == 6:
        final_map = [m for m in MAPS if m not in banned_maps][0]
        logger.info(f"[Match {match_id}] Баны завершены. Карта: {final_map}")
        
        await db_execute("UPDATE matches SET status = 'ongoing' WHERE match_id = $1", match_id)
        await finalize_match_setup(match_id, final_map, bot)
        return

    if len(banned_maps) % 2 == 0:
        current_captain_id = captain1_id
    else:
        current_captain_id = captain2_id
        
    logger.info(f"[Match {match_id}] Очередь банить: {current_captain_id}")

    await notify_all_players_of_ban_state(
        match_id, lobby_id, captain1_id, captain2_id, 
        banned_maps, current_captain_id, bot
    )

    if current_captain_id < -10000: # ID бота
        logger.info(f"[Match {match_id}] Капитан {current_captain_id} - бот. Выполняю авто-бан...")
        
        await asyncio.sleep(4) 
        
        available_maps = [m for m in MAPS if m not in banned_maps]
        if not available_maps:
             logger.error(f"[Match {match_id}] У бота нет карт для бана.")
             return
             
        map_to_ban = random.choice(available_maps)
        banned_maps.append(map_to_ban)
        
        await db_execute(
            "UPDATE matches SET banned_maps = $1 WHERE match_id = $2",
            ",".join(banned_maps), match_id
        )
        
        logger.info(f"[Match {match_id}] Бот забанил: {map_to_ban}")
        
        await handle_next_ban_turn(match_id, bot)

    elif current_captain_id > 0:
        logger.info(f"[Match {match_id}] Запуск 20-сек таймера для {current_captain_id}")
        
        current_ban_count = len(banned_maps) 
        
        asyncio.create_task(start_ban_timer(match_id, current_ban_count, bot))

# [ASYNC-REWRITE]
async def start_ban_timer(match_id: str, expected_ban_count: int, bot: Bot):
    """
    (PG) Ждет 20 секунд. Если кол-во банов не изменилось, банит карту сам.
    """
    await asyncio.sleep(20)
    
    try:
        match = await db_fetchone("SELECT banned_maps, status FROM matches WHERE match_id = $1", match_id)
        
        if not match or match['status'] != 'picking':
            logger.info(f"[Timer {match_id}] Таймер отменен, матч не в стадии пика.")
            return

        banned_maps_str = match['banned_maps']
        banned_maps = banned_maps_str.split(",") if banned_maps_str else []
        
        if len(banned_maps) == expected_ban_count:
            logger.info(f"[Timer {match_id}] Таймер истек. Авто-бан...")
            
            available_maps = [m for m in MAPS if m not in banned_maps]
            if not available_maps:
                 logger.error(f"[Timer {match_id}] Нет карт для авто-бана!")
                 return
                 
            map_to_ban = random.choice(available_maps)
            banned_maps.append(map_to_ban)
            
            await db_execute(
                "UPDATE matches SET banned_maps = $1 WHERE match_id = $2",
                ",".join(banned_maps), match_id
            )
            
            await handle_next_ban_turn(match_id, bot)
        
        else:
            logger.info(f"[Timer {match_id}] Таймер отменен, капитан успел забанить.")
    
    except Exception as e:
        logger.error(f"Критическая ошибка в таймере бана {match_id}: {e}", exc_info=True)

# [ASYNC-REWRITE]
async def start_map_banning(match_id: str, captain1_id: int, captain2_id: int, bot: Bot):
    """(PG) Начинает фазу бана карт."""
    
    match_data = await db_fetchone("SELECT lobby_id FROM matches WHERE match_id = $1", match_id)
    if not match_data:
        logger.error(f"Матч {match_id} не найден.")
        return
    lobby_id = match_data['lobby_id']

    await db_execute("UPDATE lobbies SET status = 'map_banning' WHERE lobby_id = $1", lobby_id)
    
    await db_execute(
        "UPDATE matches SET banned_maps = $1, captain_turn = $2 WHERE match_id = $3",
        "", captain1_id, match_id
    )

    logger.info(f"Запуск процесса банов для матча {match_id}. Капитаны: {captain1_id} vs {captain2_id}")
    
    await handle_next_ban_turn(match_id, bot)

# [ASYNC-REWRITE]
async def finalize_match_setup(match_id: str, final_map: str, bot: Bot):
    """
    (PG) Формирует команды и очищает лобби.
    """
    match = await db_fetchone("SELECT * FROM matches WHERE match_id = $1", match_id)
    lobby_id = match['lobby_id']

    
    await db_execute("UPDATE matches SET map_name = $1 WHERE match_id = $2", final_map, match_id)
    
    members = await db_fetchall("SELECT user_id FROM lobby_members WHERE lobby_id = $1", lobby_id)
    all_member_ids = [m['user_id'] for m in members]
    
    if len(all_member_ids) != 10:
        logger.error(f"[Match {match_id}] КРИТИЧЕСКАЯ ОШИБКА: В finalize_match_setup вошло {len(all_member_ids)} игроков.")
        # Добавляем заглушки, если игроков не 10 (аварийный режим)
        if 0 < len(all_member_ids) < 10:
             logger.warning(f"Добавляю {10 - len(all_member_ids)} ботов-заглушек...")
             for i in range(10 - len(all_member_ids)):
                 all_member_ids.append(-999 - i) # Аварийные боты
        elif not all_member_ids:
            logger.error("Нет игроков, отмена finalize_match_setup.")
            return

    
    captain1_id = match['captain1_id']
    captain2_id = match['captain2_id']
    
    team_ct = [captain1_id]
    team_t = [captain2_id]
    
    unassigned_players = set(all_member_ids)
    if captain1_id in unassigned_players:
        unassigned_players.remove(captain1_id)
    if captain2_id in unassigned_players:
        unassigned_players.remove(captain2_id)

    # [ASYNC-REWRITE]
    c1_data = await db_fetchone("SELECT teammate_user_id FROM users WHERE user_id = $1", captain1_id)
    c1_teammate = c1_data.get('teammate_user_id') if c1_data else None
    
    if c1_teammate and c1_teammate in unassigned_players:
        team_ct.append(c1_teammate)
        unassigned_players.remove(c1_teammate)
        logger.info(f"[Match {match_id}] Напарник Капитана 1 ({c1_teammate}) добавлен в CT")

    # [ASYNC-REWRITE]
    c2_data = await db_fetchone("SELECT teammate_user_id FROM users WHERE user_id = $1", captain2_id)
    c2_teammate = c2_data.get('teammate_user_id') if c2_data else None
    
    if c2_teammate and c2_teammate in unassigned_players:
        team_t.append(c2_teammate)
        unassigned_players.remove(c2_teammate)
        logger.info(f"[Match {match_id}] Напарник Капитана 2 ({c2_teammate}) добавлен в T")

    remaining_ids_list = list(unassigned_players)
    shufflable_entities = []
    processed_players = set()
    
    for user_id in remaining_ids_list:
        if user_id in processed_players:
            continue
            
        # [ASYNC-REWRITE]
        user_data = await db_fetchone("SELECT teammate_user_id FROM users WHERE user_id = $1", user_id)
        teammate_id = user_data.get('teammate_user_id') if user_data else None
        
        if teammate_id and teammate_id in unassigned_players:
            shufflable_entities.append((user_id, teammate_id))
            processed_players.add(user_id)
            processed_players.add(teammate_id)
        else:
            shufflable_entities.append(user_id)
            processed_players.add(user_id)

    random.shuffle(shufflable_entities)
    
    shuffled_player_pool = []
    for entity in shufflable_entities:
        if isinstance(entity, tuple):
            shuffled_player_pool.extend(entity)
        else:
            shuffled_player_pool.append(entity)

    ct_needed = 5 - len(team_ct)
    
    team_ct.extend(shuffled_player_pool[:ct_needed])
    team_t.extend(shuffled_player_pool[ct_needed:])
    
    logger.info(f"[Match {match_id}] Команды сформированы: CT={team_ct}, T={team_t}")

    try:
        team_ct_json = json.dumps(team_ct)
        team_t_json = json.dumps(team_t)
        # [ASYNC-REWRITE]
        await db_execute(
            "UPDATE matches SET team_ct = $1, team_t = $2 WHERE match_id = $3",
            team_ct_json, team_t_json, match_id
        )
        logger.info(f"[Match {match_id}] Составы сохранены в 'matches'")
    except Exception as e:
        logger.error(f"Ошибка сохранения составов {match_id}: {e}")

    # [ASYNC-REWRITE]
    match_info_text = await format_match_info(match_id, team_ct, team_t, final_map)
    
    for member in members:
        user_id = member['user_id']
        if user_id < 0:
            logger.info(f"Пропуск отправки (финал) для ID бота: {user_id}")
            continue 
        
        try:
            await bot.send_message(
                user_id,
                match_info_text,
                reply_markup=get_match_result_keyboard(match_id),
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка отправки информации о матче пользователю {user_id}: {e}")

    # [PG-REWRITE] Очистка лобби в транзакции
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM lobby_members WHERE lobby_id = $1", lobby_id)
                await conn.execute("UPDATE lobbies SET current_players = 0, status = 'waiting' WHERE lobby_id = $1", lobby_id)
        
        logger.info(f"[Match {match_id}] Лобби {lobby_id} очищено и сброшено на 'waiting'.")
    except Exception as e:
         logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: не удалось очистить лобби {lobby_id} после матча: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
async def format_match_info(match_id: str, team_ct: list, team_t: list, map_name: str) -> str:
    """Форматирует информацию о матче (со звездами)."""
    
    match_data = await db_fetchone("SELECT l.league FROM matches m JOIN lobbies l ON m.lobby_id = l.lobby_id WHERE m.match_id = $1", match_id)
    league_name = match_data.get('league', DEFAULT_LEAGUE) if match_data else DEFAULT_LEAGUE
    
    async def process_team(team_ids):
        players = []
        total_elo = 0
        for uid in team_ids:
            u_main = await db_fetchone("SELECT user_id, nickname, game_id FROM users WHERE user_id = $1", uid)
            u_stats = await get_user_league_stats(uid, league_name)
            
            if not u_main: 
                u_main = {'user_id': uid, 'nickname': f'Player_{uid}', 'game_id': 'N/A'}
            else:
                # [FIX] Превращаем Record в словарь, чтобы можно было менять nickname
                u_main = dict(u_main)
            
            # Звезда
            formatted_nick = await format_nickname(uid, u_main['nickname'])
            u_main['nickname'] = formatted_nick # Подменяем на форматированный
            
            data = {**u_main, **u_stats}
            players.append(data)
            total_elo += u_stats['elo']
        return players, total_elo

    ct_players, ct_total_elo = await process_team(team_ct)
    t_players, t_total_elo = await process_team(team_t)
    
    captain_ct = ct_players[0] if ct_players else None
    captain_t = t_players[0] if t_players else None
    
    text = (
        f"<b>🎮 FACEIT MATCH LOBBY — PROJECT EVO</b>\n\n"
        f"<b>Матч #{match_id}</b>\n"
        f"🗺 <b>Карта:</b> <b>{map_name}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>💙 Команда 1 — CT</b>\n"
        f"👑 <b>Капитан:</b> {captain_ct['nickname'] if captain_ct else 'N/A'}\n"
        f"📊 <b>Общее ELO:</b> <b>{ct_total_elo}</b>\n\n"
    )
    
    for player in ct_players:
        level_emoji = get_faceit_level_emoji(player['elo'])
        text += f"{level_emoji} {player['nickname']} — ID: <code>{player['user_id']}</code> — <b>{player['elo']} ELO</b>\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━\n\n"
    text += (
        f"<b>🧡 Команда 2 — T</b>\n"
        f"👑 <b>Капитан:</b> {captain_t['nickname'] if captain_t else 'N/A'}\n"
        f"📊 <b>Общее ELO:</b> <b>{t_total_elo}</b>\n\n"
    )
    
    for player in t_players:
        level_emoji = get_faceit_level_emoji(player['elo'])
        text += f"{level_emoji} {player['nickname']} — ID: <code>{player['user_id']}</code> — <b>{player['elo']} ELO</b>\n"
    
    elo_diff = abs(ct_total_elo - t_total_elo)
    advantage = "CT" if ct_total_elo > t_total_elo else "T"
    
    text += (
        f"\n━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>⚖️ Сравнение команд:</b>\n"
        f"💙 <b>CT:</b> {ct_total_elo} ELO\n"
        f"🧡 <b>T:</b> {t_total_elo} ELO\n"
        f"🏆 <b>Преимущество у:</b> <b>{advantage} (+{elo_diff} ELO)</b>\n\n"
        f"\n━━━━━━━━━━━━━━━━━━\n\n"
        f"🕓 <b>Матч начинается через:</b> <b>1 минуту</b>\n"
        f"📡 <b>Сервер:</b> <b>Russia</b>\n"
        f"🎯 <b>Карта:</b> <b>{map_name}</b>\n"
        f"👤 <b>Хост:</b> <code>{captain_ct['nickname'] if captain_ct else 'N/A'}</code>\n"
        f"🆔 <b>ID Хоста:</b> <code>{captain_ct['game_id'] if captain_ct else 'N/A'}</code>"
    )
    return text

# [ASYNC-REWRITE]
@dp.message(CommandStart())
async def command_start_handler(message: types.Message, state: FSMContext) -> None:
    """(PG) Обрабатывает команду /start."""
    user_id = message.from_user.id
    await state.clear() 
    
    user_data = await db_fetchone("SELECT * FROM users WHERE user_id = $1", user_id)
    
    try:
        if user_data and user_data['is_registered']:
            await send_main_menu(
                message.chat.id, 
                user_id, 
                message_to_edit=None
            )
            
        else:
            if not user_data:
                await db_execute(
                    "INSERT INTO users (user_id, game_key) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING", 
                    user_id, "project_evolution"
                )
            
            await message.answer(
                "👋 Привет! Добро пожаловать.\n\n"
                f"Для начала работы необходимо **подписаться на наш канал**.",
                reply_markup=get_subscription_keyboard(),
                parse_mode="Markdown"
            )
            
    except (TelegramForbiddenError, TelegramBadRequest) as e:
        logger.warning(f"Не удалось отправить /start сообщение пользователю {user_id}: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в command_start_handler для {user_id}: {e}", exc_info=True)

# ... (start_id_input_callback, admin_spawn_bots_start, 
# ... admin_spawn_bots_league_select, check_subscription_callback,
# ... start_registration_callback, process_game_choice - без изменений FSM) ...
@dp.callback_query(GameIDState.waiting_for_game_selection, F.data == "start_id_input")
async def start_id_input_callback(callback: types.CallbackQuery, state: FSMContext):
    try:
        await callback.message.edit_text(
            text=f"Отлично! Вы выбрали **{SINGLE_GAME_NAME}**.\n\n"
                 f"Теперь введите **ваш игровой ID**:",
            reply_markup=None, 
            parse_mode="Markdown"
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            raise e
            
    await state.set_state(GameIDState.waiting_for_game_id)
    await callback.answer()

@dp.callback_query(F.data == "admin_spawn_bots", MinRoleFilter(ROLE_OWNER))
async def admin_spawn_bots_start(callback: types.CallbackQuery, state: FSMContext):
    """(Функция 1) Спрашивает ЛИГУ. (ИСПРАВЛЕНО)"""
    
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_bot_league)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=DEFAULT_LEAGUE, callback_data=f"admin_spawn_league_{DEFAULT_LEAGUE}")],
        [InlineKeyboardButton(text=QUAL_LEAGUE, callback_data=f"admin_spawn_league_{QUAL_LEAGUE}")],
        [InlineKeyboardButton(text=FPL_LEAGUE, callback_data=f"admin_spawn_league_{FPL_LEAGUE}")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="main_admin")]
    ])
    
    photo_id = MAIN_MENU_FILE_ID
    text = ("<b>🤖 Спавн 8 ботов</b>\n\n"
            "<b>Шаг 1:</b> Выберите лигу, в которую нужно добавить ботов.")
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка в admin_spawn_bots_start: {e}")
            
    await callback.answer()

@dp.callback_query(AdminActions.waiting_for_bot_league, F.data.startswith("admin_spawn_league_"))
async def admin_spawn_bots_league_select(callback: types.CallbackQuery, state: FSMContext):
    """(Функция 2) Ловит ЛИГУ и спрашивает НОМЕР. (ИСПРАВЛЕНО)"""
    
    await callback.answer()
    
    league_name = callback.data.split('_', 3)[-1]
    
    await state.update_data(spawn_league=league_name)
    await state.set_state(AdminActions.waiting_for_bot_lobby_number)
    
    photo_id = MAIN_MENU_FILE_ID
    text = (f"<b>🤖 Спавн 8 ботов</b>\n\n"
            f"<b>Лига:</b> {league_name}\n"
            f"<b>Шаг 2:</b> Введите порядковый номер лобби (от 1 до 5).")

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка в admin_spawn_bots_league_select: {e}")

    await callback.answer()

@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if await is_subscribed(bot, user_id, CHANNEL_USERNAME): 
        try:
            await callback.message.edit_text(
                text="✅ Подписка подтверждена! Пожалуйста, выберите игру для ввода ID:",
                reply_markup=get_single_game_keyboard(SINGLE_GAME_NAME),
                parse_mode="HTML" 
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                pass
            else:
                raise e
                
        await state.set_state(GameIDState.waiting_for_game_selection)
        await callback.answer()
        
    else:
        try:
            await callback.message.edit_text(
                text="❌ Вы не подписаны на канал. Пожалуйста, подпишитесь и нажмите 'Проверить подписку'.",
                reply_markup=get_subscription_keyboard(),
                parse_mode="HTML"
            )
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                pass
            else:
                raise e
        
        await callback.answer("Подписка не найдена.")

@dp.callback_query(F.data == "start_registration")
async def start_registration_callback(callback: types.CallbackQuery, state: FSMContext):
    """Начинает процесс регистрации (выбор игры)."""
    await state.clear() 
    
    await callback.message.edit_text(
        "<b>📝 Выберите приватку, в которой вы хотите зарегистрироваться:</b>",
        reply_markup=get_game_choice_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(Registration.waiting_for_game_choice)
    await callback.answer()

@dp.callback_query(Registration.waiting_for_game_choice, F.data.startswith("game_select_"))
async def process_game_choice(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает выбор игры и просит ID игрока."""
    game_name = callback.data.split("_")[-1]
    await state.update_data(game_chosen=game_name)
    
    await callback.message.edit_text(
        f"<b>Отлично! Вы выбрали {game_name}.</b>\n\n"
        f"<b>Введите ваш ID игрока в {game_name}:</b>",
        parse_mode="HTML"
    )
    await state.set_state(Registration.waiting_for_game_id)
    await callback.answer()

# [ASYNC-REWRITE]
@dp.message(GameIDState.waiting_for_game_id, F.text)
async def process_game_id(message: types.Message, state: FSMContext):
    game_id = message.text.strip()
    user_id = message.from_user.id 
    
    if not (1 <= len(game_id) <= 12):
        await message.answer("❌ Ошибка! Длина ID должна быть от 1 до 12 символов.")
        return
    
    if not is_valid_game_id(game_id):
        await message.answer(
            "❌ Ошибка! Игровой ID может содержать <b>только латинские буквы (A-z) и цифры (0-9)</b>. "
            "Кириллица и другие символы запрещены.", 
            parse_mode="HTML"
        )
        return
        
    await db_execute("UPDATE users SET game_id = $1 WHERE user_id = $2", game_id, user_id)
    
    await message.answer(
        f"✅ Игровой ID: <b>{game_id}</b> сохранен.\n\n"
        f"Теперь <b>введите ваш никнейм</b> (отображаемое имя):",
        parse_mode="HTML"
    )
    
    await state.set_state(GameIDState.waiting_for_nickname)

def is_valid_nickname(nickname: str) -> bool:
    """
    Проверяет, что никнейм содержит только латинские буквы (a-z, A-Z), 
    кириллицу (а-я, А-Я) и цифры (0-9).
    """
    # Мы также убираем пробелы по краям, но не разрешаем их внутри
    if " " in nickname:
        return False
    return re.fullmatch(r"^[a-zA-Zа-яА-Я0-9]+$", nickname) is not None

# [ASYNC-REWRITE]
@dp.message(GameIDState.waiting_for_nickname, F.text)
async def process_nickname(message: types.Message, state: FSMContext):
    nickname = message.text.strip()
    user_id = message.from_user.id
    
    if not (3 <= len(nickname) <= 10):
        await message.answer("❌ Ошибка! Длина никнейма должна быть от 3 до 10 символов.")
        return
    
    if not is_valid_nickname(nickname):
        await message.answer(
            "❌ Ошибка! Никнейм может содержать <b>только латинские/русские буквы и цифры</b>.\n"
            "Символы, пробелы и эмодзи запрещены.",
            parse_mode="HTML"
        )
        return

    existing_user = await db_fetchone(
        "SELECT user_id FROM users WHERE nickname = $1 AND user_id != $2", 
        nickname, user_id
    )
    
    if existing_user:
        await message.answer("❌ Ошибка! Этот никнейм уже занят. Выберите другой.")
        return
        
    await db_execute("UPDATE users SET nickname = $1 WHERE user_id = $2", nickname, user_id)
    
    await message.answer(
        f"✅ Никнейм: <b>{nickname}</b> сохранен.\n\n"
        f"Теперь <b>выберите устройство</b>, с которого вы играете:",
        reply_markup=get_device_keyboard(), 
        parse_mode="HTML"
    )
    
    await state.set_state(GameIDState.waiting_for_device)

# [ASYNC-REWRITE]
@dp.callback_query(GameIDState.waiting_for_device, F.data.startswith("device_"))
async def process_device_selection(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    device_key = callback.data.split("_")[1] 
    
    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    await db_execute(
        "UPDATE users SET device = $1, is_registered = TRUE, registration_date = $2 WHERE user_id = $3", 
        device_key, current_date, user_id
    )
    
    await state.clear()
    
    try:
        await callback.message.edit_text(
            f"🎉 **Поздравляем, регистрация завершена!**\n\n"
            f"Ваше устройство (**{device_key}**) сохранено. Теперь вы можете пользоваться ботом.",
            parse_mode="Markdown"
        )
    except TelegramBadRequest:
        pass
        
    user_data = await db_fetchone("SELECT * FROM users WHERE user_id = $1", user_id)
    
    if user_data:
        await send_main_menu(
    callback.message.chat.id, 
    user_id, 
    message_to_edit=None
)
        
    await callback.answer("Регистрация завершена!")

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "main_profile")
async def profile_main_handler(callback: types.CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    
    # [ASYNC-REWRITE]
    profile_text, league_used = await get_profile_text(
        user_id,
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        league_to_display=None 
    )
    
    keyboard = get_profile_menu_keyboard(active_league=league_used)
    
    photo_id = PROFILE_FILE_ID
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=profile_text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка при обновлении профиля: {e}")

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "edit_profile")
async def edit_profile_callback(callback: types.CallbackQuery, state: FSMContext):
    """Показывает меню редактирования профиля (НЕ перезапускает регистрацию)."""
    user_id = callback.from_user.id
    user = await db_fetchone("SELECT is_registered FROM users WHERE user_id = $1", user_id)
    if not user or not user.get('is_registered'):
        await callback.answer("❌ Сначала завершите регистрацию!", show_alert=True)
        return

    text = "<b>📝 РЕДАКТИРОВАНИЕ ПРОФИЛЯ</b>\n\nВыберите, что хотите изменить:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Изменить никнейм", callback_data="edit_nickname")],
        [InlineKeyboardButton(text="🆔 Изменить игровой ID", callback_data="edit_gameid")],
        [InlineKeyboardButton(text="📱 Изменить устройство", callback_data="edit_device_menu")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_profile")], 
    ])

    try:
        # [PG-FIX] В оригинале была ошибка, .edit_caption() не работает с media.
        # Нужно .edit_media() или .edit_caption() если это было фото.
        # Предполагая, что мы в профиле (где есть фото), используем edit_media
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=PROFILE_FILE_ID, # Используем то же фото
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=kb
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в edit_profile_callback: {e}")
            
    await state.clear()
    await callback.answer()

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "edit_nickname")
async def edit_nickname_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(EditProfile.waiting_for_new_nickname)
    try:
        await callback.message.edit_caption(
            caption="<b>Введите новый никнейм:</b>\n\n<i>Нажмите '⬅️ Назад' чтобы отменить.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_profile")]
            ]),
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в edit_nickname_start: {e}")
    await callback.answer()

# [ASYNC-REWRITE]
@dp.message(EditProfile.waiting_for_new_nickname, F.text)
async def process_new_nickname(message: types.Message, state: FSMContext):
    new_nick = message.text.strip()
    user_id = message.from_user.id

    if not is_valid_nickname(new_nick):
        await message.answer(
            "<b>❌ Ошибка! Никнейм может содержать <b>только латинские/русские буквы и цифры</b>.</b>\n"
            "Символы, пробелы и эмодзи запрещены.",
            parse_mode="HTML"
        )
        return

    if not (3 <= len(new_nick) <= 10):
        await message.answer("<b>❌ Ошибка! Длина никнейма должна быть от 3 до 10 символов.</b>", parse_mode="HTML")
        return

    await db_execute("UPDATE users SET nickname = $1 WHERE user_id = $2", new_nick, user_id)
    await clear_user_cache(user_id)
    await message.answer("<b>✅ Никнейм обновлён.</b>", parse_mode="HTML")
    await state.clear()
    await send_main_menu(message.chat.id, user_id, message_to_edit=None)

@dp.callback_query(F.data == "edit_gameid")
async def edit_gameid_start(callback: types.CallbackQuery, state: FSMContext):
    await state.set_state(EditProfile.waiting_for_new_gameid)
    try:
        await callback.message.edit_caption(
            caption="<b>Введите новый игровой ID:</b>\n\n<i>ID должен быть уникальным.</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_profile")]
            ]),
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в edit_gameid_start: {e}")
    await callback.answer()

# [ASYNC-REWRITE]
@dp.message(EditProfile.waiting_for_new_gameid, F.text)
async def process_new_gameid(message: types.Message, state: FSMContext):
    new_id = message.text.strip()
    user_id = message.from_user.id

    if not new_id:
        await message.answer("<b>Введите корректный ID.</b>", parse_mode="HTML")
        return

    if not is_valid_game_id(new_id):
        await message.answer(
            "❌ Недопустимый формат ID. ID может содержать <b>только</b> латинские буквы (a-z, A-Z) и цифры (0-9). Длина: 1-12 символов. Пожалуйста, введите ID снова:",
            parse_mode="HTML"
        )
        return

    existing = await db_fetchone(
        "SELECT user_id FROM users WHERE game_id = $1 AND is_registered = TRUE", 
        new_id
    )
    if existing and existing['user_id'] != user_id:
        await message.answer("<b>❌ Этот игровой ID уже занят другим игроком.</b>", parse_mode="HTML")
        return

    await db_execute("UPDATE users SET game_id = $1 WHERE user_id = $2", new_id, user_id)
    
    await state.clear()
    await message.answer("<b>✅ Игровой ID обновлён.</b>", parse_mode="HTML")
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
    await send_main_menu(message.chat.id, user_id, message_to_edit=None)

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "edit_device_menu")
async def edit_device_menu(callback: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="PC", callback_data="set_device_PC")],
        [InlineKeyboardButton(text="Phone", callback_data="set_device_Phone")],
        [InlineKeyboardButton(text="Tab", callback_data="set_device_Tab")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="edit_profile")],
    ])
    try:
        await callback.message.edit_caption(
            caption="<b>Выберите устройство:</b>", 
            reply_markup=kb, 
            parse_mode="HTML"
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в edit_device_menu: {e}")
    await callback.answer()

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("set_device_"))
async def set_device_callback(callback: types.CallbackQuery):
    device = callback.data.split("_", 2)[2]
    user_id = callback.from_user.id
    await db_execute("UPDATE users SET device = $1 WHERE user_id = $2", device, user_id)
    await callback.answer(f"✅ Устройство изменено на {device}", show_alert=True)
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
    # [PG-FIX] Возвращаем в ГЛАВНОЕ МЕНЮ, а не в профиль, так как .edit_profile
    # требует state, а мы его очистили. Логичнее вернуться в главное.
    await send_main_menu(
    callback.message.chat.id, 
    user_id, 
    message_to_edit=callback.message
)

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("profile_league_"))
async def profile_league_switch_handler(callback: types.CallbackQuery):
    """
    (PG) Обрабатывает переключение лиг в профиле.
    """
    user_id = callback.from_user.id
    
    try:
        selected_league = callback.data.split("_", 2)[-1]
        if selected_league not in LEAGUE_LEVELS:
            raise ValueError("Неизвестная лига")
    except Exception:
        await callback.answer("❌ Ошибка! Не удалось определить лигу.", show_alert=True)
        return
            
    user_data = await db_fetchone("SELECT league FROM users WHERE user_id = $1", user_id)
    user_main_league = user_data.get('league', DEFAULT_LEAGUE) if user_data else DEFAULT_LEAGUE
    
    user_level = LEAGUE_LEVELS.get(user_main_league, 0)
    selected_level = LEAGUE_LEVELS.get(selected_league, 0)
    
    # [FIX] Новая проверка прав в профиле
    if user_level < selected_level:
        if selected_league == QUAL_LEAGUE:
             await callback.answer("У вас нет этой роли, приобрести можно у @jackha1337", show_alert=True)
        else:
             await callback.answer("У вас нет доступа к этой лиге", show_alert=True)
        return
    
    await callback.answer(f"Загрузка {selected_league}...")
    
    profile_text, league_used = await get_profile_text(
        user_id,
        datetime.now().strftime("%d.%m.%Y %H:%M"),
        league_to_display=selected_league
    )
    
    keyboard = get_profile_menu_keyboard(active_league=league_used)
    
    photo_id = PROFILE_FILE_ID
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=profile_text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest:
        pass

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "main_find_match")
async def find_match_handler(callback: types.CallbackQuery):
    """(PG) Обрабатывает нажатие 'Найти матч' и показывает выбор лиги."""
    logger.info(f"Пользователь {callback.from_user.id} нажал 'Найти матч'")
    user_id = callback.from_user.id
    
    game_line = f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>"
    text = f"{game_line}\n<blockquote><b>🎮 ПОИСК МАТЧА</b></blockquote>\n\n<b>Выберите лигу:</b>"
    
    keyboard = await get_league_choice_keyboard(user_id)
    
    photo_id = MAIN_MENU_FILE_ID 
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
        logger.info("Сообщение с выбором лиги (media) отправлено")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка при отправке меню лиг: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при отправке меню лиг: {e}")
    
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.warning(f"Ошибка ответа на CallbackQuery (main_find_match): {e}")

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("select_league_"))
async def league_select_handler(callback: types.CallbackQuery):
    """(PG) Обрабатывает выбор лиги и показывает список лобби."""
    league = callback.data.replace("select_league_", "")
    logger.info(f"Пользователь {callback.from_user.id} выбрал лигу: {league}")
    
    game_line = f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>"
    league_line = f"<blockquote><b>{league}</b></blockquote>"
    text = f"{game_line}\n{league_line}\n\n<b>🎮 ЛОББИ 5v5</b>"

    photo_id = LOBBY_FILE_ID 
    
    keyboard = await get_lobby_list_keyboard(league)
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
        logger.info(f"Список лобби для лиги {league} (media) отправлен")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Ошибка при отправке списка лобби: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при отправке списка лобби: {e}")

    await callback.answer()

# [PG-REWRITE] Логика входа в лобби полностью переписана
# Убрана функция check_all_db_sync, логика встроена сюда
@dp.callback_query(F.data.startswith("join_lobby_"))
async def join_lobby_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает вход в лобби с транзакциями."""
    
    try:
        lobby_id = int(callback.data.replace("join_lobby_", ""))
    except ValueError:
        await callback.answer("❌ Ошибка: ID лобби некорректен.", show_alert=True)
        return
        
    user_id = callback.from_user.id
    
    logger.info(f"Пользователь {user_id} пытается войти в лобби {lobby_id}")

    # --- 1. Проверка Бана/Мута/Доступа ---
    lobby_data = await db_fetchone("SELECT league, status FROM lobbies WHERE lobby_id = $1", lobby_id)
    if not lobby_data:
        await callback.answer("❌ Лобби не найдено!", show_alert=True)
        return
    
    lobby_league = lobby_data.get('league', DEFAULT_LEAGUE)
    lobby_level = LEAGUE_LEVELS.get(lobby_league, 0)
    
    user_data = await db_fetchone("SELECT league, banned, muted_until FROM users WHERE user_id = $1", user_id)
    if not user_data:
        await callback.answer("❌ Ваш профиль не найден. Пройдите регистрацию /start", show_alert=True)
        return

    user_league = user_data.get('league', DEFAULT_LEAGUE)
    user_level = LEAGUE_LEVELS.get(user_league, 0)
    
    if user_level < lobby_level:
        if lobby_league == QUAL_LEAGUE:
             await callback.answer("У вас нет этой роли, приобрести можно у @jackha1337", show_alert=True)
        else:
             # Для FPL и других случаев
             await callback.answer("У вас нет доступа к этой лиге", show_alert=True)
        
        logger.warning(f"Отказ: User {user_id} join Lobby {lobby_id} ({lobby_league})")
        return

    if user_data.get('muted_until'):
        mute_time = user_data['muted_until'] # PG возвращает datetime
        if datetime.now(mute_time.tzinfo) < mute_time:
            remaining = mute_time - datetime.now(mute_time.tzinfo)
            minutes = int(remaining.total_seconds() / 60)
            await callback.answer(
                f"🔇 Вы замучены! Осталось: {minutes} минут",
                show_alert=True
            )
            return
        else:
            await db_execute("UPDATE users SET muted_until = NULL WHERE user_id = $1", user_id)
    
    if user_data.get('banned'):
         await callback.answer("🚫 Вы забанены!", show_alert=True)
         return

    # --- 2. Проверка, не в этом ли лобби юзер ---
    already_in_this = await db_fetchone(
        "SELECT 1 FROM lobby_members WHERE lobby_id = $1 AND user_id = $2",
        lobby_id, user_id
    )
    if already_in_this:
        await callback.answer("⚠️ Вы уже в этом лобби!", show_alert=True)
        return
        
    # --- 3. Проверка другого лобби (с выходом) ---
    old_lobby_id_to_broadcast = None
    other_lobby = await db_fetchone("SELECT lobby_id FROM lobby_members WHERE user_id = $1", user_id)
    
    if other_lobby:
        old_lobby_id = other_lobby['lobby_id']
        logger.info(f"User {user_id} переключается из лобби {old_lobby_id} в {lobby_id}")
        
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM lobby_members WHERE lobby_id = $1 AND user_id = $2", old_lobby_id, user_id)
                await conn.execute(
                    "UPDATE lobbies SET current_players = (SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1) WHERE lobby_id = $1", 
                    old_lobby_id
                )
        old_lobby_id_to_broadcast = old_lobby_id
        
    # --- 4. Транзакция входа в новое лобби (защита от гонок) ---
    lobby_filled_by_this_user = False
    
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction(isolation='serializable'): # Строгая изоляция
                
                # Повторно проверяем статус лобби ВНУТРИ транзакции
                lobby_status_check = await conn.fetchrow(
                    "SELECT current_players, status FROM lobbies WHERE lobby_id = $1 FOR UPDATE", 
                    lobby_id
                )
                
                if not lobby_status_check:
                    await callback.answer("❌ Лобби не найдено (ошибка транзакции)!", show_alert=True)
                    return
                
                if lobby_status_check['status'] != 'waiting':
                    await callback.answer("❌ Лобби уже начало матч!", show_alert=True)
                    return
                
                # Используем SELECT COUNT(*) вместо current_players для надежности
                real_count = await conn.fetchval("SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1", lobby_id)

                if real_count >= 10:
                    await callback.answer("❌ Лобби заполнено (10/10)!", show_alert=True)
                    return
                
                # Все проверки пройдены, добавляем игрока
                await conn.execute(
                    "INSERT INTO lobby_members (lobby_id, user_id) VALUES ($1, $2)", 
                    lobby_id, user_id
                )
                
                new_count = real_count + 1
                await conn.execute(
                    "UPDATE lobbies SET current_players = $1 WHERE lobby_id = $2", 
                    new_count, lobby_id
                )
                
                if new_count == 10:
                    lobby_filled_by_this_user = True

    except asyncpg.exceptions.SerializationError:
        logger.warning(f"Конфликт транзакций (SerializationError) при входе {user_id} в {lobby_id}. Повтор не требуется.")
        await callback.answer("❌ Лобби только что заполнилось! Попробуйте другое.", show_alert=True)
        return
    except Exception as e:
        logger.error(f"Ошибка транзакции при входе в лобби {lobby_id} юзером {user_id}: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при входе в лобби!", show_alert=True)
        return

    # --- 5. Обновление UI ---
    await callback.answer("✅ Вы присоединились к лобби!")
    
    # Обновляем старое лобби, если игрок оттуда вышел
    if old_lobby_id_to_broadcast:
        logger.info(f"Обновление старого лобби {old_lobby_id_to_broadcast}, которое покинул {user_id}")
        old_lobby_text = await get_lobby_text(old_lobby_id_to_broadcast)
        await broadcast_lobby_update(old_lobby_id_to_broadcast, bot, old_lobby_text)

    # Обновляем новое лобби
    lobby_text = await get_lobby_text(lobby_id)
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=LOBBY_FILE_ID,
                caption=lobby_text,
                parse_mode="HTML"
            ),
            reply_markup=get_lobby_keyboard(lobby_id)
        )
        await db_execute(
            "UPDATE lobby_members SET lobby_message_id = $1 WHERE user_id = $2 AND lobby_id = $3",
            callback.message.message_id, user_id, lobby_id
        )
    except Exception as e:
        logger.error(f"Ошибка редактирования сообщения (join lobby): {e}")

    await broadcast_lobby_update(lobby_id, bot, lobby_text)
    
    if lobby_filled_by_this_user:
        logger.info(f"Лобби {lobby_id} заполнено (10/10). Запуск фазы подтверждения (Ready Check).")
        await start_confirmation_phase(lobby_id, bot)

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("leave_lobby_"))
async def leave_lobby_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает выход из лобби с возвратом в меню."""
    
    try:
        lobby_id = int(callback.data.replace("leave_lobby_", ""))
    except ValueError:
        await callback.answer("❌ Ошибка: ID лобби некорректен.", show_alert=True)
        return
        
    user_id = callback.from_user.id

    lobby = await db_fetchone("SELECT status FROM lobbies WHERE lobby_id = $1", lobby_id)
    
    if not lobby:
        try:
            await callback.answer("❌ Лобби не найдено!", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning(f"Ошибка ответа на callback (lobby not found): {e}")
        return

    current_status = lobby['status']

    if current_status not in ['waiting', 'full']: # Разрешаем выход, если 'full' но 'confirming' еще не начался
        status_text = {
            'captain_selection': "выбора капитанов",
            'map_banning': "бана карт",
            'confirming': "подтверждения матча",
            'starting': "запуска матча"
        }.get(current_status, "активного процесса")
        
        try:
            await callback.answer(f"🚫 Нельзя покинуть лобби во время {status_text}!", show_alert=True)
        except TelegramBadRequest as e:
            logger.warning(f"Ошибка ответа на callback (exit blocked): {e}")

        logger.warning(f"Пользователь {user_id} пытался покинуть лобби {lobby_id} во время статуса '{current_status}'")
        return
    
    # [PG-REWRITE] Транзакция выхода
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM lobby_members WHERE lobby_id = $1 AND user_id = $2", lobby_id, user_id)
                # Пересчитываем игроков
                await conn.execute(
                    "UPDATE lobbies SET current_players = (SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1) WHERE lobby_id = $1",
                    lobby_id
                )
    except Exception as e:
        logger.error(f"Ошибка выхода из лобби {lobby_id} юзером {user_id}: {e}")
        await callback.answer("❌ Ошибка БД при выходе из лобби!", show_alert=True)
        return


    logger.info(f"✅ Пользователь {user_id} покинул лобби {lobby_id}. Статус: {current_status}")

    # Обновляем лобби для оставшихся
    lobby_text = await get_lobby_text(lobby_id)
    await broadcast_lobby_update(lobby_id, bot, lobby_text)

    # Возвращаем пользователя в меню
    try:
        user_data_for_menu = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
        nickname = user_data_for_menu['nickname'] if user_data_for_menu else "Игрок"

        await send_main_menu(
    callback.message.chat.id, 
    user_id, 
    message_to_edit=callback.message
)

    except TelegramBadRequest as e:
        logger.warning(f"Ошибка редактирования сообщения после выхода: {e}")
        user_data = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
        nickname = user_data['nickname'] if user_data else "Игрок"
        await send_main_menu(
    chat_id=callback.message.chat.id, 
    user_id=user_id, 
    message_to_edit=None
)
    except Exception as e:
        logger.error(f"Ошибка при выходе из лобби для {user_id}: {e}")


@dp.callback_query(F.data == "admin_manage_roles")
async def admin_manage_roles_menu(callback: types.CallbackQuery, state: FSMContext):
    """Меню управления ролями (только для Owner)."""
    user_id = callback.from_user.id
    
    # [PG-FIX] OWNER_ID не определен. Используем ADMIN_IDS
    if 'ADMIN_IDS' not in globals() or user_id not in ADMIN_IDS:
        await callback.answer("❌ Только владелец (из ADMIN_IDS) может управлять ролями!", show_alert=True)
        return
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Назначить Administrator", callback_data="role_add_admin")],
        [InlineKeyboardButton(text="➖ Снять Administrator", callback_data="role_remove_admin")],
        [InlineKeyboardButton(text="➕ Назначить Game Reg", callback_data="role_add_gamereg")],
        [InlineKeyboardButton(text="➖ Снять Game Reg", callback_data="role_remove_gamereg")],
        [InlineKeyboardButton(text="📋 Список ролей", callback_data="role_list")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_admin")]
    ])
    
    # [PG-FIX] .edit_text() не работает с media.
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID, 
                caption="<b>👥 УПРАВЛЕНИЕ РОЛЯМИ</b>\n\n...", # (Текст как в оригинале)
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest:
        await callback.message.answer(
            "<b>👥 УПРАВЛЕНИЕ РОЛЯМИ</b>\n\n"
            "<b>Доступные действия:</b>\n"
            "• Назначить/снять Administrator\n"
            "• Назначить/снять Game Reg\n"
            "• Просмотреть список ролей\n\n"
            "<b>Иерархия:</b>\n"
            "Owner > Administrator > Game Reg > User",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    await callback.answer()

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("ban_map_"))
async def ban_map_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает бан карты ЧЕЛОВЕКОМ."""
    parts = callback.data.split("_")
    try:
        map_name = parts[2]
        captain_id = int(parts[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных callback!", show_alert=True)
        return
        
    user_id = callback.from_user.id
    
    if user_id != captain_id:
        await callback.answer("❌ Сейчас не ваша очередь банить!", show_alert=True)
        return
    
    match = await db_fetchone(
        "SELECT * FROM matches WHERE (captain1_id = $1 OR captain2_id = $1) AND status = 'picking' ORDER BY match_id DESC LIMIT 1",
        user_id
    )
    
    if not match:
        await callback.answer("❌ Матч не найден или уже начался!", show_alert=True)
        return
    
    match_id = match['match_id']
    banned_maps_str = match['banned_maps']
    banned_maps = banned_maps_str.split(",") if banned_maps_str else []
    
    if map_name in banned_maps:
        await callback.answer("❌ Эта карта уже забанена!", show_alert=True)
        return
        
    if (len(banned_maps) % 2 == 0 and user_id != match['captain1_id']) or \
       (len(banned_maps) % 2 != 0 and user_id != match['captain2_id']):
        await callback.answer("❌ Сейчас не ваша очередь!", show_alert=True)
        return


    banned_maps.append(map_name)
    
    await db_execute(
        "UPDATE matches SET banned_maps = $1 WHERE match_id = $2",
        ",".join(banned_maps), match_id
    )
    
    await callback.answer(f"✅ Карта {map_name} забанена!")
    
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
        
    await handle_next_ban_turn(match_id, bot)

# ... (submit_result_handler - без изменений FSM) ...
@dp.callback_query(F.data.startswith("submit_result_"))
async def submit_result_handler(callback: types.CallbackQuery, state: FSMContext):
    """Обрабатывает начало отправки результатов."""
    match_id = callback.data.replace("submit_result_", "")
    
    await state.update_data(match_id=match_id)
    await state.set_state(MatchResultState.waiting_for_screenshot)
    
    # Сначала отвечаем на callback
    await callback.answer()

    # Отправляем новый prompt
    await callback.message.answer(
        "📸 <b>Отправьте скриншот с результатами матча:</b>",
        parse_mode="HTML"
    )
    
    # Удаляем старое сообщение с кнопкой
    try:
        await callback.message.delete()
    except TelegramBadRequest as e:
        logger.warning(f"Не удалось удалить сообщение с кнопкой 'Отправить результаты': {e}")

# [ASYNC-REWRITE]
@dp.message(MatchResultState.waiting_for_screenshot, F.photo)
async def process_screenshot(message: types.Message, state: FSMContext):
    data = await state.get_data()
    match_id = data.get('match_id')
    if not match_id:
        await message.answer("❌ Ошибка FSM, ID матча утерян. Попробуйте снова.")
        await state.clear()
        return
        
    user_id = message.from_user.id

    match = await db_fetchone("SELECT * FROM matches WHERE match_id = $1", match_id)
    if not match:
        await message.answer("❌ Матч не найден!")
        await state.clear()
        return

    try:
        team_ct = json.loads(match['team_ct']) if match['team_ct'] else []
        team_t = json.loads(match['team_t']) if match['team_t'] else []
    except:
        team_ct = []
        team_t = []
        logger.error(f"Не удалось загрузить JSON-составы для матча {match_id}")

    # [ASYNC-REWRITE]
    match_info = await format_match_info(match_id, team_ct, team_t, match['map_name'])

    photo_caption = (
        f"<b>📊 РЕЗУЛЬТАТЫ МАТЧА (СКРИНШОТ)</b>\n\n"
        f"<b>Match ID:</b> <code>{match_id}</code>\n"
        f"<b>Отправил:</b> {html.escape(message.from_user.full_name)} (<code>{user_id}</code>)"
    )

    full_text_message = (
        f"<b>📊 РЕЗУЛЬТАТЫ МАТЧА (ДЕТАЛИ)</b>\n\n"
        f"<b>Match ID:</b> <code>{match_id}</code>\n"
        f"<b>Отправил:</b> {html.escape(message.from_user.full_name)} (<code>{user_id}</code>)\n\n"
        f"➖➖➖➖➖➖➖➖➖➖\n\n"
        f"{match_info}"
    )

    try:
        await bot.send_photo(
            chat_id=RESULTS_CHANNEL_ID,
            photo=message.photo[-1].file_id,
            caption=photo_caption, 
            message_thread_id=MATCH_THREAD_ID,
            parse_mode="HTML"
        )
        
        await bot.send_message(
            chat_id=RESULTS_CHANNEL_ID,
            text=full_text_message,
            reply_markup=get_register_match_keyboard(match_id),
            message_thread_id=MATCH_THREAD_ID,
            parse_mode="HTML"
        )

        await message.answer(
            "✅ <b>Результаты отправлены на проверку!</b>\n\n"
            "Администраторы рассмотрят их в ближайшее время.",
            parse_mode="HTML"
        )

        user_data = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
        nickname = user_data['nickname'] if user_data and user_data['nickname'] else "Игрок"
        
        await send_main_menu(
    chat_id=message.chat.id,
    user_id=user_id,
    message_to_edit=None
)
        
    except Exception as e:
        await message.answer(
            f"❌ <b>Ошибка отправки!</b> {e}",
            parse_mode="HTML"
        )
        logger.error(f"Ошибка отправки результатов: {e}") 
    
    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("register_match_"))
async def register_match_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает начало регистрации матча админом."""
    match_id = callback.data.replace("register_match_", "")
    admin_id = callback.from_user.id
    
    # [ASYNC-REWRITE]
    admin_role = await get_user_role(admin_id)
    admin_level = ROLE_LEVELS.get(admin_role, 0)

    if admin_level < ROLE_LEVELS[ROLE_GAME_REG]:
        await callback.answer("❌ У вас нет прав!", show_alert=True)
        return
    
    match_db = await db_fetchone("SELECT status FROM matches WHERE match_id = $1", match_id)
    
    if not match_db:
        await callback.answer(f"❌ Ошибка! Матч {match_id} не найден в БД!", show_alert=True)
        try:
            await callback.message.edit_text(
                callback.message.text + "\n\n<b>❌ ОШИБКА: Матч не найден в БД.</b>",
                parse_mode="HTML",
                reply_markup=None
            )
        except TelegramBadRequest:
            pass
        return

    if match_db['status'] == 'completed':
        await callback.answer("⚠️ Этот матч уже зарегистрирован!", show_alert=True)
        try:
            await callback.message.edit_text(
                callback.message.text + "\n\n<b>⚠️ Этот матч уже был зарегистрирован.</b>",
                parse_mode="HTML",
                reply_markup=get_admin_post_registration_keyboard(match_id)
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e):
                logger.warning(f"Не удалось обновить дубликат регистрации: {e}")
        return
    
    await state.update_data(match_id=match_id)
    await state.set_state(AdminMatchRegistration.waiting_for_match_data)
    
    await callback.message.answer(
        "<b>📝 Введите данные матча в формате:</b>\n\n"
        "<code>tg_id k d, tg_id k d, ...</code> (всего 10 игроков)\n\n"
        "<b>Пример (10 игроков через запятую):</b>\n"
        "<code>123 15 10, 456 12 11, 789 18 9, 101 10 12, 102 14 10, "
        "201 11 13, 202 13 11, 203 16 10, 204 9 14, 205 12 12</code>\n\n"
        "<i>❗️ Первые 5 игроков - команда <b>победителей</b>.</i>\n"
        "<i>❗️ Последние 5 игроков - команда <b>проигравших</b>.</i>",
        parse_mode="HTML"
    )
    await callback.answer()

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminMatchRegistration.waiting_for_match_data, F.text)
async def process_match_registration(message: types.Message, state: FSMContext):
    """
    (PG) Обрабатывает регистрацию матча с СТАТИЧЕСКИМ ELO + БОНУСЫ ПРЕМИУМА
    """
    data = await state.get_data()
    match_id = data.get('match_id')
    if not match_id:
        await message.answer("❌ Ошибка FSM: `match_id` не найден. Начните регистрацию заново.")
        await state.clear()
        return

    # [ИЗМЕНЕНИЕ] Мы соберем ID всех 10 игроков в этот список
    all_player_ids_to_clear_cache = []

    try:
        match_db = await db_fetchone(
            """SELECT l.league, m.status, m.last_registration_data 
               FROM matches m 
               JOIN lobbies l ON m.lobby_id = l.lobby_id 
               WHERE m.match_id = $1""", 
            match_id
        )

        if not match_db:
            await message.answer(f"❌ Ошибка! Матч {match_id} не найден или не привязан к лобби.")
            await state.clear()
            return
            
        league_name = match_db['league']
        logger.info(f"Регистрация матча {match_id} для лиги: {league_name}")

        if match_db['status'] == 'completed':
            logger.warning(f"Обнаружена ПЕРЕ-регистрация матча {match_id}. Откат старых статов...")
            
            success, error_msg, user_ids = await rollback_match_stats(match_id)
            
            if not success:
                await message.answer(f"❌ Ошибка при откате старой статистики: {error_msg}. Регистрация отменена.")
                await state.clear()
                return
            
            await message.answer(f"✅ Статистика матча <code>{match_id}</code> отменена. Применяю новую...", parse_mode="HTML")
            
            # [ИЗМЕНЕНИЕ] Очищаем кэш игроков при откате
            for user_id_to_clear in user_ids:
                await clear_user_cache(user_id_to_clear)

            await notify_players_of_change(
                bot, 
                user_ids, 
                (f"<b>⚠️ ВНИМАНИЕ!</b>\n"
                 f"Администратор производит <b>пере-регистрацию</b> матча <code>{match_id}</code>. "
                 f"Ваша предыдущая статистика для этого матча была отменена.")
            )
        
        text = message.text.strip()
        entries = [e.strip() for e in text.split(',')]
        
        if len(entries) != 10:
            await message.answer(f"❌ Неверный формат! Требуется ровно 10 игроков. Найдено: {len(entries)}")
            return
        
        players_data = []
        for entry in entries:
            parts = entry.split()
            if len(parts) != 3:
                await message.answer(f"❌ Ошибка в записи: '{entry}'\nФормат: <code>tg_id k d</code>", parse_mode="HTML")
                return
            try:
                # [PG-FIX] ID может быть отрицательным (для ботов)
                if not parts[0].lstrip('-').isdigit() or not parts[1].isdigit() or not parts[2].isdigit():
                     raise ValueError(f"ID/K/D должны быть числами (в '{entry}')")
                user_id = int(parts[0]) 
                kills = int(parts[1])
                deaths = int(parts[2])
                if kills < 0 or deaths < 0:
                    raise ValueError(f"K/D не могут быть отрицательными (в '{entry}')")
                players_data.append({'user_id': user_id, 'kills': kills, 'deaths': deaths})
            except ValueError as e: 
                await message.answer(f"❌ Ошибка в записи: '{entry}'\nУбедитесь, что ID - число (может быть -), а K/D - полож. числа. ({e})", parse_mode="HTML")
                return
        
        winners = players_data[:5]
        losers = players_data[5:10]
        
        registration_data_to_store = []
        
        # [PG-REWRITE] Используем одну транзакцию для всех обновлений
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                
                for player in winners:
                    all_player_ids_to_clear_cache.append(player['user_id'])
                    # [ASYNC-REWRITE]
                    user_stats = await get_user_league_stats(player['user_id'], league_name) # dict
                    
                    # --- [PREMIUM LOGIC START] ---
                    is_premium = False
                    # Проверяем кэш или делаем запрос, если кэша нет (для надежности внутри транзакции лучше запрос, но кэш быстрее)
                    # Используем get_cached_user_data, так как он безопасен.
                    # Но внутри транзакции лучше прямой SQL для актуальности.
                    prem_check = await conn.fetchrow("SELECT premium_until FROM users WHERE user_id = $1", player['user_id'])
                    if prem_check and prem_check['premium_until']:
                        if prem_check['premium_until'] > datetime.now():
                            is_premium = True
                    
                    current_elo = user_stats['elo']
                    base_change, _ = get_static_elo_change(current_elo)
                    
                    # Бонус +5 ELO за победу для премиума
                    elo_change_winner = base_change + 5 if is_premium else base_change
                    new_elo = current_elo + elo_change_winner
                    # --- [PREMIUM LOGIC END] ---

                    # [ASYNC-REWRITE]
                    await check_and_upgrade_league(conn, player['user_id'], new_elo) 
                    
                    new_kills = user_stats['kills'] + player['kills']
                    new_deaths = user_stats['deaths'] + player['deaths']
                    score_change = player['kills'] * 10
                    new_total_score = user_stats['total_score'] + score_change
                    
                    await conn.execute("""
                        UPDATE user_league_stats SET 
                            elo = $1, wins = wins + 1, matches_played = matches_played + 1,
                            kills = $2, deaths = $3, total_score = $4
                        WHERE user_id = $5 AND league_name = $6
                    """, new_elo, new_kills, new_deaths, new_total_score, player['user_id'], league_name)
                    
                    registration_data_to_store.append({
                        'user_id': player['user_id'], 'win': 1, 'elo_change': elo_change_winner,
                        'kills': player['kills'], 'deaths': player['deaths'], 'score_change': score_change
                    })
                    
                    if player['user_id'] > 0:
                        try:
                            premium_msg = " (👑 Premium +5)" if is_premium else ""
                            await bot.send_message(
                                player['user_id'],
                                f"<b>🏆 Матч #{match_id} ({league_name}) завершен! (Победа)</b>\n"
                                f"📊 <b>K/D:</b> {player['kills']}/{player['deaths']}\n"
                                f"💰 <b>ELO:</b> +{elo_change_winner}{premium_msg} (Стало: {new_elo})",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            logger.warning(f"Не удалось уведомить победителя {player['user_id']}: {e}")

                for player in losers:
                    all_player_ids_to_clear_cache.append(player['user_id'])
                    user_stats = await get_user_league_stats(player['user_id'], league_name) # dict
                    
                    # --- [PREMIUM LOGIC START] ---
                    is_premium = False
                    prem_check = await conn.fetchrow("SELECT premium_until FROM users WHERE user_id = $1", player['user_id'])
                    if prem_check and prem_check['premium_until']:
                        if prem_check['premium_until'] > datetime.now():
                            is_premium = True

                    current_elo = user_stats['elo']
                    _, base_loss = get_static_elo_change(current_elo) # base_loss отрицательный (напр. -25)
                    
                    # Бонус +5 ELO (теряет меньше) за поражение для премиума
                    # Пример: -25 + 5 = -20
                    elo_change_loser = base_loss + 5 if is_premium else base_loss
                    
                    new_elo = max(0, current_elo + elo_change_loser) 
                    # --- [PREMIUM LOGIC END] ---
                    
                    new_kills = user_stats['kills'] + player['kills']
                    new_deaths = user_stats['deaths'] + player['deaths']
                    score_change = player['kills'] * 10
                    new_total_score = user_stats['total_score'] + score_change
                    
                    await conn.execute("""
                        UPDATE user_league_stats SET 
                            elo = $1, losses = losses + 1, matches_played = matches_played + 1,
                            kills = $2, deaths = $3, total_score = $4
                        WHERE user_id = $5 AND league_name = $6
                    """, new_elo, new_kills, new_deaths, new_total_score, player['user_id'], league_name)
                    
                    registration_data_to_store.append({
                        'user_id': player['user_id'], 'win': 0, 'elo_change': elo_change_loser,
                        'kills': player['kills'], 'deaths': player['deaths'], 'score_change': score_change
                    })

                    if player['user_id'] > 0:
                        try:
                            premium_msg = " (👑 Premium Saved 5)" if is_premium else ""
                            await bot.send_message(
                                player['user_id'],
                                f"<b>🏆 Матч #{match_id} ({league_name}) завершен! (Поражение)</b>\n"
                                f"📊 <b>K/D:</b> {player['kills']}/{player['deaths']}\n"
                                f"💰 <b>ELO:</b> {elo_change_loser}{premium_msg} (Стало: {new_elo})",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            logger.warning(f"Не удалось уведомить проигравшего {player['user_id']}: {e}")
                
                reg_data_json = json.dumps(registration_data_to_store)
                
                await conn.execute(
                    "UPDATE matches SET status = 'completed', last_registration_data = $1 WHERE match_id = $2", 
                    reg_data_json, match_id
                )
                
                if match_db['status'] != 'completed':
                    match_lobby = await conn.fetchrow("SELECT lobby_id FROM matches WHERE match_id = $1", match_id)
                    if match_lobby:
                        await conn.execute("UPDATE lobbies SET current_players = 0, status = 'waiting' WHERE lobby_id = $1", match_lobby['lobby_id'])
                        await conn.execute("DELETE FROM lobby_members WHERE lobby_id = $1", match_lobby['lobby_id'])
        
        #
        # [ИЗМЕНЕНИЕ] Очищаем кэш ПОСЛЕ того, как транзакция завершилась
        #
        for player_id in all_player_ids_to_clear_cache:
            await clear_user_cache(player_id)
        
        
        final_confirmation_text = (
            f"✅ <b>Матч #{match_id} (Лига: {league_name}) успешно зарегистрирован!</b>\n\n"
            f"<b>Результаты:</b>\n"
            f"🏆 Победители: {len(winners)}\n"
            f"💔 Проигравшие: {len(losers)}\n\n"
            f"📊 Статистика (включая ELO и Premium-бонусы) добавлена в профили игроков."
        )
        await message.answer(
            final_confirmation_text,
            reply_markup=get_admin_post_registration_keyboard(match_id),
            parse_mode="HTML"
        )
        
        if RESULTS_CHANNEL_ID:
            try:
                await message.bot.send_message(
                    RESULTS_CHANNEL_ID,
                    final_confirmation_text,
                    message_thread_id=MATCH_THREAD_ID, # [PG-FIX] Добавлен thread_id сюда
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Не удалось отправить результаты матча {match_id} в канал {RESULTS_CHANNEL_ID}: {e}")
        
    except Exception as e:
        await message.answer(
            f"❌ <b>Критическая ошибка регистрации:</b> {e}\n\n"
            f"Проверьте формат данных и логи бота.",
            parse_mode="HTML"
        )
        logger.error(f"Критическая ошибка регистрации матча: {e}", exc_info=True)
    
    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "main_teams")  
async def party_main_handler(callback: types.CallbackQuery):
    """Отображает информацию о команде (исправлено для 3 игроков)."""
    user_id = callback.from_user.id
    
    # Получаем данные текущего пользователя
    user_data = await db_fetchone("SELECT user_id, nickname, teammate_user_id, teammate2_user_id, pending_invite_to, premium_until FROM users WHERE user_id = $1", user_id)
    
    if not user_data:
        await callback.answer("❌ Ошибка профиля", show_alert=True)
        return

    # --- ЛОГИКА ОПРЕДЕЛЕНИЯ ЛИДЕРА И СОСТАВА ---
    # По умолчанию считаем, что мы сами лидер, пока не доказано обратное
    leader_data = user_data
    
    # Если у нас есть записанный тиммейт
    if user_data['teammate_user_id']:
        potential_leader_id = user_data['teammate_user_id']
        potential_leader = await db_fetchone("SELECT user_id, nickname, teammate_user_id, teammate2_user_id, premium_until FROM users WHERE user_id = $1", potential_leader_id)
        
        if potential_leader:
            # 1. Если у "потенциального лидера" есть 3-й игрок (teammate2), то он ТОЧНО лидер (обычные члены не имеют t2)
            if potential_leader['teammate2_user_id']:
                leader_data = potential_leader
            
            # 2. Если у "потенциального лидера" в teammate1 записаны МЫ -> Это Дуо (A<->B).
            # В таком случае визуально показываем обоих, но лидером считаем того, чью карточку смотрим, или оставляем potential.
            # Чтобы 3-й игрок видел всех, важнее пункт 1. 
            # Если мы 3-й игрок (мы в слоте teammate2 у лидера), то у нас t1 = лидер. Сработает условие 1.
            
            # Дополнительная проверка: Если мы записаны у него в teammate2, он точно лидер
            elif potential_leader['teammate2_user_id'] == user_id:
                leader_data = potential_leader

    # Теперь собираем всех членов команды ОТ ЛИДЕРА
    party_members = [leader_data]
    
    # Добавляем тиммейта 1 лидера (если это не он сам, хотя такого быть не должно)
    if leader_data['teammate_user_id'] and leader_data['teammate_user_id'] != leader_data['user_id']:
        t1 = await db_fetchone("SELECT user_id, nickname FROM users WHERE user_id = $1", leader_data['teammate_user_id'])
        if t1: party_members.append(t1)
        
    # Добавляем тиммейта 2 лидера
    if leader_data['teammate2_user_id'] and leader_data['teammate2_user_id'] != leader_data['user_id']:
        t2 = await db_fetchone("SELECT user_id, nickname FROM users WHERE user_id = $1", leader_data['teammate2_user_id'])
        if t2: party_members.append(t2)

    # Удаляем дубликаты (на случай кривой БД) и формируем список
    unique_members_dict = {m['user_id']: m for m in party_members}
    unique_members = list(unique_members_dict.values())
    
    # --- ФОРМИРОВАНИЕ ТЕКСТА ---
    game_line = f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>"
    header_line = "<b>🎯 Ваша команда:</b>"
    
    member_lines = ""
    current_team_size = len(unique_members)
    has_team = current_team_size > 1
    
    if has_team:
        for i, m in enumerate(unique_members, 1):
            # Добавляем пометку (Лидер)
            role_mark = " (Лидер)" if m['user_id'] == leader_data['user_id'] else ""
            nick = await format_nickname(m['user_id'], m['nickname'])
            member_lines += f"👤 Игрок {i}: {nick} (<code>{m['user_id']}</code>){role_mark}\n"
    else:
        member_lines = "У вас нет команды."
        
    text = f"{game_line}\n{header_line}\n\n{member_lines}"
    
    # --- ЛОГИКА КНОПОК ---
    keyboard_btns = []

    # Проверка премиума ЛИДЕРА (лимиты зависят от лидера)
    leader_is_premium = False
    if leader_data.get('premium_until') and leader_data['premium_until'] > datetime.now():
        leader_is_premium = True

    max_slots = 3 if leader_is_premium else 2

    # Кнопка "Пригласить":
    # 1. Есть места
    # 2. ВЫ ЯВЛЯЕТЕСЬ ЛИДЕРОМ (user_id == leader_data['user_id'])
    if current_team_size < max_slots and user_id == leader_data['user_id']:
        keyboard_btns.append([InlineKeyboardButton(text="✉️ Пригласить в команду", callback_data="party_invite_start")])

    # Кнопка "Покинуть": Если команда есть
    if has_team:
        keyboard_btns.append([InlineKeyboardButton(text="🚪 Покинуть команду", callback_data="party_leave")])

    # Проверка входящих инвайтов (только для текущего юзера)
    if user_data['pending_invite_to']:
         inviter = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_data['pending_invite_to'])
         if inviter:
             text += f"\n\n📩 <b>Приглашение от {inviter['nickname']}</b>"
             keyboard_btns.insert(0, [
                 InlineKeyboardButton(text="✅ Принять", callback_data=f"invite_accept_{user_data['pending_invite_to']}"),
                 InlineKeyboardButton(text="❌ Отклонить", callback_data=f"invite_decline_{user_data['pending_invite_to']}")
             ])
    
    keyboard_btns.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")])
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=PARTY_FILE_ID, caption=text, parse_mode="HTML"),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_btns)
        )
    except Exception: pass
    await callback.answer()

# ... (party_invite_start_handler - без изменений FSM) ...
@dp.callback_query(F.data == "party_invite_start")
async def party_invite_start_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начинает процесс приглашения в команду."""
    
    text = ("<b>✉️ Приглашение в команду</b>\n\n"
            "Отправьте <b>Telegram ID</b> игрока, которого хотите пригласить:")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_teams")] 
    ])

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=PARTY_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass 
        else:
            logger.error(f"Ошибка в party_invite_start_handler: {e}")
        
    await state.set_state(Party.waiting_for_invite_id)
    await callback.answer()

# [ASYNC-REWRITE]
@dp.callback_query(Party.waiting_for_invite_id, F.data == "main_teams")
async def party_invite_back(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в меню команд из приглашения."""
    await state.clear()
    await party_main_handler(callback) # Он уже async

# [ASYNC-REWRITE]
@dp.message(Party.waiting_for_invite_id, F.text)
async def process_invite_id(message: types.Message, state: FSMContext):
    """(PG) Обрабатывает приглашение с учетом Премиума (3 игрока)."""
    inviter_id = message.from_user.id
    target_id_str = message.text.strip()
    
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("<b>❌ Ошибка!</b> Telegram ID должен состоять только из цифр.", parse_mode="HTML")
        return
    
    if target_id == inviter_id:
        await message.answer("<b>❌ Ошибка!</b> Вы не можете пригласить самого себя.", parse_mode="HTML")
        return
        
    inviter_data = await db_fetchone("SELECT nickname, teammate_user_id, teammate2_user_id, premium_until FROM users WHERE user_id = $1", inviter_id)
    
    # Проверка Премиума
    is_premium = False
    if inviter_data['premium_until'] and inviter_data['premium_until'] > datetime.now():
        is_premium = True
        
    # Логика слотов
    slots_full = False
    if not inviter_data['teammate_user_id']:
        pass # 1 слот свободен
    elif is_premium and not inviter_data['teammate2_user_id']:
        pass # 2 слот свободен (только для премиума)
    else:
        slots_full = True
        
    if slots_full:
        limit = "3" if is_premium else "2"
        await message.answer(f"<b>❌ Ошибка!</b> Ваша команда переполнена (макс. {limit} игрока).", parse_mode="HTML")
        await state.clear()
        return

    target_data = await db_fetchone("SELECT nickname, teammate_user_id, pending_invite_to FROM users WHERE user_id = $1 AND is_registered = TRUE", target_id)
    
    if not target_data:
        await message.answer("<b>❌ Ошибка!</b> Пользователь не найден.", parse_mode="HTML")
        return
    if target_data['teammate_user_id']:
        await message.answer(f"<b>❌ Ошибка!</b> Игрок уже в команде.", parse_mode="HTML")
        return
    if target_data['pending_invite_to']:
        await message.answer(f"<b>❌ Ошибка!</b> У игрока уже есть приглашение.", parse_mode="HTML")
        return
        
    try:
        await db_execute("UPDATE users SET pending_invite_to = $1 WHERE user_id = $2", inviter_id, target_id)
        
        # Отправляем форматированный ник
        inviter_nick = await format_nickname(inviter_id, inviter_data['nickname'])
        
        invite_text = (
            f"<b>✉️ Приглашение в команду!</b>\n\n"
            f"Игрок <b>{inviter_nick}</b> приглашает вас в свою команду."
        )
        await bot.send_message(target_id, invite_text, reply_markup=get_invite_response_keyboard(inviter_id), parse_mode="HTML")
        await message.answer(f"<b>✅ Приглашение успешно отправлено!</b>", parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"<b>❌ Ошибка отправки!</b> {e}", parse_mode="HTML")
        
    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("invite_accept_"))
async def invite_accept_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает принятие приглашения (поддержка 3 игроков)."""
    try:
        inviter_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("❌ Ошибка ID", show_alert=True)
        return
        
    target_id = callback.from_user.id
    
    target_data = await db_fetchone("SELECT nickname, pending_invite_to, teammate_user_id FROM users WHERE user_id = $1", target_id)
    if not target_data or target_data['pending_invite_to'] != inviter_id:
        await callback.message.edit_text("❌ Приглашение неактуально.", parse_mode="HTML")
        return
    if target_data['teammate_user_id']:
        await callback.message.edit_text("❌ Вы уже в команде!", parse_mode="HTML")
        return

    inviter_data = await db_fetchone("SELECT nickname, teammate_user_id, teammate2_user_id, premium_until FROM users WHERE user_id = $1", inviter_id)
    
    # Логика заполнения слотов
    slot_to_fill = None
    
    if not inviter_data['teammate_user_id']:
        slot_to_fill = "teammate_user_id"
    else:
        # Проверка на премиум и 2-й слот
        is_premium = False
        if inviter_data['premium_until'] and inviter_data['premium_until'] > datetime.now():
            is_premium = True
            
        if is_premium and not inviter_data['teammate2_user_id']:
            slot_to_fill = "teammate2_user_id"
            
    if not slot_to_fill:
        await callback.message.edit_text("❌ Команда уже заполнена!", parse_mode="HTML")
        await db_execute("UPDATE users SET pending_invite_to = NULL WHERE user_id = $1", target_id)
        return

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Записываем target_id в слот инвайтера
                await conn.execute(f"UPDATE users SET {slot_to_fill} = $1 WHERE user_id = $2", target_id, inviter_id)
                # Записываем inviter_id как тиммейта для target (всегда teammate_user_id, это ссылка на лидера)
                await conn.execute("UPDATE users SET teammate_user_id = $1, pending_invite_to = NULL WHERE user_id = $2", inviter_id, target_id)
                
                # Если это 3-й игрок, нужно связать его со 2-м игроком? 
                # В этой простой системе все ссылаются на лидера. Лидер хранит всех.
                
    except Exception as e:
        logger.error(f"Ошибка пати: {e}")
        await callback.message.edit_text("❌ Ошибка БД.", parse_mode="HTML")
        return

    inviter_nick = await format_nickname(inviter_id, inviter_data['nickname'])
    target_nick = await format_nickname(target_id, target_data['nickname'])
    
    await callback.message.edit_text(f"<b>✅ Вы приняли приглашение!</b>\nВы в команде с <b>{inviter_nick}</b>.", parse_mode="HTML")
    
    try:
        await bot.send_message(inviter_id, f"<b>🎉 Игрок {target_nick} вступил в вашу команду!</b>", parse_mode="HTML")
    except: pass
        
    await state.clear()
    await callback.answer()
    
# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("invite_decline_"))
async def invite_decline_handler(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает отклонение приглашения."""
    try:
        inviter_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("❌ Ошибка ID пригласившего", show_alert=True)
        return
        
    target_id = callback.from_user.id

    target_data = await db_fetchone("SELECT nickname, pending_invite_to FROM users WHERE user_id = $1", target_id)
    
    if not target_data or target_data['pending_invite_to'] != inviter_id:
        await callback.message.edit_text("❌ Приглашение неактуально или не найдено.", parse_mode="HTML")
        await callback.answer()
        return
        
    await db_execute("UPDATE users SET pending_invite_to = NULL WHERE user_id = $1", target_id)
    
    inviter_data = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", inviter_id)
    inviter_nickname = inviter_data['nickname'] if inviter_data else str(inviter_id)
    
    await callback.message.edit_text(
        f"<b>❌ Вы отклонили приглашение</b> от <b>{html.escape(inviter_nickname)}</b>.", 
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            inviter_id,
            f"<b>❌ Игрок {html.escape(target_data['nickname'])} отклонил ваше приглашение.</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass 
        
    await callback.answer("Приглашение отклонено.")

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "party_leave")
async def party_leave_handler(callback: types.CallbackQuery):
    """Обрабатывает выход из команды (полный сброс для всех)."""
    user_id = callback.from_user.id
    
    user_data = await db_fetchone("SELECT teammate_user_id, teammate2_user_id FROM users WHERE user_id = $1", user_id)
    if not user_data: return

    # Собираем всех причастных. 
    # В этой логике проще очистить всех, кто связан с вышедшим игроком.
    ids_to_clear = {user_id}
    
    # Если я лидер (у меня есть слоты)
    if user_data.get('teammate_user_id'): ids_to_clear.add(user_data['teammate_user_id'])
    if user_data.get('teammate2_user_id'): ids_to_clear.add(user_data['teammate2_user_id'])
    
    # Если я не лидер, а участник (мой teammate_user_id - это лидер)
    leader_id = await db_fetchone("SELECT teammate_user_id FROM users WHERE user_id = $1", user_id)
    if leader_id and leader_id['teammate_user_id'] and leader_id['teammate_user_id'] != user_id:
        leader_id = leader_id['teammate_user_id']
        ids_to_clear.add(leader_id)
        
        # Собираем остальных членов пати лидера
        leader_data = await db_fetchone("SELECT teammate_user_id, teammate2_user_id FROM users WHERE user_id = $1", leader_id)
        if leader_data:
            if leader_data['teammate_user_id']: ids_to_clear.add(leader_data['teammate_user_id'])
            if leader_data['teammate2_user_id']: ids_to_clear.add(leader_data['teammate2_user_id'])
            
    # Убираем сам user_id из списка, если он вдруг попал дважды, и очищаем от NULL
    ids_to_clear = {i for i in ids_to_clear if i is not None and i > 0} 

    if len(ids_to_clear) <= 1:
        await callback.answer("У вас нет команды.", show_alert=True)
        return

    # Транзакция очистки
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for uid in ids_to_clear:
                # Очищаем все поля, связанные с пати
                await conn.execute("UPDATE users SET teammate_user_id = NULL, teammate2_user_id = NULL, pending_invite_to = NULL WHERE user_id = $1", uid)
                
                if uid != user_id:
                    try:
                        # Убедитесь, что эта строка (и try/except) имеет 4 уровня отступа
                        await bot.send_message(uid, "<b>🚪 Команда была расформирована.</b>", parse_mode="HTML")
                    except: 
                        pass # Не страшно, если не удалось отправить

    # Обновляем экран
    await party_main_handler(callback)
    await callback.answer("🚪 Вы покинули команду.", show_alert=True)

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("main_leaderboard") | F.data.startswith("lb_"))
async def leaderboard_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_data = callback.data
    
    user_db_data = await db_fetchone("SELECT league FROM users WHERE user_id = $1", user_id)
    user_own_league = user_db_data.get('league', DEFAULT_LEAGUE) if user_db_data else DEFAULT_LEAGUE

    if current_data.startswith("lb_"):
        current_league = current_data.split("_")[1]
    else:
        current_league = user_own_league
    
    # [FIX] Исправлен фильтр ID. Теперь показываем всех пользователей с ID > 0
    top_players = await db_fetchall(
        """
        SELECT u.user_id, u.nickname, s.elo 
        FROM user_league_stats s
        JOIN users u ON u.user_id = s.user_id
        WHERE s.league_name = $1 AND u.user_id > 0
        ORDER BY s.elo DESC 
        LIMIT 10
        """,
        current_league
    )
    
    header = (
        f"<blockquote><b>🔑 {SINGLE_GAME_NAME}</b></blockquote>\n"
        f"<blockquote><b>🥇 {current_league} лига</b></blockquote>\n\n"
        "<b>🏆 ТОП-10 игроков:</b>\n\n"
    )
    
    leaderboard_list = []
    is_user_in_top = False
    
    if top_players:
        for index, player in enumerate(top_players, 1):
            nickname = player.get('nickname') or "Неизвестный"
            # Добавляем звезду
            formatted_nick = await format_nickname(player['user_id'], nickname)
            level_emoji = get_faceit_level_emoji(player['elo'])
            
            line = f"{index}. {level_emoji} {formatted_nick} — {player['elo']} ELO"
            leaderboard_list.append(line)
            
            if player['user_id'] == user_id:
                is_user_in_top = True
    else:
        leaderboard_list.append("Нет игроков в этой лиге.")
    
    warning_line = ""
    user_level = LEAGUE_LEVELS.get(user_own_league, 0)
    current_level_on_display = LEAGUE_LEVELS.get(current_league, 0)

    if not is_user_in_top:
        if user_level >= current_level_on_display:
             user_rank_data = await db_fetchone(
                 f"""WITH ranked_users AS (
                     SELECT user_id, elo, ROW_NUMBER() OVER (ORDER BY elo DESC) as rank
                     FROM user_league_stats WHERE league_name = $1
                 ) SELECT rank, elo FROM ranked_users WHERE user_id = $2""",
                 current_league, user_id
             )
             if user_rank_data:
                 warning_line = f"\n• <b>Ваше место: {user_rank_data['rank']} ({user_rank_data['elo']} ELO)</b>"
             else:
                 warning_line = "\n⚠️ Вас нет в этом топе."
        else:
             warning_line = f"\n⚠️ У вас нет доступа к <b>{current_league}</b>."

    final_text = header + "\n".join(leaderboard_list) + warning_line
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=PLAYER_RATING_FILE_ID, caption=final_text, parse_mode="HTML"),
            reply_markup=get_leaderboard_keyboard(current_league)
        )
    except TelegramBadRequest:
        await bot.send_photo(callback.message.chat.id, photo=PLAYER_RATING_FILE_ID, caption=final_text, reply_markup=get_leaderboard_keyboard(current_league), parse_mode="HTML")
    await callback.answer()

# ... (show_elo_info_handler - без изменений) ...
@dp.callback_query(F.data == "show_elo_info")
async def show_elo_info_handler(callback: types.CallbackQuery):
    """(ИСПРАВЛЕНО) Обрабатывает нажатие "Все о ELO" и показывает информацию о системе."""
    await callback.answer()
    
    photo_id = PLAYER_RATING_FILE_ID # [PG-FIX] В оригинале был PROFILE_FILE_ID, но логичнее PLAYER_RATING
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в Рейтинг", callback_data="main_leaderboard")]
    ])
    
    info_text = (
        "<b>❓ Все о системе ELO и лигах</b>\n\n"
        "<b>Система ELO</b> — это рейтинг, который определяет ваш уровень "
        "мастерства. ELO меняется после каждого матча:\n\n"
        
        "<b>🥇 Лиги:</b>\n"
        f"• <b>{DEFAULT_LEAGUE}:</b> Базовая лига.\n"
        f"• <b>{QUAL_LEAGUE}:</b> Квалификационная лига для лучших игроков.\n"
        f"• <b>{FPL_LEAGUE}:</b> Элитная лига для профессионалов.\n\n"
        
        "<b>📈 Уровни ELO:</b>\n"
        f"• {LEVEL_EMOJI_MAP[1]} Уровень 1: 0 - 300 ELO\n"
        f"• {LEVEL_EMOJI_MAP[2]} Уровень 2: 300 - 500 ELO\n"
        f"• {LEVEL_EMOJI_MAP[3]} Уровень 3: 500 - 700 ELO\n"
        f"• {LEVEL_EMOJI_MAP[4]} Уровень 4: 700 - 900 ELO\n"
        f"• {LEVEL_EMOJI_MAP[5]} Уровень 5: 900 - 1100 ELO\n"
        f"• {LEVEL_EMOJI_MAP[6]} Уровень 6: 1100 - 1350 ELO\n"
        f"• {LEVEL_EMOJI_MAP[7]} Уровень 7: 1350 - 1600 ELO\n"
        f"• {LEVEL_EMOJI_MAP[8]} Уровень 8: 1600 - 1750 ELO\n"
        f"• {LEVEL_EMOJI_MAP[9]} Уровень 9: 1750 - 2100 ELO\n"
        f"• {LEVEL_EMOJI_MAP[10]} Уровень 10: 2100+ ELO"
    )

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=info_text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            logger.error(f"Error editing ELO info message: {e}")

# ... (ticket_start_handler - без изменений FSM) ...
@dp.callback_query(F.data == "main_ticket")
async def ticket_start_handler(callback: types.CallbackQuery, state: FSMContext):
    """Начинает создание тикета с проверкой кулдауна."""
    user_id = callback.from_user.id
    
    # Проверка кулдауна (10 минут)
    user_data = await db_fetchone("SELECT last_ticket_at FROM users WHERE user_id = $1", user_id)
    if user_data and user_data['last_ticket_at']:
        last_time = user_data['last_ticket_at']
        diff = datetime.now() - last_time
        if diff.total_seconds() < 600: # 600 секунд = 10 минут
            minutes_left = int((600 - diff.total_seconds()) / 60)
            await callback.answer(f"⏳ Подождите {minutes_left} мин. перед отправкой следующего тикета.", show_alert=True)
            return

    await state.clear()
    
    ticket_game_prompt = "📝 <b>Выберите приватку, в которой вы хотите создать тикет:</b>"
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=ticket_game_prompt, parse_mode="HTML"),
            reply_markup=get_ticket_game_choice_keyboard()
        )
    except TelegramBadRequest:
        pass
            
    await state.set_state(Ticket.waiting_for_ticket_game_choice)
    await callback.answer()

# ... (process_ticket_game_choice - без изменений FSM) ...
@dp.callback_query(Ticket.waiting_for_ticket_game_choice, F.data.startswith("ticket_game_"))
async def process_ticket_game_choice(callback: types.CallbackQuery, state: FSMContext):
    """Спрашивает тип тикета."""
    game_name = callback.data.split("_")[-1]
    await state.update_data(ticket_game=game_name)
    
    text = (
        "<b>📂 Выберите тип жалобы:</b>\n\n"
        "🆔 <b>По матчу</b> — Если жалоба касается конкретной игры (читы, нарушение правил).\n"
        "💬 <b>Общая</b> — Вопросы, предложения или проблемы без ID матча."
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🆔 По матчу (Есть ID)", callback_data="ticket_type_match")],
        [InlineKeyboardButton(text="💬 Общая (Без ID)", callback_data="ticket_type_general")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="ticket_cancel")]
    ])
    
    await callback.message.edit_caption(caption=text, reply_markup=keyboard, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(Ticket.waiting_for_ticket_game_choice, F.data == "ticket_type_general")
async def process_ticket_general(callback: types.CallbackQuery, state: FSMContext):
    """Общий тикет без ID."""
    await state.update_data(match_id=None)
    
    await callback.message.edit_caption(
        caption="<b>📝 Опишите вашу проблему или вопрос:</b>",
        reply_markup=get_ticket_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(Ticket.waiting_for_ticket_text)
    await callback.answer()

@dp.callback_query(Ticket.waiting_for_ticket_game_choice, F.data == "ticket_type_match")
async def process_ticket_match_req(callback: types.CallbackQuery, state: FSMContext):
    """Запрос Match ID."""
    await callback.message.edit_caption(
        caption="<b>🔢 Введите Match ID матча:</b>",
        reply_markup=get_ticket_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(Ticket.waiting_for_match_id)
    await callback.answer()

# [ASYNC-REWRITE]
@dp.message(Ticket.waiting_for_match_id, F.text)
async def process_match_id(message: types.Message, state: FSMContext):
    """(PG) Обрабатывает ввод Match ID, проверяя его наличие в БД."""
    
    match_id_raw = message.text.strip()
    match_id = match_id_raw.lstrip('#') 

    if not re.match(r"^[a-zA-Z0-9_-]{10,36}$", match_id):
        await message.answer(
            "<b>❌ Ошибка!</b> Match ID должен быть корректным. "
            "Проверьте, что вы ввели полный и правильный идентификатор матча (10-36 символов, буквы/цифры/дефисы).",
            parse_mode="HTML"
        )
        return

    match_data = await db_fetchone("SELECT 1 FROM matches WHERE match_id = $1", match_id)
    if not match_data:
        await message.answer(
            f"<b>❌ Ошибка!</b> Матч с ID <code>{html.escape(match_id_raw)}</code> не найден в базе данных. " 
            "Пожалуйста, проверьте ID и попробуйте снова.",
            parse_mode="HTML"
        )
        return

    await state.update_data(match_id=match_id)
    await message.answer(
        "<b>✅ Match ID принят.</b>\n\n"
        "Теперь <b>опишите вашу жалобу</b> (например: 'игрок 12345 использовал читы').\n\n"
        "<i>Вы сможете прикрепить фото/видео на следующем шаге.</i>",
        reply_markup=get_ticket_cancel_keyboard(),
        parse_mode="HTML"
    )
    await state.set_state(Ticket.waiting_for_ticket_text)

# [ASYNC-REWRITE]
async def _send_ticket_to_admins(state: FSMContext, event: types.Message | types.CallbackQuery, media_file_id: str = None, media_type: str = None):
    """
    (PG) Формирует и отправляет тикет админам, включая медиа и список игроков.
    """
    # Определяем user_id и сообщение для ответа в зависимости от типа события
    user_id = event.from_user.id
    message_for_reply = event.message if isinstance(event, types.CallbackQuery) else event

    user_data = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", user_id)
    data = await state.get_data()
    
    match_id = data.get('match_id', 'N/A')
    game_name = data.get('ticket_game', SINGLE_GAME_NAME)
    ticket_text = data.get('ticket_text', 'Текст не указан.')

    players_list_text = "<b>Участники матча:</b>\n"
    try:
        match_players = await db_fetchone(
            "SELECT team_ct, team_t FROM matches WHERE match_id = $1",
            match_id
        )
        if match_players:
            team_ct = json.loads(match_players['team_ct']) if match_players['team_ct'] else []
            team_t = json.loads(match_players['team_t']) if match_players['team_t'] else []
            all_player_ids = team_ct + team_t
            
            if all_player_ids:
                # [PG-REWRITE] Используем $1 = ANY($2::bigint[]) для поиска по списку
                players_db = await db_fetchall(
                    "SELECT user_id, nickname FROM users WHERE user_id = ANY($1::bigint[])",
                    all_player_ids
                )
                
                player_map = {p['user_id']: p['nickname'] for p in players_db}
                
                for i, p_user_id in enumerate(all_player_ids, 1):
                    nickname = player_map.get(p_user_id, f"ID: {p_user_id}")
                    players_list_text += f"{i}. {html.escape(nickname)} (<code>{p_user_id}</code>)\n"
            else:
                 players_list_text += "<i>(Состав матча пуст)</i>"
        else:
            players_list_text += f"<i>(Матч {match_id} не найден для загрузки состава)</i>"
    except Exception as e:
        logger.error(f"Ошибка получения игроков для тикета {match_id}: {e}")
        players_list_text += "<i>(Ошибка загрузки состава матча)</i>"

    admin_ticket_message = (
        f"<b>🚨 НОВЫЙ ТИКЕТ</b>\n\n"
        f"<b>👤 От пользователя:</b> {html.escape(user_data['nickname'] if user_data else str(user_id))}\n"
        f"<b>🆔 Telegram ID:</b> {code(user_id)}\n"
        f"<b>🔑 Приватка:</b> {game_name}\n"
        f"<b>#️⃣ Match ID:</b> {code(match_id)}\n\n"
        f"<b>📝 Текст жалобы:</b>\n"
        f"{html.escape(ticket_text)}\n\n"
        f"➖➖➖➖➖➖➖➖➖➖\n"
        f"{players_list_text}"
    )

    admin_message_id = None
    ticket_sent = False
    
    try:
        if media_file_id and media_type:
            caption_media = f"Медиа от пользователя {code(user_id)} к тикету (см. след. сообщение)"
            if media_type == 'photo':
                await bot.send_photo(
                    chat_id=TICKET_CHAT_ID,
                    photo=media_file_id,
                    caption=caption_media,
                    message_thread_id=TICKET_THREAD_ID,
                    parse_mode="HTML"
                )
            elif media_type == 'video':
                await bot.send_video(
                    chat_id=TICKET_CHAT_ID,
                    video=media_file_id,
                    caption=caption_media,
                    message_thread_id=TICKET_THREAD_ID,
                    parse_mode="HTML"
                )

        sent_message = await bot.send_message(
            chat_id=TICKET_CHAT_ID,
            text=admin_ticket_message,
            message_thread_id=TICKET_THREAD_ID,
            parse_mode="HTML",
            reply_markup=get_admin_ticket_keyboard(user_id, 0) # Сначала 0, потом обновим
        )
        
        admin_message_id = sent_message.message_id
        ticket_sent = True
        
        # [PG-REWRITE] Сохраняем в БД
        await db_execute(
            "INSERT INTO tickets (user_id, match_id, game_name, ticket_text, admin_message_id, status) VALUES ($1, $2, $3, $4, $5, 'open')", 
            user_id, match_id, game_name, ticket_text, admin_message_id
        )
        
        # Обновляем клавиатуру у админов с ID сообщения
        await bot.edit_message_reply_markup(
            chat_id=TICKET_CHAT_ID,
            message_id=admin_message_id,
            reply_markup=get_admin_ticket_keyboard(user_id, admin_message_id)
        )

        user_confirmation_text = (
            "<blockquote><b>✅ Тикет отправлен!</b></blockquote>\n\n"
            "<b>Ваша жалоба принята в обработку.</b> "
            "Администраторы рассмотрят ее в ближайшее время и ответят вам в личные сообщения.\n\n"
            f"<b>Отправлено:</b> {html.escape(ticket_text[:100])}{'...' if len(ticket_text) > 100 else ''}"
        )
        
        # Отправляем ответ пользователю (используем message_for_reply)
        await message_for_reply.answer(
            user_confirmation_text,
            reply_markup=get_ticket_sent_keyboard(admin_message_id), 
            parse_mode="HTML"
        )
        
    except Exception as e:
        if not ticket_sent:
            error_msg = (
                "<b>❌ Ошибка!</b> Не удалось отправить тикет администраторам. "
                "Пожалуйста, попробуйте позже."
            )
            # Безопасная отправка ошибки пользователю
            try:
                await message_for_reply.answer(error_msg, parse_mode="HTML")
            except:
                pass
            logger.error(f"Ошибка отправки тикета: {e}")

    await state.clear()

@dp.message(Ticket.waiting_for_ticket_text, F.text)
async def process_ticket_text(message: types.Message, state: FSMContext):
    """
    (PG) Обрабатывает текст жалобы и просит медиа.
    """
    ticket_text = message.text.strip()
    
    if not ticket_text or len(ticket_text) < 10:
        await message.answer("<b>Пожалуйста, введите более подробный текст жалобы (минимум 10 символов).</b>", parse_mode="HTML")
        return
        
    await state.update_data(ticket_text=ticket_text)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏩ Пропустить (без фото/видео)", callback_data="ticket_skip_media")],
        [InlineKeyboardButton(text="❌ Отменить тикет", callback_data="ticket_cancel")]
    ])

    await message.answer(
        "<b>📝 Текст жалобы принят.</b>\n\n"
        "Теперь вы можете <b>прикрепить одно фото или видео</b> (не документом!) в качестве доказательства.\n\n"
        "Или нажмите 'Пропустить', чтобы отправить тикет без медиа.",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(Ticket.waiting_for_media)

# [ASYNC-REWRITE]
@dp.callback_query(Ticket.waiting_for_media, F.data == "ticket_skip_media")
async def process_ticket_skip_media(callback: types.CallbackQuery, state: FSMContext):
    """
    (PG) Обрабатывает нажатие 'Пропустить медиа'.
    """
    try:
        await callback.message.edit_text(
            "<b>Отправка тикета...</b>",
            reply_markup=None,
            parse_mode="HTML"
        )
    except TelegramBadRequest:
        pass
        
    # ИСПРАВЛЕНО: Передаем message объекта callback, но user_id берем явно из callback.from_user
    # Чтобы функция _send_ticket_to_admins брала правильный ID
    # Нам нужно немного модифицировать _send_ticket_to_admins или передать правильный контекст
    
    # Самый простой способ - передать message, но "подменить" user_id внутри логики отправки,
    # но так как _send_ticket_to_admins берет message.from_user.id, 
    # мы просто передадим фиктивный message или вызовем логику иначе.
    
    # Но проще всего здесь просто вызвать отправку, используя callback.message 
    # НО в _send_ticket_to_admins заменить user_id = message.from_user.id на user_id = state... 
    # или просто передать правильный объект.
    
    # В данном случае callback.message - это сообщение БОТА.
    # Мы не можем изменить его from_user. 
    # Поэтому мы передадим сам callback, так как он имеет атрибут from_user
    
    await _send_ticket_to_admins(state, callback) # Передаем callback вместо message
    await callback.answer()

# [ASYNC-REWRITE]
@dp.message(Ticket.waiting_for_media, F.photo | F.video)
async def process_ticket_media(message: types.Message, state: FSMContext):
    """
    (PG) Обрабатывает отправку фото или видео.
    """
    media_file_id = None
    media_type = None
    
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_file_id = message.video.file_id
        media_type = 'video'
    
    if media_file_id:
        await message.answer("<b>✅ Медиа получено. Отправка тикета...</b>", parse_mode="HTML")
        await _send_ticket_to_admins(state, message, media_file_id, media_type)
    else:
        await message.answer("Не удалось обработать медиа, попробуйте 'Пропустить'")


# ... (process_ticket_media_text_fallback - без изменений FSM) ...
@dp.message(Ticket.waiting_for_media, F.text)
async def process_ticket_media_text_fallback(message: types.Message, state: FSMContext):
    """
    (НОВАЯ ФУНКЦИЯ) Ловит случайный текст на этапе ожидания медиа.
    """
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏩ Пропустить (без фото/видео)", callback_data="ticket_skip_media")],
        [InlineKeyboardButton(text="❌ Отменить тикет", callback_data="ticket_cancel")]
    ])
    await message.answer(
        "<b>Пожалуйста, отправьте фото/видео или нажмите 'Пропустить'.</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "ticket_cancel")
async def ticket_cancel_callback(callback: types.CallbackQuery, state: FSMContext):
    
    user_id = callback.from_user.id
    
    try:
        # [ASYNC-REWRITE]
        keyboard = await get_main_menu_keyboard(user_id)
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID, 
                caption=TICKET_CANCEL_TEXT,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в ticket_cancel_callback: {e}")

    await state.clear()
    await callback.answer("Тикет отменен.")

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("admin_reregister_"), MinRoleFilter(ROLE_GAME_REG))
async def admin_reregister_handler(callback: types.CallbackQuery, state: FSMContext):
    """
    (PG) Обрабатывает нажатие "Перерегистрировать".
    """
    try:
        match_id = callback.data.replace("admin_reregister_", "")
    except Exception:
        await callback.answer("❌ Ошибка: ID матча не найден.", show_alert=True)
        return

    match_db = await db_fetchone("SELECT status FROM matches WHERE match_id = $1", match_id)
    if not match_db:
        await callback.answer(f"❌ Матч {match_id} не найден в БД!", show_alert=True)
        return

    await state.update_data(match_id=match_id)
    await state.set_state(AdminMatchRegistration.waiting_for_match_data)
    
    await callback.message.answer(
        f"<b>🔄 Пере-регистрация матча <code>{match_id}</code></b>\n\n"
        "<b>ВНИМАНИЕ:</b> Старая статистика будет автоматически отменена.\n\n"
        "Введите новые данные в формате:\n"
        "<code>tg_id k d, ...</code> (10 игроков)",
        parse_mode="HTML"
    )
    
    try:
        await callback.message.edit_text(
            callback.message.text + "\n\n<b>🔄 Ожидание новых данных для пере-регистрации...</b>",
            parse_mode="HTML",
            reply_markup=None
        )
    except TelegramBadRequest:
        pass
        
    await callback.answer()

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("admin_cancel_"), MinRoleFilter(ROLE_GAME_REG))
async def admin_cancel_match_handler(callback: types.CallbackQuery):
    """
    (PG) Обрабатывает нажатие "Отменить матч", откатывая статистику.
    """
    try:
        match_id = callback.data.replace("admin_cancel_", "")
    except Exception:
        await callback.answer("❌ Ошибка: ID матча не найден.", show_alert=True)
        return
        
    logger.info(f"Админ {callback.from_user.id} отменяет матч {match_id}")
    
    success, error_msg, user_ids = await rollback_match_stats(match_id)
    
    if not success:
        await callback.answer(f"❌ Ошибка отмены: {error_msg}", show_alert=True)
        return
    
    await db_execute("UPDATE matches SET status = 'cancelled', last_registration_data = NULL WHERE match_id = $1", match_id)
    
    try:
        await callback.message.edit_text(
            callback.message.text + "\n\n<b>❌❌ МАТЧ ОТМЕНЕН АДМИНИСТРАТОРОМ ❌❌</b>\n(Статистика отозвана)",
            parse_mode="HTML",
            reply_markup=None
        )
    except TelegramBadRequest:
        pass
    
    await callback.answer(f"Матч {match_id} отменен, статистика отозвана.", show_alert=True)
    
    await notify_players_of_change(
        bot, 
        user_ids, 
        (f"<b>❌ ВНИМАНИЕ!</b>\n"
         f"Администратор <b>ОТМЕНИЛ</b> регистрацию матча <code>{match_id}</code>. "
         f"Вся статистика (ELO, K/D) для этого матча была отозвана.")
    )

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("cancel_sent_"))
async def cancel_sent_ticket_callback(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает отмену тикета пользователем."""
    try:
        admin_message_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("❌ Ошибка ID тикета.", show_alert=True)
        return
        
    user_id = callback.from_user.id
    
    ticket_data = await db_fetchone(
        "SELECT status FROM tickets WHERE admin_message_id = $1 AND user_id = $2", 
        admin_message_id, user_id
    )
                             
    if not ticket_data:
        await callback.answer("❌ Ошибка! Тикет не найден.", show_alert=True)
        return

    if ticket_data['status'] != 'open':
        await callback.answer(f"❌ Тикет уже имеет статус: {ticket_data['status']}.", show_alert=True)
        return
        
    await db_execute("UPDATE tickets SET status = 'cancelled' WHERE admin_message_id = $1", admin_message_id)
    
    await callback.message.edit_text(
        "<blockquote><b>❌ Тикет отменен.</b></blockquote>\n\n"
        "<b>Ваша жалоба была отменена.</b>",
        reply_markup=get_ticket_cancelled_keyboard(),
        parse_mode="HTML"
    )
    
    # [PG-FIX] Исходный текст admin_ticket_message утерян. Используем admin_notification_text
    admin_notification_text = (
        "<b>❌ ТИКЕТ ОТМЕНЕН ПОЛЬЗОВАТЕЛЕМ ❌</b>\n\n"
        f"Тикет, отправленный пользователем ID {code(user_id)} (MsgID: {admin_message_id}), был им отменен. "
        "Дальнейшие действия не требуются."
    )
    
    try:
        # Пытаемся отредактировать исходное сообщение в чате тикетов
        await bot.edit_message_text(
            chat_id=TICKET_CHAT_ID,
            message_id=admin_message_id,
            text=admin_notification_text, # [PG-FIX]
            reply_markup=None,
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка редактирования сообщения админа при отмене: {e}")
        try:
            # Если не вышло (удалили?), пишем новое
            await bot.send_message(
                chat_id=TICKET_CHAT_ID,
                text=f"⚠️ **ВНИМАНИЕ!** Тикет ID {admin_message_id} от пользователя {user_id} отменен, но его исходное сообщение не было отредактировано.",
                message_thread_id=TICKET_THREAD_ID,
                parse_mode="HTML"
            )
        except:
            pass

    await callback.answer("Тикет отменен.")

# [ASYNC-REWRITE]
@dp.callback_query(F.data.startswith("admin_answer_"))
async def admin_answer_ticket(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Обрабатывает нажатие "Ответить на тикет" администратором."""
    parts = callback.data.split("_")
    try:
        original_user_id = int(parts[2])
        admin_message_id = int(parts[3])
    except (IndexError, ValueError):
        await callback.answer("❌ Ошибка данных callback!", show_alert=True)
        return
        
    admin_id = callback.from_user.id
    
    if not await check_permission(admin_id, ROLE_LEVELS[ROLE_ADMIN]):
        await callback.answer("❌ У вас нет прав (нужен Administrator или Owner).", show_alert=True)
        return
        
    ticket_data = await db_fetchone("SELECT status FROM tickets WHERE admin_message_id = $1", admin_message_id)
    if ticket_data and ticket_data['status'] != 'open':
        await callback.answer(f"❌ Тикет уже имеет статус: {ticket_data['status']}.", show_alert=True)
        return
    
    await state.update_data(
        original_user_id=original_user_id,
        admin_message_id=admin_message_id,
        original_ticket_text=callback.message.text # [PG-FIX] Сохраняем текст тикета
    )
    await state.set_state(AdminResponse.waiting_for_answer)
    
    try:
        await callback.message.edit_text(
            f"<b>✅ Введите ответ для пользователя ID {code(original_user_id)}</b>\n\n"
            "<i>(Ваш ответ будет отправлен ему в личные сообщения)</i>\n\n"
            "Исходный тикет:\n" + callback.message.text,
            reply_markup=None,
            parse_mode="HTML"
        )
    except Exception:
        await callback.message.answer(
            f"<b>✅ Введите ответ для пользователя ID {code(original_user_id)}</b>\n\n"
            "<i>(Ваш ответ будет отправлен ему в личные сообщения)</i>",
            parse_mode="HTML"
        )
        
    await callback.answer("Ожидаю ваш ответ...")

# [ASYNC-REWRITE]
@dp.message(AdminResponse.waiting_for_answer, F.text)
async def process_admin_answer(message: types.Message, state: FSMContext):
    """(PG) Отправляет ответ администратора обратно пользователю."""
    admin_response = message.text.strip()
    data = await state.get_data()
    original_user_id = data.get('original_user_id')
    admin_message_id = data.get('admin_message_id')
    original_ticket_text = data.get('original_ticket_text', '...текст тикета не сохранен...') # [PG-FIX]
    
    logger.info(f"Admin replying to original user ID: {original_user_id}")

    if original_user_id == BOT_ID:
        await message.answer(
            f"<b>❌ Ошибка!</b> Вы не можете ответить на тикет, созданный самим ботом (ID <code>{BOT_ID}</code>).",
            parse_mode="HTML"
        )
        await state.clear()
        return

    user_answer_text = (
        "<blockquote><b>📩 Ответ Администратора</b></blockquote>\n\n"
        f"<b>Администратор {bold(message.from_user.full_name)} ответил на ваш тикет:</b>\n\n"
        f"📝 {html.escape(admin_response)}\n\n"
        "<i>Спасибо за обращение!</i>"
    )
    
    try:
        await bot.send_message(original_user_id, user_answer_text, parse_mode="HTML")
        
        await db_execute("UPDATE tickets SET status = 'answered' WHERE admin_message_id = $1", admin_message_id)

        try:
            # [PG-FIX] Восстанавливаем исходный тикет + добавляем ответ
            await bot.edit_message_text(
                chat_id=TICKET_CHAT_ID,
                message_id=admin_message_id,
                text=f"{original_ticket_text}\n\n"
                     f"<b>✅ ОТВЕТ ОТПРАВЛЕН</b>\n"
                     f"Администратор: {message.from_user.full_name}",
                reply_markup=None,
                parse_mode="HTML"
            )
        except:
            pass

        await message.answer(
            f"<b>✅ Ответ успешно отправлен пользователю ID {code(original_user_id)}!</b>\n\n"
            "Статус тикета обновлен на 'answered'.", 
            parse_mode="HTML"
        )
        
    except Exception as e:
        error_text = (
            f"<b>❌ Ошибка отправки!</b>\n\n"
            f"Не удалось отправить сообщение пользователю ID {original_user_id}.\n\n"
            f"<b>Возможные причины:</b>\n"
            f"• Пользователь заблокировал бота\n"
            f"• Пользователь не начинал диалог с ботом\n"
            f"• Технические проблемы\n\n"
            f"<b>Детали:</b> <code>{str(e)}</code>"
        )
        await message.answer(error_text, parse_mode="HTML")
        print(f"Ошибка отправки ответа админа: {e}")
        
    await state.clear()

# Список админов
ADMIN_IDS = [6811394311, 8226139438]

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
async def get_admin_panel_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """(PG) Клавиатура админ-панели (красивая раскладка)."""
    
    role = await get_user_role(user_id)
    level = ROLE_LEVELS.get(role, 0)
    
    buttons = []

    # --- LEVEL 1: GAME REG ---
    if level >= ROLE_LEVELS[ROLE_GAME_REG]:
        buttons.append([InlineKeyboardButton(text="📝 Зарегистрировать игру", callback_data="admin_register_game")])

    # --- LEVEL 2: ADMIN ---
    if level >= ROLE_LEVELS[ROLE_ADMIN]:
        # Группа наказаний (2 в ряд)
        buttons.append([
            InlineKeyboardButton(text="🔇 Мут", callback_data="admin_mute_player"),
            InlineKeyboardButton(text="🔊 Размут", callback_data="admin_unmute_player")
        ])
        buttons.append([
            InlineKeyboardButton(text="🚫 Бан", callback_data="admin_ban_player"),
            InlineKeyboardButton(text="✅ Разбан", callback_data="admin_unban_player")
        ])
        
        # Группа лиг и премиума (2 в ряд)
        buttons.append([
            InlineKeyboardButton(text="🌟 Дать QUAL", callback_data="admin_give_qual"),
            InlineKeyboardButton(text="🔻 Снять QUAL", callback_data="admin_revoke_qual")
        ])
        buttons.append([
            InlineKeyboardButton(text="🏆 Дать FPL", callback_data="admin_give_fpl"),
            InlineKeyboardButton(text="🔻 Снять FPL", callback_data="admin_revoke_fpl")
        ])
        # [NEW] Кнопка снятия премиума
        buttons.append([
            InlineKeyboardButton(text="🔻 Забрать Premium", callback_data="admin_revoke_premium")
        ])
        
        # Редактирование (2 в ряд)
        buttons.append([
            InlineKeyboardButton(text="✏️ Смен. Ник", callback_data="admin_change_nickname"),
            InlineKeyboardButton(text="🆔 Смен. ID", callback_data="admin_change_gameid")
        ])
        
        buttons.append([InlineKeyboardButton(text="🧩 Упр. Game Reg", callback_data="admin_manage_gamereg")])

    # --- LEVEL 3: OWNER ---
    if level >= ROLE_LEVELS[ROLE_OWNER]:
        buttons.append([InlineKeyboardButton(text="🎁 Сгенерировать промокод", callback_data="admin_create_promo")])
        
        buttons.append([
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="⚙️ Упр. Admin", callback_data="admin_manage_admin")
        ])
        
        # Боты
        buttons.append([
            InlineKeyboardButton(text="🤖 Спавн (8)", callback_data="admin_spawn_bots"),
            InlineKeyboardButton(text="🤖 Del (Лобби)", callback_data="admin_remove_bots_lobby")
        ])
        # [NEW] Кнопка удаления ботов из БД
        buttons.append([
            InlineKeyboardButton(text="🗑 Удалить ботов из БД (Рейтинг)", callback_data="admin_wipe_bots_db"),
            InlineKeyboardButton(text="🗑 Удалить акк", callback_data="admin_delete_account")
        ])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main_menu")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "main_admin", MinRoleFilter(ROLE_GAME_REG)) 
async def admin_panel_handler(callback: types.CallbackQuery):
    """(PG) Обрабатывает нажатие "Админ-панель"."""
    
    await callback.answer()
    
    user_id = callback.from_user.id
    
    role = await get_user_role(user_id)

    admin_text = (
        f"<b>⚙️ АДМИН-ПАНЕЛЬ</b>\n\n"
        f"<b>Добро пожаловать! Ваша роль: {role}</b>\n"
        "Выберите действие из меню ниже:"
    )
    
    photo_id = MAIN_MENU_FILE_ID 
    
    keyboard = await get_admin_panel_keyboard(user_id)

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption=admin_text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass 
        else:
            logger.error(f"Ошибка в admin_panel_handler: {e}")

# ... (admin_delete_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_delete_account", MinRoleFilter(ROLE_OWNER))
async def admin_delete_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_delete_id)
    
    text = ("<b>🗑 Удаление аккаунта</b>\n\n"
            "Введите Telegram ID пользователя для удаления:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None # [PG-FIX] Убираем кнопки при вводе
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_delete_start: {e}")

# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_delete_id, F.text)
async def admin_delete_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    # [PG-REWRITE] Транзакция удаления
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Зависимые таблицы (ON DELETE CASCADE должен сработать, но для надежности)
                await conn.execute("DELETE FROM lobby_members WHERE user_id = $1", target_id)
                await conn.execute("DELETE FROM tickets WHERE user_id = $1", target_id)
                await conn.execute("DELETE FROM user_league_stats WHERE user_id = $1", target_id)
                # Основная таблица
                await conn.execute("DELETE FROM users WHERE user_id = $1", target_id)
    except Exception as e:
        await message.answer(f"❌ Ошибка БД при удалении: {e}")
        logger.error(f"Ошибка удаления {target_id}: {e}")
        await state.clear()
        return

    await message.answer(
        f"✅ Аккаунт <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> (ID: {target_id}) успешно удален!",
        parse_mode="HTML"
    )
    await state.clear()

# ... (admin_mute_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_mute_player", MinRoleFilter(ROLE_ADMIN))
async def admin_mute_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_mute_data)
    
    text = ("<b>🔇 Мут игрока</b>\n\n"
            "Введите данные в формате:\n"
            "<code>ID часы</code>\n\n"
            "Например: <code>123456789 24</code>")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_mute_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_mute_data, F.text)
async def admin_mute_process(message: types.Message, state: FSMContext):
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("❌ Неверный формат! Используйте: ID часы")
        return
        
    try:
        target_id = int(parts[0])
        hours = int(parts[1])
    except ValueError:
        await message.answer("❌ ID и часы должны быть числами!")
        return
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", target_id)
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    mute_until = datetime.now() + timedelta(hours=hours)
    await db_execute("UPDATE users SET muted_until = $1 WHERE user_id = $2", mute_until, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Игрок <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> замучен на {hours} час(ов)!",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            target_id,
            f"🔇 <b>Вы получили мут на {hours} час(ов)</b>\n"
            f"Мут истечет: {mute_until.strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode="HTML"
        )
    except:
        pass
    
    await state.clear()

# ... (admin_ban_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_ban_player", MinRoleFilter(ROLE_ADMIN))
async def admin_ban_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_ban_id)
    
    text = ("<b>🚫 Бан навсегда</b>\n\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_ban_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_ban_id, F.text)
async def admin_ban_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    await db_execute("UPDATE users SET banned = TRUE WHERE user_id = $1", target_id)
    await db_execute("DELETE FROM lobby_members WHERE user_id = $1", target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Игрок <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> забанен навсегда!",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            target_id,
            "🚫 <b>Вы получили перманентный бан</b>\n"
            "Обратитесь к администрации для разъяснений.\n\n"
            f"👮‍♂️ <b>Администратор:</b> @jackha1337",
            parse_mode="HTML"
        )
    except:
        pass
    
    await state.clear()

# ... (admin_unban_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_unban_player", MinRoleFilter(ROLE_ADMIN))
async def admin_unban_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_unban_id)
    
    text = ("<b>✅ Разбан игрока</b>\n\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_unban_start: {e}")

@dp.message(AdminActions.waiting_for_unban_id, F.text)
async def admin_unban_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    await db_execute("UPDATE users SET banned = FALSE WHERE user_id = $1", target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(f"✅ Игрок с ID {target_id} разбанен!", parse_mode="HTML")
    
    try:
        await bot.send_message(target_id, "✅ <b>Вы были разбанены!</b>", parse_mode="HTML")
    except:
        pass
    
    await state.clear()

# ... (admin_unmute_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_unmute_player", MinRoleFilter(ROLE_ADMIN))
async def admin_unmute_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_unmute_id)
    
    text = ("<b>🔊 Размут игрока</b>\n\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_unmute_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_unmute_id, F.text)
async def admin_unmute_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    await db_execute("UPDATE users SET muted_until = NULL WHERE user_id = $1", target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(f"✅ Игрок с ID {target_id} размучен!", parse_mode="HTML")
    
    try:
        await bot.send_message(target_id, "🔊 <b>Вы были размучены!</b>", parse_mode="HTML")
    except:
        pass
    
    await state.clear()

# ... (admin_qual_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_give_qual", MinRoleFilter(ROLE_ADMIN))
async def admin_qual_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_qual_access_id)
    
    text = ("<b>🌟 Выдача доступа к Qualification</b>\n\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_qual_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_qual_access_id, F.text)
async def admin_qual_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname, league FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    current_league = user.get('league', DEFAULT_LEAGUE)
    current_level = LEAGUE_LEVELS.get(current_league, 0)
    target_level = LEAGUE_LEVELS[QUAL_LEAGUE]

    if current_level == target_level:
        await message.answer(f"❌ У пользователя уже есть доступ к {QUAL_LEAGUE}!")
        await state.clear()
        return
    
    if current_level > target_level:
        await message.answer(f"❌ У пользователя уже есть доступ к {FPL_LEAGUE} (уровень выше, чем {QUAL_LEAGUE}).")
        await state.clear()
        return

    await db_execute("UPDATE users SET league = $1 WHERE user_id = $2", QUAL_LEAGUE, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Игроку <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> выдан доступ к Qualification!",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            target_id,
            f"🌟 <b>Вам выдан доступ к лиге {QUAL_LEAGUE}!</b>",
            parse_mode="HTML"
        )
    except:
        pass
    
    await state.clear()

# ... (admin_fpl_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_give_fpl", MinRoleFilter(ROLE_ADMIN))
async def admin_fpl_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_fpl_access_id)
    
    text = ("<b>🏆 Выдача доступа к FPL</b>\n\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_fpl_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_fpl_access_id, F.text)
async def admin_fpl_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname, league FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return

    current_league = user.get('league', DEFAULT_LEAGUE)
    current_level = LEAGUE_LEVELS.get(current_league, 0)
    target_level = LEAGUE_LEVELS[FPL_LEAGUE]

    if current_level == target_level:
        await message.answer(f"❌ У пользователя уже есть доступ к {FPL_LEAGUE}!")
        await state.clear()
        return
    
    await db_execute("UPDATE users SET league = $1 WHERE user_id = $2", FPL_LEAGUE, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Игроку <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> выдан доступ к FPL!",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(
            target_id,
            f"🏆 <b>Вам выдан доступ к лиге {FPL_LEAGUE}!</b>",
            parse_mode="HTML"
        )
    except:
        pass
    
    await state.clear()

# ... (admin_register_game_redirect - без изменений) ...
@dp.callback_query(F.data == "admin_register_game", MinRoleFilter(ROLE_GAME_REG))
async def admin_register_game_redirect(callback: types.CallbackQuery, state: FSMContext):
    """Перенаправление на существующий функционал регистрации матча."""
    await callback.answer("Используйте кнопку в чате поддержки", show_alert=True)
    await state.clear()
    
    text = ("<b>📝 Регистрация игры</b>\n\n"
            "<b>Пожалуйста, используйте кнопку '📝 Зарегистрировать матч' "
            "под скриншотом результатов в чате поддержки.</b>\n\n"
            "Это необходимо для автоматического определения лиги, в которой проходил матч (Default, QUAL или FPL).\n\n"
            "Формат ввода после нажатия кнопки:\n"
            "<code>123 15 10, 456 12 11, ...</code> (10 игроков)")
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_admin")]
    ])
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_register_game_redirect: {e}")

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "admin_change_nickname", MinRoleFilter(ROLE_ADMIN))
async def admin_change_nick_start(callback: types.CallbackQuery, state: FSMContext):
    """(PG) Начинает процесс смены никнейма."""
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_change_nick_data)
    
    text = ("<b>✏️ Смена Никнейма (Админ)</b>\n\n"
            "📝 Введите ID пользователя и новый никнейм через пробел.\n"
            "Пример: <code>123456789 NewNickName</code>"
    )

    try:
        # [PG-FIX] Используем edit_media, т.к. мы в админ-панели (с фото)
        await callback.message.edit_media(
            media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=text, parse_mode="HTML"),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        else:
            await callback.message.answer(text, parse_mode="HTML")

@dp.message(AdminActions.waiting_for_change_nick_data, F.text)
async def admin_change_nick_process(message: types.Message, state: FSMContext):
    # Разбиваем сообщение на ID и остаток (новый ник)
    parts = message.text.split(maxsplit=1)

    if len(parts) != 2:
        await message.answer("❌ Неверный формат. Используйте: <code>[ID пользователя] [Новый никнейм]</code>", parse_mode="HTML")
        return

    target_id_str = parts[0]
    new_nickname = parts[1].strip() # <--- Определение переменной new_nickname

    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID пользователя должен быть числом.", parse_mode="HTML")
        return

    # 1. СТРОГАЯ ПРОВЕРКА НА СИМВОЛЫ
    if not is_valid_nickname(new_nickname):
        await message.answer(
            "<b>❌ Ошибка! Никнейм может содержать <b>только латинские/русские буквы и цифры</b>.</b>\n"
            "Символы, пробелы и эмодзи запрещены.",
            parse_mode="HTML"
        )
        return
        
    # 2. ПРОВЕРКА НА ДЛИНУ
    if not (3 <= len(new_nickname) <= 10):
        await message.answer("<b>❌ Ошибка! Длина никнейма должна быть от 3 до 10 символов.</b>", parse_mode="HTML")
        return

    # Далее должен идти код проверки, что пользователь существует:
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", target_id) 
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    await db_execute("UPDATE users SET nickname = $1 WHERE user_id = $2", new_nickname, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО

    await message.answer(
        f"✅ Никнейм успешно изменен!\n"
        f"<b>ID:</b> <code>{target_id}</code>\n"
        f"<b>Старый Ник:</b> <code>{html.escape(user.get('nickname', 'N/A'))}</code>\n"
        f"<b>Новый Ник:</b> <code>{html.escape(new_nickname)}</code>",
        parse_mode="HTML"
    )
    
    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "admin_change_gameid", MinRoleFilter(ROLE_ADMIN))
async def admin_change_gameid_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_change_gameid_data)
    
    text = ("<b>🆔 Смена Game ID (Админ)</b>\n\n"
            "Введите данные в формате:\n"
            "<code>ID Новый_GameID</code>\n\n"
            "<b>Пример:</b> <code>123456789 new_game_id</code>")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_change_gameid_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_change_gameid_data, F.text)
async def admin_change_gameid_process(message: types.Message, state: FSMContext):
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("❌ Неверный формат! Используйте: <code>ID Новый_GameID</code>", parse_mode="HTML")
        return
    
    try:
        target_id = int(parts[0])
    except ValueError:
        await message.answer("❌ ID должен быть числом!", parse_mode="HTML")
        return
        
    new_gameid = parts[1]
    
    if not is_valid_game_id(new_gameid):
        await message.answer(
            "❌ Ошибка! Игровой ID может содержать <b>только латинские буквы (A-z) и цифры (0-9)</b> (1-12 симв).", 
            parse_mode="HTML"
        )
        return

    user = await db_fetchone("SELECT game_id FROM users WHERE user_id = $1", target_id)
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    await db_execute("UPDATE users SET game_id = $1 WHERE user_id = $2", new_gameid, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Game ID успешно изменен!\n"
        f"<b>ID:</b> <code>{target_id}</code>\n"
        f"<b>Старый Game ID:</b> <code>{user.get('game_id', 'N/A')}</code>\n"
        f"<b>Новый Game ID:</b> <code>{new_gameid}</code>",
        parse_mode="HTML"
    )
    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "admin_revoke_qual", MinRoleFilter(ROLE_ADMIN))
async def admin_revoke_qual_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_revoke_qual_id)
    
    text = ("<b>🔻 Забрать доступ к Qualification</b>\n\n"
            f"Лига пользователя будет сброшена на '<code>{DEFAULT_LEAGUE}</code>'.\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_revoke_qual_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_revoke_qual_id, F.text)
async def admin_revoke_qual_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname, league FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    if user['league'] != QUAL_LEAGUE:
        await message.answer(f"❌ Пользователь не состоит в {QUAL_LEAGUE}. Его лига: {user['league']}.")
        await state.clear()
        return

    await db_execute("UPDATE users SET league = $1 WHERE user_id = $2", DEFAULT_LEAGUE, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Доступ к {QUAL_LEAGUE} для <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> отозван.\n"
        f"Лига сброшена на {DEFAULT_LEAGUE}.",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(target_id, f"🔻 Администратор отозвал у вас доступ к лиге {QUAL_LEAGUE}.")
    except:
        pass

    await state.clear()

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "admin_revoke_fpl", MinRoleFilter(ROLE_ADMIN))
async def admin_revoke_fpl_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_revoke_fpl_id)
    
    text = ("<b>🔻 Забрать доступ к FPL</b>\n\n"
            f"Лига пользователя будет сброшена на '<code>{DEFAULT_LEAGUE}</code>'.\n"
            "Введите Telegram ID пользователя:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_revoke_fpl_start: {e}")

# [ASYNC-REWRITE]
# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_revoke_fpl_id, F.text)
async def admin_revoke_fpl_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname, league FROM users WHERE user_id = $1", target_id)
    
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    if user['league'] != FPL_LEAGUE:
        await message.answer(f"❌ Пользователь не состоит в {FPL_LEAGUE}. Его лига: {user['league']}.")
        await state.clear()
        return

    await db_execute("UPDATE users SET league = $1 WHERE user_id = $2", DEFAULT_LEAGUE, target_id)
    
    await clear_user_cache(target_id) # <-- ДОБАВЛЕНО
    
    await message.answer(
        f"✅ Доступ к {FPL_LEAGUE} для <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> отозван.\n"
        f"Лига сброшена на {DEFAULT_LEAGUE}.",
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(target_id, f"🔻 Администратор отозвал у вас доступ к лиге {FPL_LEAGUE}.")
    except:
        pass

    await state.clear()

@dp.message(AdminActions.waiting_for_bot_count, F.text)
async def admin_spawn_bots_final(message: types.Message, state: FSMContext):
    """(PG) Ловит КОЛИЧЕСТВО и выполняет спавн."""
    
    try:
        bots_to_spawn = int(message.text.strip())
        if not (1 <= bots_to_spawn <= 10):
             raise ValueError
    except ValueError:
        await message.answer("❌ Введите число от 1 до 10.")
        return

    data = await state.get_data()
    league_name = data.get("spawn_league")
    lobby_number = data.get("spawn_lobby_num")
    
    lobby_number_index = lobby_number - 1

    if not league_name:
        await message.answer("❌ Ошибка FSM. Начните заново.")
        await state.clear()
        return

    lobby_id = None
    
    try:
        lobbies_in_league = await db_fetchall(
            "SELECT lobby_id FROM lobbies WHERE league = $1 ORDER BY lobby_id LIMIT 5", 
            league_name
        )
        
        if not lobbies_in_league or lobby_number_index >= len(lobbies_in_league):
            await message.answer(f"❌ Лобби #{lobby_number} не найдено в БД.")
            await state.clear()
            return
            
        lobby_id = lobbies_in_league[lobby_number_index]['lobby_id']

        lobby = await db_fetchone("SELECT current_players FROM lobbies WHERE lobby_id = $1", lobby_id)
        current_players = lobby['current_players']
        
        if current_players + bots_to_spawn > 10:
            await message.answer(f"❌ В лобби уже {current_players} игроков. Нельзя добавить {bots_to_spawn} ботов (макс. 10 всего).")
            await state.clear()
            return
            
    except Exception as e:
        await message.answer(f"❌ Ошибка при поиске лобби: {e}")
        await state.clear()
        return

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                
                for i in range(1, bots_to_spawn + 1):
                    # Генерируем уникальный ID для бота (чтобы не было конфликтов при многократном добавлении)
                    # Используем random range, чтобы можно было добавлять разных ботов
                    bot_user_id = -1 * random.randint(10000, 999999)
                    bot_nickname = f"Bot_{random.randint(100, 999)}"
                    bot_game_id = f"bot{bot_user_id}"
                    bot_elo = 1000 + (i * 10)
                    
                    await conn.execute(
                        """INSERT INTO users 
                        (user_id, nickname, game_id, is_registered, league) 
                        VALUES ($1, $2, $3, TRUE, $4)
                        ON CONFLICT (user_id) DO NOTHING
                        """,
                        bot_user_id, bot_nickname, bot_game_id, league_name
                    )
                    
                    await conn.execute(
                        """INSERT INTO user_league_stats
                        (user_id, league_name, elo)
                        VALUES ($1, $2, $3)
                        ON CONFLICT(user_id, league_name) DO NOTHING""",
                        bot_user_id, league_name, bot_elo
                    )
                    
                    await conn.execute(
                        "INSERT INTO lobby_members (lobby_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        lobby_id, bot_user_id
                    )

                # Пересчитываем игроков
                final_count = await conn.fetchval("SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1", lobby_id)
                
                await conn.execute(
                    "UPDATE lobbies SET current_players = $1 WHERE lobby_id = $2", 
                    final_count, lobby_id
                )
        
        await message.answer(f"✅ {bots_to_spawn} ботов добавлено в {league_name} Лобби #{lobby_number} (ID: {lobby_id}).\nВсего игроков: {final_count}/10")
        
        lobby_text = await get_lobby_text(lobby_id)
        await broadcast_lobby_update(lobby_id, bot, lobby_text)
        
        if final_count == 10:
            logger.info(f"Лобби {lobby_id} заполнено (10/10). Запуск фазы подтверждения.")
            await start_confirmation_phase(lobby_id, bot)

    except Exception as e:
        await message.answer(f"❌ Ошибка базы данных при спавне ботов: {e}")
        logger.error(f"Ошибка спавна ботов (DB): {e}", exc_info=True)
        
    finally:
        await state.clear()

# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_bot_lobby_number, F.text)
async def admin_spawn_bots_process(message: types.Message, state: FSMContext):
    """(PG) Ловит НОМЕР лобби и спрашивает КОЛИЧЕСТВО ботов. (ИСПРАВЛЕНО)"""
    
    lobby_number_str = message.text.strip()
    
    # Проверяем, что ввели число от 1 до 5
    if not lobby_number_str.isdigit() or not (1 <= int(lobby_number_str) <= 5):
        await message.answer("❌ Номер лобби должен быть числом от 1 до 5.")
        return
        
    # Сохраняем номер лобби в память (FSM)
    await state.update_data(spawn_lobby_num=int(lobby_number_str))
    
    # Переключаем состояние на ожидание количества ботов
    await state.set_state(AdminActions.waiting_for_bot_count)
    
    # Спрашиваем количество. Следующее сообщение обработает функция admin_spawn_bots_final
    await message.answer(
        "<b>🤖 Шаг 3:</b> Введите количество ботов для добавления (от 1 до 10):",
        parse_mode="HTML"
    )

# ... (admin_remove_bots_lobby_start - без изменений FSM) ...
@dp.callback_query(F.data == "admin_remove_bots_lobby", MinRoleFilter(ROLE_OWNER))
async def admin_remove_bots_lobby_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    
    await state.set_state(AdminActions.waiting_for_remove_bot_lobby_id)
    
    text = ("<b>🤖 Удаление ботов из лобби</b>\n\n"
            "Введите ID лобби для очистки от ботов:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в admin_remove_bots_lobby_start: {e}")

# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_remove_bot_lobby_id, F.text)
async def admin_remove_bots_lobby_process(message: types.Message, state: FSMContext):
    lobby_id_str = message.text.strip()
    try:
        lobby_id = int(lobby_id_str)
    except ValueError:
        await message.answer("❌ ID лобби должен быть числом.")
        return
    
    # [PG-REWRITE] Транзакция
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM lobby_members WHERE lobby_id = $1 AND user_id < -10000", lobby_id)
                
                count = await conn.fetchval("SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1", lobby_id)
                
                await conn.execute("UPDATE lobbies SET current_players = $1 WHERE lobby_id = $2", count, lobby_id)
    except Exception as e:
        await message.answer(f"❌ Ошибка БД при удалении ботов: {e}")
        logger.error(f"Ошибка удаления ботов из {lobby_id}: {e}")
        await state.clear()
        return

    
    await message.answer(f"✅ Боты удалены из лобби {lobby_id}.")
    await state.clear()
    
    lobby_text = await get_lobby_text(lobby_id)
    await broadcast_lobby_update(lobby_id, bot, lobby_text)

# [ASYNC-REWRITE]
@dp.callback_query(F.data == "admin_clear_all_bots", MinRoleFilter(ROLE_OWNER))
async def admin_clear_all_bots_handler(callback: types.CallbackQuery):
    """Удаляет ВСЕХ ботов (ID < 0) из лобби, рейтинга и базы пользователей."""
    await callback.answer("🧹 Полная очистка ботов...", show_alert=True)
    
    lobbies_to_update = []
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # 1. Находим лобби, которые нужно будет обновить (где сидят боты)
                # Используем < 0, чтобы найти вообще всех ботов
                rows = await conn.fetch("SELECT DISTINCT lobby_id FROM lobby_members WHERE user_id < 0")
                lobbies_to_update = [r['lobby_id'] for r in rows]
                
                # 2. Удаляем ботов из таблицы матчей (если они были капитанами в незавершенных матчах)
                # Это важно, иначе может возникнуть ошибка внешнего ключа
                await conn.execute("DELETE FROM matches WHERE (captain1_id < 0 OR captain2_id < 0) AND status != 'completed'")

                # 3. Удаляем ботов из Лобби
                deleted_members = await conn.execute("DELETE FROM lobby_members WHERE user_id < 0")
                
                # 4. Удаляем ботов из РЕЙТИНГА (Самое важное для вашего вопроса)
                deleted_stats = await conn.execute("DELETE FROM user_league_stats WHERE user_id < 0")
                
                # 5. Удаляем ботов из списка Пользователей
                deleted_users = await conn.execute("DELETE FROM users WHERE user_id < 0")
                
                # 6. Пересчитываем количество игроков в затронутых лобби
                for lobby_id in lobbies_to_update:
                    count = await conn.fetchval("SELECT COUNT(*) FROM lobby_members WHERE lobby_id = $1", lobby_id)
                    await conn.execute("UPDATE lobbies SET current_players = $1 WHERE lobby_id = $2", count, lobby_id)

        # Логируем для админа
        logger.info(f"Очистка завершена. Удалено: Mem={deleted_members}, Stats={deleted_stats}, Users={deleted_users}")

    except Exception as e:
        await callback.message.answer(f"❌ Ошибка БД при удалении ботов: {e}")
        logger.error(f"Ошибка admin_clear_all_bots_handler: {e}", exc_info=True)
        return

    # Обновляем интерфейс в лобби, откуда исчезли боты
    for lobby_id in lobbies_to_update:
        lobby_text = await get_lobby_text(lobby_id)
        await broadcast_lobby_update(lobby_id, bot, lobby_text)

    text = "✅ <b>Все боты полностью удалены.</b>\n\nОни исчезли из:\n• Лобби\n• Рейтинга (Leaderboard)\n• Базы данных"
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=get_admin_panel_keyboard(callback.from_user.id) # Возвращаем клавиатуру
        )
    except TelegramBadRequest:
        await callback.message.answer(text, parse_mode="HTML")

# [ASYNC-REWRITE]
async def check_and_upgrade_league(conn: asyncpg.Connection, user_id: int, new_elo: int):
    """
    (PG) Автоматически повышает лигу. 
    Принимает `conn`, чтобы работать внутри транзакции.
    """
    if new_elo < 2100 or user_id < 0: # Не повышаем ботов
        return

    try:
        user = await conn.fetchrow("SELECT league FROM users WHERE user_id = $1", user_id)
        
        if user and user['league'] == DEFAULT_LEAGUE:
            await conn.execute("UPDATE users SET league = $1 WHERE user_id = $2", QUAL_LEAGUE, user_id)
            
            # Отправку уведомления нельзя делать внутри транзакции
            # Поэтому создаем задачу, которая выполнится *после* коммита
            asyncio.create_task(
                send_league_upgrade_notification(user_id, QUAL_LEAGUE)
            )
            logger.info(f"Пользователю {user_id} выдан QUAL_LEAGUE (достиг 2100 ELO)")
            
    except Exception as e:
        logger.error(f"Ошибка в check_and_upgrade_league для {user_id}: {e}")

# [PG-ADDED]
async def send_league_upgrade_notification(user_id: int, league_name: str):
    """Отправляет уведомление о повышении (отдельно от транзакции)."""
    try:
        await bot.send_message(
            user_id,
            f"🎉 <b>Поздравляем!</b>\n\n"
            f"Вы достигли 10 уровня и получили доступ к лиге {league_name}!",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"Не удалось уведомить {user_id} о повышении лиги: {e}")


# ... (SetRole FSM - без изменений) ...
class SetRole(StatesGroup):
    waiting_for_id = State()
    role_to_set = State()
    action = State()

# ... (start_set_role_fsm - без изменений FSM) ...
async def start_set_role_fsm(callback: types.CallbackQuery, state: FSMContext, role_to_manage: str, action: str):
    """Общая функция для запуска FSM назначения/снятия роли."""
    await callback.answer()
    await state.set_state(SetRole.waiting_for_id)
    await state.update_data(role_to_set=role_to_manage, action=action)
    
    action_text = "назначения" if action == "set" else "снятия"
    text = (
        f"<b>Управление ролью: {role_to_manage}</b>\n"
        f"Введите Telegram ID пользователя для {action_text} этой роли:"
    )
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=None # [PG-FIX]
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e): pass
        else: logger.error(f"Ошибка в start_set_role_fsm: {e}")

# ... (manage_admin, manage_admin_set, manage_admin_remove, 
# ... manage_gamereg_set, manage_gamereg_remove, manage_gamereg - без изменений FSM) ...
@dp.callback_query(F.data == "admin_manage_admin", MinRoleFilter(ROLE_OWNER))
async def manage_admin(callback: types.CallbackQuery, state: FSMContext):
    """(ИСПРАВЛЕНО) Показывает меню 'Назначить' / 'Снять' для Admin."""
    await callback.answer()
    role_to_manage = ROLE_ADMIN
    text = f"<b>Управление ролью: {role_to_manage}</b>\n\nВыберите действие:"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"➕ Назначить {role_to_manage}", callback_data=f"set_role_{role_to_manage}_set")],
        [InlineKeyboardButton(text=f"➖ Снять {role_to_manage}", callback_data=f"set_role_{role_to_manage}_remove")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_admin")]
    ])

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=text, parse_mode="HTML"),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в manage_admin (меню): {e}")

@dp.callback_query(F.data == f"set_role_{ROLE_ADMIN}_set", MinRoleFilter(ROLE_OWNER))
async def manage_admin_set(callback: types.CallbackQuery, state: FSMContext):
    """(НОВАЯ Ф-ЦИЯ) Переход к FSM для НАЗНАЧЕНИЯ Admin."""
    await start_set_role_fsm(callback, state, ROLE_ADMIN, "set")

@dp.callback_query(F.data == f"set_role_{ROLE_ADMIN}_remove", MinRoleFilter(ROLE_OWNER))
async def manage_admin_remove(callback: types.CallbackQuery, state: FSMContext):
    """(НОВАЯ Ф-ЦИЯ) Переход к FSM для СНЯТИЯ Admin."""
    await start_set_role_fsm(callback, state, ROLE_ADMIN, "remove")

@dp.callback_query(F.data == f"set_role_{ROLE_GAME_REG}_set", MinRoleFilter(ROLE_ADMIN))
async def manage_gamereg_set(callback: types.CallbackQuery, state: FSMContext):
    """(НОВАЯ Ф-ЦИЯ) Переход к FSM для НАЗНАЧЕНИЯ Game Reg."""
    await start_set_role_fsm(callback, state, ROLE_GAME_REG, "set")

@dp.callback_query(F.data == f"set_role_{ROLE_GAME_REG}_remove", MinRoleFilter(ROLE_ADMIN))
async def manage_gamereg_remove(callback: types.CallbackQuery, state: FSMContext):
    """(НОВАЯ Ф-ЦИЯ) Переход к FSM для СНЯТИЯ Game Reg."""
    await start_set_role_fsm(callback, state, ROLE_GAME_REG, "remove")

@dp.callback_query(F.data == "admin_manage_gamereg", MinRoleFilter(ROLE_ADMIN))
async def manage_gamereg(callback: types.CallbackQuery, state: FSMContext):
    """(ИСПРАВЛЕНО) Показывает меню 'Назначить' / 'Снять' для Game Reg."""
    await callback.answer()
    role_to_manage = ROLE_GAME_REG
    text = f"<b>Управление ролью: {role_to_manage}</b>\n\nВыберите действие:"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"➕ Назначить {role_to_manage}", callback_data=f"set_role_{role_to_manage}_set")],
        [InlineKeyboardButton(text=f"➖ Снять {role_to_manage}", callback_data=f"set_role_{role_to_manage}_remove")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_admin")]
    ])

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=text, parse_mode="HTML"),
            reply_markup=keyboard
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка в manage_gamereg (меню): {e}")

# [ASYNC-REWRITE]
@dp.message(SetRole.waiting_for_id, F.text)
async def process_set_role_id(message: types.Message, state: FSMContext):
    """
    (PG) Обрабатывает ввод ID для FSM 'SetRole'.
    """
    target_id_str = message.text.strip()
    
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    data = await state.get_data()
    role_to_set = data.get('role_to_set')
    action = data.get('action')

    if not role_to_set or not action:
        await message.answer("❌ Ошибка FSM, данные о роли/действии потеряны. Начните заново.")
        await state.clear()
        return

    target_user = await db_fetchone("SELECT nickname, role FROM users WHERE user_id = $1", target_id)
    
    if not target_user:
        await db_execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT(user_id) DO NOTHING", target_id)
        target_user = {'nickname': str(target_id), 'role': ROLE_PLAYER}

    current_role = target_user.get('role', ROLE_PLAYER)
    
    # Проверяем, не пытаются ли изменить роль администратора из списка ADMIN_IDS
    if 'ADMIN_IDS' in globals() and target_id in ADMIN_IDS:
         await message.answer("❌ Нельзя изменить роль владельца (указан в ADMIN_IDS)!")
         await state.clear()
         return
    
    new_role = current_role
    notification_text = ""
    result_text = ""
    
    if action == 'set':
        if current_role == role_to_set:
            result_text = f"❌ У пользователя уже есть роль <b>{role_to_set}</b>."
        else:
            new_role = role_to_set
            result_text = (
                f"✅ Пользователь <b>{html.escape(target_user.get('nickname', f'ID: {target_id}'))}</b> (ID: {target_id})\n"
                f"назначен <b>{role_to_set}</b>!"
            )
            notification_text = (
                f"🎉 <b>Вам назначена роль {role_to_set}!</b>\n"
                "Теперь у вас есть доступ к расширенным функциям."
            )
            
    elif action == 'remove':
        if current_role != role_to_set:
            result_text = f"❌ У пользователя нет роли <b>{role_to_set}</b> (текущая роль: {current_role})."
        else:
            new_role = ROLE_PLAYER
            result_text = (
                f"✅ У пользователя <b>{html.escape(target_user.get('nickname', f'ID: {target_id}'))}</b> (ID: {target_id})\n"
                f"снята роль <b>{role_to_set}</b>."
            )
            notification_text = f"⚠️ <b>С вас снята роль {role_to_set}.</b>"

    if new_role != current_role:
        new_level = ROLE_LEVELS.get(new_role, 0)
        is_admin_flag = True if new_level >= ROLE_LEVELS[ROLE_GAME_REG] else False
        
        await db_execute("UPDATE users SET role = $1, is_admin = $2 WHERE user_id = $3", new_role, is_admin_flag, target_id)
        
        await clear_user_cache(target_id)
        
        if notification_text:
            try:
                await bot.send_message(target_id, notification_text, parse_mode="HTML")
            except Exception:
                pass
    
    await message.answer(result_text, parse_mode="HTML")
    await state.clear()

# ... (admin_broadcast_start, admin_broadcast_cancel - без изменений FSM) ...
@dp.callback_query(F.data == "admin_broadcast", MinRoleFilter(ROLE_OWNER))
async def admin_broadcast_start(callback: types.CallbackQuery, state: FSMContext):
    """
    (НОВЫЙ ХЭНДЛЕР)
    Начинает FSM для рассылки, просит админа отправить сообщение.
    """
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_broadcast_message)
    
    text = (
        "<b>📢 Рассылка</b>\n\n"
        "Отправьте <b>одно</b> сообщение (текст, фото, видео, опрос и т.д.), "
        "которое вы хотите разослать всем <b>зарегистрированным</b> пользователям.\n\n"
        "<i>Рассылка начнется немедленно после отправки.</i>"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_cancel")]
    ])

    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=MAIN_MENU_FILE_ID,
                caption=text,
                parse_mode="HTML"
            ),
            reply_markup=kb
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e): 
            logger.error(f"Ошибка в admin_broadcast_start: {e}")

@dp.callback_query(AdminActions.waiting_for_broadcast_message, F.data == "admin_broadcast_cancel")
async def admin_broadcast_cancel(callback: types.CallbackQuery, state: FSMContext):
    """
    (НОВЫЙ ХЭНДЛЕР)
    Отменяет FSM рассылки и возвращает в админ-панель.
    """
    await state.clear()
    await callback.answer("Рассылка отменена.")
    
    await admin_panel_handler(callback)

# [ASYNC-REWRITE]
@dp.message(AdminActions.waiting_for_broadcast_message) # Убрали F.text, теперь ловит всё
async def admin_broadcast_process(message: types.Message, state: FSMContext):
    """
    (PG) Ловит сообщение (текст, фото, видео) от админа и запускает фоновую рассылку.
    """
    admin_id = message.from_user.id
    await state.clear()
    
    users_list = await db_fetchall(
        "SELECT user_id FROM users WHERE is_registered = TRUE AND user_id > 0"
    )
    
    if not users_list:
        await message.answer("❌ Не найдено ни одного зарегистрированного пользователя для рассылки.")
        return

    user_ids = [row['user_id'] for row in users_list]
    
    # Тип контента для отчета
    content_type = "текстовое сообщение"
    if message.photo: content_type = "фото"
    elif message.video: content_type = "видео"
    elif message.animation: content_type = "GIF"

    await message.answer(
        f"✅ Рассылка ({content_type}) запущена для <b>{len(user_ids)}</b> пользователей...\n"
        "Вы получите отчет по завершении.",
        parse_mode="HTML"
    )
    
    asyncio.create_task(start_broadcast(admin_id, message, user_ids))

@dp.callback_query(F.data == "main_season_info")
async def season_info_handler(callback: types.CallbackQuery):
    """Показывает информацию о сезоне."""
    
    season_text = (
        "<b>❄️ WINTER SEASON | PROJECT EVOLUTION</b>\n\n"
        "📅 <b>Сроки проведения:</b>\n"
        "• Начало: <b>1 Декабря</b>\n"
        "• Конец: <b>1 Января</b>\n\n"
        "🏆 <b>Награды сезона (Default-лига):</b>\n"
        "🥇 <b>1 место</b> - 80000 Gold + Qualifications + Premium (30 дней)\n"
        "🥈 <b>2 место</b> - 50000 Gold + Qualifications + Premium (30 дней)\n"
        "🥉 <b>3 место</b> - 30000 Gold + Qualifications + Premium (30 дней)\n"
        "🏅 <b>4 место</b> - 20000 Gold + Premium (30 дней)\n"
        "🏅 <b>5 место</b> - 20000 Gold + Premium (30 дней)\n\n"
        "<i>Успей подняться в рейтинге и забрать призы!</i>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main_menu")]
    ])
    
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(
                media=SEASON_INFO_FILE_ID,
                caption=season_text,
                parse_mode="HTML"
            ),
            reply_markup=keyboard
        )
    except TelegramBadRequest:
        # Если вдруг сообщение устарело, шлем новое
        await callback.message.answer_photo(
            photo=SEASON_INFO_FILE_ID,
            caption=season_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    await callback.answer()

# --- Flask Keep-Alive (Без изменений) ---
app = Flask('')

@app.route('/')
def home():
    """Эта функция будет отвечать на пинги UptimeRobot."""
    return "Bot is alive!"

def run_flask():
    """Запускает Flask-сервер для "keep-alive" пингов."""
    # Получаем порт из переменных окружения (обычно PORT, или 8080 по умолчанию)
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def start_keep_alive_thread():
    """Запускает Flask в отдельном потоке."""
    logger.info("Starting Flask Keep-Alive thread...")
    t = Thread(target=run_flask)
    t.start()

# --- АДМИН: ЗАБРАТЬ ПРЕМИУМ ---
@dp.callback_query(F.data == "admin_revoke_premium", MinRoleFilter(ROLE_ADMIN))
async def admin_revoke_premium_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminActions.waiting_for_revoke_premium_id)
    
    text = ("<b>🔻 Забрать Premium статус</b>\n\n"
            "Введите Telegram ID пользователя, у которого нужно убрать премиум:")
    try:
        await callback.message.edit_media(
            media=InputMediaPhoto(media=MAIN_MENU_FILE_ID, caption=text, parse_mode="HTML"),
            reply_markup=None
        )
    except TelegramBadRequest:
        pass

@dp.message(AdminActions.waiting_for_revoke_premium_id, F.text)
async def admin_revoke_premium_process(message: types.Message, state: FSMContext):
    target_id_str = message.text.strip()
    try:
        target_id = int(target_id_str)
    except ValueError:
        await message.answer("❌ ID должен быть числом!")
        return
    
    user = await db_fetchone("SELECT nickname FROM users WHERE user_id = $1", target_id)
    if not user:
        await message.answer("❌ Пользователь не найден!")
        await state.clear()
        return
    
    # Снимаем премиум (ставим дату в прошлое или NULL)
    await db_execute("UPDATE users SET premium_until = NULL WHERE user_id = $1", target_id)
    await clear_user_cache(target_id)
    
    await message.answer(
        f"✅ Premium статус у игрока <b>{html.escape(user.get('nickname', f'ID: {target_id}'))}</b> успешно отозван.",
        parse_mode="HTML"
    )
    try:
        await bot.send_message(target_id, "📉 <b>Ваш Premium статус был отключен администратором.</b>", parse_mode="HTML")
    except: pass
    
    await state.clear()

# --- АДМИН: ОЧИСТКА БОТОВ ИЗ РЕЙТИНГА ---
@dp.callback_query(F.data == "admin_wipe_bots_db", MinRoleFilter(ROLE_OWNER))
async def admin_wipe_bots_db_handler(callback: types.CallbackQuery):
    """Полностью удаляет данные ботов (ID < 0) из статистики и пользователей."""
    await callback.answer("Очистка базы данных от ботов...", show_alert=True)
    
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Удаляем статистику лиг
                await conn.execute("DELETE FROM user_league_stats WHERE user_id < 0")
                # Удаляем из лобби
                await conn.execute("DELETE FROM lobby_members WHERE user_id < 0")
                # Удаляем самих пользователей
                await conn.execute("DELETE FROM users WHERE user_id < 0")
                
        await callback.message.answer("✅ <b>Все боты успешно удалены из рейтинга и базы данных.</b>", parse_mode="HTML")
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка при очистке: {e}")
        logger.error(f"Wipe bots error: {e}")

# [PG-REWRITE] Главная функция запуска
async def main() -> None:
    global db_pool
    
    logger.info("=== ЗАПУСК БОТА (PostgreSQL) ===")
    
    try:
        db_pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=2,  # Мин. кол-во соединений
            max_size=10, # Макс. кол-во соединений
            command_timeout=60
        )
        if db_pool:
            logger.info("✅ Пул соединений PostgreSQL успешно создан.")
        else:
             logger.critical("❌ Пул соединений НЕ создан (db_pool is None).")
             return
            
    except Exception as e:
        logger.critical(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Не удалось подключиться к PostgreSQL: {e}", exc_info=True)
        logger.critical("Проверьте строку DATABASE_URL в .env файле!")
        return
        
    # Инициализация и миграция БД
    await init_db(db_pool)
    
    try:
        logger.info("Принудительная очистка сессий лобби при старте...")
        
        # Очищаем неактивные лобби (вдруг бот упал посреди матча)
        # lobby_members очистится через ON DELETE CASCADE
        await db_execute("DELETE FROM matches WHERE status != 'completed' AND status != 'cancelled'")
        
        # Сбрасываем все лобби в 'waiting'
        await db_execute("UPDATE lobbies SET current_players = 0, status = 'waiting'")
        await db_execute("DELETE FROM lobby_members")
        
        logger.info("✅ Таблицы lobby_members и lobbies успешно очищены.")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при очистке лобби при старте: {e}")
            
    # [PG-REMOVED] Миграции game_key и admin_ids теперь внутри init_db()

    # [PG-REWRITE] Создание тестовых игроков
        
    await cleanup_expired_mutes()
    
    logger.info("✅ БОТ ЗАПУЩЕН И ГОТОВ К РАБОТЕ")
    logger.info("Ожидание команд от пользователей...")
    
    try:
        await dp.start_polling(bot)
    finally:
        logger.info("Бот останавливается... Закрытие пула соединений.")
        await db_pool.close()
        logger.info("Пул соединений закрыт.")

if __name__ == "__main__":
    
    # Используем переменную окружения RUN_FLASK_KEEP_ALIVE. 
    # На хостинге (Render, Heroku) нужно установить эту переменную в True.
    if os.getenv("RUN_FLASK_KEEP_ALIVE", "False").lower() == "true":
        try:
            start_keep_alive_thread()
            logger.info("✅ Keep-alive-сервер (Flask) запущен в отдельном потоке.")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска Flask-сервера: {e}", exc_info=True)
    else:
        logger.warning("Flask Keep-Alive SKIPPED. Running bot without web server.")

    # Запускаем основной асинхронный процесс бота
    try:
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Критическая ошибка в главном цикле (main): {e}", exc_info=True)