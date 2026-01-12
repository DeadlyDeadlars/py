import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from cryptography.fernet import Fernet
import base64
import hashlib
import getpass

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv
import threading
import sys

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_PASSWORD = os.getenv('ADMIN_PASSWORD', 'adminpass')
DATA_FILE = 'data.json'
FOOTER = 'У нас новые слухи? Или мне кажется?🐶'

# Шифрование data.json
DATA_KEY_ENV = os.getenv('DATA_KEY')

# Если ключа нет в переменной окружения, попросить его в консоли
if DATA_KEY_ENV is None:
    user_input = input('Введите ключ шифрования (или нажмите Enter для отключения): ').strip()
    DATA_KEY_ENV = user_input if user_input else None

ENCRYPTION_ENABLED = DATA_KEY_ENV is not None

if ENCRYPTION_ENABLED:
    # Генерируем Fernet ключ из PASSWORD (производная 32 байта, кодируем в base64)
    key_hash = hashlib.sha256(DATA_KEY_ENV.encode()).digest()
    cipher_key = base64.urlsafe_b64encode(key_hash)
    cipher = Fernet(cipher_key)
    print('✅ Шифрование включено')
else:
    cipher = None
    print('⚠️ Шифрование отключено')

if not BOT_TOKEN:
    raise RuntimeError('BOT_TOKEN is not set in environment')

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

LOCK = asyncio.Lock()

# runtime admin sessions (anonymous admins who logged in with password)
admin_sessions = set()

# data structure persisted to JSON
data = {
    'users': {},       # key: str(user_id) -> {username, last_message, msg_count}
    'drafts': {},      # key: str(user_id) -> {type, content, timestamp}
    'chat': [],        # list of {from_id, username, type, content, timestamp}
    'complaints': [],  # list of {from, text, timestamp}
    'banned': [],      # list of ints
    'accepted': [],    # list of ints
    'enabled': True,
}


def now_ts():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


async def load_data():
    global data
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'rb') as f:
                file_content = f.read()
                if ENCRYPTION_ENABLED:
                    decrypted = cipher.decrypt(file_content)
                    data = json.loads(decrypted.decode('utf-8'))
                else:
                    data = json.loads(file_content.decode('utf-8'))
        except Exception as e:
            print(f'Failed to load data.json: {e}; starting fresh')


async def save_data():
    async with LOCK:
        json_content = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        if ENCRYPTION_ENABLED:
            json_content = cipher.encrypt(json_content)
        with open(DATA_FILE, 'wb') as f:
            f.write(json_content)


async def autosave_loop():
    while True:
        await asyncio.sleep(60)
        await save_data()


def _user_display_name(user: types.User) -> str:
    return f"@{user.username}" if user.username else user.full_name


def log_msg(msg_type: str, user: types.User, content: str):
    ts = now_ts()
    name = _user_display_name(user)
    kind = {'text': 'Текст', 'photo': 'Фото', 'video': 'Видео'}.get(msg_type, msg_type)
    print(f"[MSG] {ts} | {name} ({user.id}) | {kind} | {content}")


async def shutdown():
    """Graceful shutdown: save data, close bot session and exit."""
    print('Shutdown initiated...')
    try:
        await save_data()
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass
    print('Shutdown complete.')
    os._exit(0)


def console_watcher(loop):
    """Run in a thread: wait for console input 'exit' or 'quit' to stop the bot."""
    while True:
        try:
            line = input()
        except Exception:
            break
        if line.strip().lower() in ('exit', 'quit', 'stop', 'shutdown'):
            asyncio.run_coroutine_threadsafe(shutdown(), loop)
            break


# Keyboards
terms_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text='✅ Принять', callback_data='accept_terms')],
    [InlineKeyboardButton(text='❌ Не согласен', callback_data='decline_terms')],
])

user_kb = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text='⚠️ Пожаловаться'), KeyboardButton(text='ℹ️ Меню')],
], resize_keyboard=True)

admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text='Включить/Выключить бота')],
        [KeyboardButton(text='Статистика')],
        [KeyboardButton(text='Пользователи')],
        [KeyboardButton(text='Остановить бота')],
        [KeyboardButton(text='История чата')],
        [KeyboardButton(text='Бан/Разбан')],
        [KeyboardButton(text='Рассылка')],
        [KeyboardButton(text='Очистка чата')],
            [KeyboardButton(text='Стереть историю'), KeyboardButton(text='Удалить все сообщения')],
            [KeyboardButton(text='Сброс данных')],
        [KeyboardButton(text='Просмотр жалоб')],
        [KeyboardButton(text='Выход')],
    ],
    resize_keyboard=True,
)

# list of admin button texts (used to avoid treating them as user content)
ADMIN_BUTTON_TEXTS = {
    'Включить/Выключить бота', 'Статистика', 'Пользователи', 'Остановить бота',
    'История чата', 'Бан/Разбан', 'Рассылка', 'Очистка чата', 'Стереть историю', 'Удалить все сообщения', 'Просмотр жалоб', 'Выход', 'Сброс данных'
}


@dp.message(Command('start'))
async def cmd_start(message: types.Message):
    uid = str(message.from_user.id)
    data['users'].setdefault(uid, {'username': message.from_user.username, 'last_message': None})
    terms = (
    'Условия пользования:\n'
    '- Все сообщения и материалы публикуются пользователями под их личную ответственность.\n'
    '- Публикация фото и медиа с изображением других лиц возможна, но ответственность за публикацию лежит на пользователе.\n'
    '- При первой жалобе на материалы с изображением третьих лиц они будут удалены без обсуждения.\n'
    '- Администрация не поощряет публикацию материалов без согласия изображённых лиц.\n'
    '- Запрещено распространять материалы из этого бота за его пределы; ответственность лежит на пользователе.\n'
    '- Администрация не несёт ответственности за пользовательский контент, шутки, подколы или последствия общения.\n'
    '- Администраторы действуют в рамках закона и вправе ограничить доступ к боту.\n'
    '- Администраторы не связаны с учебным заведением и не представляют его интересы.\n'
    '- Использование бота означает согласие с правилами и осознание возможных рисков.\n'
    '- Незнание условий не снимает с вас ответственности.\n'
    'Нажмите ✅ Принять для продолжения или \n ❌ Не согласен для отказа.'
)
                 
    await message.answer(terms, reply_markup=terms_kb)


@dp.callback_query(lambda c: c.data == 'accept_terms')
async def cb_accept(cb: types.CallbackQuery):
    uid_int = int(cb.from_user.id)
    if uid_int in data.get('banned', []):
        await cb.message.answer('Вы забанены и не можете пользоваться ботом.')
        await cb.answer()
        return
    if uid_int not in data.get('accepted', []):
        data.setdefault('accepted', []).append(uid_int)
    await save_data()
    
    # Сообщение подтверждения
    confirmation = (
        '✅ Вы приняли условия пользования!\n\n'
        'Теперь вы можете писать сообщения в анонимный чат. 🎉\n'
    )
    await cb.message.answer(confirmation)
    
    # Отправляем меню сразу
    help_text = (
        '📋 МЕНЮ И СПРАВКА:\n\n'
        '👤 ОТПРАВКА СООБЩЕНИЙ:\n'
        '- Отправьте текст, фото или видео\n'
        '- Появится превью и кнопка подтверждения\n'
        '- После подтверждения сообщение станет анонимным\n'
        '- Лимит: 1 сообщение на 30 секунд (антиспам)\n\n'
        '⚠️ ЖАЛОБЫ:\n'
        '- Нажмите "⚠️ Пожаловаться" под сообщением\n'
        '- Или используйте кнопку "⚠️ Пожаловаться"\n\n'
        '⚠️ ПРАВИЛА:\n'
        '- Мы не поддерживаем публикацию материалов без согласия изображённых лиц (фото/видео).\n'
        '- Такие материалы могут быть удалены по просьбе через жалобу с объяснением причины.\n'
        '- Можете выражать себя как хотите — мат, шутки, подколы допускаются.\n'
        '- Мы ценим дружелюбное отношение к пользователям и стараемся поддерживать безопасную атмосферу.\n\n'
        '💬 КОМАНДЫ:\n'
        '- "ℹ️ Меню" — показать эту справку\n'
        '- "/start" — начать заново\n\n'
        '🕊️ Команда FreeBird всегда к вашим услугам!'
    )
    await cb.message.answer(help_text, reply_markup=user_kb)
    await cb.answer()


@dp.callback_query(lambda c: c.data == 'decline_terms')
async def cb_decline(cb: types.CallbackQuery):
    await cb.message.answer('Вы отказались от условий. Для использования бота нужно принять условия (/start).')
    await cb.answer()


@dp.callback_query(lambda c: c.data == 'confirm_send')
async def cb_confirm_send(cb: types.CallbackQuery):
    uid = str(cb.from_user.id)
    draft = data.get('drafts', {}).get(uid)
    if not draft:
        await cb.message.answer('Черновик не найден.')
        await cb.answer()
        return
    # Add to public chat (anonymous to users)
    msg = {
        'from_id': int(uid),
        'username': data.get('users', {}).get(uid, {}).get('username'),
        'type': draft['type'],
        'content': draft.get('content'),
        'caption': draft.get('caption', ''),
        'timestamp': now_ts(),
    }
    # Копировать индекс целевого сообщения если это ответ
    if 'reply_target_idx' in draft:
        msg['reply_target_idx'] = draft['reply_target_idx']
    # store delivered message ids per recipient to allow later deletion
    msg['delivered'] = {}
    data.setdefault('chat', []).append(msg)
    # Увеличить счетчик сообщений пользователя
    data.setdefault('users', {}).setdefault(uid, {})['msg_count'] = data['users'][uid].get('msg_count', 0) + 1
    # log for admin/console
    user_obj = cb.from_user
    if draft['type'] == 'text':
        log_msg(draft['type'], user_obj, draft['content'])
    else:
        log_msg(draft['type'], user_obj, f"file_id:{draft['content']} caption:{draft.get('caption','')}")
    # Send anonymous to all users with footer at the bottom and attach complaint button
    chat_idx = len(data.get('chat', [])) - 1
    complaint_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='⚠️ Пожаловаться', callback_data=f'complaint_{chat_idx}')]
    ])
    for uid_k in list(data.get('users', {}).keys()):
        try:
            user_id_int = int(uid_k)
            # Получить индекс целевого сообщения в чате (если это ответ)
            reply_target_idx = msg.get('reply_target_idx')
            reply_to_id = None
            
            # Если это ответ на сообщение в чате, найти message_id этого сообщения для текущего получателя
            if reply_target_idx is not None and 0 <= reply_target_idx < len(data.get('chat', [])):
                target_msg = data['chat'][reply_target_idx]
                reply_to_id = target_msg.get('delivered', {}).get(str(user_id_int))
            
            # Проверить, админ это или обычный пользователь
            is_admin = user_id_int in admin_sessions
            
            # Для админа показать подписанное сообщение, для остальных - анонимное
            if is_admin:
                sender_name = msg.get('username') if msg.get('username') else f'ID {msg["from_id"]}'
                header = f"📤 От: {sender_name} ({msg['from_id']})\n\n"
            else:
                header = ''
            
            if msg['type'] == 'text':
                content = header + msg["content"] + f'\n\n{FOOTER}'
                try:
                    sent = await bot.send_message(user_id_int, content, reply_markup=complaint_kb if not is_admin else None, reply_to_message_id=reply_to_id)
                except Exception:
                    # Если reply_to_message_id не существует, отправить без ответа
                    sent = await bot.send_message(user_id_int, content, reply_markup=complaint_kb if not is_admin else None)
                msg['delivered'][str(user_id_int)] = sent.message_id
            elif msg['type'] == 'photo':
                caption = msg.get('caption') or ''
                caption = header + caption if header else caption
                caption = f"{caption}\n\n{FOOTER}" if caption else FOOTER
                try:
                    sent = await bot.send_photo(user_id_int, msg['content'], caption=caption, reply_markup=complaint_kb if not is_admin else None, reply_to_message_id=reply_to_id)
                except Exception:
                    # Если reply_to_message_id не существует, отправить без ответа
                    sent = await bot.send_photo(user_id_int, msg['content'], caption=caption, reply_markup=complaint_kb if not is_admin else None)
                msg['delivered'][str(user_id_int)] = sent.message_id
            elif msg['type'] == 'video':
                caption = msg.get('caption') or ''
                caption = header + caption if header else caption
                caption = f"{caption}\n\n{FOOTER}" if caption else FOOTER
                try:
                    sent = await bot.send_video(user_id_int, msg['content'], caption=caption, reply_markup=complaint_kb if not is_admin else None, reply_to_message_id=reply_to_id)
                except Exception:
                    # Если reply_to_message_id не существует, отправить без ответа
                    sent = await bot.send_video(user_id_int, msg['content'], caption=caption, reply_markup=complaint_kb if not is_admin else None)
                msg['delivered'][str(user_id_int)] = sent.message_id
        except Exception:
            pass
    # save delivered ids
    try:
        await save_data()
    except Exception:
        pass
    # clear user draft and update last_message
    data['drafts'].pop(uid, None)
    data['users'].setdefault(uid, {})['last_message'] = now_ts()
    await save_data()
    # delete confirmation message
    try:
        await cb.message.delete()
    except Exception:
        pass
    # send temporary notification and auto-delete it after 3 seconds
    try:
        tmp = await bot.send_message(cb.from_user.id, 'Сообщение отправлено в чат.')
        async def _del_later(chat_id, message_id, delay=3):
            await asyncio.sleep(delay)
            try:
                await bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        asyncio.create_task(_del_later(tmp.chat.id, tmp.message_id, 3))
    except Exception:
        pass
    await cb.answer()


@dp.callback_query(lambda c: c.data == 'cancel_send')
async def cb_cancel_send(cb: types.CallbackQuery):
    uid = str(cb.from_user.id)
    # remove draft
    data.get('drafts', {}).pop(uid, None)
    await save_data()
    # delete confirmation message
    try:
        await cb.message.delete()
    except Exception:
        pass
    # send temporary cancellation notice
    try:
        tmp = await bot.send_message(cb.from_user.id, 'Отправка отменена.')
        async def _del_later(chat_id, message_id, delay=3):
            await asyncio.sleep(delay)
            try:
                await bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        asyncio.create_task(_del_later(tmp.chat.id, tmp.message_id, 3))
    except Exception:
        pass
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith('del_complaint_'))
async def cb_del_complaint(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
        if 0 <= idx < len(data.get('complaints', [])):
            data['complaints'].pop(idx)
            await save_data()
            try:
                await cb.message.edit_text('Жалоба удалена.')
            except Exception:
                pass
            async def _del_after(msg):
                await asyncio.sleep(3)
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
            asyncio.create_task(_del_after(cb.message))
        else:
            await cb.answer('Жалоба не найдена.')
    except Exception:
        await cb.answer('Ошибка.')


@dp.callback_query(lambda c: c.data.startswith('reply_complaint_'))
async def cb_reply_complaint(cb: types.CallbackQuery):
    # Admin chooses to reply to a complaint: enter reply mode
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
    except Exception:
        await cb.answer('Ошибка.')
        return
    data['admin_action'] = 'reply_complaint_pending'
    data['admin_action_target'] = idx
    await save_data()
    await cb.message.answer(f'Введите ответ на жалобу #{idx}:')
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith('delete_msg_'))
async def cb_delete_msg(cb: types.CallbackQuery):
    # Admin wants to delete the message that was targeted by the complaint
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
    except Exception:
        await cb.answer('Ошибка.')
        return
    comps = data.get('complaints', [])
    if not (0 <= idx < len(comps)):
        await cb.answer('Жалоба не найдена.')
        return
    comp = comps[idx]
    target = comp.get('target')
    if target is None:
        # no target; just remove the complaint
        data['complaints'].pop(idx)
        await save_data()
        try:
            await cb.message.edit_text('Жалоба удалена (сообщение не найдено).')
        except Exception:
            pass
        async def _del_after2(msg):
            await asyncio.sleep(3)
            try:
                await bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
        asyncio.create_task(_del_after2(cb.message))
        await cb.answer('Жалоба удалена.')
        return
    # remove target message if exists
    if 0 <= target < len(data.get('chat', [])):
        # attempt to delete delivered messages for this chat entry
        target_msg = data['chat'][target]
        delivered = target_msg.get('delivered', {}) or {}
        for recip_str, mid in list(delivered.items()):
            try:
                await bot.delete_message(int(recip_str), mid)
            except Exception:
                pass
        # remove the target from stored chat and the complaint
        data['chat'].pop(target)
        data['complaints'].pop(idx)
        await save_data()
        try:
            await cb.message.edit_text('Сообщение удалено и жалоба обработана.')
        except Exception:
            pass
        # notify the complainant that their request was fulfilled
        try:
            reporter = comp.get('from')
            if reporter:
                await bot.send_message(int(reporter), 'Ваша просьба выполнена.')
        except Exception:
            pass
        await cb.answer('Сообщение удалено.')
    else:
        data['complaints'].pop(idx)
        await save_data()
        try:
            await cb.message.edit_text('Целевое сообщение не найдено — жалоба удалена.')
        except Exception:
            pass
        async def _del_after3(msg):
            await asyncio.sleep(3)
            try:
                await bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
        asyncio.create_task(_del_after3(cb.message))
        await cb.answer('Целевое сообщение не найдено; жалоба удалена.')


@dp.callback_query(lambda c: c.data.startswith('skip_complaint_'))
async def cb_skip_complaint(cb: types.CallbackQuery):
    # Admin chooses to skip this complaint (mark as seen/ignored)
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
    except Exception:
        await cb.answer('Ошибка.')
        return
    if 0 <= idx < len(data.get('complaints', [])):
        data['complaints'].pop(idx)
        await save_data()
        try:
            await cb.message.edit_text('Жалоба пропущена (удалена из списка).')
        except Exception:
            pass
        await cb.answer('Жалоба пропущена.')
    else:
        await cb.answer('Жалоба не найдена.')


@dp.callback_query(lambda c: c.data == 'confirm_clear_history')
async def cb_confirm_clear_history(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    data['chat'].clear()
    await save_data()
    await cb.message.edit_text('✅ История чата полностью удалена.')
    await cb.answer('История стёрта.')


@dp.callback_query(lambda c: c.data == 'cancel_clear_history')
async def cb_cancel_clear_history(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    await cb.message.edit_text('❌ Отмено. История чата сохранена.')
    await cb.answer('Отменено.')


@dp.callback_query(lambda c: c.data == 'confirm_delete_all_msgs')
async def cb_confirm_delete_all_msgs(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    # Удалить последние 50 сообщений у всех пользователей
    chat_list = data.get('chat', [])
    # Оставить только сообщения, которые не в последних 50
    msgs_to_delete = chat_list[-50:] if len(chat_list) > 50 else chat_list
    
    for msg in msgs_to_delete:
        delivered = msg.get('delivered', {}) or {}
        for recip_str, mid in list(delivered.items()):
            try:
                await bot.delete_message(int(recip_str), mid)
            except Exception:
                pass
    
    # Оставить в истории только старые сообщения (удалить последние 50 из истории)
    if len(chat_list) > 50:
        data['chat'] = chat_list[:-50]
    else:
        data['chat'].clear()
    
    await save_data()
    deleted_count = len(msgs_to_delete)
    await cb.message.edit_text(f'✅ Удалено {deleted_count} последних сообщений у всех пользователей.')
    await cb.answer('Сообщения удалены.')


@dp.callback_query(lambda c: c.data == 'cancel_delete_all_msgs')
async def cb_cancel_delete_all_msgs(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    await cb.message.edit_text('❌ Отменено. Сообщения сохранены.')
    await cb.answer('Отменено.')


@dp.callback_query(lambda c: c.data == 'confirm_reset_data')
async def cb_confirm_reset_data(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    # Пометить ожидание ввода пароля
    data['admin_action'] = 'reset_pending'
    await save_data()
    await cb.message.answer('Введите пароль администратора для подтверждения удаления данных:')
    await cb.answer()


@dp.callback_query(lambda c: c.data == 'cancel_reset_data')
async def cb_cancel_reset_data(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        await cb.message.edit_text('❌ Отменено. Данные сохранены.')
    except Exception:
        pass
    data['admin_action'] = None
    await save_data()
    await cb.answer('Отменено.')


@dp.callback_query(lambda c: c.data.startswith('del_submission_'))
async def cb_del_submission(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
        if 0 <= idx < len(data.get('chat', [])):
            data['chat'].pop(idx)
            await save_data()
            await cb.message.edit_text('Сообщение удалено из чата.')
        else:
            await cb.answer('Сообщение не найдено.')
    except Exception:
        await cb.answer('Ошибка.')


@dp.callback_query(lambda c: c.data.startswith('complaint_'))
async def cb_complaint_inline(cb: types.CallbackQuery):
    # User clicked complaint on a specific chat message
    if cb.from_user.id in admin_sessions:
        await cb.answer('Админы не могут отправлять жалобы через эту кнопку.')
        return
    try:
        idx = int(cb.data.split('_')[1])
    except Exception:
        await cb.answer('Ошибка.')
        return
    uid = str(cb.from_user.id)
    data.setdefault('users', {}).setdefault(uid, {})['awaiting_complaint_for'] = idx
    await save_data()
    await cb.message.answer('Опишите, пожалуйста, причину жалобы (коротко):')
    await cb.answer()


@dp.callback_query(lambda c: c.data.startswith('del_chat_'))
async def cb_del_chat(cb: types.CallbackQuery):
    if cb.from_user.id not in admin_sessions:
        await cb.answer('Вы не админ.')
        return
    try:
        idx = int(cb.data.split('_')[2])
        if 0 <= idx < len(data.get('chat', [])):
            data['chat'].pop(idx)
            await save_data()
            await cb.message.edit_text('Сообщение удалено из чата.')
        else:
            await cb.answer('Сообщение не найдено.')
    except Exception:
        await cb.answer('Ошибка.')


def can_send_check(user_id: str) -> tuple[bool, str]:
    uid = int(user_id)
    if uid in data.get('banned', []):
        return False, 'Вы забанены.'
    if not data.get('enabled', True):
        return False, 'Бот временно отключён.'
    if uid not in data.get('accepted', []):
        return False, 'Примите условия (/start) прежде чем отправлять сообщения.'
    last = data.get('users', {}).get(user_id, {}).get('last_message')
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            diff = (datetime.now(timezone.utc) - last_dt).total_seconds()
            if diff < 30:
                return False, f'Антиспам: подождите {int(30-diff)} секунд.'
        except Exception:
            pass
    return True, ''


@dp.message()
async def on_message(message: types.Message):
    uid = str(message.from_user.id)

    # Admin entry trigger
    if message.text and message.text.startswith('/admin'):
        await message.answer('Введите пароль администратора:')
        # mark awaiting password in user record
        data['users'].setdefault(uid, {})['awaiting_admin_password'] = True
        await save_data()
        return

    # If user is replying with admin password
    if message.text and data['users'].get(uid, {}).pop('awaiting_admin_password', False):
        if message.text.strip() == ADMIN_PASSWORD:
            admin_sessions.add(int(uid))
            await message.answer('Доступ в админ-панель предоставлен.', reply_markup=admin_kb)
        else:
            await message.answer('Неверный пароль.')
        await save_data()
        return

    # Admin actions and keyboard handling (only for logged-in admins)
    if message.from_user.id in admin_sessions and message.text:
        text = message.text
        # Exit
        if text == 'Выход':
            admin_sessions.discard(message.from_user.id)
            await message.answer('Выход из админ-панели.', reply_markup=ReplyKeyboardRemove())
            return

        if text == 'Включить/Выключить бота':
            data['enabled'] = not data.get('enabled', True)
            await save_data()
            await message.answer(f"Бот {'включён' if data['enabled'] else 'выключен'}.")
            return

        if text == 'Статистика':
            users_count = len(data.get('users', {}))
            drafts = len(data.get('drafts', {}))
            complaints = len(data.get('complaints', []))
            chat_msgs = len(data.get('chat', []))
            # Вычислить общее количество сообщений от всех пользователей
            total_msgs = sum(u.get('msg_count', 0) for u in data.get('users', {}).values())
            stats = f'Пользователей: {users_count}\nЧерновиков: {drafts}\nСообщений в чате: {chat_msgs}\nВсего отправлено сообщений: {total_msgs}\nЖалоб: {complaints}'
            await message.answer(stats)
            return

        if text == 'Пользователи':
            users = data.get('users', {})
            users_count = len(users)
            if users_count == 0:
                await message.answer('Пользователей нет.')
            else:
                users_list = []
                for uid_k, uinfo in users.items():
                    uname = uinfo.get('username')
                    display_name = f'@{uname}' if uname else f'ID {uid_k}'
                    banned = ' (забанен)' if int(uid_k) in data.get('banned', []) else ''
                    msg_count = uinfo.get('msg_count', 0)
                    users_list.append(f'{display_name} - {msg_count} соо{banned}')
                await message.answer(f'Пользователей: {users_count}\n\n' + '\n'.join(users_list))
            return

        if text == 'Остановить бота':
            await message.answer('Останавливаю бота...')
            await asyncio.sleep(0.5)
            await message.answer('Бот остановлен.')
            # logout admin from admin panel and remove keyboard
            try:
                admin_sessions.discard(message.from_user.id)
                await message.answer('Вы вышли из админ-панели.', reply_markup=ReplyKeyboardRemove())
            except Exception:
                pass
            asyncio.create_task(shutdown())
            return

        if text == 'Просмотр жалоб':
            if not data.get('complaints'):
                await message.answer('Жалоб нет.')
            else:
                for idx, c in enumerate(data.get('complaints', [])):
                    user_id = c.get('from')
                    user_info = data.get('users', {}).get(str(user_id), {})
                    uname = user_info.get('username') or f'ID {user_id}'
                    del_kb = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text='✉️ Ответить', callback_data=f'reply_complaint_{idx}'),
                            InlineKeyboardButton(text='🗑️ Удалить сообщение', callback_data=f'delete_msg_{idx}'),
                        ],
                        [
                            InlineKeyboardButton(text='⚠️ Удалить жалобу', callback_data=f'del_complaint_{idx}'),
                            InlineKeyboardButton(text='⏭️ Пропустить', callback_data=f'skip_complaint_{idx}'),
                        ],
                    ])
                    target = c.get('target')
                    if target is not None and 0 <= target < len(data.get('chat', [])):
                        tmsg = data['chat'][target]
                        t_uname = tmsg.get('username') or f'ID {tmsg.get("from_id")}'
                        msg_type = tmsg.get('type')
                        caption_text = f"Жалоба #{idx} от @{uname} ({user_id})\nНа сообщение #{target} от @{t_uname}:\nПричина: {c.get('text')}"
                        
                        if msg_type == 'text':
                            t_preview = tmsg.get('content')
                            await message.answer(f"Жалоба #{idx} от @{uname} ({user_id}):\nНа сообщение #{target} от @{t_uname}:\n{t_preview}\nПричина: {c.get('text')}", reply_markup=del_kb)
                        elif msg_type == 'photo':
                            await bot.send_photo(message.chat.id, tmsg.get('content'), caption=caption_text, reply_markup=del_kb)
                        elif msg_type == 'video':
                            await bot.send_video(message.chat.id, tmsg.get('content'), caption=caption_text, reply_markup=del_kb)
                    else:
                        await message.answer(f"Жалоба #{idx} от @{uname} ({user_id}):\n{c.get('text')}", reply_markup=del_kb)
            return

        if text == 'История чата':
            chats = data.get('chat', [])
            if not chats:
                await message.answer('История чата пуста.')
            else:
                    # Aggregate chat history into a single message (with fallback to chunking)
                    parts = []
                    for idx, msg in enumerate(chats):
                        uname = msg.get('username')
                        display_name = f'@{uname}' if uname else f'ID {msg["from_id"]}'
                        if msg['type'] == 'text':
                            body = msg.get('content', '')
                        else:
                            caption = msg.get('caption') or ''
                            body = f"{msg['type']} file_id {msg.get('content')}" + (f" caption: {caption}" if caption else '')
                        # Конвертировать время в Екатеринбург (UTC+5)
                        try:
                            ts_utc = datetime.fromisoformat(msg['timestamp'])
                            ts_ekb = ts_utc.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5)))
                            time_str = ts_ekb.strftime('%H:%M:%S')
                        except Exception:
                            time_str = msg['timestamp']
                        parts.append(f"{idx+1}. {display_name} ({msg['from_id']}) в {time_str}:\n{body}")

                    combined = '\n\n'.join(parts)
                    # Telegram max message length ~4096; use safe limit
                    SAFE_LIMIT = 3900
                    try:
                        if len(combined) <= SAFE_LIMIT:
                            await message.answer(combined)
                        else:
                            # chunk by entries to avoid splitting messages mid-entry
                            chunks = []
                            cur = []
                            cur_len = 0
                            for entry in parts:
                                if cur_len + len(entry) + 2 > SAFE_LIMIT and cur:
                                    chunks.append('\n\n'.join(cur))
                                    cur = [entry]
                                    cur_len = len(entry) + 2
                                else:
                                    cur.append(entry)
                                    cur_len += len(entry) + 2
                            if cur:
                                chunks.append('\n\n'.join(cur))
                            total = len(chunks)
                            for i, chunk in enumerate(chunks, start=1):
                                header = f'История чата (часть {i}/{total}):\n\n' if total > 1 else ''
                                await message.answer(header + chunk)
                    except Exception:
                        await message.answer('Не удалось отправить историю чата.')
            return

        if text == 'Очистка чата':
            data['drafts'].clear()
            await save_data()
            await message.answer('Все черновики пользователей удалены.')
            return

        if text == 'Стереть историю':
            # Удалить всю историю чата
            confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text='✅ Да, стереть', callback_data='confirm_clear_history'),
                    InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_clear_history'),
                ],
            ])
            await message.answer('⚠️ Вы уверены? Это удалит всю историю сообщений навсегда!', reply_markup=confirm_kb)
            return

        if text == 'Удалить все сообщения':
            # Удалить последние 50 сообщений у всех пользователей
            confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text='✅ Да, удалить', callback_data='confirm_delete_all_msgs'),
                    InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_delete_all_msgs'),
                ],
            ])
            await message.answer('⚠️ Вы уверены? Это удалит последние 50 сообщений у всех пользователей в чате!', reply_markup=confirm_kb)
            return
        
        if text == 'Сброс данных':
            # Начало двухшагового подтверждения: сначала уточнение
            confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text='✅ Да, удалить', callback_data='confirm_reset_data'),
                    InlineKeyboardButton(text='❌ Отмена', callback_data='cancel_reset_data'),
                ],
            ])
            await message.answer('⚠️ Вы уверены? Это удалит ВСЕ данные (пользователи, история, жалобы) навсегда!', reply_markup=confirm_kb)
            return

        if text == 'Бан/Разбан':
            data['admin_action'] = 'ban_pending'
            await message.answer('Отправьте ID пользователя для бан/разбан:')
            await save_data()
            return

        if text == 'Рассылка':
            data['admin_action'] = 'broadcast_pending'
            await message.answer('Отправьте текст рассылки:')
            await save_data()
            return

    # Handle pending admin actions (ban or broadcast)
    if message.from_user.id in admin_sessions:
        if data.get('admin_action') == 'ban_pending':
            try:
                target = int(message.text.strip())
                if target in data.get('banned', []):
                    data['banned'].remove(target)
                    await message.answer(f'Пользователь {target} разбанен.')
                else:
                    data.setdefault('banned', []).append(target)
                    await message.answer(f'Пользователь {target} забанен.')
                data['admin_action'] = None
                await save_data()
            except Exception:
                await message.answer('Неверный ID.')
            return

        if data.get('admin_action') == 'broadcast_pending':
            text = message.text or ''
            sent = 0
            for uid_k in list(data.get('users', {}).keys()):
                try:
                    await bot.send_message(int(uid_k), f'Рассылка от админа:\n{text}')
                    sent += 1
                except Exception:
                    pass
            data['admin_action'] = None
            await message.answer(f'Рассылка отправлена ({sent}).')
            return

        if data.get('admin_action') == 'reply_complaint_pending':
            target = data.get('admin_action_target')
            try:
                comps = data.get('complaints', [])
                if target is None or not (0 <= int(target) < len(comps)):
                    await message.answer('Целевая жалоба не найдена.')
                else:
                    comp = comps[int(target)]
                    target_user = comp.get('from')
                    reply_text = message.text or ''
                    reporter_name = comp.get('from_username') or f'ID {target_user}'
                    try:
                        # Send anonymous reply from admin (do not reveal admin identity)
                        send_text = f'Ответ от администратора:\n\n{reply_text}'
                        await bot.send_message(int(target_user), send_text)
                        await message.answer('Ответ отправлен заявителю.')
                    except Exception:
                        await message.answer('Не удалось отправить ответ заявителю.')
                data['admin_action'] = None
                data['admin_action_target'] = None
                await save_data()
            except Exception:
                await message.answer('Ошибка при отправке ответа.')
            return

        if data.get('admin_action') == 'reset_pending':
            try:
                # Проверяем введённый пароль
                if message.text and message.text.strip() == ADMIN_PASSWORD:
                    # Запись в audit.log (без бэкапа)
                    try:
                        with open('audit.log', 'a', encoding='utf-8') as al:
                            al.write("no_backup\n")
                    except Exception:
                        pass
                    # Сброс данных в память и сохранение (новая структура)
                    new_data = {
                        'users': {},
                        'drafts': {},
                        'chat': [],
                        'complaints': [],
                        'banned': [],
                        'accepted': [],
                        'enabled': True,
                    }
                    data.clear()
                    data.update(new_data)
                    await save_data()
                    await message.answer('✅ Все данные удалены.')
                else:
                    await message.answer('Неверный пароль. Операция отменена.')
                data['admin_action'] = None
                await save_data()
            except Exception:
                await message.answer('Ошибка при выполнении операции.')
            return


    # If user is an admin and not performing a pending admin action, do not treat their messages as regular content.
    # Show the admin keyboard again and instruct to use buttons.
    if message.from_user.id in admin_sessions:
        await message.answer('Вы в админ-панели. Пожалуйста, используйте кнопки панели для действий.', reply_markup=admin_kb)
        return

    # removed manual 'Отправить' flow; confirmation happens immediately after user sends content

    # If user pressed '⚠️ Пожаловаться'
    if message.text == 'ℹ️ Меню':
        help_text = (
            '📋 МЕНЮ И СПРАВКА:\n\n'
            '👤 ОТПРАВКА СООБЩЕНИЙ:\n'
            '- Отправьте текст, фото или видео\n'
            '- Появится превью и кнопка подтверждения\n'
            '- После подтверждения сообщение станет анонимным\n'
            '- Лимит: 1 сообщение на 30 секунд (антиспам)\n\n'
            '⚠️ ЖАЛОБЫ:\n'
            '- Нажмите "⚠️ Пожаловаться" под сообщением\n'
            '- Или используйте кнопку "⚠️ Пожаловаться"\n\n'
            '⚠️ ПРАВИЛА:\n'
            '- Мы не поддерживаем публикацию материалов без согласия изображённых лиц (фото/видео).\n'
            '- Такие материалы могут быть удалены по просьбе через жалобу с объяснением причины.\n'
            '- Можете выражать себя как хотите — мат, шутки, подколы допускаются.\n'
            '- Мы ценим дружелюбное отношение к пользователям и стараемся поддерживать безопасную атмосферу.\n\n'
            '💬 КОМАНДЫ:\n'
            '- "ℹ️ Меню" — показать эту справку\n'
            '- "/start" — начать заново\n\n'
            '🕊️ Команда FreeBird всегда поможет вам!'
        )
        await message.answer(help_text, reply_markup=user_kb)
        return

    if message.text == '⚠️ Пожаловаться':
        data['users'].setdefault(uid, {})['awaiting_complaint'] = True
        await save_data()
        await message.answer('Отправьте текст жалобы (коротко):')
        return

    # If awaiting complaint (either general or for a specific message)
    awaiting_for = data.get('users', {}).get(uid, {}).pop('awaiting_complaint_for', None)
    awaiting_general = data.get('users', {}).get(uid, {}).pop('awaiting_complaint', False)
    if awaiting_for is not None or awaiting_general:
        comp = {
            'from': int(uid),
            'from_username': data.get('users', {}).get(uid, {}).get('username'),
            'text': message.text or '',
            'timestamp': now_ts(),
            'target': None,
        }
        if awaiting_for is not None:
            comp['target'] = int(awaiting_for)
        data.setdefault('complaints', []).append(comp)
        await save_data()
        # notify admin sessions with details (show clickable @username when available)
        for adm in list(admin_sessions):
            try:
                # Конвертировать время в Екатеринбург и форматировать
                try:
                    ts_utc = datetime.fromisoformat(comp['timestamp'])
                    ts_ekb = ts_utc.replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=5)))
                    time_str = ts_ekb.strftime('%d.%m.%Y %H:%M:%S')
                except Exception:
                    time_str = comp['timestamp']
                
                reporter_uname = comp.get('from_username')
                reporter_display = f'@{reporter_uname}' if reporter_uname else f'ID {comp["from"]}'
                if comp.get('target') is not None and 0 <= comp['target'] < len(data.get('chat', [])):
                    target_msg = data['chat'][comp['target']]
                    t_uname = target_msg.get('username')
                    target_display = f'@{t_uname}' if t_uname else f'ID {target_msg.get("from_id")}'
                    msg_type = target_msg.get('type')
                    caption_text = f'Новая жалоба от {reporter_display} ({comp["from"]})\nНа сообщение #{comp["target"]} от {target_display}:\nПричина: {comp["text"]}\nВремя: {time_str}'
                    
                    if msg_type == 'text':
                        target_preview = target_msg.get('content')
                        await bot.send_message(adm, f'Новая жалоба от {reporter_display} ({comp["from"]})\nНа сообщение #{comp["target"]} от {target_display}:\n{target_preview}\nПричина: {comp["text"]}\nВремя: {time_str}')
                    elif msg_type == 'photo':
                        await bot.send_photo(adm, target_msg.get('content'), caption=caption_text)
                    elif msg_type == 'video':
                        await bot.send_video(adm, target_msg.get('content'), caption=caption_text)
                else:
                    await bot.send_message(adm, f'Новая жалоба от {reporter_display} ({comp["from"]})\nПричина: {comp["text"]}\nВремя: {time_str}')
            except Exception:
                pass
        await message.answer('Жалоба отправлена администраторам.', reply_markup=user_kb)
        return

    # Handle incoming content -> save as draft and ask for confirmation immediately
    # Skip this flow for admins (they have separate admin handling)
    # Defensive: ignore admin-panel button presses from admins so they aren't treated as content
    if message.text and message.from_user.id in admin_sessions and message.text in ADMIN_BUTTON_TEXTS:
        return

    if message.content_type in ('text', 'photo', 'video') and message.from_user.id not in admin_sessions:
        ok, reason = can_send_check(uid)
        if not ok:
            await message.answer(reason)
            return
        if message.content_type == 'text':
            content = message.text
            t = 'text'
            # save draft
            data.setdefault('drafts', {})[uid] = {'type': t, 'content': content, 'timestamp': now_ts()}
        elif message.content_type == 'photo':
            file_id = message.photo[-1].file_id
            caption = message.caption or ''
            t = 'photo'
            # save draft with caption
            data.setdefault('drafts', {})[uid] = {'type': t, 'content': file_id, 'caption': caption, 'timestamp': now_ts()}
            content = file_id
        else:
            file_id = message.video.file_id
            caption = message.caption or ''
            t = 'video'
            data.setdefault('drafts', {})[uid] = {'type': t, 'content': file_id, 'caption': caption, 'timestamp': now_ts()}
            content = file_id
        data.setdefault('users', {}).setdefault(uid, {})
        
        # Сохранить индекс целевого сообщения в чате если это ответ
        if message.reply_to_message:
            # Найти целевое сообщение в истории чата по message_id в личном чате отправителя
            target_msg_id = message.reply_to_message.message_id
            for idx, chat_msg in enumerate(data.get('chat', [])):
                if chat_msg.get('delivered', {}).get(uid) == target_msg_id:
                    data['drafts'][uid]['reply_target_idx'] = idx
                    break
        
        await save_data()
        # prepare confirmation inline keyboard
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='✅ Отправить', callback_data='confirm_send')],
            [InlineKeyboardButton(text='❌ Отменить', callback_data='cancel_send')],
        ])
        # show preview and confirmation to the sender (anonymous for others)
        if t == 'text':
            await message.answer(f'Вы уверены, что хотите отправить следующее сообщение?\n\n{content}', reply_markup=confirm_kb)
            log_msg(t, message.from_user, content)
        elif t == 'photo':
            preview_caption = f"Вы уверены, что хотите отправить это фото?\n\n{caption}" if caption else 'Вы уверены, что хотите отправить это фото?'
            await message.reply_photo(content, caption=preview_caption, reply_markup=confirm_kb)
            log_msg(t, message.from_user, f'file_id:{content} caption:{caption}')
        else:
            preview_caption = f"Вы уверены, что хотите отправить это видео?\n\n{caption}" if caption else 'Вы уверены, что хотите отправить это видео?'
            await message.reply_video(content, caption=preview_caption, reply_markup=confirm_kb)
            log_msg(t, message.from_user, f'file_id:{content} caption:{caption}')
        return


async def main():
    await load_data()
    asyncio.create_task(autosave_loop())
    print('Бот запущен')
    try:
        await dp.start_polling(bot)
    finally:
        await save_data()


if __name__ == '__main__':
    # start console watcher thread to allow typing 'exit' or 'quit' to stop
    loop = asyncio.new_event_loop()
    try:
        # run main in the current thread loop
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    t = threading.Thread(target=console_watcher, args=(loop,), daemon=True)
    t.start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # also handle Ctrl+C
        asyncio.run(shutdown())
