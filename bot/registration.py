import os
import sqlite3
import requests
import asyncio
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes, ConversationHandler
)
from core.risk_manager import apply_manual_override, apply_stop_today

load_dotenv()

# مراحل المحادثة
GET_CAPITAL_ID, GET_CAPITAL_PASS, GET_API_KEY = range(3)

# --- 1. الدوال المساعدة ---
def get_user_from_db(chat_id):
    conn = sqlite3.connect('database/trading_saas.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS subscribers
                 (chat_id TEXT PRIMARY KEY, email TEXT, api_password TEXT, api_key TEXT, is_demo INTEGER DEFAULT 1, is_active INTEGER DEFAULT 1)''')
    c.execute("SELECT email, api_password, api_key FROM subscribers WHERE chat_id=?", (str(chat_id),))
    user = c.fetchone()
    conn.close()
    return user

def fetch_capital_balance(cap_id, cap_pass, api_key):
    base_url = "https://demo-api-capital.backend-capital.com/api/v1"
    headers = {"X-CAP-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"identifier": cap_id, "password": cap_pass}
    
    try:
        # 1. طلب الجلسة
        session = requests.post(f"{base_url}/session", json=payload, headers=headers)
        if session.status_code != 200:
            return f"خطأ {session.status_code}: بيانات الدخول غير صحيحة"

        cst = session.headers.get("CST")
        x_sec = session.headers.get("X-SECURITY-TOKEN")
        
        # 2. جلب الحسابات
        acc_headers = {"X-CAP-API-KEY": api_key, "CST": cst, "X-SECURITY-TOKEN": x_sec, "Content-Type": "application/json"}
        acc_res = requests.get(f"{base_url}/accounts", headers=acc_headers)
        data = acc_res.json()
        
        # طباعة للتصحيح (ستظهر في Terminal عندك لنعرف الهيكل)
        # print(f"DEBUG: {data}") 

        # الطريقة الأكثر أماناً لاستخراج الرصيد
        if 'accounts' in data and len(data['accounts']) > 0:
            account_info = data['accounts'][0]
            # الوصول للرصيد من داخل هيكل balance
            balance_data = account_info.get('balance', {})
            amount = balance_data.get('balance', '0.0')
            curr = balance_data.get('currency', 'USD')
            return f"{amount} {curr}"
        else:
            return "❌ لم يتم العثور على حسابات نشطة"
            
    except Exception as e:
        return f"عطل في قراءة البيانات: {str(e)}"

# --- 2. واجهة المستخدم ---
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("💰 رصيدي الحالي", callback_data='check_balance')],
        [InlineKeyboardButton("🛑 إيقاف التداول", callback_data='stop_bot'), 
         InlineKeyboardButton("⚙️ الإعدادات", callback_data='settings')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "🎮 **لوحة تحكم NATB**\nاختر من القائمة أدناه:"
    
    if update.message:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# --- 3. معالجات الأحداث ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = get_user_from_db(update.message.chat_id)
    if user:
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    await update.message.reply_text("👋 أهلاً بك! أرسل (Identifier/Email) حسابك في Capital:")
    return GET_CAPITAL_ID

async def handle_registration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # هذا الجزء يغطي مراحل التسجيل الثلاثة
    text = update.message.text
    state = context.user_data.get('step', GET_CAPITAL_ID)

    if state == GET_CAPITAL_ID:
        context.user_data['cap_id'] = text
        context.user_data['step'] = GET_CAPITAL_PASS
        await update.message.reply_text("تم. الآن أرسل (API Password) الذي أنشأته:")
        return GET_CAPITAL_PASS
    
    elif state == GET_CAPITAL_PASS:
        context.user_data['cap_pass'] = text
        context.user_data['step'] = GET_API_KEY
        await update.message.reply_text("أخيراً، أرسل (API Key) المكون من 16 حرفاً:")
        return GET_API_KEY
    
    elif state == GET_API_KEY:
        chat_id = str(update.message.chat_id)
        # حفظ البيانات
        conn = sqlite3.connect('database/trading_saas.db')
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO subscribers (chat_id, email, api_password, api_key) VALUES (?, ?, ?, ?)",
                  (chat_id, context.user_data['cap_id'], context.user_data['cap_pass'], text))
        conn.commit()
        conn.close()
        await update.message.reply_text("✅ تم الربط بنجاح!")
        await show_main_menu(update, context)
        return ConversationHandler.END

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'check_balance':
        await query.edit_message_text("🔍 جاري فحص الرصيد...")
        user = get_user_from_db(query.message.chat_id)
        balance = fetch_capital_balance(user[0], user[1], user[2])
        await query.edit_message_text(f"💰 رصيدك الحالي: **{balance}**", parse_mode='Markdown')
        await asyncio.sleep(2)
        await show_main_menu(update, context)
        
    elif query.data == 'settings':
        # مسح بيانات الوسيط فقط — لا يُحذف صف المشترك ولا الترخيص
        conn = sqlite3.connect('database/trading_saas.db')
        c = conn.cursor()
        c.execute(
            "UPDATE subscribers SET email=NULL, api_password=NULL, api_key=NULL "
            "WHERE chat_id=?",
            (str(query.message.chat_id),),
        )
        conn.commit()
        conn.close()
        await query.edit_message_text(
            "⚙️ تم مسح بيانات Capital. أرسل /start لإعادة إدخال المفتاح والبيانات."
        )

# --- 4. Circuit Breaker command handlers ---
async def override_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    success, msg = apply_manual_override(chat_id)
    await update.message.reply_text(msg, parse_mode='Markdown')


async def stop_today_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.message.chat_id)
    apply_stop_today(chat_id)
    await update.message.reply_text(
        "🛑 تم الإيقاف حتى الغد.", parse_mode='Markdown'
    )


# --- 5. التشغيل ---
if __name__ == "__main__":
    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            GET_CAPITAL_ID:   [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_registration)],
            GET_CAPITAL_PASS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_registration)],
            GET_API_KEY:      [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_registration)],
        },
        fallbacks=[CommandHandler('start', start)]
    )

    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(CommandHandler('override',   override_handler))
    app.add_handler(CommandHandler('stop_today', stop_today_handler))

    print("🚀 البوت جاهز ومستعد للإقلاع...")
    app.run_polling()