"""
Dual-language string table — Arabic (ar) / English (en).
All user-facing text goes here. Never hardcode strings in handlers.
"""

STRINGS = {
    # ── Language selection ────────────────────────────────────────────────────
    'lang_select': {
        'ar': 'مرحباً بك في نظام NATB\nاختر لغتك / Choose your language:',
        'en': 'Welcome to NATB System\nاختر لغتك / Choose your language:',
    },
    'btn_lang_ar': {'ar': '🇸🇦 العربية', 'en': '🇸🇦 Arabic'},
    'btn_lang_en': {'ar': '🇬🇧 English', 'en': '🇬🇧 English'},

    # ── Name collection ───────────────────────────────────────────────────────
    'ask_firstname': {
        'ar': '👤 أهلاً! أرسل *اسمك الأول* من فضلك:',
        'en': '👤 Hello! Please send your *First Name*:',
    },
    'ask_lastname': {
        'ar': '✅ تم. الآن أرسل *اسم العائلة*:',
        'en': '✅ Got it. Now send your *Last Name*:',
    },
    'welcome_name': {
        'ar': '🎉 مرحباً *{name}* في نظام NATB!',
        'en': '🎉 Welcome *{name}* to the NATB System!',
    },
    'ask_phone': {
        'ar': '📞 ممتاز! الآن أرسل *رقم الهاتف* للتواصل (بدون مسافات):',
        'en': '📞 Great! Now send your *Phone Number* (digits only, without spaces):',
    },
    'invalid_phone': {
        'ar': '❌ رقم هاتف غير صحيح. أدخل أرقاماً فقط بدون مسافات أو علامات.',
        'en': '❌ Invalid phone number. Enter digits only (no spaces or symbols).',
    },
    'invalid_name': {
        'ar': '❌ الاسم غير صحيح. يُسمح بالأحرف فقط (بدون أرقام).',
        'en': '❌ Invalid name. Letters only are allowed (no numbers).',
    },
    'onboard_phone_missing_restart': {
        'ar': '⚠️ رقم الهاتف غير مسجل. أرسل /start لإكمال الخطوات.',
        'en': '⚠️ Phone is not registered. Send /start to continue.',
    },

    # ── Plan (single plan) ─────────────────────────────────────────────────────
    'select_plan_title': {
        'ar': (
            '📦 *الاشتراك المتاح*\n\n'
            '🥇 *باقة المؤسسات — $100/شهر*\n'
            '• وصول كامل (Institutional)\n'
            '• صفقات غير محدودة\n'
            '• رافعة مالية حتى 10x (قابلة للتعديل)\n'
            '• قائمة مراقبة غير محدودة\n\n'
            'اضغط متابعة للانتقال إلى تفاصيل التحويل:'
        ),
        'en': (
            '📦 *Available Subscription*\n\n'
            '🥇 *Institutional Plan — $100/month*\n'
            '• Full institutional access\n'
            '• Unlimited trades\n'
            '• Leverage up to 10x (user-configurable)\n'
            '• Unlimited watchlist\n\n'
            'Press Continue to view banking details:'
        ),
    },
    'btn_continue': {'ar': '▶️ متابعة', 'en': '▶️ Continue'},

    # ── Bank details ──────────────────────────────────────────────────────────
    'bank_details_title': {
        'ar': '🏦 *تفاصيل الحساب البنكي*\n\nأرسل المبلغ إلى الحساب التالي:',
        'en': '🏦 *Banking Details*\n\nPlease transfer the amount to the following account:',
    },
    'bank_details_body': {
        'ar': (
            '🏛 البنك: *{bank_name}*\n'
            '🌿 الفرع: *{branch}*\n'
            '👤 اسم الحساب: *{account_name}*\n'
            '🔢 رقم الحساب: `{account_number}`\n'
            '🔢 IBAN: `{iban}`\n'
            '🌐 SWIFT: `{swift}`\n'
            '💵 العملة: *{currency}*\n\n'
            '📝 ملاحظة: {instructions}\n\n'
            'بعد إتمام التحويل، اضغط الزر أدناه.'
        ),
        'en': (
            '🏛 Bank: *{bank_name}*\n'
            '🌿 Branch: *{branch}*\n'
            '👤 Account Name: *{account_name}*\n'
            '🔢 Account Number: `{account_number}`\n'
            '🔢 IBAN: `{iban}`\n'
            '🌐 SWIFT: `{swift}`\n'
            '💵 Currency: *{currency}*\n\n'
            '📝 Note: {instructions}\n\n'
            'Once the transfer is complete, press the button below.'
        ),
    },
    'bank_loading': {
        'ar': '⏳ *جارٍ معالجة طلبك...* يرجى الانتظار لحظة.',
        'en': '⏳ *Processing request...* Please wait a moment.',
    },
    'btn_i_paid': {
        'ar': '✅ لقد قمت بالتحويل',
        'en': '✅ I Have Transferred',
    },

    # ── Payment proof ──────────────────────────────────────────────────────────
    'ask_payment_proof': {
        'ar': '📸 أرسل *صورة إيصال التحويل* (screenshot) من تطبيق البنك:',
        'en': '📸 Please upload a *screenshot* of your payment receipt from your banking app:',
    },
    'payment_received': {
        'ar': (
            '✅ *تم استلام إيصال الدفع!*\n\n'
            'سيتم مراجعة طلبك خلال 24 ساعة.\n'
            'سنرسل لك مفتاح الترخيص عبر تليجرام فور الموافقة.\n\n'
            'شكراً لانضمامك إلى NATB!'
        ),
        'en': (
            '✅ *Payment proof received!*\n\n'
            'Your application will be reviewed within 24 hours.\n'
            'We will send your license key via Telegram upon approval.\n\n'
            'Thank you for joining NATB!'
        ),
    },
    'payment_pending': {
        'ar': (
            '⏳ *طلبك قيد المراجعة*\n\n'
            'تم استلام إيصال الدفع الخاص بك وهو قيد المراجعة.\n'
            'سنرسل لك مفتاح الترخيص فور الموافقة.\n\n'
            'إذا كان لديك استفسار، تواصل مع الدعم.'
        ),
        'en': (
            '⏳ *Your application is under review*\n\n'
            'Your payment proof has been received and is being reviewed.\n'
            'We will send your license key upon approval.\n\n'
            'Contact support if you have any questions.'
        ),
    },
    'payment_approved_user': {
        'ar': (
            '🛡️ *أهلاً بك في نظام التداول المؤسساتي الذكي (NATB)!*\n\n'
            '✅ تم تفعيل حسابك بنجاح — *Institutional Premium ($100)*\n\n'
            '🔑 *مفتاح الترخيص:*\n`{license_key}`\n'
            '⏳ *الصلاحية:* *{days} يوم*\n\n'
            '*مزايا المؤسسات:*\n'
            '• 0.618 *Limit Orders* (تنفيذ هيكلي)\n'
            '• حد أدنى ذكاء صناعي *65%+* (AI Floor)\n'
            '• فلاتر *Market Structure* والسيولة (HTF)\n\n'
            'أرسل /start للمتابعة وربط حساب *Capital.com*.'
        ),
        'en': (
            '🛡️ *Welcome to the Institutional Trade System (NATB)!*\n\n'
            '✅ Your account is now active — *Institutional Premium ($100)*\n\n'
            '🔑 *License Key:*\n`{license_key}`\n'
            '⏳ *Validity:* *{days} days*\n\n'
            '*Institutional features:*\n'
            '• 0.618 *Limit Orders* (structure-first execution)\n'
            '• *65%+ AI Floor* (minimum confluence)\n'
            '• *Market Structure* & liquidity filters (HTF)\n\n'
            'Send /start to continue and connect your *Capital.com* account.'
        ),
    },
    'payment_rejected_user': {
        'ar': (
            '❌ *تم رفض طلب الدفع*\n\n'
            'للأسف لم يتم قبول إيصال الدفع المُرسل.\n'
            'يرجى التواصل مع الدعم لمزيد من التفاصيل.\n\n'
            'يمكنك إعادة المحاولة بإرسال /start.'
        ),
        'en': (
            '❌ *Payment rejected*\n\n'
            'Unfortunately your payment proof was not accepted.\n'
            'Please contact support for more details.\n\n'
            'You can try again by sending /start.'
        ),
    },

    # ── License entry ─────────────────────────────────────────────────────────
    'enter_license_prompt': {
        'ar': (
            '🔑 *تم تفعيل اشتراكك!*\n\n'
            'أرسل *مفتاح الترخيص* الذي وصلك عبر تليجرام:'
        ),
        'en': (
            '🔑 *Your subscription is activated!*\n\n'
            'Please enter the *License Key* you received via Telegram:'
        ),
    },

    # ── Broker connection ──────────────────────────────────────────────────────
    'broker_intro': {
        'ar': (
            '🔗 *ربط حساب Capital.com*\n\n'
            'سنقوم الآن بربط نظام NATB بحساب التداول الخاص بك.\n'
            'أرسل *البريد الإلكتروني* المسجّل في Capital.com:'
        ),
        'en': (
            '🔗 *Connect Capital.com Account*\n\n'
            'We will now link NATB to your trading account.\n'
            'Send your Capital.com *registered email*:'
        ),
    },
    'connected_success': {
        'ar': (
            '✅ *تم الربط بنجاح بنظام NATB!*\n'
            '*النظام يعمل الآن.*\n\n'
            '💰 الرصيد الابتدائي: `{balance} {currency}`'
        ),
        'en': (
            '✅ *Successfully connected to NATB Trading System.*\n'
            '*System is LIVE.*\n\n'
            '💰 Initial Account Balance: `{balance} {currency}`'
        ),
    },
    'connecting': {
        'ar': '🔗 *جارٍ الاتصال بـ Capital.com...* يرجى الانتظار.',
        'en': '🔗 *Connecting to Capital.com...* Please wait.',
    },
    'connected_error': {
        'ar': (
            '❌ *فشل الاتصال بـ Capital.com.*\n\n'
            'تأكد من صحة:\n'
            '• البريد الإلكتروني المسجّل\n'
            '• كلمة مرور API\n'
            '• مفتاح API\n\n'
            'يرجى إعادة الإدخال من البداية.'
        ),
        'en': (
            '❌ *Connection to Capital.com failed.*\n\n'
            'Please verify:\n'
            '• Your registered email\n'
            '• API Password\n'
            '• API Key\n\n'
            'Please re-enter your credentials.'
        ),
    },

    # ── System status ─────────────────────────────────────────────────────────
    'status_running': {
        'ar': (
            '✅ *النظام يعمل* | فارق التوقيت: *دبي +{offset} ساعات عن نيويورك*\n'
            '🤖 الروبوت يفحص الأسهم بنشاط...\n'
            '⏰ إغلاق السوق: *{close_gst}*'
        ),
        'en': (
            '✅ *System Running* | Offset: *Dubai is +{offset}h ahead of New York*\n'
            '🤖 Bot is actively scanning stocks...\n'
            '⏰ Market closes at: *{close_gst}*'
        ),
    },
    'status_no_opportunities': {
        'ar': '📊 *لا توجد فرص تداول متاحة في الوقت الحالي.*',
        'en': '📊 *No trading opportunities available at the moment.*',
    },
    'status_market_closed': {
        'ar': (
            '🔴 *السوق مغلق حالياً.* | فارق التوقيت: *دبي +{offset} ساعات*\n'
            '📅 الفتح القادم: *{next_gst}*'
        ),
        'en': (
            '🔴 *Market is currently closed.* | Offset: *Dubai is +{offset}h ahead of NY*\n'
            '📅 Next open: *{next_gst}*'
        ),
    },
    'status_premarket': {
        'ar': (
            '🟡 *جلسة ما قبل السوق.* | فارق التوقيت: *دبي +{offset} ساعات*\n'
            '📅 الجلسة الرسمية تفتح: *{open_gst}*'
        ),
        'en': (
            '🟡 *Pre-market session.* | Offset: *Dubai is +{offset}h ahead of NY*\n'
            '📅 Regular session opens at: *{open_gst}*'
        ),
    },
    'status_afterhours': {
        'ar': (
            '🟠 *جلسة ما بعد الإغلاق.* | فارق التوقيت: *دبي +{offset} ساعات*\n'
            '📅 الفتح القادم: *{next_gst}*'
        ),
        'en': (
            '🟠 *After-hours session.* | Offset: *Dubai is +{offset}h ahead of NY*\n'
            '📅 Next open: *{next_gst}*'
        ),
    },
    # Trading engine (main.py) — one-shot notice when regular session is closed
    'main_market_closed_notify': {
        'ar': (
            '*السوق مغلق* — يُفتح خلال ~{hours}س {minutes}د\n'
            'تبقى الصفقات المفتوحة قيد المراقبة النشطة.'
        ),
        'en': (
            '*Market Closed* — opens in ~{hours}h {minutes}m\n'
            'All open positions remain actively monitored.'
        ),
    },

    # ── Main menu ─────────────────────────────────────────────────────────────
    'main_menu_title': {
        'ar': '🎮 *لوحة تحكم NATB v2.0*\nمرحباً {name} — اختر من القائمة:',
        'en': '🎮 *NATB v2.0 Dashboard*\nWelcome {name} — choose an option:',
    },
    'btn_balance':         {'ar': '💰 رصيدي',            'en': '💰 My Balance'},
    'btn_report':          {'ar': '📊 تقرير الأداء',     'en': '📊 Performance Report'},
    'btn_manage_trades':   {'ar': '🧩 إدارة الصفقات',     'en': '🧩 Manage Trades'},
    'manage_menu_title': {
        'ar': '🧩 *إدارة الصفقات*\nاختر عملية:',
        'en': '🧩 *Manage Trades*\nChoose an action:',
    },
    'btn_show_open_trades': {
        'ar': '📋 عرض الصفقات المفتوحة',
        'en': '📋 View open trades',
    },
    'btn_close_all_trades': {
        'ar': '🛑 إغلاق جميع الصفقات المفتوحة',
        'en': '🛑 Close all open trades',
    },
    'btn_view_trade_by_id': {
        'ar': '🔎 عرض صفقة محددة',
        'en': '🔎 View specific trade',
    },
    'manage_no_open_trades': {
        'ar': '📭 لا توجد صفقات مفتوحة للإدارة.',
        'en': '📭 No open trades to manage.',
    },
    'manage_trade_prompt_id': {
        'ar': '✍️ اكتب *رقم الصفقة* (Trade ID) لعرضها. مثال: `1234`',
        'en': '✍️ Enter the *Trade ID* to view. Example: `1234`',
    },
    'manage_trade_not_found': {
        'ar': '❌ لم يتم العثور على صفقة بهذا الرقم.',
        'en': '❌ No trade found with that ID.',
    },
    'manage_nav': {
        'ar': '⬅️ السابق | ➡️ التالي',
        'en': '⬅️ Prev | ➡️ Next',
    },
    'btn_prev': {'ar': '⬅️ السابق', 'en': '⬅️ Prev'},
    'btn_next': {'ar': '➡️ التالي', 'en': '➡️ Next'},
    'manage_close_all_confirm': {
        'ar': '⚠️ *تأكيد*\nهل أنت متأكد من إغلاق *جميع الصفقات المفتوحة*؟',
        'en': '⚠️ *Confirm*\nAre you sure you want to close *all open trades*?',
    },
    'btn_confirm_yes': {'ar': '✅ نعم، أغلق الكل', 'en': '✅ Yes, close all'},
    'btn_confirm_no':  {'ar': '❎ إلغاء',          'en': '❎ Cancel'},
    'manage_close_all_result': {
        'ar': '✅ تم إرسال أوامر الإغلاق.\nتم الإغلاق: *{closed}* | فشل: *{failed}*',
        'en': '✅ Close requests sent.\nClosed: *{closed}* | Failed: *{failed}*',
    },
    'btn_mode_auto':       {'ar': '🤖 وضع آلي ✅',       'en': '🤖 Automated ✅'},
    'btn_mode_hybrid':     {'ar': '🤝 وضع هجين ✅',      'en': '🤝 Hybrid ✅'},
    'btn_switch_auto':     {'ar': '🔄 تفعيل الوضع الآلي','en': '🔄 Switch to Automated'},
    'btn_switch_hybrid':   {'ar': '🔄 تفعيل الوضع الهجين','en': '🔄 Switch to Hybrid'},
    'btn_engine_start':    {'ar': '▶ تشغيل التداول',      'en': '▶ Start Trading'},
    'btn_engine_stop':     {'ar': '⏹ إيقاف التداول',     'en': '⏹ Stop Trading'},
    'btn_day_halt': {
        'ar': '🛑 حجب التداول ليوم كامل',
        'en': '🛑 Block trading for the rest of today',
    },
    'btn_signal_profile': {
        'ar': '⚡ نوع الإشارات: {profile}',
        'en': '⚡ Signal Profile: {profile}',
    },
    'profile_fast': {'ar': 'سريع (FAST)', 'en': 'Fast (FAST)'},
    'profile_golden': {'ar': 'ذهبي (GOLDEN)', 'en': 'Golden (GOLDEN)'},
    'signal_profile_menu_title': {
        'ar': (
            '⚡ *اختيار نوع الإشارات*\n\n'
            'الملف الحالي: *{profile}*\n'
            '• FAST: إشارات أكثر وسرعة أعلى\n'
            '• GOLDEN: إشارات أقل لكن انتقائية أعلى'
        ),
        'en': (
            '⚡ *Choose Signal Profile*\n\n'
            'Current profile: *{profile}*\n'
            '• FAST: more frequent signals\n'
            '• GOLDEN: stricter, higher-selectivity signals'
        ),
    },
    'signal_profile_saved': {
        'ar': '✅ تم حفظ نوع الإشارات: *{profile}*',
        'en': '✅ Signal profile saved: *{profile}*',
    },
    'btn_resume_day_trading': {
        'ar': '▶ استئناف التداول (إلغاء الحجب)',
        'en': '▶ Resume trading (lift block)',
    },
    'btn_settings':        {'ar': '⚙️ الإعدادات',         'en': '⚙️ Settings'},

    # ── Trading engine status lines (shown in dashboard header) ───────────────
    'engine_status_off': {
        'ar': '🔴 *النظام متوقف* — اضغط ▶ لبدء التداول.',
        'en': '🔴 *System NOT Running* — press ▶ Start Trading to begin.',
    },
    'engine_status_on_open': {
        'ar': '🟢 *النظام يعمل* — جارٍ مسح السوق بحثاً عن فرص...',
        'en': '🟢 *System RUNNING* — scanning market for opportunities...',
    },
    'engine_status_on_closed': {
        'ar': '🔵 *النظام يعمل* — السوق مغلق حالياً. في انتظار الجلسة القادمة.',
        'en': '🔵 *System RUNNING* — market is closed. Waiting for next session.',
    },
    'engine_status_on_premarket': {
        'ar': '🟡 *النظام يعمل* — جلسة ما قبل السوق. تبدأ الجلسة الرسمية قريباً.',
        'en': '🟡 *System RUNNING* — pre-market session. Regular session starts soon.',
    },
    'engine_status_halted_day': {
        'ar': (
            '🔴 *النظام متوقف* — لا صفقات جديدة حتى افتتاح جلسة التداول القادمة.\n'
            'الصفقات المفتوحة تبقى نشطة حتى TP أو SL.'
        ),
        'en': (
            '🔴 *System HALTED* — no new trades until the next trading session opens.\n'
            'Open positions remain active until TP or SL.'
        ),
    },
    'engine_status_day_block': {
        'ar': (
            '🔴 *النظام متوقف* — *حجب التداول ليوم كامل* (تم إيقاف المحرك).\n'
            'لا صفقات جديدة حتى افتتاح جلسة التداول القادمة.\n'
            'لإلغاء الحجب: من *⚙️ الإعدادات* → ▶ استئناف التداول (إلغاء الحجب).'
        ),
        'en': (
            '🔴 *System HALTED* — *full-day block* active (engine stopped).\n'
            'No new trades until the next trading session opens.\n'
            'To lift the block: *⚙️ Settings* → ▶ Resume trading (lift block).'
        ),
    },
    'engine_status_maintenance': {
        'ar': '🛠 *وضع الصيانة مفعّل* — لا صفقات جديدة حالياً. المراقبة فقط.',
        'en': '🛠 *Maintenance mode active* — no new trades. Monitoring only.',
    },
    'maintenance_start_blocked': {
        'ar': 'وضع الصيانة مفعّل من الإدارة — لا يمكن تشغيل المحرك حالياً.',
        'en': 'Maintenance mode is active by admin — engine start is blocked.',
    },

    # ── Trading start/stop notifications ──────────────────────────────────────
    'trading_engine_started': {
        'ar': (
            '✅ *تم تشغيل محرك التداول.*\n\n'
            'النظام يعمل الآن ويراقب السوق بشكل نشط.\n'
            'ستصلك إشعارات فور اكتشاف فرصة تداول.'
        ),
        'en': (
            '✅ *Trading engine started.*\n\n'
            'The system is now live and actively monitoring the market.\n'
            'You will be notified when a trading opportunity is found.'
        ),
    },
    'trading_engine_stopped': {
        'ar': (
            '⏹ *تم إيقاف محرك التداول.*\n\n'
            'لن يتم فتح صفقات جديدة.\n'
            'الصفقات المفتوحة حالياً تبقى تحت المراقبة حتى إغلاقها.'
        ),
        'en': (
            '⏹ *Trading engine stopped.*\n\n'
            'No new trades will be opened.\n'
            'Any currently open positions remain monitored until closed.'
        ),
    },
    # Sent when maintenance is activated while market is OPEN (emergency)
    'admin_maintenance_on_msg': {
        'ar': (
            '🚨 *تنبيه النظام — صيانة طارئة*\n\n'
            'تم تفعيل وضع الصيانة الآن.\n'
            'تم إيقاف فتح صفقات جديدة مؤقتاً.\n'
            'الصفقات المفتوحة ستبقى تحت المراقبة بالكامل.'
        ),
        'en': (
            '🚨 *System Alert — Emergency Maintenance*\n\n'
            'Maintenance mode is now active.\n'
            'Opening new trades has been temporarily disabled.\n'
            'All open trades will remain fully monitored.'
        ),
    },
    # Sent when maintenance is activated while market is CLOSED (scheduled)
    'admin_maintenance_scheduled_msg': {
        'ar': (
            '🚨 *تنبيه النظام — صيانة مجدولة*\n\n'
            '⚠️ تم تفعيل وضع الصيانة الآن.\n'
            'شكراً لتحلّيكم بالصبر 🙏 نعمل على تحسين الأداء.'
        ),
        'en': (
            '🚨 *System Alert — Scheduled Maintenance*\n\n'
            '⚠️ Maintenance mode is now active.\n'
            'Thank you for your patience 🙏 We are working to improve performance.'
        ),
    },
    'admin_maintenance_off_msg': {
        'ar': (
            '✅ *استئناف التداول الطبيعي*\n\n'
            'اكتملت أعمال الصيانة.\n'
            'عاد النظام للعمل بشكل طبيعي.'
        ),
        'en': (
            '*Normal Trading Resumed*\n\n'
            'Maintenance is complete.\n'
            'The system is operating normally.'
        ),
    },
    'btn_lang':            {'ar': '🌐 English',           'en': '🌐 عربي'},
    'btn_back':            {'ar': '↩️ رجوع',             'en': '↩️ Back'},
    'btn_license':         {'ar': '🔑 الترخيص',          'en': '🔑 License'},

    # ── Registration (legacy keys kept for compatibility) ─────────────────────
    'reg_welcome': {
        'ar': '👋 أهلاً بك في NATB!\nأرسل *Identifier/Email* حسابك في Capital.com:',
        'en': '👋 Welcome to NATB!\nSend your Capital.com *Identifier/Email*:',
    },
    'reg_ask_pass': {
        'ar': '✅ تم.\nالآن أرسل *API Password* الذي أنشأته في Capital.com:',
        'en': '✅ Got it.\nNow send your Capital.com *API Password*:',
    },
    'reg_ask_key': {
        'ar': 'الآن أرسل *API Key* (16 حرفاً):',
        'en': 'Now send your *API Key* (16 characters):',
    },
    'reg_ask_license': {
        'ar': 'أخيراً، أرسل *مفتاح الترخيص* الخاص بك:',
        'en': 'Finally, send your *License Key*:',
    },
    'reg_success': {
        'ar': '🎉 تم الربط بنجاح! جاري فتح لوحة التحكم...',
        'en': '🎉 Connected successfully! Opening dashboard...',
    },
    'reg_invalid_license': {
        'ar': '❌ مفتاح الترخيص غير صحيح أو منتهي الصلاحية.\nتواصل مع الدعم.',
        'en': '❌ Invalid or expired license key.\nContact support.',
    },
    'approved_missing_license': {
        'ar': (
            '⚠️ *تمت الموافقة على الدفع* لكن تعذّر ربط مفتاح الترخيص تلقائياً في النظام.\n\n'
            'أرسل *مفتاح الترخيص* الذي وصلك من الإدارة.\n'
            'إذا لم يصلك مفتاح، تواصل مع الدعم.\n\n'
            'الصق المفتاح في الرسالة التالية:'
        ),
        'en': (
            '⚠️ *Payment was approved* but your license key could not be linked automatically.\n\n'
            'Send the *license key* you received from admin.\n'
            'If you did not receive a key, contact support.\n\n'
            'Paste your key in your next message:'
        ),
    },
    'license_linked_resume': {
        'ar': '✅ *تم ربط الترخيص.* استئناف لوحة التحكم…',
        'en': '✅ *License linked.* Resuming your dashboard…',
    },

    # ── Balance ───────────────────────────────────────────────────────────────
    'balance_loading': {
        'ar': '🔍 جاري جلب الرصيد...',
        'en': '🔍 Fetching balance...',
    },
    'balance_result': {
        'ar': '💰 *الرصيد اللحظي (Equity):* `{amount} {currency}`',
        'en': '💰 *Live Equity:* `{amount} {currency}`',
    },
    'balance_error': {
        'ar': '❌ تعذّر جلب الرصيد. تحقق من بياناتك.',
        'en': '❌ Could not fetch balance. Check your credentials.',
    },

    # ── Performance report ────────────────────────────────────────────────────
    'report_menu_title': {
        'ar': '📊 *تقرير الأداء*\nاختر نوع التقرير:',
        'en': '📊 *Performance Report*\nChoose report type:',
    },
    'report_loading': {
        'ar': '⏳ الرجاء الانتظار جاري العمل على تجهيز التقرير ..',
        'en': '⏳ Please wait, we are preparing your report...',
    },
    'btn_report_daily': {
        'ar': '📅 تقرير يوم محدد',
        'en': '📅 Extract Daily Report',
    },
    'btn_report_csv': {
        'ar': '📁 تصدير قاعدة البيانات (CSV)',
        'en': '📁 Export Full Database (CSV)',
    },
    'report_ask_date': {
        'ar': 'أدخل التاريخ بالصيغة YYYY-MM-DD\nمثال: 2025-03-24',
        'en': 'Enter the date in format YYYY-MM-DD\nExample: 2025-03-24',
    },
    'report_ask_start_date': {
        'ar': 'أدخل *تاريخ البداية* بالصيغة YYYY-MM-DD\nمثال: 2025-01-01',
        'en': 'Enter the *start date* in format YYYY-MM-DD\nExample: 2025-01-01',
    },
    'report_ask_end_date': {
        'ar': 'أدخل *تاريخ النهاية* بالصيغة YYYY-MM-DD\nمثال: 2025-03-24',
        'en': 'Enter the *end date* in format YYYY-MM-DD\nExample: 2025-03-24',
    },
    'report_invalid_date': {
        'ar': 'صيغة التاريخ غير صحيحة. يرجى استخدام YYYY-MM-DD',
        'en': 'Invalid date format. Please use YYYY-MM-DD',
    },
    'report_csv_empty': {
        'ar': '📭 لا توجد صفقات في هذا النطاق الزمني.',
        'en': '📭 No trades found in that date range.',
    },
    'report_csv_ready': {
        'ar': '✅ تم تجهيز التقرير وإرساله.',
        'en': '✅ The report is ready and has been sent.',
    },
    'report_title': {
        'ar': '📊 *تقرير الأداء*\n{date}',
        'en': '📊 *Performance Report*\n{date}',
    },
    'report_body': {
        'ar': (
            '📈 إجمالي الصفقات: *{total}*\n'
            '✅ رابحة: *{wins}* ({win_rate}%)\n'
            '❌ خاسرة: *{losses}* ({loss_rate}%)\n'
            '💵 صافي الربح/الخسارة: *${net_pnl}*\n'
            '🏆 أفضل صفقة: *${best}*\n'
            '📉 أسوأ صفقة: *${worst}*'
        ),
        'en': (
            '📈 Total Trades: *{total}*\n'
            '✅ Wins: *{wins}* ({win_rate}%)\n'
            '❌ Losses: *{losses}* ({loss_rate}%)\n'
            '💵 Net P&L: *${net_pnl}*\n'
            '🏆 Best Trade: *${best}*\n'
            '📉 Worst Trade: *${worst}*'
        ),
    },
    'report_empty': {
        'ar': '📭 لا توجد صفقات مكتملة بعد.',
        'en': '📭 No completed trades yet.',
    },

    # ── Open positions ────────────────────────────────────────────────────────
    'positions_title': {
        'ar': '📋 *الصفقات المفتوحة*',
        'en': '📋 *Open Positions*',
    },
    'position_row': {
        'ar': '{symbol} {dir} | UPL: ${upl} | وقف: {stop}',
        'en': '{symbol} {dir} | UPL: ${upl} | Stop: {stop}',
    },
    'no_positions': {
        'ar': '📭 لا توجد صفقات مفتوحة.',
        'en': '📭 No open positions.',
    },
    'btn_close_pos': {
        'ar': '🔒 إغلاق {symbol}',
        'en': '🔒 Close {symbol}',
    },
    'close_success': {
        'ar': '✅ تم إغلاق صفقة {symbol} بنجاح.',
        'en': '✅ {symbol} position closed successfully.',
    },
    'close_fail': {
        'ar': '❌ فشل إغلاق {symbol}.',
        'en': '❌ Failed to close {symbol}.',
    },

    # ── Mode ──────────────────────────────────────────────────────────────────
    'mode_switched_auto': {
        'ar': '🤖 تم التبديل إلى *الوضع الآلي*.\nسيُنفذ البوت الصفقات تلقائياً.',
        'en': '🤖 Switched to *Automated Mode*.\nTrades will execute automatically.',
    },
    'mode_switched_hybrid': {
        'ar': '🤝 تم التبديل إلى *الوضع الهجين*.\nستصلك إشعارات للموافقة قبل كل صفقة.',
        'en': '🤝 Switched to *Hybrid Mode*.\nYou will approve each trade before execution.',
    },

    # ── Hybrid signal prompt ──────────────────────────────────────────────────
    'signal_prompt': {
        'ar': (
            '⚡ *إشارة جديدة — موافقة مطلوبة*\n\n'
            'الأداة: *{symbol}*\n'
            'الاتجاه: *{action}*\n'
            'الثقة: *{confidence}%*\n'
            'السبب: {reason}\n\n'
            'اختر خلال 5 دقائق:'
        ),
        'en': (
            '⚡ *New Signal — Approval Required*\n\n'
            'Instrument: *{symbol}*\n'
            'Direction: *{action}*\n'
            'Confidence: *{confidence}%*\n'
            'Reason: {reason}\n\n'
            'Choose within 5 minutes:'
        ),
    },
    'btn_approve':   {'ar': '✅ موافقة', 'en': '✅ Approve'},
    'btn_reject':    {'ar': '❌ رفض',    'en': '❌ Reject'},
    'signal_approved': {
        'ar': '✅ تمت الموافقة — جاري تنفيذ الصفقة...',
        'en': '✅ Approved — executing trade...',
    },
    'signal_rejected': {
        'ar': '🚫 تم رفض الإشارة.',
        'en': '🚫 Signal rejected.',
    },
    'signal_expired': {
        'ar': '⏰ انتهت مدة الإشارة. تم الإلغاء تلقائياً.',
        'en': '⏰ Signal expired and was auto-cancelled.',
    },

    # ── Pre-market alert ──────────────────────────────────────────────────────
    'premarket_alert': {
        'ar': (
            '🔔 *تنبيه — السوق يفتح خلال 30 دقيقة*\n\n'
            '📅 ناسداك | 9:30 AM ET\n'
            '📋 قائمة اليوم: *{watchlist_count}* سهم جاهزة للمراقبة\n'
            '🤖 الوضع الحالي: *{mode}*\n\n'
            'استعد للجلسة!'
        ),
        'en': (
            '🔔 *Pre-Market Alert — Opens in 30 Minutes*\n\n'
            '📅 Nasdaq | 9:30 AM ET\n'
            '📋 Today\'s watchlist: *{watchlist_count}* stocks ready\n'
            '🤖 Current mode: *{mode}*\n\n'
            'Get ready for the session!'
        ),
    },

    # ── License ───────────────────────────────────────────────────────────────
    'license_active': {
        'ar': '🔑 *الترخيص نشط*\nينتهي في: {expiry}\nالأيام المتبقية: {days}',
        'en': '🔑 *License Active*\nExpires: {expiry}\nDays remaining: {days}',
    },
    'license_expired': {
        'ar': (
            '❌ *انتهت صلاحية ترخيصك.*\n\n'
            'لتجديد اشتراكك، يرجى دفع رسوم الاشتراك مجدداً.\n'
            'سيتم إعادة تفعيل حسابك فور مراجعة الدفعة.'
        ),
        'en': (
            '❌ *Your license has expired.*\n\n'
            'To renew your subscription, please pay the subscription fee again.\n'
            'Your account will be reactivated once payment is reviewed.'
        ),
    },
    'processing': {
        'ar': '⏳ *جارٍ المعالجة...* يرجى الانتظار.',
        'en': '⏳ *Processing...* Please wait.',
    },
    'license_none': {
        'ar': '⚠️ لا يوجد ترخيص مرتبط بحسابك.',
        'en': '⚠️ No license associated with your account.',
    },

    # ── Technical support ─────────────────────────────────────────────────────
    'btn_support': {
        'ar': '🎧 الدعم الفني',
        'en': '🎧 Technical Support',
    },
    'btn_contact_support': {
        'ar': '📞 تواصل مع الدعم',
        'en': '📞 Contact Support',
    },
    'btn_support_resolved': {
        'ar': '✅ تم حل المشكلة',
        'en': '✅ Issue Resolved',
    },
    'support_greeting': {
        'ar': (
            '🎧 *مرحباً بك في دعم NATB*\n\n'
            'كيف يمكنني مساعدتك اليوم؟\n'
            'يرجى وصف المشكلة التي تواجهها.\n\n'
            '_ستُغلق هذه الجلسة تلقائياً بعد 3 دقائق من عدم الرد._'
        ),
        'en': (
            '🎧 *Welcome to NATB Support*\n\n'
            'How can I assist you today?\n'
            'Please describe the issue you are experiencing.\n\n'
            '_This session will auto-close after 3 minutes of inactivity._'
        ),
    },
    'support_resolved_msg': {
        'ar': '✅ *تم إغلاق جلسة الدعم.* نتمنى أن تكون المشكلة قد حُلّت. شكراً!',
        'en': '✅ *Support session closed.* We hope your issue was resolved. Thank you!',
    },
    'support_timeout': {
        'ar': (
            '⏱ *تم إغلاق جلسة الدعم تلقائياً* بعد 3 دقائق من عدم الرد.\n'
            'إذا لا تزال بحاجة للمساعدة، افتح جلسة جديدة من لوحة التحكم.'
        ),
        'en': (
            '⏱ *Support session auto-closed* after 3 minutes of inactivity.\n'
            'If you still need help, open a new session from the dashboard.'
        ),
    },

    # ── Settings ──────────────────────────────────────────────────────────────
    'settings_reset': {
        'ar': '⚙️ تم مسح بيانات الوسيط. أرسل /start لإعادة الإدخال.',
        'en': '⚙️ Broker credentials cleared. Send /start to re-enter them.',
    },
    'settings_menu_intro': {
        'ar': (
            '⚙️ *الإعدادات*\n\n'
            'من هنا يمكنك *حجب التداول ليوم كامل* (بدون إغلاق الصفقات المفتوحة) أو إلغاء الحجب.\n'
            'تشغيل/إيقاف المحرك من لوحة التحكم الرئيسية هو إيقاف مؤقت فقط.\n\n'
            'اختر أحد الخيارات أدناه. لن يُمس الترخيص أو الاشتراك إلا إذا اخترت ذلك صراحة.'
        ),
        'en': (
            '⚙️ *Settings*\n\n'
            'Use *Block trading for the rest of today* here (open positions stay active) or lift that block.\n'
            'Start/Stop on the main dashboard is a temporary pause only.\n\n'
            'Choose an option below. Your license is not changed unless you explicitly cancel or update broker data.'
        ),
    },
    'btn_cancel_subscription': {
        'ar': '🚫 إلغاء الاشتراك',
        'en': '🚫 Cancel subscription',
    },
    'btn_leverage_settings': {
        'ar': '📊 الرافعة المالية',
        'en': '📊 Leverage',
    },
    'btn_update_broker': {
        'ar': '🔑 تحديث بيانات Capital',
        'en': '🔑 Update Capital credentials',
    },
    'cancel_sub_warning': {
        'ar': (
            '⚠️ *إلغاء الاشتراك — تأكيد مطلوب*\n\n'
            '• لن يُسترد مبلغ الاشتراك.\n'
            '• عند إعادة الاشتراك لاحقاً ستدفع الرسوم *من جديد* بالكامل.\n'
            '• سيتم إيقاف التداول الآلي وإلغاء الترخيص الحالي.\n\n'
            'هل أنت متأكد؟'
        ),
        'en': (
            '⚠️ *Cancel subscription — confirmation required*\n\n'
            '• Your subscription fee is *not* refunded.\n'
            '• If you subscribe again later, you must pay the full fee again.\n'
            '• Automated trading will stop and your current license will be revoked.\n\n'
            'Are you sure?'
        ),
    },
    'btn_cancel_confirm': {
        'ar': '✅ نعم، إلغاء الاشتراك',
        'en': '✅ Yes, cancel subscription',
    },
    'cancel_sub_not_available': {
        'ar': 'غير متاح: نافذة الإلغاء خلال 30 يوماً من بداية الاشتراك فقط.',
        'en': 'Not available: cancellation is only within 30 days of subscription start.',
    },
    'cancel_sub_done': {
        'ar': (
            '🚫 *تم إلغاء اشتراكك.*\n'
            'لم يعد الترخيص نشطاً. لإعادة التفعيل ادفع من جديد عبر /start.'
        ),
        'en': (
            '🚫 *Your subscription was cancelled.*\n'
            'Your license is no longer active. To reactivate, pay again via /start.'
        ),
    },
    'leverage_settings_body': {
        'ar': (
            '📊 *الرافعة المالية*\n\n'
            'حد الباقة: *{cap}x*\n'
            'اختيارك الحالي: *{current}x*\n\n'
            'اختر المستوى المناسب (ضمن حد الباقة):'
        ),
        'en': (
            '📊 *Leverage*\n\n'
            'Plan cap: *{cap}x*\n'
            'Your selection: *{current}x*\n\n'
            'Choose a level (within your plan cap):'
        ),
    },
    'leverage_disclaimer': {
        'ar': (
            '⚠️ *تحذير:* التداول بالرافعة المالية يزيد المخاطر وقد يؤدي إلى خسائر تتجاوز رأس المال المستثمر. '
            '*أنت وحدك* تتحمل المسؤولية الكاملة؛ النظام لا يضمن النتائج ولا يتحمل خسائر التداول.'
        ),
        'en': (
            '⚠️ *Warning:* Leveraged trading increases risk and can cause losses exceeding your invested capital. '
            '*You alone* bear full responsibility; the system does not guarantee results or cover trading losses.'
        ),
    },
    'leverage_saved': {
        'ar': '✅ تم حفظ الرافعة: *{leverage}x* (حد الباقة {cap}x).',
        'en': '✅ Leverage saved: *{leverage}x* (plan cap {cap}x).',
    },
    'leverage_invalid': {
        'ar': 'قيمة غير مسموحة لباقتك.',
        'en': 'Not allowed for your plan.',
    },
    'settings_broker_warn_text': {
        'ar': (
            '🔑 *تحديث بيانات Capital.com*\n\n'
            'سيتم *مسح* البريد وكلمة مرور الـ API والمفتاح المخزّنين لدينا لتتمكن من إدخال بيانات جديدة عبر /start.\n\n'
            'هل تريد المتابعة؟'
        ),
        'en': (
            '🔑 *Update Capital.com credentials*\n\n'
            'We will *clear* the stored email, API password, and API key so you can enter new ones via /start.\n\n'
            'Continue?'
        ),
    },
    'btn_broker_clear_confirm': {
        'ar': '✅ نعم، امسح البيانات',
        'en': '✅ Yes, clear credentials',
    },
    'settings_broker_cleared': {
        'ar': '🔑 تم مسح بيانات الوسيط. أرسل /start ثم أدخل مفتاح الترخيص وبيانات Capital من جديد.',
        'en': '🔑 Broker credentials cleared. Send /start, then enter your license key and Capital details again.',
    },

    # ── Stop trading ──────────────────────────────────────────────────────────
    'day_halt_applied': {
        'ar': (
            '🛑 *تم تفعيل حجب التداول ليوم كامل*\n\n'
            '• تم إيقاف محرك التداول.\n'
            '• لن تُفتح صفقات جديدة حتى افتتاح جلسة التداول القادمة.\n'
            '• الصفقات المفتوحة تبقى نشطة حتى TP أو SL.\n\n'
            'لإلغاء الحجب لاحقاً: *⚙️ الإعدادات* → ▶ استئناف التداول (إلغاء الحجب).'
        ),
        'en': (
            '🛑 *Full-day trading block enabled*\n\n'
            '• Trading engine stopped.\n'
            '• No new trades until the next trading session opens.\n'
            '• Open positions remain active until TP or SL.\n\n'
            'To lift the block later: *⚙️ Settings* → ▶ Resume trading (lift block).'
        ),
    },
    'day_halt_resumed': {
        'ar': (
            '✅ *تم إلغاء الحجب وتشغيل محرك التداول.*\n\n'
            'عاد النظام للوضع الطبيعي ويراقب السوق من جديد.'
        ),
        'en': (
            '✅ *Block lifted — trading engine started.*\n\n'
            'The system is back to normal and monitoring the market again.'
        ),
    },
    'resume_day_noop': {
        'ar': 'لا يوجد حجب يوم كامل نشط حالياً.',
        'en': 'No full-day block is active.',
    },
    'use_resume_in_settings': {
        'ar': 'حجب التداول ليوم كامل مفعّل — ألغِ الحجب من ⚙️ الإعدادات (استئناف التداول).',
        'en': 'Full-day block is on — lift it from ⚙️ Settings (Resume trading).',
    },

    # ── Trade notifications ───────────────────────────────────────────────────
    'trade_opened': {
        'ar': (
            '{sq_color} *صفقة جديدة #{index} — {symbol}*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '▶️  الاتجاه        :  *{action}*\n'
            '💰  سعر الدخول    :  *{entry}*\n'
            '🔢  الكمية         :  *{size} سهم*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '🔴  وقف الخسارة   :  *{stop}*\n'
            '🎯  الهدف 1        :  *{target1}*\n'
            '🏆  الهدف 2        :  *{target2}*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '⚠️  المخاطرة       :  *{risk}*'
        ),
        'en': (
            '{sq_color} *New Trade #{index} — {symbol}*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '▶️  Direction    :  *{action}*\n'
            '💰  Entry Price  :  *{entry}*\n'
            '🔢  Quantity     :  *{size} shares*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '🔴  Stop Loss    :  *{stop}*\n'
            '🎯  Target 1     :  *{target1}*\n'
            '🏆  Target 2     :  *{target2}*\n'
            '━━━━━━━━━━━━━━━━━━━━\n'
            '⚠️  Risk Amount  :  *{risk}*'
        ),
    },
    'trade_closed_sl': {
        'ar': (
            '🔔 *إغلاق آلي — {symbol}*\n'
            'الاتجاه: {direction}\n'
            'السبب: {reason}\n'
            'الربح/الخسارة: ${pnl}'
        ),
        'en': (
            '🔔 *Auto-Close — {symbol}*\n'
            'Direction: {direction}\n'
            'Reason: {reason}\n'
            'P&L: ${pnl}'
        ),
    },

    # ── Admin notifications (English only) ───────────────────────────────────
    'admin_payment_caption': {
        'ar': (
            '💰 *طلب اشتراك جديد*\n\n'
            '👤 الاسم: *{full_name}*\n'
            '📦 الخطة: *اشتراك المؤسسات — $100/شهر*\n'
            '🪪 Chat ID: `{chat_id}`\n\n'
            'وافق أو ارفض الطلب:'
        ),
        'en': (
            '💰 *New Subscription Request*\n\n'
            '👤 Name: *{full_name}*\n'
            '📦 Plan: *Institutional — $100/month*\n'
            '🪪 Chat ID: `{chat_id}`\n\n'
            'Approve or reject the payment:'
        ),
    },
    'admin_btn_approve': {'ar': '✅ موافقة', 'en': '✅ Approve'},
    'admin_btn_reject':  {'ar': '❌ رفض',    'en': '❌ Reject'},
    'admin_approved_ack': {
        'ar': '✅ تمت الموافقة — تم إرسال مفتاح الترخيص إلى المشترك.',
        'en': '✅ Approved — License key sent to subscriber.',
    },
    'admin_rejected_ack': {
        'ar': '❌ تم الرفض — تم إخطار المشترك.',
        'en': '❌ Rejected — Subscriber has been notified.',
    },
}


def t(key: str, lang: str = 'ar', **kwargs) -> str:
    """
    Return the translated string for `key` in `lang`.
    Supports .format(**kwargs) placeholders.
    Falls back to Arabic, then to the key itself.
    """
    entry = STRINGS.get(key, {})
    text  = entry.get(lang) or entry.get('ar') or key
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text
