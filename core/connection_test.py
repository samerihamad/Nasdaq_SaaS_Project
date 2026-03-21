import os
import requests
from dotenv import load_dotenv

# تحميل البيانات من ملف .env
load_dotenv()

def test_capital_connection():
    # الرابط الخاص بـ API Capital.com (التجريبي)
    url = "https://demo-api-capital.backend-capital.com/api/v1/session"
    
    headers = {
        "X-CAP-API-KEY": os.getenv("CAPITAL_API_KEY"),
        "Content-Type": "application/json"
    }
    
    payload = {
        "identifier": os.getenv("CAPITAL_IDENTIFIER"),
        "password": os.getenv("CAPITAL_PASSWORD")
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            print("✅ تم الاتصال بنجاح بـ Capital.com!")
            # حفظ الـ Token للجلسة (Session)
            security_token = response.headers.get("X-SECURITY-TOKEN")
            print(f"Session Token: {security_token[:10]}...")
        else:
            print(f"❌ فشل الاتصال. كود الخطأ: {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"⚠️ حدث خطأ غير متوقع: {e}")

if __name__ == "__main__":
    test_capital_connection()