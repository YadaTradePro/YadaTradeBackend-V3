نحوه راه اندازی و استفاده از ربات ها

محیط مجازی یک پوشه ایزوله شده (جدا شده) در داخل پروژه شما است که حاوی یک نسخه از Python و تمام کتابخانه‌هایی است که شما به طور خاص برای آن پروژه نصب کرده‌اید.:

مطمئن شوید که Python 3.11 در سیستم جدید نصب شده باشد  (If Python is not installed, download and install Python
. Be sure to check the box that says "Add Python to PATH" during installation.)

* در ترمینال سیستم جدید، به پوشه پروژه بروید
cd "E:\BourseAnalysis\V-3\Backend-V3"

* سپس، محیط مجازی را مجدداً ایجاد کنید:
# ایجاد محیط مجازی
python -m venv venv


* فعال‌سازی و نصب وابستگی‌ها:
# فعال‌سازی محیط مجازی
cd "E:\BourseAnalysis\V-3\Backend-V3"
.\venv\Scripts\activate
set FLASK_APP=main.py




# نصب وابستگی‌ها
pip install -r requirements.txt

pip install pytse_client-0.19.1-py3-none-any.whl



