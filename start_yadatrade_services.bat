@echo off
TITLE YadaTrade Master Starter

:: --- تنظیم مسیرها ---
SET PYTHONUTF8=1
SET PROJECT_ROOT=D:\Mahdi\New Backend\V-3\Backend-V3
SET VENV_ACTIVATE="%PROJECT_ROOT%\venv_311\Scripts\activate.bat"
SET MASTER_SCRIPT="%PROJECT_ROOT%\run_all_services.py"

:: --- تغییر دایرکتوری ---
CD /D "%PROJECT_ROOT%"

:: --- بررسی وجود محیط مجازی ---
IF NOT EXIST %VENV_ACTIVATE% (
    ECHO.
    ECHO ❌ خطا: فایل فعال سازی محیط مجازی یافت نشد.
    PAUSE
    EXIT /B 1
)

:: --- فعال سازی محیط مجازی ---
ECHO ⚙️ در حال فعال سازی محیط مجازی...
CALL %VENV_ACTIVATE%
ECHO ✅ محیط مجازی فعال شد.

ECHO.
ECHO 🚀 در حال اجرای Master Service Manager...
ECHO -----------------------------------------------------------------

:: 💡 اجرای اسکریپت در پنجره جدید و با استفاده از دستور START
:: این دستور از Python.exe داخل venv فعال شده استفاده می کند.
START "YadaTrade Master Services" cmd /k "python %MASTER_SCRIPT% start"

:: --- پایان ---
ECHO.
ECHO ✅ تمام سرویس ها در یک پنجره جدید آغاز به کار کردند.
ECHO 💡 برای خاموش کردن، از stop_yadatrade_services.bat استفاده کنید.
ECHO این پنجره ظرف 3 ثانیه بسته خواهد شد.
TIMEOUT /T 3 /NOBREAK >NUL
EXIT /B 0