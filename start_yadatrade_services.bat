@echo off
TITLE YadaTrade Master Starter
CHCP 65001 > NUL

:: --- تنظیم مسیرها ---
SET PYTHONUTF8=1
SET PROJECT_ROOT=D:\Mahdi\New Backend\V-3\Backend-V3
SET VENV_ACTIVATE="%PROJECT_ROOT%\venv_311\Scripts\activate.bat"
SET MASTER_SCRIPT="%PROJECT_ROOT%\run_all_services.py"

:: -----------------------------------------------------------------
:: 💡 کلید حل: استفاده از pythonw.exe (بدون پنجره)
:: -----------------------------------------------------------------
SET PYTHON_EXE="%PROJECT_ROOT%\venv_311\Scripts\pythonw.exe"

:: --- تغییر دایرکتوری ---
CD /D "%PROJECT_ROOT%"

:: --- بررسی وجود محیط مجازی ---
IF NOT EXIST %VENV_ACTIVATE% (
    ECHO.
    ECHO ❌ خطا: فایل فعال سازی محیط مجازی یافت نشد.
    PAUSE
    EXIT /B 1
)
IF NOT EXIST %PYTHON_EXE% (
    ECHO.
    ECHO ❌ خطا: فایل pythonw.exe در محیط مجازی یافت نشد.
    ECHO %PYTHON_EXE%
    PAUSE
    EXIT /B 1
)

:: --- فعال سازی محیط مجازی (برای اطمینان از PATH) ---
ECHO ⚙️ در حال فعال سازی محیط مجازی...
CALL %VENV_ACTIVATE%
ECHO ✅ محیط مجازی فعال شد.

ECHO.
ECHO 🚀 در حال اجرای Master Service Manager در پس زمینه (بدون پنجره)...
ECHO -----------------------------------------------------------------

:: 💡 اجرای اسکریپت در پس‌زمینه واقعی با pythonw.exe
:: 💡 و ارسال آرگومان --quiet تا لاگ کنسول غیرفعال شود
START "YadaTradeMasterBG" /B %PYTHON_EXE% %MASTER_SCRIPT% start --quiet

:: --- پایان ---
ECHO.
ECHO ✅ فرمان 'start' به Master Script ارسال شد.
ECHO 💡 این بار هیچ پنجره‌ای باز نخواهد شد.
ECHO 💡 برای بررسی وضعیت از 'status.bat' استفاده کنید.
ECHO 💡 برای خاموش کردن، از 'stop.bat' استفاده کنید.
ECHO این پنجره ظرف 3 ثانیه بسته خواهد شد.
TIMEOUT /T 3 /NOBREAK >NUL
EXIT /B 0