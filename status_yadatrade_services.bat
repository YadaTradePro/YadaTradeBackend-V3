@echo off
TITLE YadaTrade Backend Services Status
CHCP 65001 > NUL

:: --- تنظیم مسیرها ---
SET PYTHONUTF8=1
SET PROJECT_ROOT=D:\Mahdi\New Backend\V-3\Backend-V3
SET VENV_ACTIVATE="%PROJECT_ROOT%\venv_311\Scripts\activate.bat"
SET MASTER_SCRIPT="%PROJECT_ROOT%\run_all_services.py"
SET PYTHON_EXE="%PROJECT_ROOT%\venv_311\Scripts\python.exe"

:: --- تغییر دایرکتوری ---
CD /D "%PROJECT_ROOT%"

:: --- فعال سازی محیط مجازی ---
ECHO ⚙️ در حال فعال سازی محیط مجازی...
CALL %VENV_ACTIVATE%
ECHO ✅ محیط مجازی فعال شد.

:: --- اجرای فرمان وضعیت ---
ECHO.
ECHO 📊 در حال بررسی وضعیت سرویس ها...
%PYTHON_EXE% %MASTER_SCRIPT% status

:: --- پایان ---
ECHO.
ECHO 💡 بررسی وضعیت تمام شد.
ECHO برای خروج، کلیدی را فشار دهید...
PAUSE > NUL
EXIT /B 0