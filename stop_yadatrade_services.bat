@echo off
TITLE YadaTrade Backend Services Stopper
CHCP 65001 > NUL

:: --- تنظیم مسیرها ---
SET PYTHONUTF8=1
SET PROJECT_ROOT=D:\Mahdi\New Backend\V-3\Backend-V3
SET VENV_ACTIVATE=%PROJECT_ROOT%\venv_311\Scripts\activate.bat
SET MASTER_SCRIPT=%PROJECT_ROOT%\run_all_services.py
SET PYTHON_EXE=%PROJECT_ROOT%\venv_311\Scripts\python.exe
SET MASTER_PID_FILE=%PROJECT_ROOT%\logs\master.pid

:: --- تغییر دایرکتوری ---
CD /D "%PROJECT_ROOT%"

:: --- فعال سازی محیط مجازی ---
ECHO ⚙️ در حال فعال سازی محیط مجازی برای اجرای فرمان توقف...
CALL "%VENV_ACTIVATE%"
ECHO ✅ محیط مجازی فعال شد.

:: --- خواندن PID مستر ---
IF NOT EXIST "%MASTER_PID_FILE%" (
    ECHO ❌ فایل PID مستر یافت نشد. سرویس‌ها در حال اجرا نیستند؟
    GOTO STATUS_CHECK
)

SET /P MASTER_PID=<"%MASTER_PID_FILE%"

:: --- متوقف کردن Master Script و فرزندان ---
ECHO 🛑 در حال خاتمه دادن Master Script و سرویس‌ها (PID: %MASTER_PID%)...
TASKKILL /PID %MASTER_PID% /T /F

:STATUS_CHECK
:: --- بررسی وضعیت ---
"%PYTHON_EXE%" "%MASTER_SCRIPT%" status

ECHO.
ECHO 💥 تمامی سرویس ها باید متوقف شده باشند.
TIMEOUT /T 5 /NOBREAK >NUL
EXIT /B 0