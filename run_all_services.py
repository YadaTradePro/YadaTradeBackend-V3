# -*- coding: utf-8 -*-
# run_all_services.py - اسکریپت اصلی مدیریت سرویس‌ها (پیشرفته)

import subprocess
import time
import os
import sys
import atexit
import logging
import argparse
from typing import List, Dict, Any, Optional, Tuple, TextIO

# -----------------------------------------------------------------------------
# 🌈 تنظیمات و رنگ‌ها
# -----------------------------------------------------------------------------
# کدهای ANSI برای لاگ‌های رنگی در کنسول
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# --- تنظیمات لاگینگ Master ---
logging.basicConfig(
    level=logging.INFO,
    format=f"{Colors.OKCYAN}%(asctime)s{Colors.ENDC} - {Colors.BOLD}%(name)s{Colors.ENDC} - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MASTER_LAUNCHER")

# --- متغیرهای سراسری ---
PYTHON_EXECUTABLE = sys.executable 
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# ذخیره پردازش‌ها و آرگومان‌هایشان برای راه‌اندازی مجدد
processes: List[subprocess.Popen] = []
process_args: Dict[int, List[str]] = {}
# ذخیره فایل‌های لاگ برای بستن در زمان خروج
log_file_handles: Dict[int, Tuple[TextIO, TextIO]] = {}


# -----------------------------------------------------------------------------
# ⚙️ پیکربندی سرویس‌ها
# -----------------------------------------------------------------------------
SERVICE_CONFIGS = [
    {"name": "TGJU_PROXY", "script": "services/tgju.py", "delay": 5, "env": {"ENABLE_TGJU_PROXY": "true"}},
    {"name": "FLASK_MAIN", "script": "main.py", "delay": 5, "env": {"FLASK_APP": "main.py"}},
    {"name": "SCHEDULER", "script": "scheduler.py", "delay": 5, "env": {}},
]


# -----------------------------------------------------------------------------
# 🛠️ توابع مدیریت پردازش
# -----------------------------------------------------------------------------

def start_process(name: str, cmd_list: List[str], delay: int = 0, is_restart: bool = False) -> Optional[subprocess.Popen]:
    """یک پردازش را در پس‌زمینه اجرا کرده و لاگ‌ها را به فایل‌های مجزا هدایت می‌کند."""
    
    # 1. تنظیمات لاگ و محیط
    log_file_prefix = name.lower().replace(" ", "_")
    
    try:
        # ✅ ثبت لاگ جدا برای هر سرویس (نقطه ۱)
        stdout_log = open(os.path.join(LOG_DIR, f"{log_file_prefix}_stdout.log"), "a", encoding="utf-8")
        stderr_log = open(os.path.join(LOG_DIR, f"{log_file_prefix}_stderr.log"), "a", encoding="utf-8")
    except Exception as e:
        logger.error(f"❌ خطای باز کردن فایل‌های لاگ برای {name}: {e}")
        return None

    env = os.environ.copy()
    # در صورت وجود، متغیرهای محیطی سرویس را اضافه می‌کند.
    for config in SERVICE_CONFIGS:
        if config['name'] == name:
            env.update(config.get('env', {}))
            break
            
    try:
        # 2. اجرای پردازش
        process = subprocess.Popen(
            cmd_list, 
            env=env,
            stdout=stdout_log, 
            stderr=stderr_log,
            # Windows-specific: CREATE_NEW_PROCESS_GROUP
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        )
        
        # 3. ثبت و ذخیره‌سازی
        processes.append(process)
        process_args[process.pid] = cmd_list
        log_file_handles[process.pid] = (stdout_log, stderr_log)
        
        status_msg = f"راه‌اندازی مجدد" if is_restart else "راه‌اندازی"
        
        # ✅ لاگ محیطی بهتر (رنگین) (نقطه ۴)
        logger.info(f"{Colors.OKGREEN}✅ سرویس {name} با PID {process.pid} ({status_msg}) و هدایت لاگ به {log_file_prefix}.log انجام شد.{Colors.ENDC}")
        
        if delay > 0 and not is_restart:
            logger.info(f"😴 انتظار به مدت {delay} ثانیه برای اطمینان از آمادگی {name}...")
            time.sleep(delay)
            
        return process

    except FileNotFoundError:
        logger.error(f"{Colors.FAIL}❌ خطا: مفسر پایتون یا اسکریپت اصلی یافت نشد. دستور: {' '.join(cmd_list)}{Colors.ENDC}")
    except Exception as e:
        logger.error(f"{Colors.FAIL}❌ خطای غیرمنتظره در راه‌اندازی {name}: {e}{Colors.ENDC}", exc_info=True)
        # بستن فایل‌ها در صورت شکست
        stdout_log.close()
        stderr_log.close()
    
    return None


def stop_all_services():
    """خاتمه دادن به تمام پردازش‌های در حال اجرا و پاکسازی."""
    global processes
    logger.info(f"{Colors.WARNING}🛑 شروع عملیات خاتمه‌دهی {len(processes)} پردازش...{Colors.ENDC}")
    
    for process in processes:
        if process.poll() is None:
            try:
                # 🛑 ارسال سیگنال خاتمه
                process.terminate()
                process.wait(timeout=3)
                logger.info(f"✅ پردازش PID {process.pid} خاتمه یافت.")
            except subprocess.TimeoutExpired:
                # 🔪 اگر خاتمه نیافت، آن را متوقف (Kill) کنید
                process.kill()
                logger.warning(f"{Colors.FAIL}❌ پردازش PID {process.pid} اجباراً متوقف شد (Killed).{Colors.ENDC}")
            except Exception as e:
                logger.error(f"خطا در خاتمه دادن پردازش PID {process.pid}: {e}")

    # پاکسازی نهایی (فایل‌ها و لیست‌ها)
    cleanup_file_handles()
    processes = []
    logger.info(f"{Colors.HEADER}🎉 تمام سرویس‌ها متوقف و پاکسازی انجام شد.{Colors.ENDC}")


def cleanup_file_handles():
    """بستن تمام File Handlerهای باز."""
    global log_file_handles
    for pid, (stdout_f, stderr_f) in log_file_handles.items():
        try:
            stdout_f.close()
            stderr_f.close()
        except Exception as e:
            logger.warning(f"⚠️ نتوانست فایل لاگ PID {pid} را ببندد: {e}")
    log_file_handles = {}

# ثبت تابع cleanup برای اجرای خودکار در زمان خروج Master Script
atexit.register(stop_all_services)


def monitor_processes():
    """حلقه مانیتورینگ برای بررسی وضعیت و راه‌اندازی مجدد خودکار."""
    global processes
    
    # اجرای اولیه سرویس‌ها به ترتیب
    for config in SERVICE_CONFIGS:
        cmd_list = [PYTHON_EXECUTABLE, config["script"]]
        start_process(config["name"], cmd_list, config["delay"])

    # شروع حلقه مانیتورینگ
    logger.info(f"{Colors.OKBLUE}✨ تمام سرویس‌ها در حال اجرا هستند. Master Script فعال است (برای خروج CTRL+C را بزنید).{Colors.ENDC}")
    
    while True:
        time.sleep(10) # بررسی هر ۱۰ ثانیه
        
        # بررسی و راه‌اندازی مجدد (نقطه ۲)
        processes_to_remove = []
        for process in processes:
            if process.poll() is not None: # اگر متوقف شده است (کد خروج دارد)
                
                # پیدا کردن آرگومان‌ها و نام اصلی
                cmd_list = process_args.get(process.pid)
                name = os.path.basename(cmd_list[-1]).replace(".py", "").upper()
                
                if cmd_list:
                    logger.warning(f"{Colors.WARNING}⚠️ سرویس {name} با PID {process.pid} متوقف شد (Exit Code: {process.poll()}). در حال راه‌اندازی مجدد...{Colors.ENDC}")
                    
                    # بستن فایل‌های لاگ سرویس متوقف شده
                    if process.pid in log_file_handles:
                        for f_handle in log_file_handles.pop(process.pid):
                            f_handle.close()
                            
                    # راه‌اندازی مجدد
                    new_process = start_process(name, cmd_list, is_restart=True)
                    if new_process:
                        logger.info(f"{Colors.OKGREEN}✅ {name} با PID جدید {new_process.pid} با موفقیت راه‌اندازی شد.{Colors.ENDC}")
                    else:
                        logger.error(f"{Colors.FAIL}❌ شکست در راه‌اندازی مجدد {name}. عملیات متوقف شد.{Colors.ENDC}")
                
                processes_to_remove.append(process)

        # حذف پردازش‌های قدیمی از لیست فعال
        processes = [p for p in processes if p not in processes_to_remove]
        
        
def check_status():
    """بررسی و نمایش وضعیت فعلی سرویس‌ها."""
    logger.info(f"{Colors.HEADER}\n================ وضعیت سرویس‌ها ================{Colors.ENDC}")
    
    # بررسی پردازش‌های ثبت شده در Master Script
    if not process_args:
        logger.warning(f"{Colors.WARNING}⚠️ هیچ پردازش فعالی توسط Master Script ثبت نشده است. از 'start' استفاده کنید.{Colors.ENDC}")
        return

    # بررسی پردازش‌های موجود در لیست OS (تقریباً دقیق‌تر)
    current_pids = {p.pid for p in processes if p.poll() is None}

    for pid, cmd_list in process_args.items():
        name = os.path.basename(cmd_list[-1]).replace(".py", "").upper()
        
        if pid in current_pids:
            logger.info(f"{Colors.OKGREEN}🟢 [RUNNING] {name:<15} | PID: {pid}{Colors.ENDC}")
        else:
            # اگر در لیست processes فعال نبود، آخرین وضعیت را از OS می‌گیریم.
            # برای سادگی، اگر در لیست فعال نیست، آن را متوقف شده فرض می‌کنیم.
            logger.warning(f"{Colors.FAIL}🔴 [STOPPED/FAILED] {name:<15} | PID: {pid}{Colors.ENDC}")

    logger.info(f"{Colors.HEADER}================================================{Colors.ENDC}")


# -----------------------------------------------------------------------------
# 🌐 تابع اصلی CLI (نقطه ۳)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Service Manager for YadaTrade Backend.")
    parser.add_argument("command", choices=['start', 'stop', 'status'], help="Command to run: start, stop, or status.")
    
    args = parser.parse_args()
    
    if args.command == 'start':
        try:
            monitor_processes()
        except KeyboardInterrupt:
            logger.info("دریافت دستور خاتمه از کاربر. در حال خاموش کردن سرویس‌ها...")
        finally:
            # تابع atexit به طور خودکار stop_all_services را فراخوانی می‌کند.
            pass
            
    elif args.command == 'stop':
        stop_all_services()
        
    elif args.command == 'status':
        check_status()