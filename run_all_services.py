# -*- coding: utf-8 -*-
# run_all_services.py - اسکریپت اصلی مدیریت سرویس‌ها (نهایی با --quiet)

import subprocess
import time
import os
import sys
import atexit
import logging
import argparse
import json
import signal
from typing import List, Dict, Any, Optional, Tuple, TextIO
from logging import FileHandler # تغییر داده شده برای رفع خطای ImportError

# -----------------------------------------------------------------------------
# 🌈 تنظیمات و رنگ‌ها
# -----------------------------------------------------------------------------
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

# --- متغیرهای سراسری ---
PYTHON_EXECUTABLE = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
PID_DIR = LOG_DIR

MASTER_PID_FILE = os.path.join(PID_DIR, "master.pid")
SERVICES_PID_FILE = os.path.join(PID_DIR, "services.json")

os.makedirs(LOG_DIR, exist_ok=True)

# --- تنظیمات لاگینگ Master ---
# لاگ مستر علاوه بر کنسول، در فایل هم ذخیره می‌شود
master_log_file = os.path.join(LOG_DIR, "master_launcher.log")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(f"{Colors.OKCYAN}%(asctime)s{Colors.ENDC} - {Colors.BOLD}%(name)s{Colors.ENDC} - %(levelname)s - %(message)s"))

file_handler = FileHandler(master_log_file, "a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")) # بدون رنگ در فایل

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        console_handler,
        file_handler
    ]
)
logger = logging.getLogger("MASTER_LAUNCHER")

# -----------------------------------------------------------------------------
# ⚙️ پیکربندی سرویس‌ها
# -----------------------------------------------------------------------------
SERVICE_CONFIGS = [
    {"name": "TGJU_PROXY", "script": "services/tgju.py", "delay": 5, "env": {"ENABLE_TGJU_PROXY": "true"}},
    {"name": "FLASK_MAIN", "script": "main.py", "delay": 5, "env": {"FLASK_APP": "main.py"}},
    {"name": "SCHEDULER", "script": "scheduler.py", "delay": 5, "env": {}},
]

# -----------------------------------------------------------------------------
# 🛠️ توابع کمکی (بدون تغییر)
# -----------------------------------------------------------------------------

def is_pid_running(pid: int) -> bool:
    """بررسی می‌کند آیا پروسه‌ای با PID داده شده در حال اجرا است یا خیر."""
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}'],
                capture_output=True, text=True, check=True, encoding='utf-8',
                creationflags=subprocess.CREATE_NO_WINDOW # از باز شدن پنجره جلوگیری کن
            )
            return str(pid) in result.stdout
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False
    else:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

def write_pid_file(filepath: str, pid: int):
    try:
        with open(filepath, 'w') as f:
            f.write(str(pid))
    except IOError as e:
        logger.error(f"❌ امکان نوشتن فایل PID در {filepath} وجود ندارد: {e}")
        sys.exit(1)

def read_pid_file(filepath: str) -> Optional[int]:
    try:
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r') as f:
            return int(f.read().strip())
    except (IOError, ValueError) as e:
        logger.warning(f"⚠️ فایل PID در {filepath} معتبر نیست یا خوانده نشد: {e}")
        return None

def remove_file_if_exists(filepath: str):
    if os.path.exists(filepath):
        try:
            os.remove(filepath)
        except OSError as e:
            logger.warning(f"⚠️ نتوانست فایل {filepath} را حذف کند: {e}")

def write_services_json(data: Dict[str, Any]):
    try:
        with open(SERVICES_PID_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        logger.error(f"❌ امکان نوشتن {SERVICES_PID_FILE} وجود ندارد: {e}")

def read_services_json() -> Optional[Dict[str, Any]]:
    try:
        if not os.path.exists(SERVICES_PID_FILE):
            return None
        with open(SERVICES_PID_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        logger.warning(f"⚠️ فایل {SERVICES_PID_FILE} معتبر نیست یا خوانده نشد: {e}")
        return None

# -----------------------------------------------------------------------------
# 🛠️ توابع مدیریت پردازش (تغییر کوچک در Popen)
# -----------------------------------------------------------------------------

def start_process(name: str, cmd_list: List[str], env_vars: Dict[str, str]) -> Optional[Tuple[subprocess.Popen, TextIO, TextIO]]:
    log_file_prefix = name.lower().replace(" ", "_")
    
    try:
        stdout_log = open(os.path.join(LOG_DIR, f"{log_file_prefix}_stdout.log"), "a", encoding="utf-8")
        stderr_log = open(os.path.join(LOG_DIR, f"{log_file_prefix}_stderr.log"), "a", encoding="utf-8")
    except Exception as e:
        logger.error(f"❌ خطای باز کردن فایل‌های لاگ برای {name}: {e}")
        return None

    env = os.environ.copy()
    env.update(env_vars)
            
    try:
        process = subprocess.Popen(
            cmd_list,
            env=env,
            stdout=stdout_log,
            stderr=stderr_log,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
        )
        return process, stdout_log, stderr_log

    except FileNotFoundError:
        logger.error(f"{Colors.FAIL}❌ خطا: مفسر پایتون یا اسکریپت اصلی یافت نشد. دستور: {' '.join(cmd_list)}{Colors.ENDC}")
    except Exception as e:
        logger.error(f"{Colors.FAIL}❌ خطای غیرمنتظره در راه‌اندازی {name}: {e}{Colors.ENDC}", exc_info=True)
    
    stdout_log.close()
    stderr_log.close()
    return None


def stop_all_services(processes_map: Dict[subprocess.Popen, Any], log_file_handles: Dict[int, Tuple[TextIO, TextIO]]):
    logger.info(f"{Colors.WARNING}🛑 شروع عملیات خاتمه‌دهی {len(processes_map)} پردازش...{Colors.ENDC}")
    
    for process, config in processes_map.items():
        name = config.get("name", f"PID {process.pid}")
        if process.poll() is None:
            try:
                logger.info(f"🛑 در حال ارسال سیگنال خاتمه به {name} (PID: {process.pid})...")
                # استفاده از taskkill برای اطمینان از بسته شدن درست در ویندوز
                # /T فرزندان را هم می‌بندد (اگرچه اینجا فرزند ندارند)
                # /F اجباری است، اما ابتدا terminate را امتحان می‌کنیم
                process.terminate()
                process.wait(timeout=3)
                logger.info(f"✅ پردازش {name} (PID {process.pid}) خاتمه یافت.")
            except subprocess.TimeoutExpired:
                logger.warning(f"{Colors.WARNING}⚠️ {name} (PID {process.pid}) به terminate پاسخ نداد. در حال ارسال Kill...{Colors.ENDC}")
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
                logger.warning(f"{Colors.FAIL}❌ پردازش {name} (PID {process.pid}) اجباراً متوقف شد (Killed).{Colors.ENDC}")
            except Exception as e:
                logger.error(f"خطا در خاتمه دادن پردازش {name} (PID {process.pid}): {e}")

    # بستن تمام File Handlerهای باز
    for pid, (stdout_f, stderr_f) in log_file_handles.items():
        try:
            stdout_f.close()
            stderr_f.close()
        except Exception as e:
            logger.warning(f"⚠️ نتوانست فایل لاگ PID {pid} را ببندد: {e}")


# -----------------------------------------------------------------------------
# 🌐 توابع منطقی دستورات CLI (بدون تغییر)
# -----------------------------------------------------------------------------

def monitor_processes():
    master_pid = read_pid_file(MASTER_PID_FILE)
    if master_pid and is_pid_running(master_pid):
        logger.error(f"{Colors.FAIL}❌ اسکریپت Master از قبل با PID {master_pid} در حال اجراست. ابتدا آن را متوقف کنید.{Colors.ENDC}")
        sys.exit(1)

    my_pid = os.getpid()
    write_pid_file(MASTER_PID_FILE, my_pid)
    logger.info(f"✨ Master Launcher با PID {my_pid} شروع به کار کرد.")

    processes_map: Dict[subprocess.Popen, Dict[str, Any]] = {}
    log_file_handles: Dict[int, Tuple[TextIO, TextIO]] = {}
    service_pids: Dict[str, int] = {}

    @atexit.register
    def cleanup_on_exit():
        logger.info(f"{Colors.HEADER}🎉 دریافت سیگنال خروج... در حال پاکسازی...{Colors.ENDC}")
        stop_all_services(processes_map, log_file_handles)
        remove_file_if_exists(MASTER_PID_FILE)
        remove_file_if_exists(SERVICES_PID_FILE)
        logger.info(f"{Colors.HEADER}🧹 پاکسازی کامل شد. خداحافظ!{Colors.ENDC}")

    for config in SERVICE_CONFIGS:
        name = config["name"]
        cmd_list = [PYTHON_EXECUTABLE, config["script"]]
        result = start_process(name, cmd_list, config.get('env', {}))
        
        if result:
            process, stdout_log, stderr_log = result
            processes_map[process] = config
            log_file_handles[process.pid] = (stdout_log, stderr_log)
            service_pids[name] = process.pid
            logger.info(f"{Colors.OKGREEN}✅ سرویس {name} با PID {process.pid} راه‌اندازی شد.{Colors.ENDC}")
            delay = config.get("delay", 0)
            if delay > 0:
                logger.info(f"😴 انتظار به مدت {delay} ثانیه برای {name}...")
                time.sleep(delay)
        else:
            logger.error(f"{Colors.FAIL}❌ شکست در راه‌اندازی {name}. اسکریپت مستر متوقف می‌شود.{Colors.ENDC}")
            sys.exit(1) 

    write_services_json(service_pids)
    
    logger.info(f"{Colors.OKBLUE}✨ تمام سرویس‌ها در حال اجرا هستند. Master Script فعال است (برای خروج 'stop' را اجرا کنید).{Colors.ENDC}")
    
    try:
        while True:
            time.sleep(10) 
            
            for process, config in list(processes_map.items()):
                
                if process.poll() is not None: 
                    name = config["name"]
                    old_pid = process.pid
                    exit_code = process.poll()
                    
                    logger.warning(f"{Colors.WARNING}⚠️ سرویس {name} (PID: {old_pid}) متوقف شد (Exit Code: {exit_code}). در حال راه‌اندازی مجدد...{Colors.ENDC}")
                    
                    if old_pid in log_file_handles:
                        for f_handle in log_file_handles.pop(old_pid):
                            f_handle.close()
                    
                    del processes_map[process]
                    
                    cmd_list = [PYTHON_EXECUTABLE, config["script"]]
                    new_result = start_process(name, cmd_list, config.get('env', {}))
                    
                    if new_result:
                        new_process, new_stdout, new_stderr = new_result
                        processes_map[new_process] = config
                        log_file_handles[new_process.pid] = (new_stdout, new_stderr)
                        service_pids[name] = new_process.pid
                        write_services_json(service_pids)
                        logger.info(f"{Colors.OKGREEN}✅ {name} با PID جدید {new_process.pid} با موفقیت راه‌اندازی شد.{Colors.ENDC}")
                    else:
                        logger.error(f"{Colors.FAIL}❌ شکست در راه‌اندازی مجدد {name}. از مانیتورینگ حذف شد.{Colors.ENDC}")
                        if name in service_pids:
                            del service_pids[name]
                            write_services_json(service_pids)

    except KeyboardInterrupt:
        logger.info("دریافت دستور خاتمه (Ctrl+C). در حال خاموش کردن سرویس‌ها...")

def stop_command_logic():
    logger.info("در حال تلاش برای متوقف کردن Master Service...")
    
    master_pid = read_pid_file(MASTER_PID_FILE)
    
    if not master_pid:
        logger.warning(f"{Colors.WARNING}⚠️ فایل PID مستر ({MASTER_PID_FILE}) یافت نشد. آیا سرویس‌ها در حال اجرا هستند؟{Colors.ENDC}")
        return

    if not is_pid_running(master_pid):
        logger.warning(f"{Colors.WARNING}⚠️ پروسه مستر (PID: {master_pid}) یافت نشد. پاکسازی فایل PID...{Colors.ENDC}")
        remove_file_if_exists(MASTER_PID_FILE)
        return

    try:
        logger.info(f"{Colors.OKGREEN}✅ سیگنال خاتمه به Master (PID: {master_pid}) ارسال شد. در حال انتظار برای خاموش شدن...{Colors.ENDC}")
        if sys.platform == "win32":
            # ارسال سیگنال شکستن (Ctrl+Break) که توسط atexit دریافت می‌شود
            # این روش بسیار تمیزتر از taskkill برای پروسه اصلی است
            os.kill(master_pid, signal.CTRL_BREAK_EVENT)
        else:
            os.kill(master_pid, signal.SIGTERM)
            
        for _ in range(10): 
            if not is_pid_running(master_pid):
                logger.info(f"{Colors.OKGREEN}🎉 Master Service با موفقیت متوقف شد.{Colors.ENDC}")
                return
            time.sleep(1)
            
        logger.error(f"{Colors.FAIL}❌ Master Service (PID: {master_pid}) پس از ۱۰ ثانیه هنوز در حال اجراست. استفاده از taskkill...{Colors.ENDC}")
        subprocess.run(["taskkill", "/PID", str(master_pid), "/T", "/F"], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)

    except Exception as e:
        logger.error(f"{Colors.FAIL}❌ خطایی هنگام ارسال سیگنال توقف به PID {master_pid} رخ داد: {e}{Colors.ENDC}")

def status_command_logic():
    logger.info(f"{Colors.HEADER}\n================ وضعیت سرویس‌ها ================{Colors.ENDC}")
    
    master_running = False
    master_pid = read_pid_file(MASTER_PID_FILE)
    
    if master_pid and is_pid_running(master_pid):
        logger.info(f"{Colors.OKGREEN}🟢 [RUNNING] MASTER_LAUNCHER   | PID: {master_pid}{Colors.ENDC}")
        master_running = True
    elif master_pid:
        logger.warning(f"{Colors.FAIL}🔴 [STOPPED] MASTER_LAUNCHER   | PID: {master_pid} (فایل PID وجود دارد اما پروسه مرده است){Colors.ENDC}")
        remove_file_if_exists(MASTER_PID_FILE)
    else:
        logger.info(f"{Colors.OKBLUE}⚪️ [STOPPED] MASTER_LAUNCHER   | (هیچ سرویس مستری ثبت نشده است){Colors.ENDC}")

    services_pids = read_services_json()
    
    if not services_pids:
        if not master_running:
            logger.info(f"{Colors.OKBLUE}⚪️ هیچ سرویس فرزندی ثبت نشده است.{Colors.ENDC}")
        else:
             logger.warning(f"{Colors.WARNING}⚠️ مستر در حال اجراست اما {SERVICES_PID_FILE} یافت نشد (در حال راه‌اندازی؟).{Colors.ENDC}")
        logger.info(f"{Colors.HEADER}================================================{Colors.ENDC}")
        return

    for name, pid in services_pids.items():
        if is_pid_running(pid):
            logger.info(f"{Colors.OKGREEN}🟢 [RUNNING] {name:<18} | PID: {pid}{Colors.ENDC}")
        else:
            status_text = "[RESTARTING...]" if master_running else "[STOPPED]"
            logger.warning(f"{Colors.FAIL}🔴 {status_text:<10} {name:<18} | PID: {pid} (پروسه مرده است){Colors.ENDC}")

    if not master_running and any(is_pid_running(p) for p in services_pids.values()):
         logger.warning(f"{Colors.WARNING}⚠️ هشدار: مستر خاموش است اما سرویس‌های فرزند یافت شدند (Orphaned?).{Colors.ENDC}")

    logger.info(f"{Colors.HEADER}================================================{Colors.ENDC}")


# -----------------------------------------------------------------------------
# 🚀 نقطه ورود اصلی (تغییر یافته برای --quiet)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Service Manager for YadaTrade Backend.")
    parser.add_argument("command", choices=['start', 'stop', 'status'], help="Command to run: start, stop, or status.")
    parser.add_argument("--quiet", action="store_true", help="Run in background mode without console output.")
    
    args = parser.parse_args()
    
    # -------------------------------------------------------------------------
    # 💡 بخش کلیدی: مدیریت حالت Quiet (ساکت)
    # -------------------------------------------------------------------------
    if args.quiet:
        # ۱. غیرفعال کردن رنگ‌ها
        for attr in dir(Colors):
            if not attr.startswith('__'):
                setattr(Colors, attr, "")
        
        # ۲. حذف لاگر کنسول تا pythonw.exe کرش نکند
        root_logger = logging.getLogger()
        # فقط FileHandler را نگه می‌داریم
        root_logger.handlers = [h for h in root_logger.handlers if isinstance(h, FileHandler)]
        logger.info("Quiet mode activated. Console logging disabled.")
    # -------------------------------------------------------------------------
    
    if args.command == 'start':
        monitor_processes()
            
    elif args.command == 'stop':
        stop_command_logic()
        
    elif args.command == 'status':
        status_command_logic()
