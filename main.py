# -*- coding: utf-8 -*-
"""
📘 main.py
فایل اصلی اجرای برنامه Flask API برای YadaTrade Backend.
این فایل فقط وظیفه دارد اپلیکیشن را بسازد و اجرا کند.
وظایف زمان‌بندی و عملیات تحلیلی در فایل scheduler.py انجام می‌شوند.
"""

import os
import sys
import time
import logging
import subprocess
from flask import Flask, jsonify, current_app
from flask_cors import CORS
from flask_restx import Api, Namespace, Resource
from flask_migrate import Migrate
from flask_jwt_extended import jwt_required
from extensions import db, bcrypt, jwt
from config import Config


# -----------------------------------------------------------------------------
# 📦 پیکربندی لاگ و تنظیمات اولیه
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


# -----------------------------------------------------------------------------
# ⚙️ تابع ساخت اپ Flask
# -----------------------------------------------------------------------------
def create_app(test_config=None):
    """ایجاد و پیکربندی اپ Flask و اضافه‌کردن namespaceها"""
    app = Flask(__name__)
    app.config.from_object(Config)

    # تنظیمات Cross-Origin
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

    # اتصال اکستنشن‌ها
    db.init_app(app)
    jwt.init_app(app)
    bcrypt.init_app(app)
    Migrate(app, db)

    # -----------------------------------------------------------------------------
    # 🔗 Flask-RESTX: API & Swagger Config
    # -----------------------------------------------------------------------------
    authorizations = {
        "Bearer Auth": {
            "type": "apiKey",
            "in": "header",
            "name": "Authorization",
            "description": 'JWT token. Example: "Authorization: Bearer <token>"'
        }
    }

    api = Api(
        app,
        version="3.0", # نسخه به‌روز شد
        title="YadaTrade API",
        description="Backend API for Tehran Stock Analysis and Data Services",
        doc="/api/swagger-ui/",
        prefix="/api",
        security="Bearer Auth",
        authorizations=authorizations
    )

    # -----------------------------------------------------------------------------
    # 📚 اضافه کردن Namespaceها
    # -----------------------------------------------------------------------------
    from routes.auth import auth_ns
    from routes.data_fetching_routes import data_ns  # <<< تغییر یافته
    from routes.golden_key import golden_key_ns
    from routes.weekly_watchlist import weekly_watchlist_ns
    from routes.potential_queues import potential_queues_ns
    from routes.performance import performance_ns
    from routes.market_data import market_overview_ns
    from routes.combined_analysis_route import SymbolAnalysis_ns
    from routes.dynamic_support import dynamic_support_ns

    # اضافه‌کردن namespace‌ها به Swagger
    api.add_namespace(auth_ns, path="/auth")
    api.add_namespace(data_ns, path="/data")  # <<< تغییر یافته
    api.add_namespace(SymbolAnalysis_ns, path="/SymbolAnalysis") 
    api.add_namespace(market_overview_ns, path="/market-overview")
    api.add_namespace(golden_key_ns, path="/golden_key")
    api.add_namespace(weekly_watchlist_ns, path="/weekly_watchlist")
    api.add_namespace(potential_queues_ns, path="/potential_queues")
    api.add_namespace(performance_ns, path="/performance")
    api.add_namespace(dynamic_support_ns, path="/dynamic_support")

    # -----------------------------------------------------------------------------
    # ⚙️ Namespace ساده برای Settings (در آینده توسعه می‌یابد)
    # -----------------------------------------------------------------------------
    settings_ns = Namespace("settings", description="User settings operations")

    @settings_ns.route("/")
    class SettingsResource(Resource):
        @settings_ns.doc(security="Bearer Auth")
        @jwt_required()
        def get(self):
            return {"message": "Settings endpoint (reserved for future use)."}, 200

    api.add_namespace(settings_ns, path="/settings")

    # -----------------------------------------------------------------------------
    # 🔐 هندلرهای JWT Errors
    # -----------------------------------------------------------------------------
    @jwt.unauthorized_loader
    def unauthorized_response(callback):
        return jsonify({"message": "توکن احراز هویت یافت نشد یا نامعتبر است."}), 401

    @jwt.invalid_token_loader
    def invalid_token_response(callback):
        app.logger.error(f"Invalid JWT: {callback}")
        return jsonify({"message": "توکن نامعتبر است."}), 403

    @jwt.expired_token_loader
    def expired_token_response(jwt_header, jwt_data):
        current_app.logger.warning("JWT Expired.")
        return jsonify({
            "message": "توکن منقضی شده است، لطفاً دوباره وارد شوید.",
            "code": "token_expired"
        }), 401

    # -----------------------------------------------------------------------------
    # 🏠 مسیر اصلی (Root)
    # -----------------------------------------------------------------------------
    @app.route("/")
    def home():
        return jsonify({
            "message": "به API یداترید خوش آمدید!",
            "docs": "/api/swagger-ui/"
        })

    return app


# -----------------------------------------------------------------------------
# 🛰 اجرای پراکسی TGJU (در صورت نیاز)
# -----------------------------------------------------------------------------
tgju_proxy_process = None

def start_tgju_proxy_service():
    """اجرای پراکسی TGJU به عنوان فرآیند پس‌زمینه (اختیاری)"""
    global tgju_proxy_process
    if tgju_proxy_process and tgju_proxy_process.poll() is None:
        return

    logger.info("در حال راه‌اندازی سرور پراکسی TGJU...")
    try:
        tgju_proxy_process = subprocess.Popen(
            [sys.executable, "services/tgju.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)
        logger.info("✅ پراکسی TGJU با موفقیت راه‌اندازی شد.")
    except FileNotFoundError:
        logger.warning("⚠️ فایل services/tgju.py یافت نشد (اختیاری است).")
    except Exception as e:
        logger.error(f"خطا در راه‌اندازی پراکسی TGJU: {e}", exc_info=True)


# -----------------------------------------------------------------------------
# 🚀 نقطه شروع برنامه
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app = create_app()

    # اجرای پراکسی TGJU فقط در حالت توسعه یا اجرای مستقیم
    if os.environ.get("ENABLE_TGJU_PROXY", "true").lower() == "true":
        start_tgju_proxy_service()

    # هشدار در حالت توسعه
    if os.environ.get("FLASK_ENV") == "development":
        app.logger.info("⚙️ توجه: Scheduler باید از فایل scheduler.py اجرا شود، نه main.py")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)
