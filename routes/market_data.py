# -*- coding: utf-8 -*-
import requests
from extensions import db
import jdatetime
import logging
import socket
from urllib.parse import urlparse
from flask import current_app, jsonify
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from requests.exceptions import ConnectionError, Timeout



# وارد کردن ابزارهای مورد نیاز
from services.index_data_fetcher import get_market_indices
from services.global_commodities_data import fetch_global_commodities
from services import market_analysis_service # برای Market Summary
from services.index_data_processor import store_market_indices_data # ✅ NEW: برای ذخیره داده‌های شاخص
from services.sector_analysis_service import run_daily_sector_analysis

# تنظیم لاگینگ
logger = logging.getLogger(__name__)

#_____________________________________________
# --- تعریف namespace و مدل‌ها برای Swagger UI ---
#______________________________________________


market_overview_ns = Namespace('market-overview', description='Market overview data')

#______________________________________________


# ✅ مدل برای پاسخ Market Summary
market_summary_model = market_overview_ns.model('MarketSummary', {
    'summary_report': fields.Raw(description='Structured daily/weekly market analysis report.')
})

# ✅ مدل برای پاسخ IndexUpdate
index_update_model = market_overview_ns.model('IndexUpdateStatus', {
    'success': fields.Boolean(description='True if the operation was successful.'),
    'message': fields.String(description='Status or error message.')
})


# مدل داده برای TGJU
tgju_data_model = market_overview_ns.model('TGJUData', {
    'gold_prices': fields.Raw(description='List of gold prices from TGJU.'),
    'coin_prices': fields.Raw(description='List of coin prices from TGJU.') # تغییر از currency به coin
})

# مدل داده برای شاخص‌های بورس ایران
iran_indices_model = market_overview_ns.model('IranMarketIndices', {
    'Total_Index': fields.Raw(description='Overall Bourse Index'),
    'Equal_Weighted_Index': fields.Raw(description='Equal-weighted Bourse Index'),
    'Price_Equal_Weighted_Index': fields.Raw(description='Price Equal-weighted Bourse Index'),
    'Industry_Index': fields.Raw(description='Industry Bourse Index')
})

# مدل داده برای کالاهای جهانی
global_commodities_model = market_overview_ns.model('GlobalCommodities', {
    'gold': fields.Float(description='Price of Gold'),
    'silver': fields.Float(description='Price of Silver'),
    'platinum': fields.Float(description='Price of Platinum'),
    'copper': fields.Float(description='Price of Copper')
})

# مدل اصلی برای پاسخ API
market_overview_model = market_overview_ns.model('MarketOverview', {
    'date': fields.String(description='Current Persian date (YYYY/MM/DD)'),
    'tgju_data': fields.Nested(tgju_data_model, description='Data from TGJU proxy.'),
    'iran_market_indices': fields.Nested(iran_indices_model, description='Indices from Iran Bourse (TSETMC).'),
    'global_commodities': fields.Nested(global_commodities_model, description='Prices of global commodities.')
})

# --- منطق اصلی ---

def is_port_open(host, port, timeout=1):
    """
    بررسی می‌کند که آیا پورت TCP روی هاست مشخص باز است یا نه.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        sock.close()
        return True
    except (socket.timeout, socket.error):
        return False

def get_tgju_url():
    """
    بررسی اولویت‌های URL برای پراکسی TGJU و بازگرداندن آدرس معتبر.
    """
    # اولویت‌ها به ترتیب: Docker, localhost
    proxy_urls = [
        "http://tgju_proxy:5001/api/price",
        "http://localhost:5001/api/price"
    ]

    # بررسی هر URL به ترتیب اولویت
    for url in proxy_urls:
        parsed_url = urlparse(url)
        host = parsed_url.hostname
        port = parsed_url.port

        # ابتدا با یک بررسی سریع سوکت چک می‌کنیم که پورت باز است یا نه
        if host and port and is_port_open(host, port, timeout=0.5):
            logger.info(f"پورت {host}:{port} باز است. تلاش برای اتصال به پراکسی...")
            return url
        else:
            logger.warning(f"پورت {host}:{port} بسته یا غیرقابل دسترس است.")

    # اگر هیچ پراکسی کار نکرد، از آدرس fallback استفاده کن
    fallback_url = current_app.config.get("TGJU_FALLBACK_URL", "https://call5.tgju.org")
    logger.warning(f"تمام تلاش‌ها برای اتصال به پراکسی ناموفق بود. استفاده از URL فال‌بک: {fallback_url}")
    return fallback_url

# --- منطق Endpoint ---
@market_overview_ns.route('/')
class MarketOverviewResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth')
    @jwt_required()
    @market_overview_ns.marshal_with(market_overview_model)
    def get(self):
        """
        بازگرداندن داده‌های کلی بازار شامل TGJU، بورس و کالاهای جهانی.
        """
        overview_data = {
            "date": jdatetime.date.today().strftime("%Y/%m/%d"),
            "tgju_data": {
                "gold_prices": [],
                "coin_prices": [] # تغییر از currency به coin
            },
            "iran_market_indices": {},
            "global_commodities": {}
        }

        timeout = current_app.config.get("TGJU_TIMEOUT", 8)

        # دریافت URL مناسب با منطق اولویت‌بندی
        tgju_base_url = get_tgju_url()

        # 1. دریافت داده‌های TGJU
        tgju_data = {"gold_prices": [], "coin_prices": []} # تغییر از currency به coin

        # اگر URL یک پراکسی است، درخواست را به آن ارسال کن
        if "tgju.org" not in tgju_base_url:
            try:
                gold_response = requests.get(f"{tgju_base_url}/gold", timeout=timeout)
                gold_response.raise_for_status()
                tgju_data["gold_prices"] = gold_response.json()
                logger.info("داده‌های طلا از پراکسی با موفقیت دریافت شد.")
            except Exception as e:
                logger.error(f"خطا در دریافت Gold از پراکسی: {e}", exc_info=True)

            try:
                coin_response = requests.get(f"{tgju_base_url}/coin", timeout=timeout) # تغییر از /currency به /coin
                coin_response.raise_for_status()
                tgju_data["coin_prices"] = coin_response.json() # تغییر از currency_prices به coin_prices
                logger.info("داده‌های سکه از پراکسی با موفقیت دریافت شد.")
            except Exception as e:
                logger.error(f"خطا در دریافت Coin از پراکسی: {e}", exc_info=True)

        # اگر از URL فال‌بک استفاده می‌شود، داده‌ها را مستقیماً از آن دریافت کن
        else:
            try:
                fallback_resp = requests.get(f"{tgju_base_url}/ajax.json", timeout=timeout)
                fallback_resp.raise_for_status()
                raw_data = fallback_resp.json()
                tgju_data["gold_prices"] = [i for i in raw_data.get("last", []) if "gold" in i.get("name", "")]
                tgju_data["coin_prices"] = [i for i in raw_data.get("last", []) if "coin" in i.get("name", "").lower() or "bahar" in i.get("name", "").lower()] # تغییر از currency به coin و افزودن 'bahar'
                logger.info("داده‌های TGJU از فال‌بک خارجی با موفقیت دریافت شد.")
            except Exception as e:
                logger.error(f"خطا در دریافت داده از فال‌بک: {e}", exc_info=True)

        overview_data["tgju_data"] = tgju_data

        # 2. دریافت داده‌های شاخص بورس ایران
        # این تابع از services.index_data_fetcher فراخوانی می‌شود
        try:
            iran_indices = get_market_indices()
            overview_data["iran_market_indices"] = iran_indices
            logger.info("داده‌های شاخص بورس ایران با موفقیت دریافت شد.")
        except Exception as e:
            logger.error(f"خطا در دریافت داده‌های شاخص بورس ایران: {e}", exc_info=True)
            overview_data["iran_market_indices"] = {"error": "Failed to fetch Iran market indices."}

        # 3. دریافت داده‌های کالاهای جهانی
        try:
            global_commodities = fetch_global_commodities()
            overview_data["global_commodities"] = global_commodities
        except Exception as e:
            logger.error(f"خطا در دریافت داده‌های کالاهای جهانی: {e}", exc_info=True)
            overview_data["global_commodities"] = {"error": "Failed to fetch global commodities data."}

        return overview_data, 200



@market_overview_ns.route('/summary')
class MarketSummaryResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth')
    @jwt_required()
    @market_overview_ns.marshal_with(market_summary_model) # از مدل جدید استفاده می‌کنیم
    def get(self):
        """
        Generates and returns a structured summary of the market analysis (daily/weekly report).
        """
        current_app.logger.info("API request for market summary.")
        
        # فراخوانی تابع سرویس از market_analysis_service
        # فرض می‌شود که این سرویس در market_analysis_service قرار دارد.
        try:
            summary_data = market_analysis_service.generate_market_summary()
            current_app.logger.info("Market summary generated successfully.")
            return summary_data, 200
        except Exception as e:
            current_app.logger.error(f"Error generating market summary: {e}", exc_info=True)
            return {"error": "Failed to generate market summary report."}, 500




@market_overview_ns.route('/indices-update')
class IndexDataProcessorResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth', 
                            description='Triggers a fetch and Upsert operation for Iran Bourse indices to DailyIndexData table.')
    @jwt_required()
    @market_overview_ns.marshal_with(index_update_model)
    def post(self):
        """
        واکشی داده‌های شاخص بورس از منابع خارجی و ذخیره/بروزرسانی آنها در جدول DailyIndexData.
        این Endpoint برای تضمین به‌روز بودن داده‌های مورد نیاز Market Summary استفاده می‌شود.
        """
        try:
            # 💡 فراخوانی تابع سرویس با استفاده از session دیتابیس
            success = store_market_indices_data(db.session)
            
            if success:
                return {
                    "success": True,
                    "message": "✅ داده‌های شاخص بازار با موفقیت واکشی، پردازش و در دیتابیس ذخیره/بروزرسانی شدند."
                }, 200
            else:
                return {
                    "success": False,
                    "message": "❌ عملیات ذخیره داده‌های شاخص ناموفق بود. جزئیات بیشتر در لاگ‌ها."
                }, 500
        except Exception as e:
            logger.error(f"❌ خطای حین اجرای عملیات ذخیره شاخص‌ها: {e}", exc_info=True)
            db.session.rollback() # اطمینان از Rollback در صورت خطای کلی
            return {
                "success": False,
                "message": f"❌ خطای غیرمنتظره: {str(e)}"
            }, 500





# --- منطق Endpoint 4: Sector Performance Processor
@market_overview_ns.route('/sector-performance-update')
class SectorPerformanceProcessorResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth', 
                            description='Calculates and stores the daily sector performance analysis.')
    @jwt_required()
    @market_overview_ns.marshal_with(index_update_model) # استفاده مجدد از مدل IndexUpdateStatus
    def post(self):
        """
        اجرای تحلیل عملکرد صنایع بر اساس ارزش معاملات و جریان پول برای ۵ روز اخیر و ذخیره نتایج.
        """
        logger.info("⚡️ درخواست API برای به‌روزرسانی عملکرد روزانه صنایع دریافت شد.")
        try:
            # فراخوانی تابع سرویس که محاسبات و ذخیره‌سازی را انجام می‌دهد
            run_daily_sector_analysis() # این تابع مستقیماً با db.session کار می‌کند
            
            return {
                "success": True,
                "message": "✅ تحلیل و رتبه‌بندی عملکرد صنایع با موفقیت انجام و ذخیره شد."
            }, 200
        except Exception as e:
            logger.error(f"❌ خطای حین اجرای عملیات تحلیل صنایع: {e}", exc_info=True)
            # Rollback در خود سرویس تحلیل مدیریت می‌شود، اما برای اطمینان می‌توان اینجا هم افزود
            # db.session.rollback() 
            return {
                "success": False,
                "message": f"❌ خطای غیرمنتظره در تحلیل صنایع: {str(e)}"
            }, 500
