# -*- coding: utf-8 -*-
# routes/dynamic_support.py

from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
import logging

import services.dynamic_support_service as dynamic_support_service 
from extensions import db # اضافه شدن برای دسترسی احتمالی به دیتابیس در آینده

# تنظیمات لاگینگ
logger = logging.getLogger(__name__)

# نام‌گذاری به "Sniper Mode" که توصیف بهتری از استراتژی دقیق شماست.
dynamic_support_ns = Namespace('dynamic_support', description='Dynamic Support Analysis (Sniper Mode)')

# ----------------- Models -----------------
# مدل‌ها بدون تغییر باقی می‌مانند، اما توضیحات کمی شفاف‌تر شد.
dynamic_support_item_model = dynamic_support_ns.model('DynamicSupportItem', {
    'symbol_name': fields.String(description='نام نماد'),
    'symbol_id': fields.String(description='شناسه نماد'),
    'current_price': fields.Float(description='قیمت پایانی'),
    'support_level': fields.Float(description='سطح حمایت ۱۶۰ روزه'), # نام‌گذاری دقیق‌تر
    'distance_from_support': fields.Float(description='فاصله تا حمایت (درصد)'),
    'support_slope': fields.Float(description='قدرت خریدار حقیقی (Power Ratio)'), # نام‌گذاری دقیق‌تر
    'jdate': fields.String(description='تاریخ تحلیل')
})

dynamic_support_response_model = dynamic_support_ns.model('DynamicSupportResponse', {
    'opportunities': fields.List(fields.Nested(dynamic_support_item_model)),
    'count': fields.Integer(description='تعداد نتایج'),
    'last_updated_date': fields.String(description='تاریخ آخرین بروزرسانی'),
    'last_updated_time': fields.String(description='ساعت آخرین بروزرسانی')
})

# پارسر قدیمی برای refresh حذف شد.

# ----------------- Resources -----------------

@dynamic_support_ns.route('/')
class DynamicSupportResource(Resource):
    
    @dynamic_support_ns.doc(security='Bearer Auth')
    @jwt_required()
    @dynamic_support_ns.marshal_with(dynamic_support_response_model)
    def get(self):
        """
        دریافت سیگنال‌های تحلیل شده (از کش یا دیتابیس).
        
        - نتایج آخرین تحلیل را که در دیتابیس ذخیره شده یا در حافظه موقت (Cache) موجود است، بازیابی می‌کند.
        - این عملیات تحلیل جدید را اجرا نمی‌کند.
        """
        logger.info("API Call: GET Dynamic Support (Retrieving Cached/DB data)")

        try:
            # فراخوانی سرویس با force_refresh=False (فقط بازیابی)
            result_data = dynamic_support_service.get_dynamic_support_data(force_refresh=False)
            
            return {
                "opportunities": result_data['opportunities'],
                "count": len(result_data['opportunities']),
                "last_updated_date": result_data['last_updated_date'],
                "last_updated_time": result_data['last_updated_time']
            }, 200

        except Exception as e:
            logger.error(f"Error in Dynamic Support GET endpoint: {e}", exc_info=True)
            dynamic_support_ns.abort(500, f"خطای سرور در بازیابی داده‌ها: {str(e)}")

    @dynamic_support_ns.doc('Force run the Dynamic Support analysis', security='Bearer Auth')
    @jwt_required()
    @dynamic_support_ns.marshal_with(dynamic_support_response_model)
    def post(self):
        """
        اجرای اجباری تحلیل حمایت داینامیک و ذخیره نتیجه در دیتابیس.
        
        - تحلیل به صورت کامل اجرا شده، جدول را پاک کرده و نتایج جدید را در دیتابیس ذخیره می‌کند.
        - نتایج تحلیل جدید را باز می‌گرداند.
        """
        logger.info("API Call: POST Dynamic Support (Force running analysis)")

        try:
            # فراخوانی سرویس با force_refresh=True (اجرای مجدد تحلیل و ذخیره در دیتابیس)
            result_data = dynamic_support_service.get_dynamic_support_data(force_refresh=True)
            

            return {
                "opportunities": result_data['opportunities'],
                "count": len(result_data['opportunities']),
                "last_updated_date": result_data['last_updated_date'],
                "last_updated_time": result_data['last_updated_time']
            }, 200 

        except Exception as e:
            logger.error(f"Error in Dynamic Support POST endpoint: {e}", exc_info=True)
            dynamic_support_ns.abort(500, f"خطای سرور در اجرای تحلیل: {str(e)}")