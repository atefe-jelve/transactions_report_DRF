import jdatetime
from rest_framework.views import APIView
from rest_framework.response import Response
from utils.mongodb import mongo_db
from bson import ObjectId
from enum import Enum

MONTH_NAMES = [
    '', 'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
    'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
]

DATE_FORMATS = {
    'daily': '{year}/{month:02d}/{day:02d}',
    'weekly': '{year} هفته {week}',
    'monthly': '{year} {month_name}'
}


class ReportType(Enum):
    COUNT = 'count'
    AMOUNT = 'amount'


class TimeMode(Enum):
    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'


class SummaryReportAPI(APIView):
    """
    API for getting pre-aggregated transaction summaries
    """

    def get(self, request):
        """
        Get transaction summaries filtered by merchant and time mode
        """
        report_type = request.query_params.get('type', ReportType.COUNT.value)
        mode = request.query_params.get('mode', TimeMode.DAILY.value)
        merchant_id = request.query_params.get('merchantId')

        query = {'type': mode}
        if merchant_id:
            query['merchantId'] = ObjectId(merchant_id)

        projection = self._build_projection(mode, report_type)
        pipeline = [
            {'$match': query},
            {'$project': projection},
            {'$sort': {'key': 1}}
        ]

        results = list(mongo_db.transaction_summary.aggregate(pipeline))
        return Response(results)

    def _build_projection(self, mode, report_type):
        """Build MongoDB projection based on mode and report type"""
        value_field = '$amount' if report_type == ReportType.AMOUNT.value else '$count'

        if mode == TimeMode.DAILY.value:
            key_expr = {
                '$concat': [
                    {'$toString': '$date.year'}, '/',
                    {'$toString': '$date.month'}, '/',
                    {'$toString': '$date.day'}
                ]
            }
        elif mode == TimeMode.WEEKLY.value:
            key_expr = {
                '$concat': [
                    {'$toString': '$date.year'}, ' هفته ',
                    {'$toString': '$date.week'}
                ]
            }
        else:  # monthly
            key_expr = {
                '$concat': [
                    {'$toString': '$date.year'}, ' ',
                    {'$arrayElemAt': [MONTH_NAMES, '$date.month']}
                ]
            }

        return {
            '_id': 0,
            'key': key_expr,
            'value': value_field
        }


class TransactionReportAPI(APIView):
    """
    API for generating real-time transaction reports
    """

    def get(self, request):
        """
        Generate transaction reports
        """
        report_type = request.query_params.get('type', ReportType.COUNT.value)
        mode = request.query_params.get('mode', TimeMode.DAILY.value)
        merchant_id = request.query_params.get('merchantId')

        match_stage = {}
        if merchant_id:
            match_stage['merchantId'] = ObjectId(merchant_id)

        group_stage = self._build_group_stage(mode, report_type)
        pipeline = [
            {'$match': match_stage},
            {'$group': group_stage},
            {'$sort': {'_id': 1}}
        ]

        results = list(mongo_db.transaction.aggregate(pipeline))
        formatted_results = [self._format_result(res, mode) for res in results]

        return Response(formatted_results)

    def _build_group_stage(self, mode, report_type):
        """Build MongoDB group stage based on mode and report type"""
        value_expr = {'$sum': '$amount' if report_type == ReportType.AMOUNT.value else 1}

        if mode == TimeMode.DAILY.value:
            id_expr = {
                'year': {'$year': '$createdAt'},
                'month': {'$month': '$createdAt'},
                'day': {'$dayOfMonth': '$createdAt'}
            }
        elif mode == TimeMode.WEEKLY.value:
            id_expr = {
                'year': {'$year': '$createdAt'},
                'week': {'$week': '$createdAt'}
            }
        else:  # monthly
            id_expr = {
                'year': {'$year': '$createdAt'},
                'month': {'$month': '$createdAt'}
            }

        return {
            '_id': id_expr,
            'value': value_expr
        }

    def _format_result(self, result, mode):
        """Format a single result based on time mode"""
        if mode == TimeMode.DAILY.value:
            jalali_date = jdatetime.date.fromgregorian(
                year=result['_id']['year'],
                month=result['_id']['month'],
                day=result['_id']['day']
            )
            key = DATE_FORMATS['daily'].format(
                year=jalali_date.year,
                month=jalali_date.month,
                day=jalali_date.day
            )
        elif mode == TimeMode.WEEKLY.value:
            key = DATE_FORMATS['weekly'].format(
                year=result['_id']['year'],
                week=result['_id']['week']
            )
        else:  # monthly
            key = DATE_FORMATS['monthly'].format(
                year=result['_id']['year'],
                month_name=MONTH_NAMES[result['_id']['month']]
            )

        return {
            'key': key,
            'value': result['value']
        }