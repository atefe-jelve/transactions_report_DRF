
import jdatetime
from rest_framework.views import APIView
from rest_framework.response import Response
# from django.conf import settings
from utils.mongodb import mongo_db
from bson import ObjectId

class SummaryReportAPI(APIView):
    def get(self, request):
        report_type = request.query_params.get('type', 'count')
        mode = request.query_params.get('mode', 'daily')

        query = {'type': mode}

        if mode == 'daily':
            projection = {
                '_id': {'$toString': '$_id'},
                'key': {
                    '$concat': [
                        {'$toString': '$date.year'},
                        '/',
                        {'$toString': '$date.month'},
                        '/',
                        {'$toString': '$date.day'}
                    ]
                },
                'value': '$count' if report_type == 'count' else '$amount'
            }
        elif mode == 'weekly':
            projection = {
                '_id': {'$toString': '$_id'},
                'key': {
                    '$concat': [
                        {'$toString': '$date.year'},
                        ' هفته ',
                        {'$toString': '$date.week'}
                    ]
                },
                'value': '$count' if report_type == 'count' else '$amount'
            }
        else:  # monthly
            month_names = [
                '', 'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
                'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
            ]
            projection = {
                '_id': {'$toString': '$_id'},
                'key': {
                    '$concat': [
                        {'$toString': '$date.year'},
                        ' ',
                        {'$arrayElemAt': [month_names, '$date.month']}
                    ]
                },
                'value': '$count' if report_type == 'count' else '$amount'
            }

        pipeline = [
            {'$match': query},
            {'$project': projection},
            {'$sort': {'key': 1}}
        ]

        results = list(mongo_db.transaction_summary.aggregate(pipeline))
        return Response(results)


class TransactionReportAPI(APIView):
    def get(self, request):
        report_type = request.query_params.get('type', 'count')  # count or amount
        mode = request.query_params.get('mode', 'daily')  # daily, weekly, monthly
        merchant_id = request.query_params.get('merchantId')

        match_stage = {}
        if merchant_id:
            match_stage['merchantId'] = ObjectId(merchant_id)

        if mode == 'daily':
            group_stage = {
                '_id': {
                    'year': {'$year': '$createdAt'},
                    'month': {'$month': '$createdAt'},
                    'day': {'$dayOfMonth': '$createdAt'}
                },
                'value': {'$sum': '$amount' if report_type == 'amount' else 1}
            }
        elif mode == 'weekly':
            group_stage = {
                '_id': {
                    'year': {'$year': '$createdAt'},
                    'week': {'$week': '$createdAt'}
                },
                'value': {'$sum': '$amount' if report_type == 'amount' else 1}
            }
        else:  # monthly
            group_stage = {
                '_id': {
                    'year': {'$year': '$createdAt'},
                    'month': {'$month': '$createdAt'}
                },
                'value': {'$sum': '$amount' if report_type == 'amount' else 1}
            }

        pipeline = [
            {'$match': match_stage},
            {'$group': group_stage},
            {'$sort': {'_id': 1}}
        ]

        results = list(mongo_db.transaction.aggregate(pipeline))

        formatted_results = []
        for res in results:
            if mode == 'daily':
                # Convert Gregorian to Jalali
                jalali_date = jdatetime.date.fromgregorian(
                    year=res['_id']['year'],
                    month=res['_id']['month'],
                    day=res['_id']['day']
                )
                key = f"{jalali_date.year}/{jalali_date.month:02d}/{jalali_date.day:02d}"
            elif mode == 'weekly':
                key = f"{res['_id']['year']} هفته {res['_id']['week']}"
            else:
                month_names = [
                    '', 'فروردین', 'اردیبهشت', 'خرداد', 'تیر', 'مرداد', 'شهریور',
                    'مهر', 'آبان', 'آذر', 'دی', 'بهمن', 'اسفند'
                ]
                key = f"{res['_id']['year']} {month_names[res['_id']['month']]}"

            formatted_results.append({
                'key': key,
                'value': res['value']
            })

        return Response(formatted_results)





