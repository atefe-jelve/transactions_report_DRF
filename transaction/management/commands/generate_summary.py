
from utils.mongodb import summary_collection, transactions_collection
import jdatetime
from django.core.management.base import BaseCommand
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = 'Generates transaction summaries'

    SUMMARY_TYPES = {
        'daily': {'group': ['year', 'month', 'dayOfMonth'], 'format': 'daily'},
        'weekly': {'group': ['year', 'week'], 'format': 'weekly'},
        'monthly': {'group': ['year', 'month'], 'format': 'monthly'}
    }

    def handle(self, *args, **options):
        merchants = transactions_collection.distinct("merchantId")

        for merchant_id in merchants:
            self.stdout.write(f"Processing merchant: {merchant_id}")

            for summary_type, config in self.SUMMARY_TYPES.items():
                self._process_summary_type(merchant_id, summary_type, config['group'])

        self.stdout.write(self.style.SUCCESS('Successfully generated all summaries'))

    def _process_summary_type(self, merchant_id, summary_type, group_fields):
        pipeline = self._build_pipeline(merchant_id, group_fields)
        results = transactions_collection.aggregate(pipeline)

        for result in results:
            self._save_summary_result(merchant_id, result, summary_type)

    def _build_pipeline(self, merchant_id, group_fields):
        group_expr = {}
        for field in group_fields:
            if field == 'dayOfMonth':
                group_expr['day'] = {'$dayOfMonth': '$createdAt'}
            else:
                group_expr[field] = {f'${field}': '$createdAt'}

        return [
            {
                '$match': {
                    'merchantId': merchant_id,
                    'createdAt': {'$exists': True}
                }
            },
            {
                '$group': {
                    '_id': group_expr,
                    'count': {'$sum': 1},
                    'amount': {'$sum': '$amount'}
                }
            }
        ]

    def _save_summary_result(self, merchant_id, result, summary_type):
        try:
            if summary_type == 'daily':
                jalali_date = jdatetime.date.fromgregorian(
                    year=result['_id']['year'],
                    month=result['_id']['month'],
                    day=result['_id']['day']
                )
                first_day_of_year = jdatetime.date(jalali_date.year, 1, 1)
                week_number = (jalali_date - first_day_of_year).days // 7 + 1

                date_fields = {
                    'year': jalali_date.year,
                    'month': jalali_date.month,
                    'day': jalali_date.day,
                    'week': week_number
                }
                gregorian_fields = {
                    'year': result['_id']['year'],
                    'month': result['_id']['month'],
                    'day': result['_id']['day']
                }

            elif summary_type == 'weekly':
                gregorian_date = datetime(result['_id']['year'], 1, 1) + timedelta(weeks=result['_id']['week'] - 1)
                jalali_date = jdatetime.date.fromgregorian(date=gregorian_date.date())

                date_fields = {
                    'year': jalali_date.year,
                    'week': result['_id']['week']
                }
                gregorian_fields = {
                    'year': result['_id']['year'],
                    'week': result['_id']['week']
                }

            else:  # monthly
                jalali_date = jdatetime.date.fromgregorian(
                    year=result['_id']['year'],
                    month=result['_id']['month'],
                    day=1
                )

                date_fields = {
                    'year': jalali_date.year,
                    'month': jalali_date.month
                }
                gregorian_fields = {
                    'year': result['_id']['year'],
                    'month': result['_id']['month']
                }

            summary_collection.insert_one({
                'merchantId': merchant_id,
                'date': date_fields,
                'gregorian_date': gregorian_fields,
                'type': summary_type,
                'count': result['count'],
                'amount': result['amount'],
                'created_at': datetime.now()
            })

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing {summary_type} record: {e}"))
