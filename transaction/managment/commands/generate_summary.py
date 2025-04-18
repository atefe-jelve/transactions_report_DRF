from utils.mongodb import summary_collection, transactions_collection
import jdatetime
from django.core.management.base import BaseCommand
from datetime import datetime

class Command(BaseCommand):
    help = 'Generates transaction summaries'

    def handle(self, *args, **options):
        # Daily summary
        daily_pipeline = [
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$createdAt'},
                        'month': {'$month': '$createdAt'},
                        'day': {'$dayOfMonth': '$createdAt'}
                    },
                    'count': {'$sum': 1},
                    'amount': {'$sum': '$amount'}
                }
            }
        ]

        daily_results = transactions_collection.aggregate(daily_pipeline)
        for res in daily_results:
            try:
                # Convert to jalali
                jalali_date = jdatetime.date.fromgregorian(
                    year=res['_id']['year'],
                    month=res['_id']['month'],
                    day=res['_id']['day']
                )

                # Calculate week number (alternative method)
                first_day_of_year = jdatetime.date(jalali_date.year, 1, 1)
                week_number = (jalali_date - first_day_of_year).days // 7 + 1

                # Insert into summary collection (daily)
                summary_collection.insert_one({
                    'date': {
                        'year': jalali_date.year,
                        'month': jalali_date.month,
                        'day': jalali_date.day,
                        'week': week_number
                    },
                    'gregorian_date': {
                        'year': res['_id']['year'],
                        'month': res['_id']['month'],
                        'day': res['_id']['day']
                    },
                    'type': 'daily',
                    'count': res['count'],
                    'amount': res['amount'],
                    'created_at': datetime.now()
                })

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing record: {e}"))
                continue

        self.stdout.write(self.style.SUCCESS('Successfully generated daily summaries'))

        # Weekly summary
        weekly_pipeline = [
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$createdAt'},
                        'week': {'$week': '$createdAt'}
                    },
                    'count': {'$sum': 1},
                    'amount': {'$sum': '$amount'}
                }
            }
        ]

        weekly_results = transactions_collection.aggregate(weekly_pipeline)
        for res in weekly_results:
            try:
                # Insert into summary collection (weekly)
                summary_collection.insert_one({
                    'date': {
                        'year': res['_id']['year'],
                        'week': res['_id']['week']
                    },
                    'type': 'weekly',
                    'count': res['count'],
                    'amount': res['amount'],
                    'created_at': datetime.now()
                })

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing record: {e}"))
                continue

        self.stdout.write(self.style.SUCCESS('Successfully generated weekly summaries'))

        # Monthly summary
        monthly_pipeline = [
            {
                '$group': {
                    '_id': {
                        'year': {'$year': '$createdAt'},
                        'month': {'$month': '$createdAt'}
                    },
                    'count': {'$sum': 1},
                    'amount': {'$sum': '$amount'}
                }
            }
        ]

        monthly_results = transactions_collection.aggregate(monthly_pipeline)
        for res in monthly_results:
            try:
                # Insert into summary collection (monthly)
                summary_collection.insert_one({
                    'date': {
                        'year': res['_id']['year'],
                        'month': res['_id']['month']
                    },
                    'type': 'monthly',
                    'count': res['count'],
                    'amount': res['amount'],
                    'created_at': datetime.now()
                })

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing record: {e}"))
                continue

        self.stdout.write(self.style.SUCCESS('Successfully generated monthly summaries'))
