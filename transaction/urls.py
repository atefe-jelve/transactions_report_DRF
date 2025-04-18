
# urls.py
from django.urls import path
from .views import TransactionReportAPI, SummaryReportAPI

urlpatterns = [
    path('transaction/report/', TransactionReportAPI.as_view(), name='transaction-report'),
    path('transaction/summary/', SummaryReportAPI.as_view(), name='summary-report'),
]


