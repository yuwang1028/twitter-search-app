# searchApp/urls.py

from django.urls import path
from .views import index, search_tweets

urlpatterns = [
    path('', index, name='index'),  # Main page
    path('search/', search_tweets, name='search_tweets'),

 
]





