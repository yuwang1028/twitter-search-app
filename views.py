from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.csrf import csrf_exempt

from django.shortcuts import render
from searchApp.user_search import fetch_user_results
from searchApp.search import fetch_results

import logging

logger = logging.getLogger(__name__)

def index(request):
    return render(request, 'searchApp/search.html')


"""
@csrf_protect
@require_http_methods(["GET", "POST"])
def user_search_view(request):
    user_id = request.GET.get('user_id') or request.POST.get('user_id')
    if not user_id:
        logger.error("User ID not provided")
        return JsonResponse({'error': 'User ID is required'}, status=400)

    try:
        tweets_df, quoted_tweets_df, retweets_df, replies_df = fetch_user_results(user_id)

        results = {
            'original_tweets': list(tweets_df['desired_column'].values) if 'desired_column' in tweets_df.columns else [],
            'quoted_tweets': list(quoted_tweets_df['desired_column'].values) if 'desired_column' in quoted_tweets_df.columns else [],
            'retweets': list(retweets_df['desired_column'].values) if 'desired_column' in retweets_df.columns else [],
            'replies': list(replies_df['desired_column'].values) if 'desired_column' in replies_df.columns else [],
        }

        return JsonResponse(results)

    except Exception as e:
        logger.error(f"An error occurred while processing user_id {user_id}: {str(e)}", exc_info=True)
        return JsonResponse({'error': 'Internal server error'}, status=500)


@require_http_methods(["GET", "POST"])
@csrf_exempt  # Consider using csrf_protect instead for security
def search_view(request):
    # Handle GET request by just rendering the form
    if request.method == 'GET':
        return render(request, 'searchapp/search_form.html')

    # Handle POST request when form is submitted
    if request.method == 'POST':
        # Extract form data
        username = request.POST.get('username')
        userscreenname = request.POST.get('userscreenname')
        userverification = request.POST.get('userverification')
        tweetstring = request.POST.get('tweetstring')
        hashtags = request.POST.get('hashtags')
        tweetsensitivity = request.POST.get('tweetsensitivity')
        tweetcontenttype = request.POST.get('tweetcontenttype')
        start_datetime = request.POST.get('datetimerange').split(' - ')[0]
        end_datetime = request.POST.get('datetimerange').split(' - ')[1]

        # Call the search function from search.py
        results = fetch_results(
            username, userscreenname, userverification,
            tweetstring, hashtags, tweetsensitivity,
            tweetcontenttype, start_datetime, end_datetime
        )

        # You might need to convert results to a suitable format to return as a response
        # For example, if results is a DataFrame:
        results_json = results.to_json(orient='records')
        parsed_results = json.loads(results_json)

        return JsonResponse({'results': parsed_results})

    # In case of an unexpected method
    return JsonResponse({'error': 'Invalid request method'}, status=405)
"""


from django.shortcuts import render
from django.http import JsonResponse
from .tweet_search import fetch_tweet_results  # assuming your code is in utils.py

def search_tweets(request):
    tweet_id = request.GET.get('tweet_id', '')
    key = request.GET.get('key', 'all')
    if tweet_id:
        retweets, quotes, replies = fetch_tweet_results(tweet_id, key)
        context = {
            'retweets': retweets.to_dict(orient='records'),
            'quotes': quotes.to_dict(orient='records'),
            'replies': replies.to_dict(orient='records')
        }
        return JsonResponse(context)
    else:
        return JsonResponse({'error': 'No tweet ID provided'}, status=400)
