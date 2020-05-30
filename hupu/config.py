# urls
real_bbs_url = "https://bbs.hupu.com"
plate_url = 'https://bbs.hupu.com/%s-postdate-%s'
user_profile_url = 'https://my.hupu.com/%s/profile'

# http
fetch_timeout = 15
common_headers = {
    'User-Agent': 'Aweme 3.1.0 rv:31006 (iPhone; iOS 12.0; zh_CN) Cronet'
}

# retrying
retry_max_number = 5
retry_min_random_wait = 1000  # ms
retry_max_random_wait = 5000  # ms
