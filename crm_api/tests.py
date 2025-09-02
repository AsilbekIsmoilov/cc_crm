import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
import django
django.setup()
from crm_api.models import move_suspends_with_status_call_to_fixeds
move_suspends_with_status_call_to_fixeds()