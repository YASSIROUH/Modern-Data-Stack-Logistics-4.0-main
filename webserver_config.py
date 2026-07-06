import os
from airflow.www.fab_security.manager import AUTH_DB

# Use basic database auth
AUTH_TYPE = AUTH_DB

# Allow users without a login to be identified as Admin
AUTH_ROLE_PUBLIC = 'Admin'

# Additional settings
WTF_CSRF_ENABLED = True
