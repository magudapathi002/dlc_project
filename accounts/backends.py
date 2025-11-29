# accounts/backends.py
from django.contrib.auth.backends import ModelBackend
from django.contrib.auth import get_user_model

class EmailOrUsernameModelBackend(ModelBackend):
    """
    Authenticate with username OR email.
    """
    def authenticate(self, request, username=None, password=None, **kwargs):
        UserModel = get_user_model()
        if username is None:
            username = kwargs.get('email')
        # try username first
        try:
            user = UserModel.objects.get(username__iexact=username)
        except UserModel.DoesNotExist:
            # fallback to email lookup
            try:
                user = UserModel.objects.get(email__iexact=username)
            except UserModel.DoesNotExist:
                return None
        if user.check_password(password) and self.user_can_authenticate(user):
            return user
        return None
