# accounts/forms.py
from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User


COMMON_INPUT_CLASSES = (
    "mt-2 w-full px-4 py-2.5 rounded-lg border border-gray-300 "
    "bg-white text-sm text-black focus:outline-none focus:ring-2 focus:ring-blue-500"
)


class SignUpForm(UserCreationForm):
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            "placeholder": "you@example.com",
        })
    )

    class Meta:
        model = User
        fields = ("username", "email", "password1", "password2")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Apply Tailwind classes to all fields
        for name, field in self.fields.items():
            existing = field.widget.attrs.get("class", "")
            field.widget.attrs["class"] = (existing + " " + COMMON_INPUT_CLASSES).strip()

        # Custom placeholders
        self.fields["username"].widget.attrs.setdefault("placeholder", "Choose a username")
        self.fields["password1"].widget.attrs.setdefault("placeholder", "At least 8 characters")
        self.fields["password2"].widget.attrs.setdefault("placeholder", "Re-enter your password")

    # Ensure email is lowercased
    def clean_email(self):
        email = self.cleaned_data.get("email", "").lower()
        if User.objects.filter(email__iexact=email).exists():
            raise forms.ValidationError("An account with this email already exists.")
        return email

    # Optional: Lowercase username also
    def clean_username(self):
        username = self.cleaned_data.get("username", "").lower()
        if User.objects.filter(username__iexact=username).exists():
            raise forms.ValidationError("This username is already taken.")
        return username
